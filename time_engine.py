"""Time evolution engine for the simulation.

Evolves the simulation forward by N months:
1. Updates all fund NAVs using cyclic returns
2. Processes ALL pending requests in a single chronological queue
   (created_date ASC, id ASC), dispatching by type
"""

import calendar
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import models
import tax_engine


def _verify_cert_ownership(cert, req):
    """Verify that the certificate belongs to the request user."""
    if cert['user_id'] != req['user_id']:
        raise ValueError(
            f"Certificate #{cert['id']} belongs to user #{cert['user_id']}, "
            f"but request #{req['id']} is from user #{req['user_id']}"
        )


def evolve_time(db, steps=1):
    """Advance simulation by N months. Returns a log of what happened."""
    log = []

    for step in range(steps):
        current_month = models.get_sim_month(db)
        new_month = current_month + 1

        # Advance date by one month (clamp day to valid range)
        current_date = models.get_sim_date(db)
        dt = datetime.strptime(current_date, '%Y-%m-%d')
        if dt.month == 12:
            next_year, next_month = dt.year + 1, 1
        else:
            next_year, next_month = dt.year, dt.month + 1
        max_day = calendar.monthrange(next_year, next_month)[1]
        new_dt = dt.replace(year=next_year, month=next_month, day=min(dt.day, max_day))
        new_date = new_dt.strftime('%Y-%m-%d')

        step_log = {'month': new_month, 'date': new_date, 'events': []}

        # 1. Update all fund NAVs
        nav_changes = _update_fund_navs(db, new_month)
        for fund_name, old_nav, new_nav, ret in nav_changes:
            step_log['events'].append(
                f"Fund '{fund_name}': NAV {old_nav:.4f} -> {new_nav:.4f} ({ret*100:+.2f}%)"
            )

        # 2. Process ALL pending requests in single chronological queue
        _process_all_pending_requests(db, new_date, step_log)

        # Update sim state
        models.set_sim_month(db, new_month)
        models.set_sim_date(db, new_date)

        log.append(step_log)

    return log


def _update_fund_navs(db, month):
    """Update all fund NAVs using cyclic returns. Returns list of changes."""
    changes = []
    funds = models.list_funds(db)
    for fund in funds:
        returns = models.get_fund_returns(db, fund['id'])
        if not returns:
            continue
        n_returns = len(returns)
        idx = (month - 1) % n_returns  # 0-based cyclic index
        ret = returns[idx]['return_pct']
        old_nav = fund['current_nav']
        new_nav = old_nav * (1 + ret)
        db.execute("UPDATE funds SET current_nav = ? WHERE id = ?",
                   (new_nav, fund['id']))
        changes.append((fund['name'], old_nav, new_nav, ret))
    db.commit()
    return changes


def _process_all_pending_requests(db, current_date, step_log):
    """Process ALL pending requests in a single chronological queue (created_date ASC, id ASC).
    Dispatches each request to the appropriate executor based on its type."""
    _REQUEST_EXECUTORS = {
        'fund_swap': _execute_fund_swap,
        'withdrawal': _execute_withdrawal,
        'contribution': _execute_contribution,
        'portability_out': _execute_portability,
        'brokerage_withdrawal': _execute_brokerage_withdrawal,
        'transfer_internal': _execute_transfer_internal,
        'transfer_external_out': _execute_transfer_external_out,
        'transfer_external_in': _execute_transfer_external_in,
    }
    pending = db.execute(
        "SELECT * FROM requests WHERE status='pending' ORDER BY created_date ASC, id ASC"
    ).fetchall()
    for req in pending:
        req_type = req['type']
        executor = _REQUEST_EXECUTORS.get(req_type)
        if executor is None:
            # Unknown or non-executable type (e.g. portability_in handled by portability_out)
            continue
        try:
            details = json.loads(req['details']) if req['details'] else {}
            # Atomic execution: savepoint so failures roll back partial mutations
            db.execute("SAVEPOINT req_exec")
            executor(db, req, details, current_date, step_log)
            db.execute("RELEASE req_exec")
        except Exception as e:
            db.execute("ROLLBACK TO req_exec")
            db.execute("RELEASE req_exec")
            models.fail_request(db, req['id'])
            step_log['events'].append(
                f"Request #{req['id']} ({req_type}) FAILED: {e}"
            )


def _execute_fund_swap(db, req, details, current_date, step_log):
    """
    Execute a fund swap: sell current holdings, buy new allocation.
    Fund swaps do NOT touch lots (tax-neutral).
    details: {new_allocations: [{fund_id, pct}, ...]}
    """
    cert_id = req['certificate_id']
    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'], commit=False)
        return
    _verify_cert_ownership(cert, req)

    # Validate and normalize new allocations BEFORE selling
    new_allocs = details.get('new_allocations', [])
    if not new_allocs:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(f"Fund swap FAILED for certificate #{cert_id}: no allocations provided")
        return
    total_pct = sum(a['pct'] for a in new_allocs if a.get('pct', 0) > 0)
    if total_pct <= 0:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(f"Fund swap FAILED for certificate #{cert_id}: allocation total is 0%")
        return

    # Update target allocations first (validates sum=100% or raises)
    alloc_tuples = [(a['fund_id'], a['pct']) for a in new_allocs]
    models.set_target_allocations(db, cert_id, alloc_tuples, commit=False)

    # Sell all current holdings
    holdings = models.get_holdings(db, cert_id)
    total_cash = 0.0
    for h in holdings:
        total_cash += h['units'] * h['current_nav']
        models.set_holding(db, cert_id, h['fund_id'], 0, commit=False)

    # Buy new allocation with fractional units (normalized)
    for alloc in new_allocs:
        fund = models.get_fund(db, alloc['fund_id'])
        if not fund or fund['current_nav'] <= 0:
            continue
        target_amount = total_cash * (alloc['pct'] / total_pct)
        units = target_amount / fund['current_nav']
        if units > 1e-9:
            models.set_holding(db, cert_id, alloc['fund_id'], units, commit=False)

    models.complete_request(db, req['id'], current_date, commit=False)
    step_log['events'].append(
        f"Fund swap completed for certificate #{cert_id} "
        f"(R${total_cash:,.2f} reallocated, 100% invested)"
    )


def _execute_withdrawal(db, req, details, current_date, step_log):
    """
    Execute a withdrawal using certificate units for correct per-lot valuation.
    FIFO by units, days-based regressive brackets.
    Does NOT switch phase to spending.
    """
    cert_id = req['certificate_id']
    user_id = req['user_id']
    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'], commit=False)
        return
    _verify_cert_ownership(cert, req)

    amount = details.get('amount', 0)
    if amount <= 0:
        models.fail_request(db, req['id'], commit=False)
        return

    # Set tax regime from request details (deferred from portal submission)
    regime_from_request = details.get('tax_regime')
    if cert['tax_regime'] is None:
        if regime_from_request in ('progressive', 'regressive'):
            models.set_tax_regime(db, cert_id, regime_from_request, commit=False)
            cert = models.get_certificate(db, cert_id)
        else:
            models.fail_request(db, req['id'], commit=False)
            step_log['events'].append(
                f"Withdrawal FAILED for certificate #{cert_id}: "
                f"tax regime must be set before first withdrawal"
            )
            return

    total_value = models.get_certificate_total_value(db, cert_id)
    amount = min(amount, total_value)

    # Compute unit price BEFORE selling
    unit_price = models.get_certificate_unit_price(db, cert_id)
    units_to_redeem = amount / unit_price if unit_price > 0 else 0

    # Sell fund holdings proportionally to raise the gross amount
    if not _sell_holdings(db, cert_id, amount):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Withdrawal FAILED for certificate #{cert_id}: insufficient funds to raise R${amount:,.2f}"
        )
        return

    # Consume lots FIFO by units
    consumed_lots = models.consume_lots_fifo(db, cert_id, units_to_redeem, commit=False)

    # Get VGBL P_rem BEFORE updating it (for earnings ratio)
    P_rem = models.get_vgbl_premium_remaining(db, cert_id)

    # Compute per-lot tax using days-based brackets
    current_dt = datetime.strptime(current_date, '%Y-%m-%d')
    total_tax = 0.0
    breakdown = []

    # Earnings ratio for VGBL: certificate-level
    if cert['plan_type'] == 'VGBL' and total_value > 0:
        earnings_ratio = max(0, 1 - (P_rem / total_value))
        taxable_total = amount * earnings_ratio
    else:
        earnings_ratio = 0
        taxable_total = 0

    for lot in consumed_lots:
        contrib_dt = datetime.strptime(lot['contribution_date'], '%Y-%m-%d')
        days_held = max(0, (current_dt - contrib_dt).days)
        lot_gross_value = lot['units_consumed'] * unit_price

        if cert['tax_regime'] == 'regressive':
            rate = tax_engine.regressive_rate(lot['contribution_date'], current_date)
            if cert['plan_type'] == 'PGBL':
                taxable = lot_gross_value
            else:
                # VGBL: proportional share of taxable_total
                taxable = (lot_gross_value / amount) * taxable_total if amount > 0 else 0
            tax_on_lot = taxable * rate
        else:
            # Progressive: 15% withholding on taxable base
            rate = 0.15
            if cert['plan_type'] == 'PGBL':
                taxable = lot_gross_value
            else:
                taxable = (lot_gross_value / amount) * taxable_total if amount > 0 else 0
            tax_on_lot = taxable * rate

        total_tax += tax_on_lot
        breakdown.append({
            'contribution_id': lot['contribution_id'],
            'units_consumed': round(lot['units_consumed'], 6),
            'lot_gross_value': round(lot_gross_value, 2),
            'days_held': days_held,
            'rate': rate,
            'taxable': round(taxable, 2),
            'tax': round(tax_on_lot, 2),
        })

    total_tax = round(total_tax, 2)
    net_amount = amount - total_tax

    tax_result = {
        'gross': round(amount, 2),
        'tax': total_tax,
        'net': round(net_amount, 2),
        'effective_rate': round(total_tax / amount, 4) if amount > 0 else 0,
        'breakdown': breakdown,
        'regime': cert['tax_regime'],
        'unit_price': round(unit_price, 6),
    }

    # Record withdrawal
    withdrawal_id = models.add_withdrawal(db, cert_id, amount, total_tax, net_amount,
                                          current_date, tax_result, commit=False)

    # Record lot allocations for audit (days-based)
    def _rate_fn(lot, dh):
        if cert['tax_regime'] == 'regressive':
            return tax_engine.regressive_rate(lot['contribution_date'], current_date)
        return 0.15

    def _taxable_fn(lot, dh):
        lgv = lot.get('units_consumed', 0) * unit_price
        if cert['plan_type'] == 'PGBL':
            return lgv
        return (lgv / amount) * taxable_total if amount > 0 else 0

    def _tax_fn(lot, dh):
        return _taxable_fn(lot, dh) * _rate_fn(lot, dh)

    models.record_lot_allocations(db, 'withdrawal', withdrawal_id, consumed_lots,
                                  current_date, _rate_fn, _taxable_fn, _tax_fn, commit=False)

    # Update VGBL premium_remaining (clamp fraction to [0,1] for loss scenarios)
    if cert['plan_type'] == 'VGBL' and total_value > 0:
        prem_frac = min(1.0, max(0.0, P_rem / total_value))
        premium_returned = amount * prem_frac
        models.update_vgbl_premium_remaining(db, cert_id, -premium_returned, commit=False)

    # Update certificate unit supply
    models.update_certificate_units(db, cert_id, -units_to_redeem, commit=False)

    # Net goes to brokerage
    models.add_brokerage_cash(db, user_id, net_amount, commit=False)

    # Do NOT switch to spending phase (withdrawals allowed in accumulation)

    models.complete_request(db, req['id'], current_date, commit=False)
    step_log['events'].append(
        f"Withdrawal from certificate #{cert_id}: "
        f"gross R${amount:,.2f}, tax R${total_tax:,.2f}, "
        f"net R${net_amount:,.2f} -> brokerage"
    )


def _execute_contribution(db, req, details, current_date, step_log):
    """
    Execute a contribution: issue certificate units, buy fund units per target allocation.
    IOF enforcement for VGBL. Updates unit_supply and vgbl_premium_remaining.
    """
    cert_id = req['certificate_id']
    user_id = req['user_id']
    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'], commit=False)
        return
    _verify_cert_ownership(cert, req)

    amount = details.get('amount', 0)
    if amount <= 0:
        models.fail_request(db, req['id'], commit=False)
        return

    # Deduct from brokerage
    brokerage_cash = models.get_brokerage_cash(db, user_id)
    if brokerage_cash < amount:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Contribution to certificate #{cert_id} FAILED: "
            f"insufficient brokerage cash (R${brokerage_cash:,.2f} < R${amount:,.2f})"
        )
        return

    models.set_brokerage_cash(db, user_id, brokerage_cash - amount, commit=False)

    # IOF enforcement for VGBL
    iof = 0.0
    net_invest = amount
    if cert['plan_type'] == 'VGBL':
        year = int(current_date[:4])
        iof = tax_engine.calculate_iof_vgbl(db, user_id, amount, year)
        if iof > 0:
            net_invest = amount - iof

    # Guard: net investment must be positive after IOF
    if net_invest <= 0:
        # Refund brokerage
        models.set_brokerage_cash(db, user_id, brokerage_cash, commit=False)
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Contribution to certificate #{cert_id} FAILED: "
            f"IOF (R${iof:,.2f}) exceeds contribution amount (R${amount:,.2f})"
        )
        return

    # Compute certificate unit price and issue units
    unit_price = models.get_certificate_unit_price(db, cert_id)
    units_issued = net_invest / unit_price

    # Record contribution with unit info
    models.add_contribution(db, cert_id, net_invest, current_date,
                            remaining_amount=net_invest,
                            units_total=units_issued, units_remaining=units_issued,
                            issue_unit_price=unit_price,
                            gross_amount=amount, iof_amount=iof,
                            commit=False)

    # Update certificate unit supply
    models.update_certificate_units(db, cert_id, units_issued, commit=False)

    # Update VGBL premium_remaining
    if cert['plan_type'] == 'VGBL':
        models.update_vgbl_premium_remaining(db, cert_id, net_invest, commit=False)

    # Buy fractional fund units per target allocation
    _buy_into_certificate(db, cert_id, net_invest)

    models.complete_request(db, req['id'], current_date, commit=False)
    if iof > 0:
        step_log['events'].append(
            f"Contribution to certificate #{cert_id}: "
            f"gross R${amount:,.2f} from brokerage, IOF R${iof:,.2f}, "
            f"net invested R${net_invest:,.2f}, "
            f"{units_issued:.4f} units issued at R${unit_price:.4f}/unit"
        )
    else:
        step_log['events'].append(
            f"Contribution to certificate #{cert_id}: R${amount:,.2f} invested, "
            f"{units_issued:.4f} cert units issued at R${unit_price:.4f}/unit"
        )


def _execute_portability(db, req, details, current_date, step_log):
    """
    Execute portability: transfer value from source to destination certificate.
    Consumes lots FIFO at source, recreates them at destination with original dates.
    """
    source_cert_id = req['certificate_id']
    dest_cert_id = details.get('destination_cert_id')
    if not dest_cert_id:
        models.fail_request(db, req['id'], commit=False)
        return

    source_cert = models.get_certificate(db, source_cert_id)
    dest_cert = models.get_certificate(db, dest_cert_id)
    if not source_cert or not dest_cert:
        models.fail_request(db, req['id'], commit=False)
        return
    _verify_cert_ownership(source_cert, req)
    _verify_cert_ownership(dest_cert, req)

    # Validate destination has target allocations
    if not models.get_target_allocations(db, dest_cert_id):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Portability #{req['id']} FAILED: destination certificate #{dest_cert_id} "
            f"has no target allocations set"
        )
        return

    # Validate same type
    if source_cert['plan_type'] != dest_cert['plan_type']:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Portability #{req['id']} FAILED: type mismatch "
            f"({source_cert['plan_type']} -> {dest_cert['plan_type']})"
        )
        return

    # Tax regime compatibility
    if (source_cert['tax_regime'] and dest_cert['tax_regime'] and
            source_cert['tax_regime'] != dest_cert['tax_regime']):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Portability #{req['id']} FAILED: tax regime mismatch "
            f"({source_cert['tax_regime']} -> {dest_cert['tax_regime']})"
        )
        return

    # Inherit tax regime if needed
    if source_cert['tax_regime'] and not dest_cert['tax_regime']:
        models.set_tax_regime(db, dest_cert_id, source_cert['tax_regime'], commit=False)

    # Calculate transfer amount
    source_value = models.get_certificate_total_value(db, source_cert_id)
    amount = details.get('amount', source_value)
    amount = min(amount, source_value)

    # Compute source unit price and units to transfer
    src_unit_price = models.get_certificate_unit_price(db, source_cert_id)
    units_to_transfer = amount / src_unit_price if src_unit_price > 0 else 0

    # Sell source holdings proportionally
    if not _sell_holdings(db, source_cert_id, amount):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Portability FAILED: insufficient funds in certificate #{source_cert_id}"
        )
        return

    # Consume lots FIFO at source by units
    consumed_lots = models.consume_lots_fifo(db, source_cert_id, units_to_transfer, commit=False)

    # Record lot allocations for audit
    models.record_lot_allocations(db, 'portability_out', req['id'], consumed_lots, current_date, commit=False)

    # Update source unit supply
    models.update_certificate_units(db, source_cert_id, -units_to_transfer, commit=False)

    # Compute destination unit price and issue units
    dest_unit_price = models.get_certificate_unit_price(db, dest_cert_id)
    total_dest_units = amount / dest_unit_price if dest_unit_price > 0 else amount

    # Compute VGBL premium_remaining to move BEFORE creating destination lots
    prem_to_move = 0.0
    if source_cert['plan_type'] == 'VGBL' and source_value > 1e-9:
        src_P_rem = models.get_vgbl_premium_remaining(db, source_cert_id)
        prem_frac = min(1.0, max(0.0, src_P_rem / source_value))
        prem_to_move = amount * prem_frac
        models.update_vgbl_premium_remaining(db, source_cert_id, -prem_to_move, commit=False)
        models.update_vgbl_premium_remaining(db, dest_cert_id, prem_to_move, commit=False)

    # Recreate lots at destination with original dates
    # Use gross transferred value for lot amount; premium basis tracked separately
    total_consumed_units = sum(lot['units_consumed'] for lot in consumed_lots)
    for lot in consumed_lots:
        if total_consumed_units > 1e-9:
            lot_fraction = lot['units_consumed'] / total_consumed_units
        else:
            lot_fraction = 1.0 / max(1, len(consumed_lots))
        dest_lot_units = total_dest_units * lot_fraction
        dest_lot_gross = amount * lot_fraction  # gross value, not basis
        if source_cert['plan_type'] == 'VGBL' and prem_to_move > 0:
            dest_lot_remaining = prem_to_move * lot_fraction
        else:
            dest_lot_remaining = lot['consumed_amount']

        models.add_contribution(db, dest_cert_id, dest_lot_gross,
                                lot['contribution_date'],
                                source_type='portability',
                                remaining_amount=dest_lot_remaining,
                                units_total=dest_lot_units,
                                units_remaining=dest_lot_units,
                                issue_unit_price=dest_unit_price, commit=False)

    # Update destination unit supply
    models.update_certificate_units(db, dest_cert_id, total_dest_units, commit=False)

    # Buy into destination per its target allocation
    _buy_into_certificate(db, dest_cert_id, amount)

    # Complete the portability_in request if one exists
    in_reqs = db.execute(
        "SELECT id, details FROM requests WHERE type = 'portability_in' AND status = 'pending' "
        "AND certificate_id = ?",
        (dest_cert_id,)
    ).fetchall()
    for ir in in_reqs:
        try:
            ir_details = json.loads(ir['details'])
        except (json.JSONDecodeError, TypeError):
            continue
        if ir_details.get('source_cert_id') == source_cert_id:
            models.complete_request(db, ir['id'], current_date, commit=False)

    models.complete_request(db, req['id'], current_date, commit=False)
    step_log['events'].append(
        f"Portability: R${amount:,.2f} from certificate #{source_cert_id} "
        f"to #{dest_cert_id} (lots moved FIFO, dates preserved)"
    )


def _execute_brokerage_withdrawal(db, req, details, current_date, step_log):
    """Execute a brokerage withdrawal: cash just disappears from simulation."""
    user_id = req['user_id']
    amount = details.get('amount', 0)
    if amount <= 0:
        models.fail_request(db, req['id'], commit=False)
        return

    current_cash = models.get_brokerage_cash(db, user_id)
    if current_cash < amount:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Brokerage withdrawal FAILED: insufficient cash "
            f"(R${current_cash:,.2f} < R${amount:,.2f})"
        )
        return

    models.set_brokerage_cash(db, user_id, current_cash - amount, commit=False)
    models.complete_request(db, req['id'], current_date, commit=False)
    step_log['events'].append(
        f"Brokerage withdrawal: R${amount:,.2f} removed from user #{user_id}'s account"
    )


def _sell_holdings(db, cert_id, amount):
    """Sell holdings proportionally to raise a given amount. Fractional units.
    Returns True on success, False if insufficient funds."""
    holdings = models.get_holdings(db, cert_id)
    total_holdings_value = sum(h['units'] * h['current_nav'] for h in holdings)

    if amount > total_holdings_value * 1.001:  # 0.1% tolerance
        return False

    if total_holdings_value <= 1e-9:
        return False

    # Sell proportionally
    sell_fraction = min(1.0, amount / total_holdings_value)
    for h in holdings:
        if h['units'] <= 1e-9:
            continue
        units_to_sell = h['units'] * sell_fraction
        new_units = h['units'] - units_to_sell
        models.set_holding(db, cert_id, h['fund_id'], new_units, commit=False)

    return True


def _execute_transfer_internal(db, req, details, current_date, step_log):
    """Internal transfer using certificate units. Moves units + P_rem proportionally."""
    source_cert_id = req['certificate_id']
    dest_cert_id = details.get('destination_cert_id')
    amount = details.get('amount', 0)

    source = models.get_certificate(db, source_cert_id)
    dest = models.get_certificate(db, dest_cert_id)
    if not source or not dest:
        models.fail_request(db, req['id'], commit=False)
        return
    _verify_cert_ownership(source, req)
    _verify_cert_ownership(dest, req)

    # Validate destination has target allocations
    if not models.get_target_allocations(db, dest_cert_id):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Internal transfer FAILED: destination certificate #{dest_cert_id} "
            f"has no target allocations set"
        )
        return

    if source['plan_type'] != dest['plan_type']:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Internal transfer FAILED: type mismatch "
            f"({source['plan_type']} -> {dest['plan_type']})"
        )
        return

    # Tax regime compatibility
    if (source['tax_regime'] and dest['tax_regime'] and
            source['tax_regime'] != dest['tax_regime']):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Internal transfer FAILED: tax regime mismatch "
            f"({source['tax_regime']} -> {dest['tax_regime']})"
        )
        return

    # Inherit tax regime if needed
    if source['tax_regime'] and not dest['tax_regime']:
        models.set_tax_regime(db, dest_cert_id, source['tax_regime'], commit=False)

    source_value = models.get_certificate_total_value(db, source_cert_id)
    amount = min(amount, source_value)
    if amount <= 0:
        models.fail_request(db, req['id'], commit=False)
        return

    # Compute source unit price and units to transfer
    src_unit_price = models.get_certificate_unit_price(db, source_cert_id)
    units_to_transfer = amount / src_unit_price if src_unit_price > 0 else 0

    # Sell source holdings
    if not _sell_holdings(db, source_cert_id, amount):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"Internal transfer FAILED: insufficient funds in certificate #{source_cert_id}"
        )
        return

    # Consume lots FIFO at source (by units)
    consumed_lots = models.consume_lots_fifo(db, source_cert_id, units_to_transfer, commit=False)

    # Record lot allocations for audit
    models.record_lot_allocations(db, 'transfer_internal', req['id'], consumed_lots, current_date, commit=False)

    # Update source unit supply
    models.update_certificate_units(db, source_cert_id, -units_to_transfer, commit=False)

    # Compute destination unit price and units to issue
    dest_unit_price = models.get_certificate_unit_price(db, dest_cert_id)
    total_dest_units = amount / dest_unit_price if dest_unit_price > 0 else amount

    # Compute VGBL premium_remaining to move BEFORE creating destination lots
    prem_to_move = 0.0
    if source['plan_type'] == 'VGBL':
        src_P_rem = models.get_vgbl_premium_remaining(db, source_cert_id)
        if source_value > 0:
            prem_frac = min(1.0, max(0.0, src_P_rem / source_value))
            prem_to_move = amount * prem_frac
        models.update_vgbl_premium_remaining(db, source_cert_id, -prem_to_move, commit=False)
        models.update_vgbl_premium_remaining(db, dest_cert_id, prem_to_move, commit=False)

    # Recreate consumed lots at destination with original dates, translated to dest units
    # Use gross transferred value (not consumed_amount/basis) for lot amount
    total_consumed_units = sum(lot['units_consumed'] for lot in consumed_lots)
    for lot in consumed_lots:
        if total_consumed_units > 1e-9:
            lot_fraction = lot['units_consumed'] / total_consumed_units
        else:
            lot_fraction = 1.0 / max(1, len(consumed_lots))
        dest_lot_units = total_dest_units * lot_fraction
        dest_lot_gross = amount * lot_fraction  # gross value, not basis
        # For VGBL, remaining_amount tracks premium basis proportionally
        if source['plan_type'] == 'VGBL' and prem_to_move > 0:
            dest_lot_remaining = prem_to_move * lot_fraction
        else:
            dest_lot_remaining = lot['consumed_amount']

        models.add_contribution(db, dest_cert_id, dest_lot_gross,
                                lot['contribution_date'],
                                source_type='transfer_internal',
                                remaining_amount=dest_lot_remaining,
                                units_total=dest_lot_units,
                                units_remaining=dest_lot_units,
                                issue_unit_price=dest_unit_price, commit=False)

    # Update destination unit supply
    models.update_certificate_units(db, dest_cert_id, total_dest_units, commit=False)

    # Buy into destination per target allocations
    _buy_into_certificate(db, dest_cert_id, amount)

    models.complete_request(db, req['id'], current_date, commit=False)
    step_log['events'].append(
        f"Internal transfer: R${amount:,.2f} from #{source_cert_id} to #{dest_cert_id} "
        f"(lots moved FIFO, dates preserved, units transferred)"
    )


def _execute_transfer_external_out(db, req, details, current_date, step_log):
    """External port-out: sell holdings, consume FIFO units, reduce P_rem, money leaves simulation."""
    cert_id = req['certificate_id']
    amount = details.get('amount', 0)
    dest_institution = details.get('destination_institution', 'unknown')

    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'], commit=False)
        return
    _verify_cert_ownership(cert, req)

    cert_value = models.get_certificate_total_value(db, cert_id)
    amount = min(amount, cert_value)
    if amount <= 0:
        models.fail_request(db, req['id'], commit=False)
        return

    # Compute unit price and units to redeem
    unit_price = models.get_certificate_unit_price(db, cert_id)
    units_to_redeem = amount / unit_price if unit_price > 0 else 0

    if not _sell_holdings(db, cert_id, amount):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"External transfer-out FAILED: insufficient funds in certificate #{cert_id}"
        )
        return

    # Consume lots FIFO by units
    consumed_lots = models.consume_lots_fifo(db, cert_id, units_to_redeem, commit=False)

    # Record lot allocations for audit
    models.record_lot_allocations(db, 'transfer_external_out', req['id'], consumed_lots, current_date, commit=False)

    # Update unit supply
    models.update_certificate_units(db, cert_id, -units_to_redeem, commit=False)

    # Reduce VGBL premium_remaining proportionally (clamp for loss scenarios)
    if cert['plan_type'] == 'VGBL' and cert_value > 1e-9:
        P_rem = models.get_vgbl_premium_remaining(db, cert_id)
        prem_frac = min(1.0, max(0.0, P_rem / cert_value))
        premium_returned = amount * prem_frac
        models.update_vgbl_premium_remaining(db, cert_id, -premium_returned, commit=False)

    # Money simply leaves the simulation
    models.complete_request(db, req['id'], current_date, commit=False)
    step_log['events'].append(
        f"External transfer-out: R${amount:,.2f} from certificate #{cert_id} "
        f"to {dest_institution}"
    )


def _execute_transfer_external_in(db, req, details, current_date, step_log):
    """External port-in: money enters simulation. Creates backdated lots per schedule.
    Uses embedded gain config for VGBL premium_remaining."""
    cert_id = req['certificate_id']
    amount = details.get('amount', 0)
    source_institution = details.get('source_institution', 'unknown')

    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"External transfer-in FAILED: certificate #{cert_id} not valid"
        )
        return
    _verify_cert_ownership(cert, req)

    # Validate target allocations exist
    if not models.get_target_allocations(db, cert_id):
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"External transfer-in FAILED: certificate #{cert_id} "
            f"has no target allocations set"
        )
        return

    if amount <= 0:
        models.fail_request(db, req['id'], commit=False)
        return

    # Get configurable port-in schedule and embedded gain config
    schedule = models.get_external_portin_schedule(db)
    embedded_gain_pct = models.get_external_portin_gain_pct(db)

    # Compute unit price for issuing certificate units
    unit_price = models.get_certificate_unit_price(db, cert_id)
    total_units = amount / unit_price

    # Validate schedule percentages sum to 100%
    schedule_total_pct = sum(t['pct'] for t in schedule)
    if abs(schedule_total_pct - 100) > 0.01:
        models.fail_request(db, req['id'], commit=False)
        step_log['events'].append(
            f"External transfer-in FAILED: port-in schedule pct sums to "
            f"{schedule_total_pct:.1f}% (must be 100%)"
        )
        return

    # Create backdated lots per schedule with certificate units
    dt = datetime.strptime(current_date, '%Y-%m-%d')
    lot_details = []
    for tranche in schedule:
        tranche_pct = tranche['pct'] / 100.0
        tranche_amount = amount * tranche_pct
        tranche_units = total_units * tranche_pct
        if tranche_amount <= 0:
            continue
        try:
            backdated = dt - relativedelta(years=tranche['years_ago'])
        except Exception:
            backdated = dt - timedelta(days=tranche['years_ago'] * 365)
        contrib_date = backdated.strftime('%Y-%m-%d')
        # For VGBL: remaining_amount = premium basis (not full tranche value)
        if cert['plan_type'] == 'VGBL':
            tranche_remaining = tranche_amount * embedded_gain_pct
        else:
            tranche_remaining = tranche_amount
        models.add_contribution(db, cert_id, tranche_amount, contrib_date,
                                source_type='transfer_external',
                                remaining_amount=tranche_remaining,
                                units_total=tranche_units,
                                units_remaining=tranche_units,
                                issue_unit_price=unit_price, commit=False)
        lot_details.append(f"R${tranche_amount:,.2f} dated {contrib_date}")

    # Update certificate unit supply
    models.update_certificate_units(db, cert_id, total_units, commit=False)

    # Update VGBL premium_remaining using embedded gain config
    if cert['plan_type'] == 'VGBL':
        premium_basis = amount * embedded_gain_pct
        models.update_vgbl_premium_remaining(db, cert_id, premium_basis, commit=False)

    # Buy fractional fund units for full amount
    _buy_into_certificate(db, cert_id, amount)

    models.complete_request(db, req['id'], current_date, commit=False)
    gain_info = f", premium basis: R${amount * embedded_gain_pct:,.2f} ({embedded_gain_pct*100:.0f}%)" if cert['plan_type'] == 'VGBL' else ""
    step_log['events'].append(
        f"External transfer-in: R${amount:,.2f} to certificate #{cert_id} "
        f"from {source_institution} ({len(lot_details)} lots: {'; '.join(lot_details)}{gain_info})"
    )


def _buy_into_certificate(db, cert_id, amount):
    """Buy into a certificate per its target allocations. Fractional units, no cash residual.
    Normalizes allocations to sum to 100% to prevent cash leakage.
    Raises ValueError if no target allocations are set."""
    target_allocs = models.get_target_allocations(db, cert_id)
    if not target_allocs:
        raise ValueError(f"Certificate #{cert_id} has no target allocations set")

    # Normalize allocation percentages to sum to 100%
    total_pct = sum(ta['pct'] for ta in target_allocs)
    if total_pct <= 0:
        return

    for ta in target_allocs:
        alloc_amount = amount * (ta['pct'] / total_pct)
        fund = models.get_fund(db, ta['fund_id'])
        if not fund or fund['current_nav'] <= 0:
            continue
        units = alloc_amount / fund['current_nav']  # fractional, no int()

        existing = db.execute(
            "SELECT units FROM holdings WHERE certificate_id = ? AND fund_id = ?",
            (cert_id, ta['fund_id'])
        ).fetchone()
        new_units = (existing['units'] if existing else 0) + units
        models.set_holding(db, cert_id, ta['fund_id'], new_units, commit=False)
