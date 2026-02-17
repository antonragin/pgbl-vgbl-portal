"""Time evolution engine for the simulation.

Evolves the simulation forward by N months:
1. Updates all fund NAVs using cyclic returns
2. Auto-completes pending fund swap requests
3. Auto-completes pending withdrawal requests
4. Auto-completes pending contribution requests
5. Auto-completes pending portability requests
6. Auto-completes pending brokerage withdrawal requests
"""

import calendar
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import models
import tax_engine


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

        # 2. Process pending fund swaps
        _process_pending_requests(db, 'fund_swap', new_date, step_log)

        # 3. Process pending withdrawals
        _process_pending_requests(db, 'withdrawal', new_date, step_log)

        # 4. Process pending contributions
        _process_pending_requests(db, 'contribution', new_date, step_log)

        # 5. Process pending portability
        _process_pending_requests(db, 'portability_out', new_date, step_log)

        # 6. Process pending brokerage withdrawals
        _process_pending_requests(db, 'brokerage_withdrawal', new_date, step_log)

        # 7. Process pending transfers
        _process_pending_requests(db, 'transfer_internal', new_date, step_log)
        _process_pending_requests(db, 'transfer_external_out', new_date, step_log)
        _process_pending_requests(db, 'transfer_external_in', new_date, step_log)

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


def _process_pending_requests(db, req_type, current_date, step_log):
    """Process all pending requests of a given type."""
    pending = models.list_requests(db, status='pending')
    for req in pending:
        if req['type'] != req_type:
            continue
        try:
            details = json.loads(req['details']) if req['details'] else {}
            if req_type == 'fund_swap':
                _execute_fund_swap(db, req, details, current_date, step_log)
            elif req_type == 'withdrawal':
                _execute_withdrawal(db, req, details, current_date, step_log)
            elif req_type == 'contribution':
                _execute_contribution(db, req, details, current_date, step_log)
            elif req_type == 'portability_out':
                _execute_portability(db, req, details, current_date, step_log)
            elif req_type == 'brokerage_withdrawal':
                _execute_brokerage_withdrawal(db, req, details, current_date, step_log)
            elif req_type == 'transfer_internal':
                _execute_transfer_internal(db, req, details, current_date, step_log)
            elif req_type == 'transfer_external_out':
                _execute_transfer_external_out(db, req, details, current_date, step_log)
            elif req_type == 'transfer_external_in':
                _execute_transfer_external_in(db, req, details, current_date, step_log)
        except Exception as e:
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
        models.fail_request(db, req['id'])
        return

    # Sell all current holdings
    holdings = models.get_holdings(db, cert_id)
    total_cash = 0.0
    for h in holdings:
        total_cash += h['units'] * h['current_nav']
        models.set_holding(db, cert_id, h['fund_id'], 0)

    # Buy new allocation with fractional units
    new_allocs = details.get('new_allocations', [])
    for alloc in new_allocs:
        fund = models.get_fund(db, alloc['fund_id'])
        if not fund or fund['current_nav'] <= 0:
            continue
        target_amount = total_cash * (alloc['pct'] / 100.0)
        units = target_amount / fund['current_nav']  # fractional, no int()
        if units > 1e-9:
            models.set_holding(db, cert_id, alloc['fund_id'], units)

    # Update target allocations
    alloc_tuples = [(a['fund_id'], a['pct']) for a in new_allocs]
    models.set_target_allocations(db, cert_id, alloc_tuples)

    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"Fund swap completed for certificate #{cert_id} "
        f"(R${total_cash:,.2f} reallocated, 100% invested)"
    )


def _execute_withdrawal(db, req, details, current_date, step_log):
    """
    Execute a withdrawal: sell units, consume FIFO lots, calculate tax per lot, net to brokerage.
    Does NOT switch phase to spending.
    """
    cert_id = req['certificate_id']
    user_id = req['user_id']
    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'])
        return

    amount = details.get('amount', 0)
    if amount <= 0:
        models.fail_request(db, req['id'])
        return

    # Set tax regime if provided and not yet set
    if cert['tax_regime'] is None and 'tax_regime' in details:
        models.set_tax_regime(db, cert_id, details['tax_regime'])
        cert = models.get_certificate(db, cert_id)

    total_value = models.get_certificate_total_value(db, cert_id)
    amount = min(amount, total_value)

    total_remaining = models.total_remaining_contributions(db, cert_id)

    # Sell holdings proportionally to raise the gross amount
    if not _sell_holdings(db, cert_id, amount):
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Withdrawal FAILED for certificate #{cert_id}: insufficient funds to raise R${amount:,.2f}"
        )
        return

    # Compute cost basis to consume: proportional to withdrawal/value ratio
    if total_value > 0 and total_remaining > 0:
        cost_basis_to_consume = (amount / total_value) * total_remaining
    else:
        cost_basis_to_consume = 0

    # Consume lots FIFO
    consumed_lots = models.consume_lots_fifo(db, cert_id, cost_basis_to_consume)

    # Compute per-lot tax
    sim_month = models.get_sim_month(db)
    growth_factor = total_value / total_remaining if total_remaining > 0 else 1.0
    total_tax = 0.0
    breakdown = []

    for lot in consumed_lots:
        contrib_month = tax_engine._date_to_sim_month(lot['contribution_date'])
        months_held = max(0, sim_month - contrib_month)
        consumed = lot['consumed_amount']

        if cert['tax_regime'] == 'regressive':
            rate = tax_engine.regressive_rate(months_held)
            if cert['plan_type'] == 'PGBL':
                taxable = consumed * growth_factor
            else:
                # VGBL: only earnings
                lot_value = consumed * growth_factor
                taxable = lot_value - consumed
            tax_on_lot = taxable * rate
        else:
            # Progressive: 15% withholding on taxable base
            rate = 0.15
            if cert['plan_type'] == 'PGBL':
                taxable = consumed * growth_factor
            else:
                lot_value = consumed * growth_factor
                taxable = lot_value - consumed
            tax_on_lot = taxable * rate

        total_tax += tax_on_lot
        breakdown.append({
            'contribution_id': lot['contribution_id'],
            'consumed_amount': round(consumed, 2),
            'months_held': months_held,
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
    }

    # Record withdrawal
    withdrawal_id = models.add_withdrawal(db, cert_id, amount, total_tax, net_amount,
                                          current_date, tax_result)

    # Record lot allocations for audit
    def _rate_fn(lot, mh):
        return tax_engine.regressive_rate(mh) if cert['tax_regime'] == 'regressive' else 0.15

    def _taxable_fn(lot, mh):
        c = lot['consumed_amount']
        if cert['plan_type'] == 'PGBL':
            return c * growth_factor
        return c * growth_factor - c

    def _tax_fn(lot, mh):
        return _taxable_fn(lot, mh) * _rate_fn(lot, mh)

    models.record_lot_allocations(db, 'withdrawal', withdrawal_id, consumed_lots,
                                  sim_month, _rate_fn, _taxable_fn, _tax_fn)

    # Net goes to brokerage
    models.add_brokerage_cash(db, user_id, net_amount)

    # Do NOT switch to spending phase (withdrawals allowed in accumulation)

    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"Withdrawal from certificate #{cert_id}: "
        f"gross R${amount:,.2f}, tax R${total_tax:,.2f}, "
        f"net R${net_amount:,.2f} -> brokerage"
    )


def _execute_contribution(db, req, details, current_date, step_log):
    """
    Execute a contribution: buy fractional units at current NAV per target allocation.
    IOF enforcement for VGBL.
    """
    cert_id = req['certificate_id']
    user_id = req['user_id']
    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'])
        return

    amount = details.get('amount', 0)
    if amount <= 0:
        models.fail_request(db, req['id'])
        return

    # Deduct from brokerage
    brokerage_cash = models.get_brokerage_cash(db, user_id)
    if brokerage_cash < amount:
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Contribution to certificate #{cert_id} FAILED: "
            f"insufficient brokerage cash (R${brokerage_cash:,.2f} < R${amount:,.2f})"
        )
        return

    models.set_brokerage_cash(db, user_id, brokerage_cash - amount)

    # IOF enforcement for VGBL
    iof = 0.0
    net_invest = amount
    if cert['plan_type'] == 'VGBL':
        year = int(current_date[:4])
        iof = tax_engine.calculate_iof_vgbl(db, user_id, amount, year)
        if iof > 0:
            net_invest = amount - iof

    # Record contribution at net_invest (after IOF deduction)
    models.add_contribution(db, cert_id, net_invest, current_date)

    # Buy fractional units per target allocation
    _buy_into_certificate(db, cert_id, net_invest)

    iof_msg = f", IOF R${iof:,.2f}" if iof > 0 else ""
    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"Contribution to certificate #{cert_id}: R${amount:,.2f} invested{iof_msg}, "
        f"100% in funds"
    )


def _execute_portability(db, req, details, current_date, step_log):
    """
    Execute portability: transfer value from source to destination certificate.
    Consumes lots FIFO at source, recreates them at destination with original dates.
    """
    source_cert_id = req['certificate_id']
    dest_cert_id = details.get('destination_cert_id')
    if not dest_cert_id:
        models.fail_request(db, req['id'])
        return

    source_cert = models.get_certificate(db, source_cert_id)
    dest_cert = models.get_certificate(db, dest_cert_id)
    if not source_cert or not dest_cert:
        models.fail_request(db, req['id'])
        return

    # Validate same type
    if source_cert['plan_type'] != dest_cert['plan_type']:
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Portability #{req['id']} FAILED: type mismatch "
            f"({source_cert['plan_type']} -> {dest_cert['plan_type']})"
        )
        return

    # Tax regime compatibility
    if (source_cert['tax_regime'] and dest_cert['tax_regime'] and
            source_cert['tax_regime'] != dest_cert['tax_regime']):
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Portability #{req['id']} FAILED: tax regime mismatch "
            f"({source_cert['tax_regime']} -> {dest_cert['tax_regime']})"
        )
        return

    # Inherit tax regime if needed
    if source_cert['tax_regime'] and not dest_cert['tax_regime']:
        models.set_tax_regime(db, dest_cert_id, source_cert['tax_regime'])

    # Calculate transfer amount
    source_value = models.get_certificate_total_value(db, source_cert_id)
    amount = details.get('amount', source_value)
    amount = min(amount, source_value)

    source_remaining = models.total_remaining_contributions(db, source_cert_id)

    # Sell source holdings proportionally
    if not _sell_holdings(db, source_cert_id, amount):
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Portability FAILED: insufficient funds in certificate #{source_cert_id}"
        )
        return

    # Consume lots FIFO at source, recreate at destination
    if source_value > 0 and source_remaining > 0:
        cost_basis_to_consume = (amount / source_value) * source_remaining
    else:
        cost_basis_to_consume = 0

    consumed_lots = models.consume_lots_fifo(db, source_cert_id, cost_basis_to_consume)

    # Record lot allocations for audit
    sim_month = models.get_sim_month(db)
    models.record_lot_allocations(db, 'portability_out', req['id'], consumed_lots, sim_month)

    # Recreate lots at destination with original dates
    for lot in consumed_lots:
        models.add_contribution(db, dest_cert_id, lot['consumed_amount'],
                                lot['contribution_date'],
                                source_type='transfer_external',
                                remaining_amount=lot['consumed_amount'])

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
            models.complete_request(db, ir['id'], current_date)

    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"Portability: R${amount:,.2f} from certificate #{source_cert_id} "
        f"to #{dest_cert_id} (lots moved FIFO, dates preserved)"
    )


def _execute_brokerage_withdrawal(db, req, details, current_date, step_log):
    """Execute a brokerage withdrawal: cash just disappears from simulation."""
    user_id = req['user_id']
    amount = details.get('amount', 0)
    if amount <= 0:
        models.fail_request(db, req['id'])
        return

    current_cash = models.get_brokerage_cash(db, user_id)
    if current_cash < amount:
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Brokerage withdrawal FAILED: insufficient cash "
            f"(R${current_cash:,.2f} < R${amount:,.2f})"
        )
        return

    models.set_brokerage_cash(db, user_id, current_cash - amount)
    models.complete_request(db, req['id'], current_date)
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

    if total_holdings_value <= 0:
        return False

    # Sell proportionally
    sell_fraction = min(1.0, amount / total_holdings_value)
    for h in holdings:
        if h['units'] <= 1e-9:
            continue
        units_to_sell = h['units'] * sell_fraction
        new_units = h['units'] - units_to_sell
        models.set_holding(db, cert_id, h['fund_id'], new_units)

    return True


def _execute_transfer_internal(db, req, details, current_date, step_log):
    """Internal transfer: sell source, consume lots FIFO, recreate at dest, buy fractional units."""
    source_cert_id = req['certificate_id']
    dest_cert_id = details.get('destination_cert_id')
    amount = details.get('amount', 0)

    source = models.get_certificate(db, source_cert_id)
    dest = models.get_certificate(db, dest_cert_id)
    if not source or not dest:
        models.fail_request(db, req['id'])
        return

    if source['plan_type'] != dest['plan_type']:
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Internal transfer FAILED: type mismatch "
            f"({source['plan_type']} -> {dest['plan_type']})"
        )
        return

    # Tax regime compatibility
    if (source['tax_regime'] and dest['tax_regime'] and
            source['tax_regime'] != dest['tax_regime']):
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Internal transfer FAILED: tax regime mismatch "
            f"({source['tax_regime']} -> {dest['tax_regime']})"
        )
        return

    # Inherit tax regime if needed
    if source['tax_regime'] and not dest['tax_regime']:
        models.set_tax_regime(db, dest_cert_id, source['tax_regime'])

    source_value = models.get_certificate_total_value(db, source_cert_id)
    amount = min(amount, source_value)
    if amount <= 0:
        models.fail_request(db, req['id'])
        return

    source_remaining = models.total_remaining_contributions(db, source_cert_id)

    # Sell source holdings
    if not _sell_holdings(db, source_cert_id, amount):
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"Internal transfer FAILED: insufficient funds in certificate #{source_cert_id}"
        )
        return

    # Consume lots FIFO at source
    if source_value > 0 and source_remaining > 0:
        cost_basis_to_consume = (amount / source_value) * source_remaining
    else:
        cost_basis_to_consume = 0

    consumed_lots = models.consume_lots_fifo(db, source_cert_id, cost_basis_to_consume)

    # Record lot allocations for audit
    sim_month = models.get_sim_month(db)
    models.record_lot_allocations(db, 'transfer_internal', req['id'], consumed_lots, sim_month)

    # Recreate consumed lots at destination with original dates
    for lot in consumed_lots:
        models.add_contribution(db, dest_cert_id, lot['consumed_amount'],
                                lot['contribution_date'],
                                source_type='transfer_internal',
                                remaining_amount=lot['consumed_amount'])

    # Buy into destination per target allocations
    _buy_into_certificate(db, dest_cert_id, amount)

    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"Internal transfer: R${amount:,.2f} from #{source_cert_id} to #{dest_cert_id} "
        f"(lots moved FIFO, dates preserved)"
    )


def _execute_transfer_external_out(db, req, details, current_date, step_log):
    """External port-out: sell source holdings, consume lots FIFO, money leaves simulation."""
    cert_id = req['certificate_id']
    amount = details.get('amount', 0)
    dest_institution = details.get('destination_institution', 'unknown')

    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'])
        return

    cert_value = models.get_certificate_total_value(db, cert_id)
    amount = min(amount, cert_value)
    if amount <= 0:
        models.fail_request(db, req['id'])
        return

    cert_remaining = models.total_remaining_contributions(db, cert_id)

    if not _sell_holdings(db, cert_id, amount):
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"External transfer-out FAILED: insufficient funds in certificate #{cert_id}"
        )
        return

    # Consume lots FIFO
    if cert_value > 0 and cert_remaining > 0:
        cost_basis_to_consume = (amount / cert_value) * cert_remaining
    else:
        cost_basis_to_consume = 0

    consumed_lots = models.consume_lots_fifo(db, cert_id, cost_basis_to_consume)

    # Record lot allocations for audit
    sim_month = models.get_sim_month(db)
    models.record_lot_allocations(db, 'transfer_external_out', req['id'], consumed_lots, sim_month)

    # Money simply leaves the simulation
    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"External transfer-out: R${amount:,.2f} from certificate #{cert_id} "
        f"to {dest_institution}"
    )


def _execute_transfer_external_in(db, req, details, current_date, step_log):
    """External port-in: money enters simulation. Creates backdated lots per configurable schedule."""
    cert_id = req['certificate_id']
    amount = details.get('amount', 0)
    source_institution = details.get('source_institution', 'unknown')

    cert = models.get_certificate(db, cert_id)
    if not cert:
        models.fail_request(db, req['id'])
        step_log['events'].append(
            f"External transfer-in FAILED: certificate #{cert_id} not valid"
        )
        return

    if amount <= 0:
        models.fail_request(db, req['id'])
        return

    # Get configurable port-in schedule
    schedule = models.get_external_portin_schedule(db)

    # Create backdated lots per schedule
    dt = datetime.strptime(current_date, '%Y-%m-%d')
    lot_details = []
    for tranche in schedule:
        tranche_amount = amount * (tranche['pct'] / 100.0)
        if tranche_amount <= 0:
            continue
        try:
            backdated = dt - relativedelta(years=tranche['years_ago'])
        except Exception:
            backdated = dt - timedelta(days=tranche['years_ago'] * 365)
        contrib_date = backdated.strftime('%Y-%m-%d')
        models.add_contribution(db, cert_id, tranche_amount, contrib_date,
                                source_type='transfer_external')
        lot_details.append(f"R${tranche_amount:,.2f} dated {contrib_date}")

    # Buy fractional units for full amount
    _buy_into_certificate(db, cert_id, amount)

    models.complete_request(db, req['id'], current_date)
    step_log['events'].append(
        f"External transfer-in: R${amount:,.2f} to certificate #{cert_id} "
        f"from {source_institution} ({len(lot_details)} lots: {'; '.join(lot_details)})"
    )


def _buy_into_certificate(db, cert_id, amount):
    """Buy into a certificate per its target allocations. Fractional units, no cash residual."""
    target_allocs = models.get_target_allocations(db, cert_id)
    if not target_allocs:
        return

    for ta in target_allocs:
        alloc_amount = amount * (ta['pct'] / 100.0)
        fund = models.get_fund(db, ta['fund_id'])
        if not fund or fund['current_nav'] <= 0:
            continue
        units = alloc_amount / fund['current_nav']  # fractional, no int()

        existing = db.execute(
            "SELECT units FROM holdings WHERE certificate_id = ? AND fund_id = ?",
            (cert_id, ta['fund_id'])
        ).fetchone()
        new_units = (existing['units'] if existing else 0) + units
        models.set_holding(db, cert_id, ta['fund_id'], new_units)
