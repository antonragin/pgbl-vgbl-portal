"""Investor Portal blueprint."""

import io
import json
import functools
from datetime import datetime
from flask import (Blueprint, render_template, session, redirect, url_for,
                   request, flash, g, Response)
import models
import tax_engine

portal_bp = Blueprint('portal', __name__, template_folder='../templates/portal')


def safe_float(value, default=0.0):
    """Convert form value to float, returning default on empty/invalid input."""
    try:
        return float(value) if value else default
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('portal.login'))
        return f(*args, **kwargs)
    return decorated


@portal_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        if password != '1234':
            flash('Invalid password. (Hint: 1234)', 'error')
            return render_template('portal/login.html')
        user = models.get_user_by_username(g.db, username)
        if not user:
            flash(f'User "{username}" not found. Create one in Admin first.', 'error')
            return render_template('portal/login.html')
        session['user_id'] = user['id']
        session['username'] = user['username']
        return redirect(url_for('portal.home'))
    return render_template('portal/login.html')


@portal_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('portal.login'))


# ---------------------------------------------------------------------------
# Home Dashboard
# ---------------------------------------------------------------------------

@portal_bp.route('/')
@login_required
def home():
    db = g.db
    user_id = session['user_id']
    certificates = models.list_certificates(db, user_id)
    brokerage_cash = models.get_brokerage_cash(db, user_id)
    sim_date = models.get_sim_date(db)

    cert_data = []
    for c in certificates:
        total_val = models.get_certificate_total_value(db, c['id'])
        holdings = models.get_holdings(db, c['id'])
        total_contribs = models.total_contributions(db, c['id'])
        # Use total_invested_basis (all sources) for accurate gain calculation
        invested_basis = models.total_invested_basis(db, c['id'])
        gain = total_val - invested_basis
        cert_data.append({
            'cert': c, 'total_value': total_val, 'holdings': holdings,
            'total_contribs': total_contribs, 'invested_basis': invested_basis,
            'gain': gain,
        })

    total_invested = sum(cd['total_value'] for cd in cert_data)
    total_basis = sum(cd['invested_basis'] for cd in cert_data)
    total_contributed = sum(cd['total_contribs'] for cd in cert_data)
    total_gain = total_invested - total_basis
    overall_return_pct = (total_gain / total_basis * 100) if total_basis > 0 else 0
    pending = models.list_requests(db, user_id=user_id, status='pending')

    return render_template('portal/home.html',
                           cert_data=cert_data, brokerage_cash=brokerage_cash,
                           total_invested=total_invested,
                           total_contributed=total_contributed,
                           total_gain=total_gain,
                           overall_return_pct=overall_return_pct,
                           pending_count=len(pending), sim_date=sim_date)


# ---------------------------------------------------------------------------
# Browse Plans & Funds
# ---------------------------------------------------------------------------

@portal_bp.route('/plans')
@login_required
def browse_plans():
    db = g.db
    plans = models.list_plans(db)
    funds = models.list_funds(db)
    user = models.get_user(db, session['user_id'])
    if user['is_retail']:
        funds = [f for f in funds if not f['is_qualified_only']]
    return render_template('portal/plans.html', plans=plans, funds=funds)


@portal_bp.route('/funds/<int:fund_id>')
@login_required
def fund_detail(fund_id):
    db = g.db
    fund = models.get_fund(db, fund_id)
    if not fund:
        flash('Fund not found.', 'error')
        return redirect(url_for('portal.browse_plans'))
    returns = models.get_fund_returns(db, fund_id)
    return render_template('portal/fund_detail.html', fund=fund, returns=returns)


@portal_bp.route('/charts/fund/<int:fund_id>/performance.png')
@login_required
def fund_chart(fund_id):
    """Generate a matplotlib performance chart for a fund."""
    db = g.db
    fund = models.get_fund(db, fund_id)
    returns = models.get_fund_returns(db, fund_id)
    if not fund or not returns:
        return Response(status=404)

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    # Simulate NAV history
    nav = fund['initial_nav']
    navs = [nav]
    for r in returns:
        nav *= (1 + r['return_pct'])
        navs.append(nav)

    fig, ax = plt.subplots(figsize=(8, 3))
    ax.plot(range(len(navs)), navs, color='#26C6DA', linewidth=2)
    ax.fill_between(range(len(navs)), navs, alpha=0.15, color='#26C6DA')
    ax.set_facecolor('#263238')
    fig.patch.set_facecolor('#263238')
    ax.tick_params(colors='white')
    ax.set_xlabel('Month', color='white')
    ax.set_ylabel('NAV (R$)', color='white')
    ax.set_title(f'{fund["name"]} - Performance', color='white')
    for spine in ax.spines.values():
        spine.set_color('#546E7A')

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight',
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype='image/png')


# ---------------------------------------------------------------------------
# Create Certificate
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/new', methods=['GET', 'POST'])
@login_required
def new_certificate():
    db = g.db
    user_id = session['user_id']

    if request.method == 'POST':
        plan_id = int(request.form.get('plan_id', 0))
        plan = models.get_plan(db, plan_id)
        if not plan:
            flash('Invalid plan.', 'error')
            return redirect(url_for('portal.new_certificate'))

        # Collect and validate allocations BEFORE creating the certificate
        funds = models.list_funds(db)
        allocs = []
        for f in funds:
            pct = safe_float(request.form.get(f'alloc_{f["id"]}'))
            if pct > 0:
                allocs.append((f['id'], pct))
        if allocs:
            total_pct = sum(pct for _, pct in allocs)
            if abs(total_pct - 100) > 0.01:
                flash(f'Allocation must sum to 100% (got {total_pct:.2f}%).', 'error')
                return redirect(url_for('portal.new_certificate'))

        sim_date = models.get_sim_date(db)
        cert_id = models.create_certificate(db, user_id, plan_id, sim_date)

        # Set target allocations from form
        if allocs:
            models.set_target_allocations(db, cert_id, allocs)

        flash(f'Certificate #{cert_id} created under {plan["type"]} - {plan["name"]}.', 'success')
        return redirect(url_for('portal.certificate_detail', cert_id=cert_id))

    plans = models.list_plans(db)
    funds = models.list_funds(db)
    user = models.get_user(db, user_id)
    if user['is_retail']:
        funds = [f for f in funds if not f['is_qualified_only']]
    return render_template('portal/certificate_new.html', plans=plans, funds=funds)


# ---------------------------------------------------------------------------
# Certificate Detail
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/<int:cert_id>')
@login_required
def certificate_detail(cert_id):
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert or cert['user_id'] != session['user_id']:
        flash('Certificate not found.', 'error')
        return redirect(url_for('portal.home'))

    holdings = models.get_holdings(db, cert_id)
    contributions = models.list_contributions(db, cert_id)
    withdrawals = models.list_withdrawals(db, cert_id)
    total_value = models.get_certificate_total_value(db, cert_id)
    total_contribs = models.total_contributions(db, cert_id)
    total_remaining = models.total_remaining_contributions(db, cert_id)
    target_allocs = models.get_target_allocations(db, cert_id)
    cert_requests = models.list_requests(db, cert_id=cert_id)

    # Certificate units info
    unit_price = models.get_certificate_unit_price(db, cert_id)
    unit_supply = models.get_certificate_unit_supply(db, cert_id)
    P_rem = models.get_vgbl_premium_remaining(db, cert_id)

    # Contribution aging data (tax lots) — calendar-year brackets
    sim_date = models.get_sim_date(db)
    current_dt = datetime.strptime(sim_date, '%Y-%m-%d')
    plan_type = cert['plan_type']

    # VGBL earnings ratio (certificate-level)
    if plan_type == 'VGBL' and total_value > 0:
        earnings_ratio = max(0, 1 - P_rem / total_value)
    else:
        earnings_ratio = None

    contrib_aging = []
    for c in contributions:
        contrib_dt = datetime.strptime(c['contribution_date'], '%Y-%m-%d')
        days_held = max(0, (current_dt - contrib_dt).days)
        current_rate = tax_engine.regressive_rate(c['contribution_date'], sim_date)
        next_bracket_info = tax_engine.next_bracket_drop(c['contribution_date'], sim_date)

        # Per-lot current value using certificate units
        lot_current_value = c['units_remaining'] * unit_price if c['units_remaining'] > 0 else 0

        # VGBL: lot earnings use certificate-level earnings_ratio
        if earnings_ratio is not None:
            lot_earnings = lot_current_value * earnings_ratio
        else:
            lot_earnings = lot_current_value - c['remaining_amount'] if c['remaining_amount'] > 0 else 0

        contrib_aging.append({
            'contribution': c,
            'days_held': days_held,
            'months_held': days_held // 30,
            'current_rate': current_rate,
            'next_bracket': next_bracket_info,
            'lot_current_value': lot_current_value,
            'lot_earnings': lot_earnings,
        })

    invested_basis = models.total_invested_basis(db, cert_id)
    return render_template('portal/certificate.html',
                           cert=cert, holdings=holdings,
                           contributions=contributions, withdrawals=withdrawals,
                           total_value=total_value, total_contribs=total_contribs,
                           invested_basis=invested_basis,
                           total_remaining=total_remaining,
                           target_allocs=target_allocs, requests=cert_requests,
                           gain=total_value - invested_basis,
                           contrib_aging=contrib_aging,
                           plan_type=cert['plan_type'],
                           unit_price=unit_price, unit_supply=unit_supply,
                           P_rem=P_rem)


# ---------------------------------------------------------------------------
# Contribute
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/<int:cert_id>/contribute', methods=['GET', 'POST'])
@login_required
def contribute(cert_id):
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert or cert['user_id'] != session['user_id']:
        flash('Certificate not found.', 'error')
        return redirect(url_for('portal.home'))

    brokerage_cash = models.get_brokerage_cash(db, session['user_id'])

    if request.method == 'POST':
        # Check target allocations exist before accepting contribution
        target_allocs = models.get_target_allocations(db, cert_id)
        if not target_allocs:
            flash('Cannot contribute: no target allocations set for this certificate. '
                  'Please set allocations first via Switch Funds.', 'error')
            return redirect(url_for('portal.certificate_detail', cert_id=cert_id))

        amount = safe_float(request.form.get('amount'))
        if amount <= 0:
            flash('Amount must be positive.', 'error')
        elif amount > brokerage_cash:
            flash(f'Insufficient brokerage cash (R${brokerage_cash:,.2f}).', 'error')
        else:
            # Check IOF for VGBL
            iof = 0
            if cert['plan_type'] == 'VGBL':
                sim_date = models.get_sim_date(db)
                year = int(sim_date[:4])
                iof = tax_engine.calculate_iof_vgbl(db, session['user_id'], amount, year)

            sim_date = models.get_sim_date(db)
            models.create_request(db, session['user_id'], cert_id,
                                  'contribution',
                                  {'amount': amount, 'iof_estimated': iof},
                                  sim_date)
            msg = f'Contribution of R${amount:,.2f} submitted (pending next time evolution).'
            if iof > 0:
                net = amount - iof
                msg += f' Estimated IOF: R${iof:,.2f} (est. net: R${net:,.2f})'
            flash(msg, 'success')
            return redirect(url_for('portal.certificate_detail', cert_id=cert_id))

        return redirect(url_for('portal.contribute', cert_id=cert_id))

    # IOF warning (using configurable threshold)
    iof_warning = None
    if cert['plan_type'] == 'VGBL':
        sim_date = models.get_sim_date(db)
        year = int(sim_date[:4])
        iof_limit, iof_rate = models.get_iof_limit_for_year(db, year)
        # Only count source_type='contribution' for IOF base (use gross_amount for pre-IOF total)
        existing_vgbl = db.execute("""
            SELECT COALESCE(SUM(COALESCE(co.gross_amount, co.amount)), 0) as total
            FROM contributions co
            JOIN certificates ce ON co.certificate_id = ce.id
            JOIN plans p ON ce.plan_id = p.id
            WHERE ce.user_id = ? AND p.type = 'VGBL'
            AND co.contribution_date BETWEEN ? AND ?
            AND co.source_type = 'contribution'
        """, (session['user_id'], f'{year}-01-01', f'{year}-12-31')).fetchone()['total']
        declared = models.get_iof_declaration(db, session['user_id'], year)
        total_vgbl = existing_vgbl + declared
        remaining_exempt = max(0, iof_limit - total_vgbl)
        if remaining_exempt < 100000:
            iof_warning = (f'VGBL contributions this year (internal + declared): R${total_vgbl:,.2f}. '
                          f'R${remaining_exempt:,.2f} remaining before {iof_rate*100:.0f}% IOF applies '
                          f'(limit: R${iof_limit:,.0f}).')

    target_allocs = models.get_target_allocations(db, cert_id)
    return render_template('portal/contribute.html',
                           cert=cert, brokerage_cash=brokerage_cash,
                           iof_warning=iof_warning, target_allocs=target_allocs)


# ---------------------------------------------------------------------------
# Withdraw
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/<int:cert_id>/withdraw', methods=['GET', 'POST'])
@login_required
def withdraw(cert_id):
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert or cert['user_id'] != session['user_id']:
        flash('Certificate not found.', 'error')
        return redirect(url_for('portal.home'))

    total_value = models.get_certificate_total_value(db, cert_id)

    if request.method == 'POST':
        # Handle tax regime selection (validate but defer persistence to execution)
        if cert['tax_regime'] is None:
            regime = request.form.get('tax_regime')
            if regime not in ('progressive', 'regressive'):
                flash('You must select a tax regime before withdrawing.', 'error')
                return redirect(url_for('portal.withdraw', cert_id=cert_id))
        else:
            regime = cert['tax_regime']

        amount = safe_float(request.form.get('amount'))
        if amount <= 0 or amount > total_value:
            flash(f'Invalid amount. Maximum: R${total_value:,.2f}', 'error')
            return redirect(url_for('portal.withdraw', cert_id=cert_id))

        sim_date = models.get_sim_date(db)
        models.create_request(db, session['user_id'], cert_id,
                              'withdrawal',
                              {'amount': amount, 'tax_regime': regime},
                              sim_date)
        flash(f'Withdrawal of R${amount:,.2f} submitted (pending next time evolution). '
              f'Tax regime ({regime}) will be locked irrevocably on execution.', 'success')
        return redirect(url_for('portal.certificate_detail', cert_id=cert_id))

    total_contribs = models.total_contributions(db, cert_id)
    return render_template('portal/withdraw.html',
                           cert=cert, total_value=total_value,
                           total_contribs=total_contribs)


@portal_bp.route('/certificates/<int:cert_id>/tax-preview')
@login_required
def tax_preview(cert_id):
    """HTMX endpoint: returns a tax estimate HTML fragment."""
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert or cert['user_id'] != session['user_id']:
        return '<p>Error: certificate not found</p>'

    amount = safe_float(request.args.get('amount'))
    if amount <= 0:
        return '<p>Enter an amount to see tax estimate.</p>'

    result = tax_engine.estimate_tax(db, cert_id, amount)
    total_value = models.get_certificate_total_value(db, cert_id)
    brokerage_cash = models.get_brokerage_cash(db, session['user_id'])
    result['projected_cert_value'] = total_value - amount
    if result.get('regressive'):
        result['projected_brokerage_regressive'] = brokerage_cash + result['regressive']['net']
    if result.get('progressive'):
        result['projected_brokerage_progressive'] = brokerage_cash + result['progressive']['net']
    return render_template('portal/tax_preview_fragment.html', result=result)


# ---------------------------------------------------------------------------
# Fund Switch
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/<int:cert_id>/switch-funds', methods=['GET', 'POST'])
@login_required
def switch_funds(cert_id):
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert or cert['user_id'] != session['user_id']:
        flash('Certificate not found.', 'error')
        return redirect(url_for('portal.home'))

    if request.method == 'POST':
        funds = models.list_funds(db)
        new_allocs = []
        for f in funds:
            pct = safe_float(request.form.get(f'alloc_{f["id"]}'))
            if pct > 0:
                new_allocs.append({'fund_id': f['id'], 'pct': pct})

        total_pct = sum(a['pct'] for a in new_allocs)
        if abs(total_pct - 100) > 0.01:
            flash(f'Allocation must sum to 100% (got {total_pct}%).', 'error')
            return redirect(url_for('portal.switch_funds', cert_id=cert_id))

        sim_date = models.get_sim_date(db)
        models.create_request(db, session['user_id'], cert_id,
                              'fund_swap',
                              {'new_allocations': new_allocs},
                              sim_date)
        flash('Fund switch submitted (will execute on next time evolution).', 'success')
        return redirect(url_for('portal.certificate_detail', cert_id=cert_id))

    holdings = models.get_holdings(db, cert_id)
    total_value = models.get_certificate_total_value(db, cert_id)
    target_allocs = models.get_target_allocations(db, cert_id)
    funds = models.list_funds(db)
    user = models.get_user(db, session['user_id'])
    if user['is_retail']:
        funds = [f for f in funds if not f['is_qualified_only']]

    # Build current allocation percentages
    current_allocs = {}
    for h in holdings:
        if total_value > 0:
            current_allocs[h['fund_id']] = round(h['market_value'] / total_value * 100, 1)

    return render_template('portal/fund_switch.html',
                           cert=cert, holdings=holdings, total_value=total_value,
                           target_allocs=target_allocs, funds=funds,
                           current_allocs=current_allocs)


# ---------------------------------------------------------------------------
# Portability (legacy redirect)
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/<int:cert_id>/portability')
@login_required
def portability(cert_id):
    return redirect(url_for('portal.transfers'))


# ---------------------------------------------------------------------------
# Transfers
# ---------------------------------------------------------------------------

@portal_bp.route('/transfers', methods=['GET', 'POST'])
@login_required
def transfers():
    db = g.db
    user_id = session['user_id']
    all_certs = models.list_certificates(db, user_id)

    # Compute values for each certificate
    cert_values = {}
    for c in all_certs:
        cert_values[c['id']] = models.get_certificate_total_value(db, c['id'])

    if request.method == 'POST':
        action = request.form.get('action')
        sim_date = models.get_sim_date(db)

        if action == 'internal':
            source_id = int(request.form.get('source_cert_id', 0))
            dest_id = int(request.form.get('dest_cert_id', 0))
            amount = safe_float(request.form.get('amount'))

            source = models.get_certificate(db, source_id)
            dest = models.get_certificate(db, dest_id)

            if not source or source['user_id'] != user_id:
                flash('Invalid source certificate.', 'error')
            elif not dest or dest['user_id'] != user_id:
                flash('Invalid destination certificate.', 'error')
            elif source_id == dest_id:
                flash('Source and destination must be different.', 'error')
            elif source['plan_type'] != dest['plan_type']:
                flash(f'Cannot transfer between different types '
                      f'({source["plan_type"]} -> {dest["plan_type"]}). '
                      f'Only same-type transfers are allowed.', 'error')
            elif (source['tax_regime'] and dest['tax_regime'] and
                  source['tax_regime'] != dest['tax_regime']):
                flash(f'Tax regime mismatch: source has {source["tax_regime"]}, '
                      f'destination has {dest["tax_regime"]}. '
                      f'Regimes must match or destination must not have a regime set.', 'error')
            elif amount <= 0 or amount > cert_values.get(source_id, 0):
                flash(f'Invalid amount. Maximum: R${cert_values.get(source_id, 0):,.2f}', 'error')
            elif not models.get_target_allocations(db, dest_id):
                flash(f'Destination certificate #{dest_id} has no target allocations set. '
                      f'Please set allocations on the destination before transferring.', 'error')
            else:
                models.create_request(db, user_id, source_id, 'transfer_internal',
                                      {'destination_cert_id': dest_id, 'amount': amount},
                                      sim_date)
                flash(f'Internal transfer of R${amount:,.2f} from #{source_id} to #{dest_id} submitted.', 'success')
                return redirect(url_for('portal.transfers'))

        elif action == 'external_out':
            source_id = int(request.form.get('source_cert_id', 0))
            dest_institution = request.form.get('dest_institution', '').strip()
            amount = safe_float(request.form.get('amount'))

            source = models.get_certificate(db, source_id)
            if not source or source['user_id'] != user_id:
                flash('Invalid source certificate.', 'error')
            elif not dest_institution:
                flash('Please specify the destination institution.', 'error')
            elif amount <= 0 or amount > cert_values.get(source_id, 0):
                flash(f'Invalid amount. Maximum: R${cert_values.get(source_id, 0):,.2f}', 'error')
            else:
                models.create_request(db, user_id, source_id, 'transfer_external_out',
                                      {'destination_institution': dest_institution, 'amount': amount},
                                      sim_date)
                flash(f'External transfer-out of R${amount:,.2f} to {dest_institution} submitted.', 'success')
                return redirect(url_for('portal.transfers'))

        elif action == 'external_in':
            dest_id = int(request.form.get('dest_cert_id', 0))
            source_institution = request.form.get('source_institution', '').strip()
            amount = safe_float(request.form.get('amount'))

            dest = models.get_certificate(db, dest_id)
            if not dest or dest['user_id'] != user_id:
                flash('Invalid destination certificate.', 'error')
            elif not source_institution:
                flash('Please specify the source institution.', 'error')
            elif amount <= 0:
                flash('Amount must be positive.', 'error')
            elif not models.get_target_allocations(db, dest_id):
                flash(f'Destination certificate #{dest_id} has no target allocations set. '
                      f'Please set allocations on the destination before transferring.', 'error')
            else:
                models.create_request(db, user_id, dest_id, 'transfer_external_in',
                                      {'source_institution': source_institution, 'amount': amount},
                                      sim_date)
                flash(f'External transfer-in of R${amount:,.2f} from {source_institution} submitted.', 'success')
                return redirect(url_for('portal.transfers'))

    # Recent transfer requests
    all_requests = models.list_requests(db, user_id=user_id)
    transfer_reqs = [r for r in all_requests
                     if r['type'] in ('transfer_internal', 'transfer_external_out',
                                      'transfer_external_in', 'portability_out', 'portability_in')]

    # Get port-in schedule for display
    portin_schedule = models.get_external_portin_schedule(db)

    return render_template('portal/transfers.html',
                           certs=all_certs, cert_values=cert_values,
                           transfer_requests=transfer_reqs,
                           portin_schedule=portin_schedule)


# ---------------------------------------------------------------------------
# IOF Declaration
# ---------------------------------------------------------------------------

@portal_bp.route('/iof-declaration', methods=['GET', 'POST'])
@login_required
def iof_declaration():
    db = g.db
    user_id = session['user_id']
    sim_date = models.get_sim_date(db)
    year = int(sim_date[:4])

    if request.method == 'POST':
        amount = safe_float(request.form.get('declared_amount'))
        if amount < 0:
            flash('Declared amount cannot be negative.', 'error')
        else:
            models.set_iof_declaration(db, user_id, year, amount)
            flash(f'IOF declaration updated: R${amount:,.2f} declared for {year}.', 'success')
        return redirect(url_for('portal.iof_declaration'))

    current_declaration = models.get_iof_declaration(db, user_id, year)
    iof_limit, iof_rate = models.get_iof_limit_for_year(db, year)

    # Get internal VGBL contributions this year (use gross_amount for pre-IOF total)
    internal_vgbl = db.execute("""
        SELECT COALESCE(SUM(COALESCE(co.gross_amount, co.amount)), 0) as total
        FROM contributions co
        JOIN certificates ce ON co.certificate_id = ce.id
        JOIN plans p ON ce.plan_id = p.id
        WHERE ce.user_id = ? AND p.type = 'VGBL'
        AND co.contribution_date BETWEEN ? AND ?
        AND co.source_type = 'contribution'
    """, (user_id, f'{year}-01-01', f'{year}-12-31')).fetchone()['total']

    total = internal_vgbl + current_declaration
    remaining_exempt = max(0, iof_limit - total)

    return render_template('portal/iof_declaration.html',
                           year=year, current_declaration=current_declaration,
                           internal_vgbl=internal_vgbl, total=total,
                           remaining_exempt=remaining_exempt,
                           iof_limit=iof_limit, iof_rate=iof_rate)


# ---------------------------------------------------------------------------
# Tax Lots
# ---------------------------------------------------------------------------

@portal_bp.route('/certificates/<int:cert_id>/tax-lots')
@login_required
def tax_lots(cert_id):
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert or cert['user_id'] != session['user_id']:
        flash('Certificate not found.', 'error')
        return redirect(url_for('portal.home'))

    contributions = models.list_contributions(db, cert_id)
    total_value = models.get_certificate_total_value(db, cert_id)
    total_remaining = models.total_remaining_contributions(db, cert_id)
    unit_price = models.get_certificate_unit_price(db, cert_id)
    unit_supply = models.get_certificate_unit_supply(db, cert_id)
    P_rem = models.get_vgbl_premium_remaining(db, cert_id)
    sim_date = models.get_sim_date(db)
    current_dt = datetime.strptime(sim_date, '%Y-%m-%d')

    plan_type = cert['plan_type']

    # VGBL earnings ratio (certificate-level)
    if plan_type == 'VGBL' and total_value > 0:
        earnings_ratio = max(0, 1 - P_rem / total_value)
    else:
        earnings_ratio = None

    lots = []
    for c in contributions:
        contrib_dt = datetime.strptime(c['contribution_date'], '%Y-%m-%d')
        days_held = max(0, (current_dt - contrib_dt).days)
        current_rate = tax_engine.regressive_rate(c['contribution_date'], sim_date)
        next_bracket_info = tax_engine.next_bracket_drop(c['contribution_date'], sim_date)

        lot_current_value = c['units_remaining'] * unit_price if c['units_remaining'] > 0 else 0

        # VGBL: lot earnings use certificate-level earnings_ratio
        if earnings_ratio is not None:
            lot_earnings = lot_current_value * earnings_ratio
        else:
            lot_earnings = lot_current_value - c['remaining_amount'] if c['remaining_amount'] > 0 else 0

        lots.append({
            'id': c['id'],
            'date': c['contribution_date'],
            'source_type': c['source_type'],
            'original_amount': c['amount'],
            'remaining_amount': c['remaining_amount'],
            'units_total': c['units_total'],
            'units_remaining': c['units_remaining'],
            'days_held': days_held,
            'months_held': days_held // 30,
            'current_rate': current_rate,
            'next_bracket': next_bracket_info,
            'current_value': lot_current_value,
            'earnings': lot_earnings,
        })

    return render_template('portal/tax_lots.html',
                           cert=cert, lots=lots,
                           total_value=total_value,
                           total_remaining=total_remaining,
                           unit_price=unit_price, unit_supply=unit_supply,
                           P_rem=P_rem)


# ---------------------------------------------------------------------------
# Brokerage
# ---------------------------------------------------------------------------

@portal_bp.route('/brokerage', methods=['GET', 'POST'])
@login_required
def brokerage():
    db = g.db
    user_id = session['user_id']

    if request.method == 'POST':
        amount = safe_float(request.form.get('amount'))
        cash = models.get_brokerage_cash(db, user_id)
        if amount <= 0 or amount > cash:
            flash(f'Invalid amount. Available: R${cash:,.2f}', 'error')
        else:
            sim_date = models.get_sim_date(db)
            models.create_request(db, user_id, None, 'brokerage_withdrawal',
                                  {'amount': amount}, sim_date)
            flash(f'Brokerage withdrawal of R${amount:,.2f} submitted.', 'success')
        return redirect(url_for('portal.brokerage'))

    cash = models.get_brokerage_cash(db, user_id)
    requests_list = models.list_requests(db, user_id=user_id)
    brokerage_reqs = [r for r in requests_list if r['type'] == 'brokerage_withdrawal']
    return render_template('portal/brokerage.html', cash=cash, requests=brokerage_reqs)


# ---------------------------------------------------------------------------
# Request Log
# ---------------------------------------------------------------------------

@portal_bp.route('/requests')
@login_required
def request_log():
    db = g.db
    all_requests = models.list_requests(db, user_id=session['user_id'])
    return render_template('portal/requests.html', requests=all_requests)


@portal_bp.route('/requests/<int:req_id>/cancel', methods=['POST'])
@login_required
def cancel_request(req_id):
    db = g.db
    req = models.get_request(db, req_id)
    if not req or req['user_id'] != session['user_id']:
        flash('Request not found.', 'error')
    elif req['status'] != 'pending':
        flash('Only pending requests can be cancelled.', 'error')
    else:
        models.cancel_request(db, req_id)
        flash(f'Request #{req_id} cancelled.', 'success')
    return redirect(url_for('portal.request_log'))


# ---------------------------------------------------------------------------
# My Account (disabled placeholder)
# ---------------------------------------------------------------------------

@portal_bp.route('/account')
@login_required
def account():
    flash('Account management is not available in this prototype.', 'info')
    return redirect(url_for('portal.home'))
