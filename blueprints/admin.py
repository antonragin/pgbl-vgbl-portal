"""Admin / Sim-Backend blueprint."""

import os
import json
from flask import (Blueprint, render_template, g, request, redirect,
                   url_for, flash, current_app)
import models
import time_engine

admin_bp = Blueprint('admin', __name__, template_folder='../templates/admin')


def safe_float(value, default=0.0):
    """Convert form value to float, returning default on empty/invalid input."""
    try:
        return float(value) if value else default
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """Convert form value to int, returning default on empty/invalid input."""
    try:
        return int(value) if value else default
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@admin_bp.route('/')
def dashboard():
    db = g.db
    user_count = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
    plan_count = db.execute("SELECT COUNT(*) as c FROM plans").fetchone()['c']
    fund_count = db.execute("SELECT COUNT(*) as c FROM funds").fetchone()['c']
    cert_count = db.execute("SELECT COUNT(*) as c FROM certificates").fetchone()['c']
    pending_count = db.execute(
        "SELECT COUNT(*) as c FROM requests WHERE status = 'pending'"
    ).fetchone()['c']
    sim_month = models.get_sim_month(db)
    sim_date = models.get_sim_date(db)
    return render_template('admin/dashboard.html',
                           user_count=user_count, plan_count=plan_count,
                           fund_count=fund_count, cert_count=cert_count,
                           pending_count=pending_count,
                           sim_month=sim_month, sim_date=sim_date)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@admin_bp.route('/users', methods=['GET', 'POST'])
def users():
    db = g.db
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        is_retail = request.form.get('is_retail') == '1'
        if username:
            try:
                models.create_user(db, username, is_retail)
                flash(f'User "{username}" created.', 'success')
            except Exception as e:
                flash(f'Error creating user: {e}', 'error')
        return redirect(url_for('admin.users'))
    all_users = models.list_users(db)
    return render_template('admin/users.html', users=all_users)


@admin_bp.route('/users/<int:user_id>/delete', methods=['POST'])
def delete_user(user_id):
    models.delete_user(g.db, user_id)
    flash('User deleted.', 'success')
    return redirect(url_for('admin.users'))


@admin_bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
def edit_user(user_id):
    db = g.db
    user = models.get_user(db, user_id)
    if not user:
        flash('User not found.', 'error')
        return redirect(url_for('admin.users'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        is_retail = request.form.get('is_retail') == '1'
        if username:
            models.update_user(db, user_id, username, is_retail)
            flash(f'User "{username}" updated.', 'success')
            return redirect(url_for('admin.users'))
        flash('Username is required.', 'error')
    return render_template('admin/user_edit.html', user=user)


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

@admin_bp.route('/plans', methods=['GET', 'POST'])
def plans():
    db = g.db
    if request.method == 'POST':
        type_ = request.form.get('type')
        name = request.form.get('name', '').strip()
        fees_info = request.form.get('fees_info', '').strip() or None
        plan_code = request.form.get('plan_code', '').strip() or None
        if type_ in ('PGBL', 'VGBL') and name:
            models.create_plan(db, type_, name, fees_info, plan_code)
            flash(f'Plan "{name}" created.', 'success')
        else:
            flash('Invalid plan data.', 'error')
        return redirect(url_for('admin.plans'))
    all_plans = models.list_plans(db)
    return render_template('admin/plans.html', plans=all_plans)


@admin_bp.route('/plans/<int:plan_id>/delete', methods=['POST'])
def delete_plan(plan_id):
    models.delete_plan(g.db, plan_id)
    flash('Plan deleted.', 'success')
    return redirect(url_for('admin.plans'))


@admin_bp.route('/plans/<int:plan_id>/edit', methods=['GET', 'POST'])
def edit_plan(plan_id):
    db = g.db
    plan = models.get_plan(db, plan_id)
    if not plan:
        flash('Plan not found.', 'error')
        return redirect(url_for('admin.plans'))
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        type_ = request.form.get('type')
        fees_info = request.form.get('fees_info', '').strip() or None
        plan_code = request.form.get('plan_code', '').strip() or None
        if name and type_ in ('PGBL', 'VGBL'):
            models.update_plan(db, plan_id, name, type_, fees_info, plan_code)
            flash(f'Plan "{name}" updated.', 'success')
            return redirect(url_for('admin.plans'))
        flash('Invalid plan data.', 'error')
    return render_template('admin/plan_edit.html', plan=plan)


# ---------------------------------------------------------------------------
# Funds
# ---------------------------------------------------------------------------

@admin_bp.route('/funds', methods=['GET', 'POST'])
def funds():
    db = g.db
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip() or None
        cnpj = request.form.get('cnpj', '').strip() or None
        is_qualified = request.form.get('is_qualified_only') == '1'
        initial_nav = safe_float(request.form.get('initial_nav'), 1.0)

        csv_filename = None
        csv_file = request.files.get('returns_csv')
        if csv_file and csv_file.filename:
            csv_filename = csv_file.filename
            upload_path = os.path.join(current_app.root_path,
                                       current_app.config['UPLOAD_FOLDER'],
                                       csv_filename)
            csv_file.save(upload_path)

        if name:
            fund_id = models.create_fund(db, name, description, cnpj,
                                         is_qualified, initial_nav, csv_filename)
            if csv_filename:
                upload_path = os.path.join(current_app.root_path,
                                           current_app.config['UPLOAD_FOLDER'],
                                           csv_filename)
                models.parse_and_store_returns(db, fund_id, upload_path)
            flash(f'Fund "{name}" created.', 'success')
        else:
            flash('Fund name is required.', 'error')
        return redirect(url_for('admin.funds'))

    all_funds = models.list_funds(db)
    # Add return count for each fund
    fund_data = []
    for f in all_funds:
        returns = models.get_fund_returns(db, f['id'])
        fund_data.append({'fund': f, 'return_count': len(returns)})
    return render_template('admin/funds.html', fund_data=fund_data)


@admin_bp.route('/funds/<int:fund_id>/delete', methods=['POST'])
def delete_fund(fund_id):
    models.delete_fund(g.db, fund_id)
    flash('Fund deleted.', 'success')
    return redirect(url_for('admin.funds'))


@admin_bp.route('/funds/<int:fund_id>/edit', methods=['GET', 'POST'])
def edit_fund(fund_id):
    db = g.db
    fund = models.get_fund(db, fund_id)
    if not fund:
        flash('Fund not found.', 'error')
        return redirect(url_for('admin.funds'))
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip() or None
        cnpj = request.form.get('cnpj', '').strip() or None
        is_qualified = request.form.get('is_qualified_only') == '1'

        if not name:
            flash('Fund name is required.', 'error')
            return render_template('admin/fund_edit.html', fund=fund,
                                   returns=models.get_fund_returns(db, fund_id))

        models.update_fund(db, fund_id, name, description, cnpj, is_qualified)

        # Handle return series CSV replacement
        csv_file = request.files.get('returns_csv')
        if csv_file and csv_file.filename:
            upload_path = os.path.join(current_app.root_path,
                                       current_app.config['UPLOAD_FOLDER'],
                                       csv_file.filename)
            csv_file.save(upload_path)
            models.parse_and_store_returns(db, fund_id, upload_path)
            db.execute("UPDATE funds SET returns_csv = ? WHERE id = ?",
                       (csv_file.filename, fund_id))
            db.commit()
            flash(f'Fund "{name}" updated with new return series.', 'success')
        else:
            flash(f'Fund "{name}" updated.', 'success')
        return redirect(url_for('admin.funds'))

    returns = models.get_fund_returns(db, fund_id)
    return render_template('admin/fund_edit.html', fund=fund, returns=returns)


# ---------------------------------------------------------------------------
# User Detail (certificates, brokerage, requests)
# ---------------------------------------------------------------------------

@admin_bp.route('/users/<int:user_id>')
def user_detail(user_id):
    db = g.db
    user = models.get_user(db, user_id)
    if not user:
        flash('User not found.', 'error')
        return redirect(url_for('admin.users'))
    certificates = models.list_certificates(db, user_id)
    brokerage_cash = models.get_brokerage_cash(db, user_id)
    user_requests = models.list_requests(db, user_id=user_id)
    all_plans = models.list_plans(db)

    # Compute total value per certificate
    cert_data = []
    for c in certificates:
        total_val = models.get_certificate_total_value(db, c['id'])
        holdings = models.get_holdings(db, c['id'])
        cert_data.append({'cert': c, 'total_value': total_val, 'holdings': holdings})

    return render_template('admin/user_detail.html',
                           user=user, cert_data=cert_data,
                           brokerage_cash=brokerage_cash,
                           requests=user_requests, plans=all_plans,
                           sim_date=models.get_sim_date(db))


@admin_bp.route('/users/<int:user_id>/certificates', methods=['POST'])
def add_certificate(user_id):
    db = g.db
    plan_id = int(request.form.get('plan_id', 0))
    created_date = request.form.get('created_date', models.get_sim_date(db))
    if plan_id:
        models.create_certificate(db, user_id, plan_id, created_date)
        flash('Certificate created.', 'success')
    return redirect(url_for('admin.user_detail', user_id=user_id))


@admin_bp.route('/users/<int:user_id>/certificates/<int:cert_id>/delete', methods=['POST'])
def delete_certificate(user_id, cert_id):
    models.delete_certificate(g.db, cert_id)
    flash('Certificate deleted.', 'success')
    return redirect(url_for('admin.user_detail', user_id=user_id))


@admin_bp.route('/users/<int:user_id>/certificates/<int:cert_id>', methods=['GET', 'POST'])
def edit_certificate(user_id, cert_id):
    db = g.db
    cert = models.get_certificate(db, cert_id)
    if not cert:
        flash('Certificate not found.', 'error')
        return redirect(url_for('admin.user_detail', user_id=user_id))

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'add_contribution':
            amount = safe_float(request.form.get('amount'))
            date = request.form.get('date', models.get_sim_date(db))
            if amount > 0:
                unit_price = models.get_certificate_unit_price(db, cert_id)
                units_issued = amount / unit_price
                models.add_contribution(db, cert_id, amount, date,
                                        remaining_amount=amount,
                                        units_total=units_issued,
                                        units_remaining=units_issued,
                                        issue_unit_price=unit_price)
                models.update_certificate_units(db, cert_id, units_issued)
                if cert['plan_type'] == 'VGBL':
                    models.update_vgbl_premium_remaining(db, cert_id, amount)
                flash(f'Contribution of R${amount:,.2f} added ({units_issued:.4f} units at R${unit_price:.6f}).', 'success')

        elif action == 'set_holding':
            fund_id = int(request.form.get('fund_id', 0))
            units = safe_float(request.form.get('units', 0))
            if fund_id:
                models.set_holding(db, cert_id, fund_id, units)
                flash('Holding updated.', 'success')

        elif action == 'set_phase':
            phase = request.form.get('phase')
            if phase in ('accumulation', 'spending'):
                models.set_certificate_phase(db, cert_id, phase)
                flash(f'Phase set to {phase}.', 'success')

        elif action == 'set_regime':
            regime = request.form.get('tax_regime')
            if regime in ('progressive', 'regressive'):
                models.set_tax_regime(db, cert_id, regime)
                flash(f'Tax regime set to {regime}.', 'success')

        elif action == 'add_withdrawal':
            gross = safe_float(request.form.get('gross_amount'))
            tax = safe_float(request.form.get('tax_withheld'))
            date = request.form.get('date', models.get_sim_date(db))
            if gross > 0:
                net = gross - tax
                models.add_withdrawal(db, cert_id, gross, tax, net, date)
                flash(f'Withdrawal of R${gross:,.2f} recorded.', 'success')

        elif action == 'reconcile_units':
            old_supply, new_supply = models.reconcile_certificate_units(db, cert_id)
            if abs(old_supply - new_supply) > 1e-6:
                flash(f'Unit supply reconciled: {old_supply:.6f} → {new_supply:.6f}', 'success')
            else:
                flash(f'Unit supply already consistent ({new_supply:.6f}).', 'info')

        return redirect(url_for('admin.edit_certificate',
                                user_id=user_id, cert_id=cert_id))

    holdings = models.get_holdings(db, cert_id)
    contributions = models.list_contributions(db, cert_id)
    withdrawals = models.list_withdrawals(db, cert_id)
    total_value = models.get_certificate_total_value(db, cert_id)
    total_contribs = models.total_contributions(db, cert_id)
    total_remaining = models.total_remaining_contributions(db, cert_id)
    all_funds = models.list_funds(db)
    cert_requests = models.list_requests(db, cert_id=cert_id)
    target_allocs = models.get_target_allocations(db, cert_id)

    # Certificate units info
    unit_price = models.get_certificate_unit_price(db, cert_id)
    unit_supply = models.get_certificate_unit_supply(db, cert_id)
    P_rem = models.get_vgbl_premium_remaining(db, cert_id)

    # Lot allocations audit trail
    lot_allocations = db.execute(
        "SELECT * FROM lot_allocations WHERE contribution_id IN "
        "(SELECT id FROM contributions WHERE certificate_id = ?) "
        "ORDER BY id DESC", (cert_id,)
    ).fetchall()

    return render_template('admin/certificate_detail.html',
                           user_id=user_id, cert=cert,
                           holdings=holdings, contributions=contributions,
                           withdrawals=withdrawals, total_value=total_value,
                           total_contribs=total_contribs,
                           total_remaining=total_remaining,
                           funds=all_funds,
                           requests=cert_requests, target_allocs=target_allocs,
                           lot_allocations=lot_allocations,
                           unit_price=unit_price, unit_supply=unit_supply,
                           P_rem=P_rem,
                           sim_date=models.get_sim_date(db))


@admin_bp.route('/users/<int:user_id>/cash', methods=['POST'])
def inject_cash(user_id):
    amount = safe_float(request.form.get('amount'))
    if amount > 0:
        models.add_brokerage_cash(g.db, user_id, amount)
        flash(f'R${amount:,.2f} injected into brokerage account.', 'success')
    return redirect(url_for('admin.user_detail', user_id=user_id))


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

@admin_bp.route('/requests')
def requests_log():
    db = g.db
    status_filter = request.args.get('status')
    all_requests = models.list_requests(db, status=status_filter)
    return render_template('admin/requests.html', requests=all_requests,
                           current_filter=status_filter)


@admin_bp.route('/requests/<int:req_id>/reject', methods=['POST'])
def reject_request(req_id):
    db = g.db
    req = models.get_request(db, req_id)
    if not req:
        flash('Request not found.', 'error')
    elif req['status'] != 'pending':
        flash('Only pending requests can be rejected.', 'error')
    else:
        reason = request.form.get('reason', '').strip() or None
        models.reject_request(db, req_id, reason)
        flash(f'Request #{req_id} rejected.', 'success')
    return redirect(url_for('admin.requests_log'))


# ---------------------------------------------------------------------------
# Time Control
# ---------------------------------------------------------------------------

@admin_bp.route('/time/evolve', methods=['POST'])
def evolve_time():
    db = g.db
    steps = safe_int(request.form.get('steps'), 1)
    steps = max(1, min(steps, 120))
    log = time_engine.evolve_time(db, steps)
    total_events = sum(len(s['events']) for s in log)
    flash(f'Evolved {steps} month(s). {total_events} events processed.', 'success')
    # Store log in session for display
    from flask import session
    session['evolution_log'] = log
    return redirect(url_for('admin.dashboard'))
