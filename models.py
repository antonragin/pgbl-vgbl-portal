"""Data access layer — thin repository functions wrapping parameterized SQL."""

import csv
import json
import os

# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def create_user(db, username, is_retail=True):
    cur = db.execute(
        "INSERT INTO users (username, is_retail) VALUES (?, ?)",
        (username, int(is_retail))
    )
    db.commit()
    # Ensure brokerage account exists
    db.execute(
        "INSERT OR IGNORE INTO brokerage_accounts (user_id, cash) VALUES (?, 0.0)",
        (cur.lastrowid,)
    )
    db.commit()
    return cur.lastrowid


def delete_user(db, user_id):
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()


def update_user(db, user_id, username, is_retail=True):
    db.execute(
        "UPDATE users SET username = ?, is_retail = ? WHERE id = ?",
        (username, int(is_retail), user_id)
    )
    db.commit()


def get_user(db, user_id):
    return db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def get_user_by_username(db, username):
    return db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()


def list_users(db):
    return db.execute("SELECT * FROM users ORDER BY id").fetchall()


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

def create_plan(db, type_, name, fees_info=None, plan_code=None):
    cur = db.execute(
        "INSERT INTO plans (type, name, fees_info, plan_code) VALUES (?, ?, ?, ?)",
        (type_, name, fees_info, plan_code)
    )
    db.commit()
    return cur.lastrowid


def delete_plan(db, plan_id):
    db.execute("DELETE FROM plans WHERE id = ?", (plan_id,))
    db.commit()


def update_plan(db, plan_id, name, type_=None, fees_info=None, plan_code=None):
    db.execute(
        "UPDATE plans SET name = ?, type = COALESCE(?, type), fees_info = ?, plan_code = ? WHERE id = ?",
        (name, type_, fees_info, plan_code, plan_id)
    )
    db.commit()


def get_plan(db, plan_id):
    return db.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()


def list_plans(db, type_filter=None):
    if type_filter:
        return db.execute(
            "SELECT * FROM plans WHERE type = ? ORDER BY id", (type_filter,)
        ).fetchall()
    return db.execute("SELECT * FROM plans ORDER BY id").fetchall()


# ---------------------------------------------------------------------------
# Funds
# ---------------------------------------------------------------------------

def create_fund(db, name, description=None, cnpj=None, is_qualified_only=False,
                initial_nav=1.0, returns_csv=None):
    cur = db.execute(
        """INSERT INTO funds (name, description, cnpj, is_qualified_only,
           initial_nav, current_nav, returns_csv)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (name, description, cnpj, int(is_qualified_only), initial_nav, initial_nav,
         returns_csv)
    )
    db.commit()
    return cur.lastrowid


def delete_fund(db, fund_id):
    db.execute("DELETE FROM fund_returns WHERE fund_id = ?", (fund_id,))
    db.execute("DELETE FROM funds WHERE id = ?", (fund_id,))
    db.commit()


def update_fund(db, fund_id, name, description=None, cnpj=None, is_qualified_only=False):
    db.execute(
        "UPDATE funds SET name = ?, description = ?, cnpj = ?, is_qualified_only = ? WHERE id = ?",
        (name, description, cnpj, int(is_qualified_only), fund_id)
    )
    db.commit()


def get_fund(db, fund_id):
    return db.execute("SELECT * FROM funds WHERE id = ?", (fund_id,)).fetchone()


def list_funds(db, retail_only=False):
    if retail_only:
        return db.execute(
            "SELECT * FROM funds WHERE is_qualified_only = 0 ORDER BY id"
        ).fetchall()
    return db.execute("SELECT * FROM funds ORDER BY id").fetchall()


def get_fund_returns(db, fund_id):
    return db.execute(
        "SELECT month_idx, return_pct FROM fund_returns WHERE fund_id = ? ORDER BY month_idx",
        (fund_id,)
    ).fetchall()


def parse_and_store_returns(db, fund_id, csv_path):
    """Parse a two-column CSV (month, return) and store in fund_returns."""
    db.execute("DELETE FROM fund_returns WHERE fund_id = ?", (fund_id,))
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader, None)  # skip header
        for idx, row in enumerate(reader):
            if len(row) >= 2:
                return_pct = float(row[1].strip().replace('%', '')) / 100 \
                    if '%' in row[1] else float(row[1].strip())
                db.execute(
                    "INSERT INTO fund_returns (fund_id, month_idx, return_pct) VALUES (?, ?, ?)",
                    (fund_id, idx, return_pct)
                )
    db.commit()


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------

def create_certificate(db, user_id, plan_id, created_date, notes=None):
    cur = db.execute(
        "INSERT INTO certificates (user_id, plan_id, created_date, notes) VALUES (?, ?, ?, ?)",
        (user_id, plan_id, created_date, notes)
    )
    db.commit()
    return cur.lastrowid


def delete_certificate(db, cert_id):
    db.execute("DELETE FROM lot_allocations WHERE contribution_id IN "
               "(SELECT id FROM contributions WHERE certificate_id = ?)", (cert_id,))
    db.execute("DELETE FROM contributions WHERE certificate_id = ?", (cert_id,))
    db.execute("DELETE FROM withdrawals WHERE certificate_id = ?", (cert_id,))
    db.execute("DELETE FROM holdings WHERE certificate_id = ?", (cert_id,))
    db.execute("DELETE FROM target_allocations WHERE certificate_id = ?", (cert_id,))
    db.execute("DELETE FROM requests WHERE certificate_id = ?", (cert_id,))
    db.execute("DELETE FROM certificates WHERE id = ?", (cert_id,))
    db.commit()


def get_certificate(db, cert_id):
    return db.execute(
        """SELECT c.*, p.type as plan_type, p.name as plan_name
           FROM certificates c JOIN plans p ON c.plan_id = p.id
           WHERE c.id = ?""",
        (cert_id,)
    ).fetchone()


def list_certificates(db, user_id=None):
    if user_id:
        return db.execute(
            """SELECT c.*, p.type as plan_type, p.name as plan_name
               FROM certificates c JOIN plans p ON c.plan_id = p.id
               WHERE c.user_id = ? ORDER BY c.id""",
            (user_id,)
        ).fetchall()
    return db.execute(
        """SELECT c.*, p.type as plan_type, p.name as plan_name
           FROM certificates c JOIN plans p ON c.plan_id = p.id ORDER BY c.id"""
    ).fetchall()


def set_certificate_phase(db, cert_id, phase):
    db.execute("UPDATE certificates SET phase = ? WHERE id = ?", (phase, cert_id))
    db.commit()


def set_tax_regime(db, cert_id, regime):
    db.execute("UPDATE certificates SET tax_regime = ? WHERE id = ?", (regime, cert_id))
    db.commit()


# ---------------------------------------------------------------------------
# Holdings
# ---------------------------------------------------------------------------

def get_holdings(db, cert_id):
    return db.execute(
        """SELECT h.*, f.name as fund_name, f.current_nav,
                  (h.units * f.current_nav) as market_value
           FROM holdings h JOIN funds f ON h.fund_id = f.id
           WHERE h.certificate_id = ? ORDER BY f.name""",
        (cert_id,)
    ).fetchall()


def set_holding(db, cert_id, fund_id, units):
    if units <= 1e-9:
        db.execute(
            "DELETE FROM holdings WHERE certificate_id = ? AND fund_id = ?",
            (cert_id, fund_id)
        )
    else:
        db.execute(
            """INSERT INTO holdings (certificate_id, fund_id, units) VALUES (?, ?, ?)
               ON CONFLICT(certificate_id, fund_id) DO UPDATE SET units = ?""",
            (cert_id, fund_id, units, units)
        )
    db.commit()


def get_certificate_total_value(db, cert_id):
    """Total value = sum(units * current_nav)."""
    row = db.execute(
        """SELECT COALESCE(SUM(h.units * f.current_nav), 0) as holdings_value
           FROM holdings h JOIN funds f ON h.fund_id = f.id
           WHERE h.certificate_id = ?""",
        (cert_id,)
    ).fetchone()
    return row['holdings_value'] or 0


# ---------------------------------------------------------------------------
# Target Allocations
# ---------------------------------------------------------------------------

def set_target_allocations(db, cert_id, allocations):
    """allocations: list of (fund_id, pct) tuples."""
    try:
        db.execute("DELETE FROM target_allocations WHERE certificate_id = ?", (cert_id,))
        for fund_id, pct in allocations:
            if pct > 0:
                db.execute(
                    "INSERT INTO target_allocations (certificate_id, fund_id, pct) VALUES (?, ?, ?)",
                    (cert_id, fund_id, pct)
                )
        db.commit()
    except Exception:
        db.rollback()
        raise


def get_target_allocations(db, cert_id):
    return db.execute(
        """SELECT ta.*, f.name as fund_name
           FROM target_allocations ta JOIN funds f ON ta.fund_id = f.id
           WHERE ta.certificate_id = ? ORDER BY f.name""",
        (cert_id,)
    ).fetchall()


# ---------------------------------------------------------------------------
# Contributions
# ---------------------------------------------------------------------------

def add_contribution(db, cert_id, amount, contribution_date, source_type='contribution',
                     remaining_amount=None, units_total=0.0, units_remaining=0.0,
                     issue_unit_price=0.0):
    if remaining_amount is None:
        remaining_amount = amount
    cur = db.execute(
        "INSERT INTO contributions (certificate_id, amount, remaining_amount, "
        "contribution_date, source_type, units_total, units_remaining, issue_unit_price) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cert_id, amount, remaining_amount, contribution_date, source_type,
         units_total, units_remaining, issue_unit_price)
    )
    db.commit()
    return cur.lastrowid


def list_contributions(db, cert_id):
    return db.execute(
        "SELECT * FROM contributions WHERE certificate_id = ? ORDER BY contribution_date, id",
        (cert_id,)
    ).fetchall()


def total_contributions(db, cert_id):
    row = db.execute(
        "SELECT COALESCE(SUM(amount), 0) as total FROM contributions WHERE certificate_id = ?",
        (cert_id,)
    ).fetchone()
    return row['total']


def total_remaining_contributions(db, cert_id):
    """Sum of remaining_amount for contributions with remaining > 0."""
    row = db.execute(
        "SELECT COALESCE(SUM(remaining_amount), 0) as total FROM contributions "
        "WHERE certificate_id = ? AND remaining_amount > 0",
        (cert_id,)
    ).fetchone()
    return row['total']


def consume_lots_fifo(db, cert_id, units_to_consume):
    """Consume oldest lots first by reducing units_remaining.
    Also reduces remaining_amount proportionally.
    Returns list of dicts with units_consumed and consumed_amount per lot."""
    contributions = db.execute(
        "SELECT * FROM contributions WHERE certificate_id = ? AND units_remaining > 1e-9 "
        "ORDER BY contribution_date, id",
        (cert_id,)
    ).fetchall()

    consumed = []
    remaining_units = units_to_consume

    for c in contributions:
        if remaining_units <= 1e-9:
            break

        available_units = c['units_remaining']
        take_units = min(available_units, remaining_units)
        new_units_remaining = available_units - take_units

        # Reduce remaining_amount proportionally
        if c['units_total'] > 1e-9:
            fraction_consumed = take_units / c['units_total']
        else:
            fraction_consumed = take_units / available_units if available_units > 1e-9 else 1.0
        consumed_amount = c['remaining_amount'] * (take_units / available_units) if available_units > 1e-9 else c['remaining_amount']
        new_remaining_amount = c['remaining_amount'] - consumed_amount

        # Epsilon cleanup: snap near-zero values to 0
        if new_units_remaining < 1e-9:
            new_units_remaining = 0.0
        if new_remaining_amount < 0.01:
            new_remaining_amount = 0.0

        db.execute(
            "UPDATE contributions SET units_remaining = ?, remaining_amount = ? WHERE id = ?",
            (new_units_remaining, new_remaining_amount, c['id'])
        )

        consumed.append({
            'contribution_id': c['id'],
            'units_consumed': take_units,
            'consumed_amount': consumed_amount,
            'contribution_date': c['contribution_date'],
            'original_amount': c['amount'],
            'units_remaining_after': new_units_remaining,
            'remaining_after': max(0, new_remaining_amount),
            'source_type': c['source_type'],
        })

        remaining_units -= take_units

    db.commit()
    return consumed


def record_lot_allocations(db, outflow_type, outflow_id, consumed_lots,
                           current_date_str, tax_rate_fn=None, taxable_base_fn=None,
                           tax_amount_fn=None):
    """Insert rows into lot_allocations for audit trail.
    consumed_lots: list from consume_lots_fifo.
    current_date_str: YYYY-MM-DD for computing days_held.
    tax_rate_fn, taxable_base_fn, tax_amount_fn: optional callables (lot, days_held) -> float."""
    from datetime import datetime

    current_dt = datetime.strptime(current_date_str, '%Y-%m-%d')

    for lot in consumed_lots:
        contrib_dt = datetime.strptime(lot['contribution_date'], '%Y-%m-%d')
        days_held = max(0, (current_dt - contrib_dt).days)
        months_held = days_held // 30  # approximate for backward compat

        tax_rate = tax_rate_fn(lot, days_held) if tax_rate_fn else 0.0
        taxable_base = taxable_base_fn(lot, days_held) if taxable_base_fn else 0.0
        tax_amount = tax_amount_fn(lot, days_held) if tax_amount_fn else 0.0

        db.execute(
            "INSERT INTO lot_allocations (outflow_type, outflow_id, contribution_id, "
            "consumed_amount, months_held, days_held, tax_rate, taxable_base, tax_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (outflow_type, outflow_id, lot['contribution_id'],
             lot['consumed_amount'], months_held, days_held,
             tax_rate, taxable_base, tax_amount)
        )
    db.commit()


# ---------------------------------------------------------------------------
# Withdrawals
# ---------------------------------------------------------------------------

def add_withdrawal(db, cert_id, gross_amount, tax_withheld, net_amount,
                   withdrawal_date, tax_details=None):
    cur = db.execute(
        """INSERT INTO withdrawals
           (certificate_id, gross_amount, tax_withheld, net_amount, withdrawal_date, tax_details)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (cert_id, gross_amount, tax_withheld, net_amount, withdrawal_date,
         json.dumps(tax_details) if tax_details else None)
    )
    db.commit()
    return cur.lastrowid


def list_withdrawals(db, cert_id):
    return db.execute(
        "SELECT * FROM withdrawals WHERE certificate_id = ? ORDER BY withdrawal_date",
        (cert_id,)
    ).fetchall()


# ---------------------------------------------------------------------------
# Brokerage Accounts
# ---------------------------------------------------------------------------

def get_brokerage_cash(db, user_id):
    row = db.execute(
        "SELECT cash FROM brokerage_accounts WHERE user_id = ?", (user_id,)
    ).fetchone()
    if row is None:
        db.execute(
            "INSERT INTO brokerage_accounts (user_id, cash) VALUES (?, 0.0)", (user_id,)
        )
        db.commit()
        return 0.0
    return row['cash']


def set_brokerage_cash(db, user_id, amount):
    db.execute(
        """INSERT INTO brokerage_accounts (user_id, cash) VALUES (?, ?)
           ON CONFLICT(user_id) DO UPDATE SET cash = ?""",
        (user_id, amount, amount)
    )
    db.commit()


def add_brokerage_cash(db, user_id, amount):
    current = get_brokerage_cash(db, user_id)
    set_brokerage_cash(db, user_id, current + amount)


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def create_request(db, user_id, cert_id, type_, details, created_date):
    cur = db.execute(
        """INSERT INTO requests (user_id, certificate_id, type, details, created_date)
           VALUES (?, ?, ?, ?, ?)""",
        (user_id, cert_id, type_, json.dumps(details) if details else None, created_date)
    )
    db.commit()
    return cur.lastrowid


def list_requests(db, user_id=None, status=None, cert_id=None):
    query = "SELECT * FROM requests WHERE 1=1"
    params = []
    if user_id is not None:
        query += " AND user_id = ?"
        params.append(user_id)
    if status is not None:
        query += " AND status = ?"
        params.append(status)
    if cert_id is not None:
        query += " AND certificate_id = ?"
        params.append(cert_id)
    query += " ORDER BY id DESC"
    return db.execute(query, params).fetchall()


def get_request(db, req_id):
    return db.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()


def complete_request(db, req_id, completed_date):
    db.execute(
        "UPDATE requests SET status = 'completed', completed_date = ? WHERE id = ?",
        (completed_date, req_id)
    )
    db.commit()


def fail_request(db, req_id):
    db.execute("UPDATE requests SET status = 'failed' WHERE id = ?", (req_id,))
    db.commit()


def reject_request(db, req_id, reason=None):
    db.execute(
        "UPDATE requests SET status = 'rejected', rejected_reason = ? WHERE id = ? AND status = 'pending'",
        (reason, req_id)
    )
    db.commit()


def cancel_request(db, req_id):
    db.execute(
        "UPDATE requests SET status = 'cancelled' WHERE id = ? AND status = 'pending'",
        (req_id,)
    )
    db.commit()


# ---------------------------------------------------------------------------
# Sim State
# ---------------------------------------------------------------------------

def get_sim_month(db):
    row = db.execute("SELECT value FROM sim_state WHERE key = 'current_month'").fetchone()
    return int(row['value']) if row else 0


def set_sim_month(db, month):
    db.execute(
        "INSERT OR REPLACE INTO sim_state (key, value) VALUES ('current_month', ?)",
        (str(month),)
    )
    db.commit()


def get_sim_date(db):
    row = db.execute("SELECT value FROM sim_state WHERE key = 'current_date'").fetchone()
    return row['value'] if row else '2026-01-01'


def set_sim_date(db, date_str):
    db.execute(
        "INSERT OR REPLACE INTO sim_state (key, value) VALUES ('current_date', ?)",
        (date_str,)
    )
    db.commit()


# ---------------------------------------------------------------------------
# IOF Declarations (per-user-per-year in sim_state)
# ---------------------------------------------------------------------------

def get_iof_declaration(db, user_id, year):
    """Get declared VGBL contributions at other issuers for a user in a given year."""
    key = f'iof_declared_{user_id}_{year}'
    row = db.execute("SELECT value FROM sim_state WHERE key = ?", (key,)).fetchone()
    return float(row['value']) if row else 0.0


def set_iof_declaration(db, user_id, year, amount):
    """Set declared VGBL contributions at other issuers for a user in a given year."""
    key = f'iof_declared_{user_id}_{year}'
    db.execute(
        "INSERT OR REPLACE INTO sim_state (key, value) VALUES (?, ?)",
        (key, str(amount))
    )
    db.commit()


# ---------------------------------------------------------------------------
# External Port-in Schedule
# ---------------------------------------------------------------------------

def get_external_portin_schedule(db):
    """Get the configurable schedule for splitting external port-in amounts into dated tranches.
    Returns list of dicts: [{pct, years_ago}, ...]
    Default: 30% 1yr ago, 30% 5yr ago, 40% 11yr ago."""
    row = db.execute(
        "SELECT value FROM sim_state WHERE key = 'external_portin_schedule'"
    ).fetchone()
    if row:
        return json.loads(row['value'])
    return [
        {'pct': 30, 'years_ago': 1},
        {'pct': 30, 'years_ago': 5},
        {'pct': 40, 'years_ago': 11},
    ]


def set_external_portin_schedule(db, schedule):
    """Set the external port-in schedule. schedule: list of {pct, years_ago}."""
    db.execute(
        "INSERT OR REPLACE INTO sim_state (key, value) VALUES ('external_portin_schedule', ?)",
        (json.dumps(schedule),)
    )
    db.commit()


# ---------------------------------------------------------------------------
# Certificate Units
# ---------------------------------------------------------------------------

def get_certificate_unit_price(db, cert_id):
    """Compute unit_price = total_value / unit_supply. Returns 1.0 if no units yet."""
    cert = db.execute("SELECT unit_supply FROM certificates WHERE id = ?", (cert_id,)).fetchone()
    if not cert or cert['unit_supply'] <= 1e-9:
        return 1.0
    total_value = get_certificate_total_value(db, cert_id)
    if total_value <= 0:
        return 1.0
    return total_value / cert['unit_supply']


def update_certificate_units(db, cert_id, delta):
    """Add (or subtract) from certificate's unit_supply. Clamped to >= 0."""
    db.execute(
        "UPDATE certificates SET unit_supply = max(0, unit_supply + ?) WHERE id = ?",
        (delta, cert_id)
    )
    db.commit()


def get_certificate_unit_supply(db, cert_id):
    """Read unit_supply from certificates."""
    row = db.execute("SELECT unit_supply FROM certificates WHERE id = ?", (cert_id,)).fetchone()
    return row['unit_supply'] if row else 0.0


def reconcile_certificate_units(db, cert_id):
    """Recompute unit_supply from sum of lot units_remaining.
    Returns (old_supply, new_supply). Only writes if there's a meaningful difference."""
    row = db.execute("SELECT unit_supply FROM certificates WHERE id = ?", (cert_id,)).fetchone()
    old_supply = row['unit_supply'] if row else 0.0
    lot_sum = db.execute(
        "SELECT COALESCE(SUM(units_remaining), 0) as total "
        "FROM contributions WHERE certificate_id = ?",
        (cert_id,)
    ).fetchone()['total']
    if abs(old_supply - lot_sum) > 1e-6:
        db.execute("UPDATE certificates SET unit_supply = ? WHERE id = ?",
                   (max(0, lot_sum), cert_id))
        db.commit()
    return old_supply, lot_sum


def get_vgbl_premium_remaining(db, cert_id):
    """Read vgbl_premium_remaining from certificates."""
    row = db.execute("SELECT vgbl_premium_remaining FROM certificates WHERE id = ?", (cert_id,)).fetchone()
    return row['vgbl_premium_remaining'] if row else 0.0


def update_vgbl_premium_remaining(db, cert_id, delta):
    """Add (or subtract) from certificate's vgbl_premium_remaining."""
    db.execute(
        "UPDATE certificates SET vgbl_premium_remaining = max(0, vgbl_premium_remaining + ?) WHERE id = ?",
        (delta, cert_id)
    )
    db.commit()


# ---------------------------------------------------------------------------
# IOF Config (configurable thresholds/rates)
# ---------------------------------------------------------------------------

def get_iof_config(db):
    """Get IOF configuration from sim_state. Returns list of threshold rules."""
    row = db.execute("SELECT value FROM sim_state WHERE key = 'iof_config'").fetchone()
    if row:
        return json.loads(row['value'])
    return {
        'thresholds': [
            {'year_from': 2025, 'year_to': 2025, 'limit': 300000, 'rate': 0.05},
            {'year_from': 2026, 'year_to': 9999, 'limit': 600000, 'rate': 0.05},
        ]
    }


def set_iof_config(db, config):
    """Set IOF configuration."""
    db.execute(
        "INSERT OR REPLACE INTO sim_state (key, value) VALUES ('iof_config', ?)",
        (json.dumps(config),)
    )
    db.commit()


def get_iof_limit_for_year(db, year):
    """Get the IOF exemption limit and rate for a given year."""
    config = get_iof_config(db)
    for rule in config.get('thresholds', []):
        if rule['year_from'] <= year <= rule['year_to']:
            return rule['limit'], rule['rate']
    return 600000, 0.05  # fallback default


# ---------------------------------------------------------------------------
# External Port-in Embedded Gain
# ---------------------------------------------------------------------------

def get_external_portin_gain_pct(db):
    """Get the assumed premium percentage for external port-in.
    E.g., 0.80 means 80% of transferred value is premium basis (20% embedded gain)."""
    row = db.execute(
        "SELECT value FROM sim_state WHERE key = 'external_portin_embedded_gain_pct'"
    ).fetchone()
    return float(row['value']) if row else 0.80


def set_external_portin_gain_pct(db, pct):
    """Set the external port-in embedded gain percentage."""
    db.execute(
        "INSERT OR REPLACE INTO sim_state (key, value) VALUES ('external_portin_embedded_gain_pct', ?)",
        (str(pct),)
    )
    db.commit()
