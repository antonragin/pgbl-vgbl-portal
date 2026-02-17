"""FIFO-based tax calculation for PGBL/VGBL regressive and progressive regimes.

Key rules:
- PGBL: tax on total withdrawal amount (contributions were tax-deductible)
- VGBL: tax only on earnings portion (contributions were not deductible)
- Regressive (Lei 11.053): rates decrease with holding time, FIFO ordering
- Progressive: standard IRPF brackets, 15% withheld at source (IRRF antecipação)
- IOF (Decree 12.499/2025): configurable thresholds per year

Certificate units approach:
- unit_price = total_value / unit_supply
- Per-lot value = lot.units_remaining * unit_price (captures actual growth per lot)
- VGBL earnings_ratio = 1 - (premium_remaining / total_value)
- Regressive brackets use calendar-year deltas via dateutil.relativedelta
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import models

# Regressive tax brackets: (years, rate)
# Legal boundaries: 2, 4, 6, 8, 10 years — computed as calendar-year deltas
REGRESSIVE_YEAR_BRACKETS = [
    (2, 0.35),    # <= 2 years
    (4, 0.30),    # 2-4 years
    (6, 0.25),    # 4-6 years
    (8, 0.20),    # 6-8 years
    (10, 0.15),   # 8-10 years
]

# Progressive IRPF monthly brackets (2026): (up_to, rate, deduction)
PROGRESSIVE_BRACKETS = [
    (2259.20, 0.0, 0.0),
    (2826.65, 0.075, 169.44),
    (3751.05, 0.15, 381.44),
    (4664.68, 0.225, 662.77),
    (float('inf'), 0.275, 896.00),
]


def regressive_rate(contribution_date, current_date):
    """Calendar-year based regressive rate lookup.

    Uses dateutil.relativedelta for exact calendar-year boundaries,
    which correctly handles leap years (e.g. 2024-02-29 + 2yr = 2026-02-28).

    Args:
        contribution_date: 'YYYY-MM-DD' string or datetime object
        current_date: 'YYYY-MM-DD' string or datetime object
    """
    if isinstance(contribution_date, str):
        contribution_date = datetime.strptime(contribution_date, '%Y-%m-%d')
    if isinstance(current_date, str):
        current_date = datetime.strptime(current_date, '%Y-%m-%d')
    for years, rate in REGRESSIVE_YEAR_BRACKETS:
        boundary = contribution_date + relativedelta(years=years)
        if current_date < boundary:
            return rate
    return 0.10


def next_bracket_drop(contribution_date, current_date):
    """Compute next regressive bracket transition using calendar-year boundaries.

    Returns dict with {rate, days_until, months_until} or None if already at 10%.
    """
    if isinstance(contribution_date, str):
        contribution_date = datetime.strptime(contribution_date, '%Y-%m-%d')
    if isinstance(current_date, str):
        current_date = datetime.strptime(current_date, '%Y-%m-%d')
    for i, (years, rate) in enumerate(REGRESSIVE_YEAR_BRACKETS):
        boundary = contribution_date + relativedelta(years=years)
        if current_date < boundary:
            days_until = (boundary - current_date).days
            if i + 1 < len(REGRESSIVE_YEAR_BRACKETS):
                next_rate = REGRESSIVE_YEAR_BRACKETS[i + 1][1]
            else:
                next_rate = 0.10
            return {
                'rate': next_rate,
                'days_until': days_until,
                'months_until': max(1, days_until // 30),
            }
    return None  # Already at minimum 10%


def calculate_regressive_tax(contributions, withdrawal_amount, plan_type,
                             current_total_value, unit_price=None,
                             vgbl_premium_remaining=0.0, current_date=None):
    """
    FIFO-based regressive tax calculation using certificate units.

    Args:
        contributions: list of dicts with 'units_remaining', 'contribution_date'
                       sorted oldest-first (FIFO). May also include 'days_held'.
        withdrawal_amount: gross withdrawal amount
        plan_type: 'PGBL' or 'VGBL'
        current_total_value: current certificate total value
        unit_price: certificate unit price (total_value / unit_supply)
        vgbl_premium_remaining: certificate-level P_rem for VGBL
        current_date: 'YYYY-MM-DD' string for calendar-year bracket lookups
    """
    if current_total_value <= 0 or withdrawal_amount <= 0:
        return {'gross': 0, 'tax': 0, 'net': 0, 'effective_rate': 0, 'breakdown': []}

    if unit_price is None or unit_price <= 0:
        unit_price = 1.0

    units_to_consume = withdrawal_amount / unit_price

    # VGBL earnings ratio (certificate-level)
    if plan_type == 'VGBL' and current_total_value > 0:
        earnings_ratio = max(0, 1 - (vgbl_premium_remaining / current_total_value))
        taxable_total = withdrawal_amount * earnings_ratio
    else:
        earnings_ratio = 0
        taxable_total = 0

    remaining_units = units_to_consume
    total_tax = 0.0
    breakdown = []

    for contrib in contributions:
        if remaining_units <= 1e-9:
            break

        available = contrib.get('units_remaining', 0)
        if available <= 1e-9:
            continue
        consumed_units = min(remaining_units, available)
        remaining_units -= consumed_units

        lot_gross_value = consumed_units * unit_price
        contrib_date = contrib.get('contribution_date') or contrib.get('date', '')
        rate = regressive_rate(contrib_date, current_date) if current_date else regressive_rate(contrib_date, datetime.now().strftime('%Y-%m-%d'))
        days_held = contrib.get('days_held', 0)

        if plan_type == 'PGBL':
            taxable = lot_gross_value
        else:
            # VGBL: proportional share of taxable_total
            taxable = (lot_gross_value / withdrawal_amount) * taxable_total if withdrawal_amount > 0 else 0

        tax_on_tranche = taxable * rate
        total_tax += tax_on_tranche

        breakdown.append({
            'tranche_amount': round(lot_gross_value, 2),
            'units_consumed': round(consumed_units, 6),
            'days_held': days_held,
            'contribution_date': contrib_date,
            'rate': rate,
            'taxable': round(taxable, 2),
            'tax': round(tax_on_tranche, 2),
        })

    net = withdrawal_amount - total_tax
    effective_rate = total_tax / withdrawal_amount if withdrawal_amount > 0 else 0

    return {
        'gross': round(withdrawal_amount, 2),
        'tax': round(total_tax, 2),
        'net': round(net, 2),
        'effective_rate': round(effective_rate, 4),
        'breakdown': breakdown,
    }


def calculate_progressive_tax(withdrawal_amount, plan_type,
                              current_total_value, vgbl_premium_remaining=0.0):
    """
    Progressive tax calculation — IRRF antecipação.
    15% withheld at source as advance payment against annual IRPF.
    Uses certificate-level earnings_ratio for VGBL.
    """
    if withdrawal_amount <= 0:
        return {'gross': 0, 'taxable_base': 0, 'tax_withheld_15pct': 0,
                'net': 0, 'effective_rate': 0, 'is_estimated': True,
                'label': 'IRRF antecipação'}

    if plan_type == 'PGBL':
        taxable_base = withdrawal_amount
    else:
        # VGBL: only earnings are taxable
        if current_total_value > 0:
            earnings_ratio = max(0, 1 - (vgbl_premium_remaining / current_total_value))
            taxable_base = withdrawal_amount * earnings_ratio
        else:
            taxable_base = 0

    # 15% withholding at source (IRRF antecipação)
    tax_withheld = taxable_base * 0.15

    # Estimated final tax using progressive brackets (monthly basis)
    estimated_tax = 0
    for up_to, rate, deduction in PROGRESSIVE_BRACKETS:
        if taxable_base <= up_to:
            estimated_tax = taxable_base * rate - deduction
            break
    estimated_tax = max(0, estimated_tax)

    net = withdrawal_amount - tax_withheld
    effective_rate = tax_withheld / withdrawal_amount if withdrawal_amount > 0 else 0

    return {
        'gross': round(withdrawal_amount, 2),
        'taxable_base': round(taxable_base, 2),
        'tax_withheld_15pct': round(tax_withheld, 2),
        'estimated_final_tax': round(estimated_tax, 2),
        'net': round(net, 2),
        'effective_rate': round(effective_rate, 4),
        'is_estimated': True,
        'label': 'IRRF antecipação',
        'note': 'Final tax depends on annual income; this is illustrative only.',
    }


def estimate_tax(db, certificate_id, withdrawal_amount):
    """
    Pre-withdrawal tax estimate using certificate units.
    If regime not yet chosen, returns both estimates.
    """
    cert = models.get_certificate(db, certificate_id)
    if not cert:
        return {'error': 'Certificate not found'}

    plan_type = cert['plan_type']
    total_value = models.get_certificate_total_value(db, certificate_id)
    unit_price = models.get_certificate_unit_price(db, certificate_id)
    P_rem = models.get_vgbl_premium_remaining(db, certificate_id)

    if withdrawal_amount > total_value:
        withdrawal_amount = total_value

    # Build FIFO-ordered contribution list with days-based ages
    sim_date = models.get_sim_date(db)
    current_dt = datetime.strptime(sim_date, '%Y-%m-%d')
    raw_contribs = models.list_contributions(db, certificate_id)

    contributions = []
    for c in raw_contribs:
        if c['units_remaining'] <= 1e-9:
            continue
        contrib_dt = datetime.strptime(c['contribution_date'], '%Y-%m-%d')
        days_held = max(0, (current_dt - contrib_dt).days)
        contributions.append({
            'units_remaining': c['units_remaining'],
            'remaining_amount': c['remaining_amount'],
            'days_held': days_held,
            'contribution_date': c['contribution_date'],
        })

    result = {
        'certificate_id': certificate_id,
        'plan_type': plan_type,
        'total_value': round(total_value, 2),
        'unit_price': round(unit_price, 6),
        'withdrawal_amount': round(withdrawal_amount, 2),
        'regime': cert['tax_regime'],
    }

    if cert['tax_regime'] == 'regressive' or cert['tax_regime'] is None:
        result['regressive'] = calculate_regressive_tax(
            contributions, withdrawal_amount, plan_type,
            total_value, unit_price, P_rem, current_date=sim_date
        )

    if cert['tax_regime'] == 'progressive' or cert['tax_regime'] is None:
        result['progressive'] = calculate_progressive_tax(
            withdrawal_amount, plan_type, total_value, P_rem
        )

    return result


def calculate_iof_vgbl(db, user_id, contribution_amount, year=2026):
    """
    IOF on VGBL contributions exceeding configurable annual threshold.
    Uses configurable limits/rates from sim_state.
    Includes user declaration for contributions at other issuers.
    Excludes transfers/portabilities from IOF base.
    """
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"

    # Get configurable IOF limit and rate for this year
    iof_limit, iof_rate = models.get_iof_limit_for_year(db, year)

    # Only count source_type='contribution' (exclude transfers/portabilities)
    existing = db.execute("""
        SELECT COALESCE(SUM(co.amount), 0) as total
        FROM contributions co
        JOIN certificates ce ON co.certificate_id = ce.id
        JOIN plans p ON ce.plan_id = p.id
        WHERE ce.user_id = ? AND p.type = 'VGBL'
        AND co.contribution_date BETWEEN ? AND ?
        AND co.source_type = 'contribution'
    """, (user_id, year_start, year_end)).fetchone()['total']

    # Add user declaration for other issuers
    declared = models.get_iof_declaration(db, user_id, year)

    total_before = existing + declared
    total_after = total_before + contribution_amount

    if total_after <= iof_limit:
        return 0.0

    excess_before = max(0, total_before - iof_limit)
    excess_after = max(0, total_after - iof_limit)
    new_excess = excess_after - excess_before

    return round(new_excess * iof_rate, 2)


def _date_to_sim_month(date_str):
    """Convert YYYY-MM-DD to a sim month offset from 2026-01-01."""
    parts = date_str.split('-')
    year = int(parts[0])
    month = int(parts[1])
    return (year - 2026) * 12 + (month - 1)


def days_between(date_str1, date_str2):
    """Compute exact days between two YYYY-MM-DD date strings."""
    dt1 = datetime.strptime(date_str1, '%Y-%m-%d')
    dt2 = datetime.strptime(date_str2, '%Y-%m-%d')
    return abs((dt2 - dt1).days)
