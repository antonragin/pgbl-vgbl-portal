"""FIFO-based tax calculation for PGBL/VGBL regressive and progressive regimes.

Key rules:
- PGBL: tax on total withdrawal amount (contributions were tax-deductible)
- VGBL: tax only on earnings portion (contributions were not deductible)
- Regressive (Lei 11.053): rates decrease with holding time, FIFO ordering
- Progressive: standard IRPF brackets, 15% withheld at source
- IOF 2026 (Decree 12.499/2025): 5% on VGBL contributions > R$600k/year

FIFO approach: each contribution has its own tax clock. On withdrawal, oldest
contributions are consumed first. Each contribution's remaining_amount tracks
unconsumed cost basis. Growth factor = total_value / total_remaining.
"""

import models

# Regressive tax brackets: (max_months, rate)
REGRESSIVE_BRACKETS = [
    (24, 0.35),           # <= 2 years
    (48, 0.30),           # 2-4 years
    (72, 0.25),           # 4-6 years
    (96, 0.20),           # 6-8 years
    (120, 0.15),          # 8-10 years
    (float('inf'), 0.10), # > 10 years
]

# Progressive IRPF monthly brackets (2026): (up_to, rate, deduction)
PROGRESSIVE_BRACKETS = [
    (2259.20, 0.0, 0.0),
    (2826.65, 0.075, 169.44),
    (3751.05, 0.15, 381.44),
    (4664.68, 0.225, 662.77),
    (float('inf'), 0.275, 896.00),
]

IOF_VGBL_THRESHOLD = 600_000.0  # R$ per calendar year
IOF_VGBL_RATE = 0.05            # 5%


def regressive_rate(months_held):
    """Look up regressive tax rate for a given holding period in months."""
    for max_months, rate in REGRESSIVE_BRACKETS:
        if months_held <= max_months:
            return rate
    return 0.10


def calculate_regressive_tax(contributions, withdrawal_amount, plan_type,
                             total_remaining_sum, current_total_value):
    """
    FIFO-based regressive tax calculation using remaining contributions.

    Uses growth_factor = total_value / total_remaining (remaining, not original).
    On withdrawal, cost_basis_to_consume = withdrawal / growth_factor.
    Per lot: PGBL taxable = consumed * growth_factor; VGBL taxable = consumed * growth_factor - consumed.

    Args:
        contributions: list of dicts with 'amount' (remaining_amount), 'months_held',
                       sorted oldest-first (FIFO)
        withdrawal_amount: gross withdrawal amount
        plan_type: 'PGBL' or 'VGBL'
        total_remaining_sum: sum of remaining_amount for active contributions
        current_total_value: current certificate total value
    """
    if current_total_value <= 0 or withdrawal_amount <= 0 or total_remaining_sum <= 0:
        return {'gross': 0, 'tax': 0, 'net': 0, 'effective_rate': 0, 'breakdown': []}

    growth_factor = current_total_value / total_remaining_sum
    cost_basis_to_consume = withdrawal_amount / growth_factor

    remaining = cost_basis_to_consume
    total_tax = 0.0
    breakdown = []

    for contrib in contributions:
        if remaining <= 1e-9:
            break

        available = contrib['amount']  # this is remaining_amount
        consumed = min(remaining, available)
        remaining -= consumed

        rate = regressive_rate(contrib['months_held'])

        if plan_type == 'PGBL':
            taxable = consumed * growth_factor
        else:
            # VGBL: only earnings portion is taxable
            lot_value = consumed * growth_factor
            taxable = lot_value - consumed

        tax_on_tranche = taxable * rate
        total_tax += tax_on_tranche

        breakdown.append({
            'tranche_amount': round(consumed * growth_factor, 2),
            'cost_basis': round(consumed, 2),
            'months_held': contrib['months_held'],
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
                              total_remaining_sum, current_total_value):
    """
    Progressive tax calculation (IRPF brackets).
    15% withheld at source as advance payment.
    Uses remaining-based growth factor for VGBL.
    """
    if withdrawal_amount <= 0:
        return {'gross': 0, 'taxable_base': 0, 'tax_withheld_15pct': 0,
                'net': 0, 'effective_rate': 0, 'is_estimated': True}

    if plan_type == 'PGBL':
        taxable_base = withdrawal_amount
    else:
        # VGBL: only earnings are taxable
        if current_total_value > 0 and total_remaining_sum > 0:
            earnings_ratio = max(0, 1 - total_remaining_sum / current_total_value)
            taxable_base = withdrawal_amount * earnings_ratio
        else:
            taxable_base = 0

    # 15% withholding at source
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
    }


def estimate_tax(db, certificate_id, withdrawal_amount):
    """
    Pre-withdrawal tax estimate. Reads DB state and returns preview.
    If regime not yet chosen, returns both estimates.
    Uses remaining contributions only.
    """
    cert = models.get_certificate(db, certificate_id)
    if not cert:
        return {'error': 'Certificate not found'}

    plan_type = cert['plan_type']
    total_remaining = models.total_remaining_contributions(db, certificate_id)
    total_value = models.get_certificate_total_value(db, certificate_id)

    if withdrawal_amount > total_value:
        withdrawal_amount = total_value

    # Build FIFO-ordered contribution list with ages (only those with remaining > 0)
    sim_month = models.get_sim_month(db)
    raw_contribs = models.list_contributions(db, certificate_id)

    contributions = []
    for c in raw_contribs:
        if c['remaining_amount'] <= 1e-9:
            continue
        contrib_month = _date_to_sim_month(c['contribution_date'])
        months_held = max(0, sim_month - contrib_month)
        contributions.append({
            'amount': c['remaining_amount'],
            'months_held': months_held,
            'date': c['contribution_date'],
        })

    result = {
        'certificate_id': certificate_id,
        'plan_type': plan_type,
        'total_value': round(total_value, 2),
        'total_remaining': round(total_remaining, 2),
        'withdrawal_amount': round(withdrawal_amount, 2),
        'regime': cert['tax_regime'],
    }

    if cert['tax_regime'] == 'regressive' or cert['tax_regime'] is None:
        result['regressive'] = calculate_regressive_tax(
            contributions, withdrawal_amount, plan_type, total_remaining, total_value
        )

    if cert['tax_regime'] == 'progressive' or cert['tax_regime'] is None:
        result['progressive'] = calculate_progressive_tax(
            withdrawal_amount, plan_type, total_remaining, total_value
        )

    return result


def calculate_iof_vgbl(db, user_id, contribution_amount, year=2026):
    """
    IOF 2026 rule: 5% on VGBL contributions exceeding R$600k/year.
    Includes user declaration for contributions at other issuers.
    Excludes transfers/portabilities from IOF base.
    """
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"

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

    if total_after <= IOF_VGBL_THRESHOLD:
        return 0.0

    excess_before = max(0, total_before - IOF_VGBL_THRESHOLD)
    excess_after = max(0, total_after - IOF_VGBL_THRESHOLD)
    new_excess = excess_after - excess_before

    return round(new_excess * IOF_VGBL_RATE, 2)


def _date_to_sim_month(date_str):
    """Convert YYYY-MM-DD to a sim month offset from 2026-01-01."""
    parts = date_str.split('-')
    year = int(parts[0])
    month = int(parts[1])
    return (year - 2026) * 12 + (month - 1)
