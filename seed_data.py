"""Populate the database with realistic sample data for demonstration."""

import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from database import get_db
import models


# Monthly return series (annualized ~10% for RF, ~15% for multi, ~20% for equities)
RF_RETURNS = [0.008, 0.007, 0.009, 0.008, 0.007, 0.008,
              0.009, 0.008, 0.007, 0.008, 0.009, 0.008]  # ~10% p.a.

MULTI_RETURNS = [0.012, 0.010, -0.005, 0.015, 0.011, 0.008,
                 0.014, -0.003, 0.013, 0.010, 0.016, 0.009]  # ~13% p.a.

EQUITY_RETURNS = [0.020, -0.015, 0.025, 0.010, -0.010, 0.030,
                  0.015, -0.020, 0.035, 0.005, 0.018, -0.008]  # ~11% p.a.

INFLATION_RETURNS = [0.006, 0.005, 0.007, 0.006, 0.005, 0.006,
                     0.007, 0.006, 0.005, 0.006, 0.007, 0.006]  # ~7.5% p.a.

GLOBAL_RETURNS = [0.015, -0.010, 0.020, 0.008, 0.012, -0.005,
                  0.018, 0.010, -0.008, 0.022, 0.005, 0.014]  # ~12% p.a.


def seed():
    app = create_app()
    with app.app_context():
        db = get_db(app)

        # Check if already seeded
        if db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c'] > 0:
            print("Database already has data. Delete data/portal.db to re-seed.")
            return

        print("Seeding database...")

        # === Plans ===
        p1 = models.create_plan(db, 'PGBL', 'PGBL Renda Fixa Conservador',
                                'Admin fee: 1.0% p.a., Loading: 0%, Carencia: 60 days',
                                'PGBL-RF-001')
        p2 = models.create_plan(db, 'PGBL', 'PGBL Multimercado Moderado',
                                'Admin fee: 1.5% p.a., Loading: 0%, Carencia: 60 days',
                                'PGBL-MM-002')
        p3 = models.create_plan(db, 'VGBL', 'VGBL Renda Fixa Plus',
                                'Admin fee: 0.8% p.a., Loading: 0%, Carencia: 60 days',
                                'VGBL-RF-001')
        p4 = models.create_plan(db, 'VGBL', 'VGBL Acoes Crescimento',
                                'Admin fee: 2.0% p.a., Loading: 0%, Carencia: 90 days. '
                                'Qualified investors only.',
                                'VGBL-EQ-002')
        print(f"  Created {4} plans")

        # === Funds ===
        f1 = models.create_fund(db, 'FIE Renda Fixa DI',
                                'Government bonds and bank CDs. Low risk, CDI-linked returns.',
                                '12.345.678/0001-01', False, 10.0)
        _store_returns(db, f1, RF_RETURNS)

        f2 = models.create_fund(db, 'FIE Multimercado Balanceado',
                                'Diversified: 40% fixed income, 30% equities, 20% FX, 10% alternatives.',
                                '23.456.789/0001-02', False, 10.0)
        _store_returns(db, f2, MULTI_RETURNS)

        f3 = models.create_fund(db, 'FIE Acoes Ibovespa',
                                'Tracks Ibovespa index. High risk, equity exposure.',
                                '34.567.890/0001-03', False, 10.0)
        _store_returns(db, f3, EQUITY_RETURNS)

        f4 = models.create_fund(db, 'FIE Inflacao IPCA+',
                                'IPCA-linked government bonds (NTN-B). Inflation protection.',
                                '45.678.901/0001-04', False, 10.0)
        _store_returns(db, f4, INFLATION_RETURNS)

        f5 = models.create_fund(db, 'FIE Global Equity',
                                'International equity exposure via BDRs. Qualified investors.',
                                '56.789.012/0001-05', True, 10.0)
        _store_returns(db, f5, GLOBAL_RETURNS)
        print(f"  Created {5} funds with return series")

        # === Users ===
        # User 1: New investor (mostly empty)
        u1 = models.create_user(db, 'ana.silva@email.com', True)

        # User 2: Veteran investor with history
        u2 = models.create_user(db, 'carlos.souza@email.com', True)

        # User 3: Qualified investor
        u3 = models.create_user(db, 'maria.santos@empresa.com', False)

        print(f"  Created {3} users")

        # === User 1 (Ana): new, only brokerage cash ===
        models.set_brokerage_cash(db, u1, 25000.0)
        print(f"  Ana: R$25,000 brokerage cash")

        # === User 2 (Carlos): veteran with PGBL and VGBL certificates ===
        models.set_brokerage_cash(db, u2, 15000.0)

        # PGBL certificate - 24 months of contributions
        c1 = models.create_certificate(db, u2, p1, '2024-01-01')
        for m in range(24):
            year = 2024 + (m // 12)
            month = 1 + (m % 12)
            date = f"{year}-{month:02d}-01"
            models.add_contribution(db, c1, 2000.0, date,
                                    units_total=2000.0, units_remaining=2000.0,
                                    issue_unit_price=1.0)
        # Set holdings (24 months of R$2000 = R$48000, grown by ~20% to ~57600)
        # At NAV 10.0, ~5760 units
        models.set_holding(db, c1, f1, 3000)  # RF DI: 60%
        models.set_holding(db, c1, f4, 2000)  # IPCA: 40%
        models.set_target_allocations(db, c1, [(f1, 60), (f4, 40)])
        _init_certificate_units(db, c1, 'PGBL')
        print(f"  Carlos PGBL cert #{c1}: 24 months of R$2k contributions, RF/IPCA allocation")

        # VGBL certificate - 12 months of contributions
        c2 = models.create_certificate(db, u2, p3, '2025-01-01')
        for m in range(12):
            month = 1 + m
            date = f"2025-{month:02d}-01"
            models.add_contribution(db, c2, 3000.0, date,
                                    units_total=3000.0, units_remaining=3000.0,
                                    issue_unit_price=1.0)
        models.set_holding(db, c2, f1, 1500)  # RF DI: 40%
        models.set_holding(db, c2, f2, 1200)  # Multi: 30%
        models.set_holding(db, c2, f3, 800)   # Equities: 20%
        models.set_holding(db, c2, f4, 400)   # IPCA: 10%
        models.set_target_allocations(db, c2, [(f1, 40), (f2, 30), (f3, 20), (f4, 10)])
        _init_certificate_units(db, c2, 'VGBL')
        print(f"  Carlos VGBL cert #{c2}: 12 months of R$3k contributions, diversified")

        # === User 3 (Maria): qualified investor with large VGBL ===
        models.set_brokerage_cash(db, u3, 100000.0)

        c3 = models.create_certificate(db, u3, p4, '2023-06-01')
        # Large single contribution
        models.add_contribution(db, c3, 500000.0, '2023-06-01',
                                units_total=500000.0, units_remaining=500000.0,
                                issue_unit_price=1.0)
        # Additional contributions
        models.add_contribution(db, c3, 100000.0, '2024-01-01',
                                units_total=100000.0, units_remaining=100000.0,
                                issue_unit_price=1.0)
        models.add_contribution(db, c3, 50000.0, '2024-06-01',
                                units_total=50000.0, units_remaining=50000.0,
                                issue_unit_price=1.0)
        models.set_holding(db, c3, f3, 30000)  # Heavy equities
        models.set_holding(db, c3, f5, 20000)  # Global equity
        models.set_holding(db, c3, f2, 10000)  # Multi
        models.set_target_allocations(db, c3, [(f3, 50), (f5, 33), (f2, 17)])
        models.set_tax_regime(db, c3, 'regressive')

        # Maria: sample external transfer-in contribution
        # External port-in: embedded_gain_pct = 0.80, so P_rem for this = 75000 * 0.80 = 60000
        # remaining_amount = premium basis = 75000 * 0.80 = 60000
        models.add_contribution(db, c3, 75000.0, '2025-06-01',
                                source_type='transfer_external',
                                remaining_amount=75000.0 * 0.80,
                                units_total=75000.0, units_remaining=75000.0,
                                issue_unit_price=1.0)
        _init_certificate_units(db, c3, 'VGBL', external_transfer_amounts=[75000.0])
        print(f"  Maria VGBL cert #{c3}: R$725k total, equities-heavy, regressive regime")

        # Maria: second VGBL certificate (for internal transfers demo)
        c4 = models.create_certificate(db, u3, p3, '2024-01-01')
        models.add_contribution(db, c4, 50000.0, '2024-01-01',
                                units_total=50000.0, units_remaining=50000.0,
                                issue_unit_price=1.0)
        models.set_holding(db, c4, f1, 3000)
        models.set_holding(db, c4, f4, 2000)
        models.set_target_allocations(db, c4, [(f1, 60), (f4, 40)])
        _init_certificate_units(db, c4, 'VGBL')
        print(f"  Maria VGBL cert #{c4}: R$50k, RF/IPCA allocation (for transfer demo)")

        # Maria: IOF declaration — she has R$200k VGBL contributions at another issuer
        models.set_iof_declaration(db, u3, 2025, 200000.0)
        print(f"  Maria: IOF declaration set to R$200,000 for 2025")

        # Seed default external port-in schedule
        models.set_external_portin_schedule(db, [
            {'pct': 30, 'years_ago': 1},
            {'pct': 30, 'years_ago': 5},
            {'pct': 40, 'years_ago': 11},
        ])
        print(f"  Default external port-in schedule seeded (30%/1yr, 30%/5yr, 40%/11yr)")

        # Seed IOF config (configurable thresholds per year)
        models.set_iof_config(db, {
            'thresholds': [
                {'year_from': 2025, 'year_to': 2025, 'limit': 300000, 'rate': 0.05},
                {'year_from': 2026, 'year_to': 9999, 'limit': 600000, 'rate': 0.05},
            ]
        })
        print(f"  IOF config seeded (2025: R$300k, 2026+: R$600k, 5% rate)")

        # Seed embedded gain percentage for external port-in
        models.set_external_portin_gain_pct(db, 0.80)
        print(f"  External port-in embedded gain pct: 80%")

        # === Sample Requests ===
        # Completed requests
        rid = models.create_request(db, u2, c1, 'contribution',
                                    {'amount': 2000}, '2025-12-01')
        models.complete_request(db, rid, '2025-12-01')
        rid = models.create_request(db, u2, c2, 'fund_swap',
                                    {'new_allocations': [
                                        {'fund_id': f1, 'pct': 40},
                                        {'fund_id': f2, 'pct': 30},
                                        {'fund_id': f3, 'pct': 20},
                                        {'fund_id': f4, 'pct': 10},
                                    ]}, '2025-10-01')
        models.complete_request(db, rid, '2025-10-15')

        # Sample rejected request
        rid = models.create_request(db, u3, c3, 'withdrawal',
                                    {'amount': 200000, 'tax_regime': 'regressive'},
                                    '2025-11-01')
        models.reject_request(db, rid, 'Withdrawal amount exceeds policy limit for this period')

        # Sample cancelled request
        rid = models.create_request(db, u2, c1, 'contribution',
                                    {'amount': 5000}, '2025-12-15')
        models.cancel_request(db, rid)

        print(f"\nSeeding complete!")
        print(f"  Users: 3 (ana, carlos, maria)")
        print(f"  Plans: 4 (2 PGBL, 2 VGBL)")
        print(f"  Funds: 5 (RF DI, Multi, Equities, IPCA, Global)")
        print(f"  Certificates: 4 (1 Carlos PGBL, 1 Carlos VGBL, 2 Maria VGBL)")
        print(f"  Sample requests: completed, rejected, cancelled")
        print(f"  Login password for all users: 1234")

        db.close()


def _init_certificate_units(db, cert_id, plan_type, external_transfer_amounts=None):
    """Initialize unit_supply and vgbl_premium_remaining for a seeded certificate.

    At seed time, unit_price=1.0, so unit_supply = sum of all contribution amounts.
    For VGBL: P_rem = sum of regular contributions + (external_transfers * embedded_gain_pct).
    """
    contribs = models.list_contributions(db, cert_id)
    total_units = sum(c['units_total'] for c in contribs)

    # Set certificate unit_supply
    db.execute("UPDATE certificates SET unit_supply = ? WHERE id = ?",
               (total_units, cert_id))

    # For VGBL, compute premium_remaining
    if plan_type == 'VGBL':
        # Regular contributions: full amount is premium
        regular_premium = sum(c['amount'] for c in contribs
                              if c['source_type'] == 'contribution')
        # External transfers: use embedded_gain_pct
        embedded_gain_pct = models.get_external_portin_gain_pct(db)
        ext_premium = 0.0
        if external_transfer_amounts:
            ext_premium = sum(a * embedded_gain_pct for a in external_transfer_amounts)
        total_prem = regular_premium + ext_premium
        db.execute("UPDATE certificates SET vgbl_premium_remaining = ? WHERE id = ?",
                   (total_prem, cert_id))

    db.commit()


def _store_returns(db, fund_id, returns):
    """Store a list of monthly returns for a fund."""
    for idx, ret in enumerate(returns):
        db.execute(
            "INSERT INTO fund_returns (fund_id, month_idx, return_pct) VALUES (?, ?, ?)",
            (fund_id, idx, ret)
        )
    db.commit()


if __name__ == '__main__':
    seed()
