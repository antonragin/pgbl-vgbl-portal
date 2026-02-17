"""Acceptance tests for Amendments v1.

Tests FIFO lot consumption, portability-then-withdrawal, VGBL earnings taxation,
fund switch lot preservation, and IOF calculation with declaration.
"""

import os
import sys
import json
import tempfile

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from database import get_db, init_db
import models
import tax_engine
import time_engine


def _setup_db():
    """Create a fresh temp-file-backed app/db for testing."""
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    tmp.close()
    tmp_path = tmp.name

    app = create_app()
    app.config['TESTING'] = True
    # Use absolute path so os.path.join in get_db works correctly
    app.config['DATABASE'] = tmp_path
    init_db(app)

    ctx = app.app_context()
    ctx.push()
    db = get_db(app)
    return app, db, ctx, tmp_path


def _create_plan(db, plan_type='VGBL'):
    return models.create_plan(db, plan_type, f'{plan_type} Test Plan', 'Test', f'{plan_type}-TEST')


def _create_fund(db, nav=10.0, name='Test Fund'):
    fid = models.create_fund(db, name, 'Test fund', '00.000.000/0001-00', False, nav)
    # Add flat returns (0% growth) so NAV stays at 10.0 through evolve
    for idx in range(12):
        db.execute("INSERT INTO fund_returns (fund_id, month_idx, return_pct) VALUES (?, ?, 0.0)",
                   (fid, idx))
    db.commit()
    return fid


def _create_fund_with_growth(db, monthly_return=0.01, nav=10.0, name='Growth Fund'):
    fid = models.create_fund(db, name, 'Test fund', '11.111.111/0001-00', False, nav)
    for idx in range(12):
        db.execute("INSERT INTO fund_returns (fund_id, month_idx, return_pct) VALUES (?, ?, ?)",
                   (fid, idx, monthly_return))
    db.commit()
    return fid


def _setup_cert(db, user_id, plan_id, fund_id, contributions, start_date='2024-01-01'):
    """Create a certificate with target allocation and contributions, then buy units."""
    cert_id = models.create_certificate(db, user_id, plan_id, start_date)
    models.set_target_allocations(db, cert_id, [(fund_id, 100)])

    for amount, date in contributions:
        models.add_contribution(db, cert_id, amount, date)

    # Buy units for total contributed
    total = sum(a for a, _ in contributions)
    time_engine._buy_into_certificate(db, cert_id, total)
    return cert_id


# ==========================================================================
# Test 1: Two withdrawals consume lots progressively
# ==========================================================================
def test_progressive_lot_consumption():
    """Contribute R$10k (old) + R$10k (recent). Withdraw R$15k -> oldest lot
    fully consumed, younger partially. Withdraw remaining -> uses younger lot."""
    app, db, ctx, tmp_path = _setup_db()

    try:
        plan_id = _create_plan(db, 'PGBL')
        fund_id = _create_fund(db, nav=10.0)
        user_id = models.create_user(db, 'test1@test.com', False)
        models.set_brokerage_cash(db, user_id, 0)

        # Old contribution (month 0)
        models.set_sim_date(db, '2024-01-01')
        models.set_sim_month(db, 0)
        cert_id = models.create_certificate(db, user_id, plan_id, '2024-01-01')
        models.set_target_allocations(db, cert_id, [(fund_id, 100)])
        models.add_contribution(db, cert_id, 10000.0, '2024-01-01')

        # Recent contribution (month 12)
        models.add_contribution(db, cert_id, 10000.0, '2025-01-01')

        # Buy units for total
        time_engine._buy_into_certificate(db, cert_id, 20000.0)

        # Verify initial state
        total_value = models.get_certificate_total_value(db, cert_id)
        assert abs(total_value - 20000.0) < 1.0, f"Expected ~R$20000, got R${total_value}"

        total_remaining = models.total_remaining_contributions(db, cert_id)
        assert abs(total_remaining - 20000.0) < 0.01, f"Expected remaining R$20000, got R${total_remaining}"

        # Set sim_month far enough for age calculation
        models.set_sim_month(db, 24)
        models.set_sim_date(db, '2026-01-01')

        # First withdrawal: R$15k (regressive regime)
        models.set_tax_regime(db, cert_id, 'regressive')
        req_id = models.create_request(db, user_id, cert_id, 'withdrawal',
                                        {'amount': 15000, 'tax_regime': 'regressive'}, '2026-01-01')
        step_log = {'month': 24, 'events': []}
        req = db.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()
        details = json.loads(req['details'])
        time_engine._execute_withdrawal(db, req, details, '2026-01-01', step_log)

        # Check lots after first withdrawal
        contribs = models.list_contributions(db, cert_id)
        old_lot = [c for c in contribs if c['contribution_date'] == '2024-01-01'][0]
        new_lot = [c for c in contribs if c['contribution_date'] == '2025-01-01'][0]

        # cost_basis_to_consume = (15000/20000)*20000 = 15000
        # old lot (10000) fully consumed, new lot partially consumed (5000 of 10000)
        assert old_lot['remaining_amount'] < 0.01, \
            f"Old lot should be fully consumed, remaining: {old_lot['remaining_amount']}"
        assert abs(new_lot['remaining_amount'] - 5000.0) < 1.0, \
            f"New lot should have ~5000 remaining, got: {new_lot['remaining_amount']}"

        # Second withdrawal: withdraw remaining value
        total_value_after = models.get_certificate_total_value(db, cert_id)
        assert total_value_after > 0, "Should still have value left"

        req_id2 = models.create_request(db, user_id, cert_id, 'withdrawal',
                                         {'amount': total_value_after}, '2026-01-01')
        step_log2 = {'month': 24, 'events': []}
        req2 = db.execute("SELECT * FROM requests WHERE id = ?", (req_id2,)).fetchone()
        details2 = json.loads(req2['details'])
        time_engine._execute_withdrawal(db, req2, details2, '2026-01-01', step_log2)

        # After full withdrawal, all remaining should be ~0
        total_remaining_after = models.total_remaining_contributions(db, cert_id)
        assert total_remaining_after < 0.01, \
            f"All lots should be consumed, remaining: {total_remaining_after}"

        # Lot allocations should exist
        allocs = db.execute("SELECT COUNT(*) as c FROM lot_allocations").fetchone()['c']
        assert allocs > 0, "Lot allocations should be recorded"

        print("  PASS: test_progressive_lot_consumption")
    finally:
        ctx.pop()
        os.unlink(tmp_path)


# ==========================================================================
# Test 2: Portability then withdrawal
# ==========================================================================
def test_portability_then_withdrawal():
    """Contribute R$20k. Port out R$10k to another cert. Withdraw R$10k from source.
    Lots should be properly consumed across both operations."""
    app, db, ctx, tmp_path = _setup_db()

    try:
        plan_id = _create_plan(db, 'PGBL')
        fund_id = _create_fund(db, nav=10.0)
        user_id = models.create_user(db, 'test2@test.com', False)
        models.set_brokerage_cash(db, user_id, 50000)

        models.set_sim_date(db, '2024-01-01')
        models.set_sim_month(db, 0)

        # Source cert with R$20k
        src_cert = models.create_certificate(db, user_id, plan_id, '2024-01-01')
        models.set_target_allocations(db, src_cert, [(fund_id, 100)])
        models.add_contribution(db, src_cert, 20000.0, '2024-01-01')
        time_engine._buy_into_certificate(db, src_cert, 20000.0)

        # Dest cert (empty)
        dest_cert = models.create_certificate(db, user_id, plan_id, '2024-01-01')
        models.set_target_allocations(db, dest_cert, [(fund_id, 100)])

        # Advance time
        models.set_sim_month(db, 24)
        models.set_sim_date(db, '2026-01-01')

        # Internal transfer of R$10k
        req_id = models.create_request(db, user_id, src_cert, 'transfer_internal',
                                        {'amount': 10000, 'destination_cert_id': dest_cert}, '2026-01-01')
        step_log = {'month': 24, 'events': []}
        req = db.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()
        details = json.loads(req['details'])
        time_engine._execute_transfer_internal(db, req, details, '2026-01-01', step_log)

        # Source should have ~R$10k remaining in lots
        src_remaining = models.total_remaining_contributions(db, src_cert)
        assert abs(src_remaining - 10000.0) < 1.0, \
            f"Source should have ~R$10k remaining, got R${src_remaining}"

        # Dest should have new lot(s) with original date
        dest_contribs = models.list_contributions(db, dest_cert)
        assert len(dest_contribs) > 0, "Destination should have contribution(s) from transfer"
        dest_remaining = models.total_remaining_contributions(db, dest_cert)
        assert abs(dest_remaining - 10000.0) < 1.0, \
            f"Dest should have ~R$10k remaining, got R${dest_remaining}"

        # Check that transferred lots preserve original date
        for dc in dest_contribs:
            assert dc['contribution_date'] == '2024-01-01', \
                f"Transferred lot should preserve original date, got {dc['contribution_date']}"
            assert dc['source_type'] == 'transfer_internal', \
                f"Source type should be transfer_internal, got {dc['source_type']}"

        # Now withdraw R$10k from source
        models.set_tax_regime(db, src_cert, 'regressive')
        req_id2 = models.create_request(db, user_id, src_cert, 'withdrawal',
                                         {'amount': 10000}, '2026-01-01')
        step_log2 = {'month': 24, 'events': []}
        req2 = db.execute("SELECT * FROM requests WHERE id = ?", (req_id2,)).fetchone()
        details2 = json.loads(req2['details'])
        time_engine._execute_withdrawal(db, req2, details2, '2026-01-01', step_log2)

        # Source lots should be fully consumed
        src_remaining_after = models.total_remaining_contributions(db, src_cert)
        assert src_remaining_after < 0.01, \
            f"Source lots should be fully consumed, remaining: R${src_remaining_after}"

        # Certificate phase should still be accumulation (not spending!)
        src_cert_obj = models.get_certificate(db, src_cert)
        assert src_cert_obj['phase'] == 'accumulation', \
            f"Phase should stay accumulation, got {src_cert_obj['phase']}"

        print("  PASS: test_portability_then_withdrawal")
    finally:
        ctx.pop()
        os.unlink(tmp_path)


# ==========================================================================
# Test 3: VGBL zero-gain then with-gain
# ==========================================================================
def test_vgbl_earnings_taxation():
    """VGBL: contribute R$10k, immediate withdraw -> taxable ~R$0.
    Contribute, grow 10%, withdraw -> taxable = earnings on redeemed lots."""
    app, db, ctx, tmp_path = _setup_db()

    try:
        plan_id = _create_plan(db, 'VGBL')
        fund_id = _create_fund(db, nav=10.0)  # 0% growth
        user_id = models.create_user(db, 'test3@test.com', False)
        models.set_brokerage_cash(db, user_id, 100000)

        models.set_sim_date(db, '2024-01-01')
        models.set_sim_month(db, 0)

        # --- Part A: zero-gain immediate withdrawal ---
        cert_id = models.create_certificate(db, user_id, plan_id, '2024-01-01')
        models.set_target_allocations(db, cert_id, [(fund_id, 100)])
        models.set_tax_regime(db, cert_id, 'regressive')

        # Contribute and buy
        models.add_contribution(db, cert_id, 10000.0, '2024-01-01')
        time_engine._buy_into_certificate(db, cert_id, 10000.0)

        # Immediate withdraw (no growth, so growth_factor = 1.0)
        total_value = models.get_certificate_total_value(db, cert_id)
        assert abs(total_value - 10000.0) < 1.0, f"Expected ~R$10k, got R${total_value}"

        req_id = models.create_request(db, user_id, cert_id, 'withdrawal',
                                        {'amount': 10000}, '2024-01-01')
        step_log = {'month': 0, 'events': []}
        req = db.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()
        details = json.loads(req['details'])
        time_engine._execute_withdrawal(db, req, details, '2024-01-01', step_log)

        # Tax should be ~0 for VGBL with no earnings
        withdrawal = models.list_withdrawals(db, cert_id)[0]
        assert withdrawal['tax_withheld'] < 0.01, \
            f"VGBL with no earnings should have ~0 tax, got R${withdrawal['tax_withheld']}"
        assert abs(withdrawal['net_amount'] - withdrawal['gross_amount']) < 0.01, \
            "Net should equal gross when no earnings"

        # --- Part B: with growth ---
        fund_id_growth = _create_fund_with_growth(db, monthly_return=0.01, nav=10.0, name='Growth Fund')
        cert_id2 = models.create_certificate(db, user_id, plan_id, '2024-01-01')
        models.set_target_allocations(db, cert_id2, [(fund_id_growth, 100)])
        models.set_tax_regime(db, cert_id2, 'regressive')

        # Contribute
        models.add_contribution(db, cert_id2, 10000.0, '2024-01-01')
        time_engine._buy_into_certificate(db, cert_id2, 10000.0)

        # Evolve 12 months to get growth (~12.7% compound growth)
        for step in range(12):
            time_engine._update_fund_navs(db, step + 1)
        models.set_sim_month(db, 12)
        models.set_sim_date(db, '2025-01-01')

        total_value2 = models.get_certificate_total_value(db, cert_id2)
        assert total_value2 > 11000, f"Should have grown past R$11k, got R${total_value2}"

        earnings = total_value2 - 10000.0

        # Withdraw full amount
        req_id2 = models.create_request(db, user_id, cert_id2, 'withdrawal',
                                         {'amount': total_value2}, '2025-01-01')
        step_log2 = {'month': 12, 'events': []}
        req2 = db.execute("SELECT * FROM requests WHERE id = ?", (req_id2,)).fetchone()
        details2 = json.loads(req2['details'])
        time_engine._execute_withdrawal(db, req2, details2, '2025-01-01', step_log2)

        withdrawal2 = models.list_withdrawals(db, cert_id2)[0]

        # VGBL taxable = earnings only, not total amount
        # For 12 months held, regressive rate = 35%
        # Tax should be roughly 35% of earnings
        # Due to the withdrawal mechanics (growth_factor computed before sell),
        # the actual tax may differ slightly from simple earnings * rate
        assert withdrawal2['tax_withheld'] > 0, "VGBL should have some tax when there are earnings"

        # Tax should NOT be 35% of total (that would be PGBL behavior)
        pgbl_tax = total_value2 * 0.35
        assert withdrawal2['tax_withheld'] < pgbl_tax * 0.5, \
            f"VGBL tax (R${withdrawal2['tax_withheld']}) should be much less than " \
            f"PGBL-equivalent (R${pgbl_tax:.2f})"

        # Tax should be roughly proportional to earnings (with reasonable tolerance)
        # earnings/total_value should be ~ tax/(total_value * 0.35)
        effective_rate_on_total = withdrawal2['tax_withheld'] / total_value2
        earnings_ratio = earnings / total_value2
        # For VGBL, effective rate on total should be ~ earnings_ratio * 0.35
        expected_effective = earnings_ratio * 0.35
        assert abs(effective_rate_on_total - expected_effective) < 0.02, \
            f"Effective rate on total ({effective_rate_on_total:.4f}) should be near " \
            f"earnings_ratio*35% ({expected_effective:.4f})"

        print("  PASS: test_vgbl_earnings_taxation")
    finally:
        ctx.pop()
        os.unlink(tmp_path)


# ==========================================================================
# Test 4: Fund switch preserves lots
# ==========================================================================
def test_fund_switch_preserves_lots():
    """Contribute, switch funds, verify lots unchanged (same remaining_amount/dates),
    holdings in new funds."""
    app, db, ctx, tmp_path = _setup_db()

    try:
        plan_id = _create_plan(db, 'VGBL')
        fund_a = _create_fund(db, nav=10.0, name='Fund A')
        fund_b = _create_fund(db, nav=10.0, name='Fund B')
        user_id = models.create_user(db, 'test4@test.com', False)
        models.set_brokerage_cash(db, user_id, 50000)

        models.set_sim_date(db, '2024-01-01')
        models.set_sim_month(db, 0)

        cert_id = models.create_certificate(db, user_id, plan_id, '2024-01-01')
        models.set_target_allocations(db, cert_id, [(fund_a, 100)])

        # Multiple contributions at different dates
        models.add_contribution(db, cert_id, 5000.0, '2024-01-01')
        models.add_contribution(db, cert_id, 3000.0, '2024-06-01')
        models.add_contribution(db, cert_id, 2000.0, '2025-01-01')
        time_engine._buy_into_certificate(db, cert_id, 10000.0)

        # Record lot state before switch
        contribs_before = models.list_contributions(db, cert_id)
        lot_data_before = [(c['amount'], c['remaining_amount'], c['contribution_date'])
                           for c in contribs_before]

        # Verify holdings in Fund A
        holdings_before = models.get_holdings(db, cert_id)
        assert len(holdings_before) == 1
        assert holdings_before[0]['fund_id'] == fund_a
        assert holdings_before[0]['units'] > 0

        # Execute fund swap to Fund B
        models.set_target_allocations(db, cert_id, [(fund_b, 100)])
        req_id = models.create_request(db, user_id, cert_id, 'fund_swap',
                                        {'new_allocations': [{'fund_id': fund_b, 'pct': 100}]},
                                        '2025-01-15')
        models.set_sim_month(db, 12)
        models.set_sim_date(db, '2025-01-15')

        step_log = {'month': 12, 'events': []}
        req = db.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()
        details = json.loads(req['details'])
        time_engine._execute_fund_swap(db, req, details, '2025-01-15', step_log)

        # Verify lots unchanged
        contribs_after = models.list_contributions(db, cert_id)
        lot_data_after = [(c['amount'], c['remaining_amount'], c['contribution_date'])
                          for c in contribs_after]

        assert lot_data_before == lot_data_after, \
            f"Lots should be unchanged after fund switch.\nBefore: {lot_data_before}\nAfter: {lot_data_after}"

        # Verify holdings now in Fund B
        holdings_after = models.get_holdings(db, cert_id)
        fund_b_holdings = [h for h in holdings_after if h['fund_id'] == fund_b and h['units'] > 1e-9]
        fund_a_holdings = [h for h in holdings_after if h['fund_id'] == fund_a and h['units'] > 1e-9]

        assert len(fund_b_holdings) > 0, "Should have holdings in Fund B after switch"
        assert len(fund_a_holdings) == 0, "Should have no holdings in Fund A after switch"

        # Total value should be preserved
        total_value_after = models.get_certificate_total_value(db, cert_id)
        assert abs(total_value_after - 10000.0) < 1.0, \
            f"Total value should be preserved after switch, got R${total_value_after}"

        print("  PASS: test_fund_switch_preserves_lots")
    finally:
        ctx.pop()
        os.unlink(tmp_path)


# ==========================================================================
# Test 5: IOF with declaration
# ==========================================================================
def test_iof_with_declaration():
    """Set declaration to R$550k. Contribute R$100k VGBL.
    R$50k exempt (fills up to R$600k), R$50k excess -> R$2500 IOF.
    Net invested = R$97,500."""
    app, db, ctx, tmp_path = _setup_db()

    try:
        plan_id = _create_plan(db, 'VGBL')
        fund_id = _create_fund(db, nav=10.0)
        user_id = models.create_user(db, 'test5@test.com', False)
        models.set_brokerage_cash(db, user_id, 200000)

        models.set_sim_date(db, '2026-01-01')
        models.set_sim_month(db, 0)

        cert_id = models.create_certificate(db, user_id, plan_id, '2026-01-01')
        models.set_target_allocations(db, cert_id, [(fund_id, 100)])

        # Declare R$550k at other issuers
        models.set_iof_declaration(db, user_id, 2026, 550000.0)

        # Calculate IOF for R$100k contribution
        iof = tax_engine.calculate_iof_vgbl(db, user_id, 100000.0, 2026)

        # Total before: 0 (internal) + 550k (declared) = 550k
        # Total after: 550k + 100k = 650k
        # Excess before: max(0, 550k - 600k) = 0
        # Excess after: max(0, 650k - 600k) = 50k
        # IOF = 50k * 5% = 2500
        assert abs(iof - 2500.0) < 0.01, f"IOF should be R$2500, got R${iof}"

        # Now execute the contribution through the engine
        req_id = models.create_request(db, user_id, cert_id, 'contribution',
                                        {'amount': 100000}, '2026-01-01')
        step_log = {'month': 0, 'events': []}
        req = db.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()
        details = json.loads(req['details'])
        time_engine._execute_contribution(db, req, details, '2026-01-01', step_log)

        # Check that contribution was recorded at net amount (100k - 2500 = 97500)
        contribs = models.list_contributions(db, cert_id)
        assert len(contribs) == 1
        assert abs(contribs[0]['amount'] - 97500.0) < 0.01, \
            f"Contribution should be recorded at R$97500 (after IOF), got R${contribs[0]['amount']}"

        # Check that remaining_amount also defaults to the net amount
        assert abs(contribs[0]['remaining_amount'] - 97500.0) < 0.01, \
            f"remaining_amount should be R$97500, got R${contribs[0]['remaining_amount']}"

        # Check that units were bought for the net amount
        total_value = models.get_certificate_total_value(db, cert_id)
        assert abs(total_value - 97500.0) < 1.0, \
            f"Total value should be ~R$97500, got R${total_value}"

        # Brokerage should have been debited the full R$100k (not R$97.5k)
        brokerage = models.get_brokerage_cash(db, user_id)
        assert abs(brokerage - 100000.0) < 0.01, \
            f"Brokerage should be R$100k (200k - 100k), got R${brokerage}"

        # Verify IOF log message
        assert any('IOF' in e for e in step_log['events']), \
            f"Should have IOF in event log: {step_log['events']}"

        print("  PASS: test_iof_with_declaration")
    finally:
        ctx.pop()
        os.unlink(tmp_path)


# ==========================================================================
# Runner
# ==========================================================================
def run_all():
    print("Running Amendment v1 acceptance tests...\n")
    tests = [
        test_progressive_lot_consumption,
        test_portability_then_withdrawal,
        test_vgbl_earnings_taxation,
        test_fund_switch_preserves_lots,
        test_iof_with_declaration,
    ]
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {test.__name__}: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    if failed == 0:
        print("All tests passed!")
    return failed == 0


if __name__ == '__main__':
    success = run_all()
    sys.exit(0 if success else 1)
