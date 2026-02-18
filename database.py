"""SQLite schema and connection helpers."""

import sqlite3
import os

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    username    TEXT UNIQUE NOT NULL,
    is_retail   INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS plans (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    type        TEXT NOT NULL CHECK(type IN ('PGBL', 'VGBL')),
    name        TEXT NOT NULL,
    fees_info   TEXT,
    plan_code   TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS funds (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    description         TEXT,
    cnpj                TEXT,
    is_qualified_only   INTEGER NOT NULL DEFAULT 0,
    initial_nav         REAL NOT NULL DEFAULT 1.0,
    current_nav         REAL NOT NULL DEFAULT 1.0,
    returns_csv         TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS fund_returns (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_id     INTEGER NOT NULL REFERENCES funds(id) ON DELETE CASCADE,
    month_idx   INTEGER NOT NULL,
    return_pct  REAL NOT NULL,
    UNIQUE(fund_id, month_idx)
);

CREATE TABLE IF NOT EXISTS certificates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan_id         INTEGER NOT NULL REFERENCES plans(id),
    created_date    TEXT NOT NULL,
    phase           TEXT NOT NULL DEFAULT 'accumulation'
                    CHECK(phase IN ('accumulation', 'spending')),
    tax_regime      TEXT DEFAULT NULL
                    CHECK(tax_regime IN ('progressive', 'regressive', NULL)),
    unit_supply             REAL NOT NULL DEFAULT 0.0,
    vgbl_premium_remaining  REAL NOT NULL DEFAULT 0.0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS contributions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    certificate_id      INTEGER NOT NULL REFERENCES certificates(id) ON DELETE CASCADE,
    amount              REAL NOT NULL,
    gross_amount        REAL,
    remaining_amount    REAL NOT NULL,
    contribution_date   TEXT NOT NULL,
    source_type         TEXT NOT NULL DEFAULT 'contribution'
                        CHECK(source_type IN ('contribution', 'transfer_internal', 'transfer_external')),
    units_total         REAL NOT NULL DEFAULT 0.0,
    units_remaining     REAL NOT NULL DEFAULT 0.0,
    issue_unit_price    REAL NOT NULL DEFAULT 0.0,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS withdrawals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    certificate_id      INTEGER NOT NULL REFERENCES certificates(id) ON DELETE CASCADE,
    gross_amount        REAL NOT NULL,
    tax_withheld        REAL NOT NULL DEFAULT 0.0,
    net_amount          REAL NOT NULL,
    withdrawal_date     TEXT NOT NULL,
    tax_details         TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS holdings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    certificate_id      INTEGER NOT NULL REFERENCES certificates(id) ON DELETE CASCADE,
    fund_id             INTEGER NOT NULL REFERENCES funds(id),
    units               REAL NOT NULL DEFAULT 0.0,
    UNIQUE(certificate_id, fund_id)
);

CREATE TABLE IF NOT EXISTS target_allocations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    certificate_id      INTEGER NOT NULL REFERENCES certificates(id) ON DELETE CASCADE,
    fund_id             INTEGER NOT NULL REFERENCES funds(id),
    pct                 REAL NOT NULL,
    UNIQUE(certificate_id, fund_id)
);

CREATE TABLE IF NOT EXISTS brokerage_accounts (
    user_id     INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    cash        REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS requests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    certificate_id  INTEGER,
    type            TEXT NOT NULL CHECK(type IN (
                        'fund_swap', 'withdrawal', 'contribution',
                        'portability_out', 'portability_in',
                        'brokerage_withdrawal',
                        'transfer_internal', 'transfer_external_out',
                        'transfer_external_in'
                    )),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'completed', 'failed',
                                     'rejected', 'cancelled')),
    details         TEXT,
    rejected_reason TEXT,
    created_date    TEXT NOT NULL,
    completed_date  TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lot_allocations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    outflow_type    TEXT NOT NULL,
    outflow_id      INTEGER NOT NULL,
    contribution_id INTEGER NOT NULL REFERENCES contributions(id),
    consumed_amount REAL NOT NULL,
    months_held     INTEGER NOT NULL DEFAULT 0,
    days_held       INTEGER NOT NULL DEFAULT 0,
    tax_rate        REAL NOT NULL DEFAULT 0.0,
    taxable_base    REAL NOT NULL DEFAULT 0.0,
    tax_amount      REAL NOT NULL DEFAULT 0.0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sim_state (
    key     TEXT PRIMARY KEY,
    value   TEXT
);
"""

INITIAL_STATE = [
    ('current_month', '0'),
    ('current_date', '2026-01-01'),
]


def get_db(app):
    """Get a database connection with row factory."""
    db_path = os.path.join(app.root_path, app.config['DATABASE'])
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(app):
    """Create tables and seed initial sim state if needed."""
    db_path = os.path.join(app.root_path, app.config['DATABASE'])
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    for key, value in INITIAL_STATE:
        conn.execute(
            "INSERT OR IGNORE INTO sim_state (key, value) VALUES (?, ?)",
            (key, value)
        )
    conn.commit()
    upgrade_schema(conn)
    conn.close()


def upgrade_schema(db):
    """Idempotent migration for existing databases.
    Handles: fractional units, removal of cash_balance, addition of remaining_amount,
    creation of lot_allocations, and updated CHECK constraints."""

    # --- Contributions: add remaining_amount if missing ---
    cols = [r[1] for r in db.execute("PRAGMA table_info(contributions)").fetchall()]
    if 'remaining_amount' not in cols:
        db.execute(
            "ALTER TABLE contributions ADD COLUMN remaining_amount REAL NOT NULL DEFAULT 0"
        )
        # Set remaining_amount = amount for existing rows
        db.execute("UPDATE contributions SET remaining_amount = amount WHERE remaining_amount = 0")
        db.commit()

    if 'source_type' not in cols:
        db.execute(
            "ALTER TABLE contributions ADD COLUMN source_type TEXT NOT NULL DEFAULT 'contribution'"
        )
        db.commit()

    # --- Holdings: recreate if units is INTEGER (need REAL) ---
    holdings_cols = db.execute("PRAGMA table_info(holdings)").fetchall()
    units_col = [r for r in holdings_cols if r[1] == 'units']
    if units_col and units_col[0][2].upper() == 'INTEGER':
        db.execute("ALTER TABLE holdings RENAME TO holdings_old")
        db.execute("""
            CREATE TABLE holdings (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                certificate_id      INTEGER NOT NULL REFERENCES certificates(id) ON DELETE CASCADE,
                fund_id             INTEGER NOT NULL REFERENCES funds(id),
                units               REAL NOT NULL DEFAULT 0.0,
                UNIQUE(certificate_id, fund_id)
            )
        """)
        db.execute("""
            INSERT INTO holdings (id, certificate_id, fund_id, units)
            SELECT id, certificate_id, fund_id, CAST(units AS REAL) FROM holdings_old
        """)
        db.execute("DROP TABLE holdings_old")
        db.commit()

    # --- Certificates: remove cash_balance if present ---
    cert_cols2 = [r[1] for r in db.execute("PRAGMA table_info(certificates)").fetchall()]
    if 'cash_balance' in cert_cols2:
        db.execute("ALTER TABLE certificates RENAME TO certificates_old")
        db.execute("""
            CREATE TABLE certificates (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                plan_id         INTEGER NOT NULL REFERENCES plans(id),
                created_date    TEXT NOT NULL,
                phase           TEXT NOT NULL DEFAULT 'accumulation'
                                CHECK(phase IN ('accumulation', 'spending')),
                tax_regime      TEXT DEFAULT NULL
                                CHECK(tax_regime IN ('progressive', 'regressive', NULL)),
                unit_supply             REAL NOT NULL DEFAULT 0.0,
                vgbl_premium_remaining  REAL NOT NULL DEFAULT 0.0,
                notes           TEXT
            )
        """)
        db.execute("""
            INSERT INTO certificates (id, user_id, plan_id, created_date, phase, tax_regime, notes)
            SELECT id, user_id, plan_id, created_date, phase, tax_regime, notes
            FROM certificates_old
        """)
        db.execute("DROP TABLE certificates_old")
        db.commit()

    # --- lot_allocations: create if not exists ---
    existing_tables = [r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if 'lot_allocations' not in existing_tables:
        db.execute("""
            CREATE TABLE lot_allocations (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                outflow_type    TEXT NOT NULL,
                outflow_id      INTEGER NOT NULL,
                contribution_id INTEGER NOT NULL REFERENCES contributions(id),
                consumed_amount REAL NOT NULL,
                months_held     INTEGER NOT NULL,
                tax_rate        REAL NOT NULL DEFAULT 0.0,
                taxable_base    REAL NOT NULL DEFAULT 0.0,
                tax_amount      REAL NOT NULL DEFAULT 0.0,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        db.commit()

    # --- Certificates: add unit_supply and vgbl_premium_remaining if missing ---
    cert_cols = [r[1] for r in db.execute("PRAGMA table_info(certificates)").fetchall()]
    if 'unit_supply' not in cert_cols:
        db.execute("ALTER TABLE certificates ADD COLUMN unit_supply REAL NOT NULL DEFAULT 0.0")
        db.commit()
    if 'vgbl_premium_remaining' not in cert_cols:
        db.execute("ALTER TABLE certificates ADD COLUMN vgbl_premium_remaining REAL NOT NULL DEFAULT 0.0")
        db.commit()

    # --- Contributions: add unit columns if missing ---
    contrib_cols = [r[1] for r in db.execute("PRAGMA table_info(contributions)").fetchall()]
    if 'units_total' not in contrib_cols:
        db.execute("ALTER TABLE contributions ADD COLUMN units_total REAL NOT NULL DEFAULT 0.0")
        db.commit()
    if 'units_remaining' not in contrib_cols:
        db.execute("ALTER TABLE contributions ADD COLUMN units_remaining REAL NOT NULL DEFAULT 0.0")
        db.commit()
    if 'issue_unit_price' not in contrib_cols:
        db.execute("ALTER TABLE contributions ADD COLUMN issue_unit_price REAL NOT NULL DEFAULT 0.0")
        db.commit()
    if 'gross_amount' not in contrib_cols:
        db.execute("ALTER TABLE contributions ADD COLUMN gross_amount REAL")
        # Backfill: for existing contributions, gross_amount = amount (no IOF retroactively)
        db.execute("UPDATE contributions SET gross_amount = amount WHERE gross_amount IS NULL AND source_type = 'contribution'")
        db.commit()

    # --- lot_allocations: add days_held if missing ---
    la_cols = [r[1] for r in db.execute("PRAGMA table_info(lot_allocations)").fetchall()]
    if 'days_held' not in la_cols:
        db.execute("ALTER TABLE lot_allocations ADD COLUMN days_held INTEGER NOT NULL DEFAULT 0")
        db.commit()

    # --- Requests: ensure CHECK constraints include all types ---
    cols = [r[1] for r in db.execute("PRAGMA table_info(requests)").fetchall()]
    if 'rejected_reason' not in cols:
        db.execute("ALTER TABLE requests ADD COLUMN rejected_reason TEXT")
        db.commit()

    needs_recreate = False
    try:
        db.execute("SAVEPOINT check_schema")
        db.execute(
            "INSERT INTO requests (user_id, type, status, created_date) "
            "VALUES (0, 'transfer_internal', 'rejected', '2000-01-01')"
        )
        db.execute("ROLLBACK TO check_schema")
    except sqlite3.IntegrityError:
        db.execute("ROLLBACK TO check_schema")
        needs_recreate = True
    finally:
        db.execute("RELEASE check_schema")

    if needs_recreate:
        db.execute("ALTER TABLE requests RENAME TO requests_old")
        db.executescript("""
            CREATE TABLE requests (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL REFERENCES users(id),
                certificate_id  INTEGER,
                type            TEXT NOT NULL CHECK(type IN (
                                    'fund_swap', 'withdrawal', 'contribution',
                                    'portability_out', 'portability_in',
                                    'brokerage_withdrawal',
                                    'transfer_internal', 'transfer_external_out',
                                    'transfer_external_in'
                                )),
                status          TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending', 'completed', 'failed',
                                                 'rejected', 'cancelled')),
                details         TEXT,
                rejected_reason TEXT,
                created_date    TEXT NOT NULL,
                completed_date  TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
        db.execute("""
            INSERT INTO requests (id, user_id, certificate_id, type, status,
                                  details, rejected_reason, created_date,
                                  completed_date, created_at)
            SELECT id, user_id, certificate_id, type, status,
                   details, rejected_reason, created_date,
                   completed_date, created_at
            FROM requests_old
        """)
        db.execute("DROP TABLE requests_old")
        db.commit()
