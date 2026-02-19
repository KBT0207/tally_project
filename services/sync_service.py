"""
sync_service.py
===============
Orchestrates syncing data from Tally into the database.

HOW IT DECIDES WHAT TO FETCH
─────────────────────────────────────────────────────────────────────
For each company + voucher type, we keep a row in the `sync_state` table:

    company_name    | voucher_type | is_initial_done | last_alter_id | last_synced_month
    ────────────────┼──────────────┼─────────────────┼───────────────┼──────────────────
    My Company      | sales        | False           | 0             | None
    My Company      | sales        | True            | 9742          | 202503

FIRST RUN  →  is_initial_done = False
    We do a full snapshot: fetch all vouchers month by month, save to DB.
    When done, we set is_initial_done = True and save the max AlterID.

EVERY RUN AFTER THAT  →  is_initial_done = True
    We do CDC: only fetch vouchers whose AlterID > last_alter_id.
    We update last_alter_id to the new max we received.
─────────────────────────────────────────────────────────────────────
"""

from datetime import datetime, date
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed

from logging_config import logger
from database.models.sync_state import SyncState

from services.tally_connector import TallyConnector
from services.data_processor import (
    parse_inventory_voucher,
    parse_ledger_voucher,
    parse_ledgers,
    parse_trial_balance,
)
from database.database_processor import (
    get_sync_state,
    update_sync_state,
    upsert_ledgers,
    upsert_and_advance_month,
    upsert_sales_vouchers,
    upsert_purchase_vouchers,
    upsert_credit_notes,
    upsert_debit_notes,
    upsert_receipt_vouchers,
    upsert_payment_vouchers,
    upsert_journal_vouchers,
    upsert_contra_vouchers,
    upsert_trial_balance,
    INVENTORY_MODEL_MAP,
    LEDGER_MODEL_MAP,
    _upsert_inventory_voucher_in_session,
    _upsert_ledger_voucher_in_session,
    _get_session,
)

# How many months to pull in one API call during the initial snapshot.
# Smaller = safer (less memory, easier to resume if it crashes mid-way).
SNAPSHOT_CHUNK_MONTHS = 3

# How many voucher types to sync in parallel (uses Python threads).
#
# WHY 2 AND NOT 8:
# Tally is single-threaded software. When 8 requests arrive at once, Tally
# processes them one by one internally — so you get no parallelism inside Tally,
# but you DO get 8 open HTTP connections all waiting. This actually SLOWS things
# down vs sequential requests because Tally context-switches between them.
#
# With 2 workers: one request is being processed by Tally while the other thread
# is parsing the previous XML response or writing to the DB. That overlap is real
# and useful. Going higher than 2-3 gives diminishing returns and can make Tally
# unresponsive for users working in it simultaneously.
VOUCHER_WORKERS = 2

# ─────────────────────────────────────────────────────────────────────────────
# VOUCHER CONFIG
# One entry per voucher type. The sync logic loops over this list automatically,
# so adding a new voucher type is as simple as adding one dict here.
#
# Keys:
#   voucher_type      – unique identifier stored in sync_state table
#   snapshot_fetch    – method name on TallyConnector for the full snapshot
#   cdc_fetch         – method name on TallyConnector for CDC (incremental)
#   parser            – function that turns XML bytes → list of dicts
#   upsert            – function that saves those dicts to the database
#   parser_type_name  – label passed to the parser for logging
#   kind              – 'inventory' (has stock items) or 'ledger' (ledger only)
# ─────────────────────────────────────────────────────────────────────────────
VOUCHER_CONFIG = [
    {
        'voucher_type'    : 'sales',
        'snapshot_fetch'  : 'fetch_sales',          # TallyConnector.fetch_sales(...)
        'cdc_fetch'       : 'fetch_sales_cdc',      # TallyConnector.fetch_sales_cdc(...)
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_sales_vouchers,
        'parser_type_name': 'Sales Vouchers',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'purchase',
        'snapshot_fetch'  : 'fetch_purchase',
        'cdc_fetch'       : 'fetch_purchase_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_purchase_vouchers,
        'parser_type_name': 'Purchase Vouchers',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'credit_note',
        'snapshot_fetch'  : 'fetch_credit_note',
        'cdc_fetch'       : 'fetch_credit_note_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_credit_notes,
        'parser_type_name': 'Credit Note',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'debit_note',
        'snapshot_fetch'  : 'fetch_debit_note',
        'cdc_fetch'       : 'fetch_debit_note_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_debit_notes,
        'parser_type_name': 'Debit Note',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'receipt',
        'snapshot_fetch'  : 'fetch_receipt',
        'cdc_fetch'       : 'fetch_receipt_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_receipt_vouchers,
        'parser_type_name': 'Receipt Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'payment',
        'snapshot_fetch'  : 'fetch_payment',
        'cdc_fetch'       : 'fetch_payment_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_payment_vouchers,
        'parser_type_name': 'Payment Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'journal',
        'snapshot_fetch'  : 'fetch_journal',
        'cdc_fetch'       : 'fetch_journal_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_journal_vouchers,
        'parser_type_name': 'Journal Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'contra',
        'snapshot_fetch'  : 'fetch_contra',
        'cdc_fetch'       : 'fetch_contra_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_contra_vouchers,
        'parser_type_name': 'Contra Vouchers',
        'kind'            : 'ledger',
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_max_alter_id(rows: list) -> int:
    """
    Find the highest AlterID in a list of parsed rows.
    We save this so the next CDC call can ask for records with AlterID > this value.
    """
    if not rows:
        return 0
    return max(int(r.get('alter_id', 0)) for r in rows)


def _resolve_from_date(company: dict) -> str:
    """
    Get the date we should start fetching from.
    Uses the company's 'starting_from' field from Tally; falls back to a default.
    """
    starting_from = company.get('starting_from', '')
    if starting_from:
        cleaned = str(starting_from).strip().replace('-', '')
        if len(cleaned) == 8 and cleaned.isdigit():
            return cleaned
    fallback = '20240401'
    logger.warning(f"No valid starting_from for '{company.get('name')}' — using fallback {fallback}")
    return fallback


def _generate_chunks(from_date_str: str, to_date_str: str, chunk_months: int = SNAPSHOT_CHUNK_MONTHS):
    """
    Break a long date range into smaller chunks so we don't ask Tally for
    years of data in a single request.

    Example: 20220401 → 20250331 with chunk_months=3 yields:
        (20220401, 20220630, '202206')
        (20220701, 20220930, '202209')
        … and so on until 20250331

    Yields tuples of (chunk_from, chunk_to, month_str).
    month_str is used to record progress in the sync_state table.
    """
    start = datetime.strptime(from_date_str, '%Y%m%d').date()
    end   = datetime.strptime(to_date_str,   '%Y%m%d').date()
    chunk_start = start

    while chunk_start <= end:
        # Calculate the last month of this chunk
        end_month = chunk_start.month + chunk_months - 1
        end_year  = chunk_start.year + (end_month - 1) // 12
        end_month = (end_month - 1) % 12 + 1
        last_day  = monthrange(end_year, end_month)[1]

        chunk_end  = min(date(end_year, end_month, last_day), end)
        month_str  = chunk_end.strftime('%Y%m')          # e.g. '202206'
        chunk_from = chunk_start.strftime('%Y%m%d')
        chunk_to   = chunk_end.strftime('%Y%m%d')

        yield chunk_from, chunk_to, month_str

        if chunk_end >= end:
            break

        next_month  = chunk_end.month + 1 if chunk_end.month < 12 else 1
        next_year   = chunk_end.year      if chunk_end.month < 12 else chunk_end.year + 1
        chunk_start = date(next_year, next_month, 1)


def _mark_chunk_done(company_name: str, voucher_type: str, month_str: str, engine):
    """
    Record in the DB that we have finished this chunk.
    Even if the chunk had zero rows, we need to mark it so we don't re-fetch it.
    """
    db = _get_session(engine)
    try:
        state = db.query(SyncState).filter_by(
            company_name=company_name,
            voucher_type=voucher_type,
        ).first()

        if state:
            state.last_synced_month = month_str
            state.last_sync_time    = datetime.utcnow()
        else:
            db.add(SyncState(
                company_name      = company_name,
                voucher_type      = voucher_type,
                last_alter_id     = 0,
                is_initial_done   = False,
                last_synced_month = month_str,
                last_sync_time    = datetime.utcnow(),
            ))

        db.commit()
    except Exception:
        db.rollback()
        logger.exception(f"[{company_name}][{voucher_type}] Failed to mark chunk {month_str} done")
        raise
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# SYNC – TRIAL BALANCE
# ─────────────────────────────────────────────────────────────────────────────

def _sync_trial_balance(company_name: str, tally: TallyConnector, engine, from_date: str, to_date: str):
    """
    Fetch and save the trial balance.

    Trial Balance has no CDC XML in Tally, so we always do a full fetch.
    However, we can avoid the expensive DB upsert (9s for 9000+ rows) by
    comparing the max AlterID in the fresh data against what we saved last time.

    If the max AlterID has NOT changed  →  ledger balances have not moved
                                        →  skip the upsert entirely (save ~9s)
    If the max AlterID HAS changed      →  something in Tally was edited
                                        →  upsert all rows as usual

    The fetch itself (~8s) is unavoidable — Tally has no way to tell us
    "nothing changed" without us asking. But at least we skip the DB work.
    """
    logger.info(f"[{company_name}] Syncing Trial Balance | {from_date} -> {to_date}")
    try:
        # Read last saved AlterID from DB
        state              = get_sync_state(company_name, 'trial_balance', engine)
        saved_alter_id     = state.last_alter_id if state else 0

        xml = tally.fetch_trial_balance(company_name=company_name, from_date=from_date, to_date=to_date)
        if not xml:
            logger.warning(f"[{company_name}] No trial balance data from Tally")
            return

        rows = parse_trial_balance(xml, company_name, from_date, to_date)
        if not rows:
            logger.warning(f"[{company_name}] Trial balance parsed 0 rows")
            return

        max_alter_id = _get_max_alter_id(rows)

        # Skip the upsert if nothing has changed in Tally since our last sync
        if max_alter_id == saved_alter_id and saved_alter_id > 0:
            logger.info(
                f"[{company_name}] Trial Balance SKIPPED upsert — "
                f"max_alter_id unchanged ({max_alter_id}), no changes in Tally"
            )
            return

        # AlterID changed (or first run) — upsert all rows
        upsert_trial_balance(rows, engine)
        update_sync_state(company_name, 'trial_balance', max_alter_id, engine)
        logger.info(
            f"[{company_name}] Trial Balance done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id} "
            f"(was {saved_alter_id})"
        )

    except Exception:
        logger.exception(f"[{company_name}] Trial Balance sync failed")


# ─────────────────────────────────────────────────────────────────────────────
# SYNC – LEDGERS
# ─────────────────────────────────────────────────────────────────────────────

def _sync_ledgers(company_name: str, tally: TallyConnector, engine):
    """
    Sync ledgers using the same snapshot -> CDC pattern as vouchers.

    FIRST RUN  (is_initial_done=False):
        fetch_ledgers()     -> fetches ALL ledgers from Tally, saves max AlterID,
                               marks is_initial_done=True

    EVERY RUN AFTER (is_initial_done=True):
        fetch_ledger_cdc()  -> only fetches ledgers with AlterID > last_alter_id
                               Much faster - only new/edited ledgers come back.

    Why CDC for ledgers?
        Ledger masters get a new AlterID every time they are created or edited
        in Tally (e.g. name change, address, GST number update, new ledger added).
        Without CDC we would re-fetch ALL 1000+ ledgers on every sync run,
        even if only 2 changed.
    """
    logger.info(f"[{company_name}] Syncing Ledgers")
    try:
        state           = get_sync_state(company_name, 'ledger', engine)
        is_initial_done = state.is_initial_done if state else False
        last_alter_id   = state.last_alter_id   if state else 0

        # -- CDC MODE (every run after the first) --
        if is_initial_done:
            logger.info(f"[{company_name}][ledger] CDC | last_alter_id={last_alter_id}")
            xml = tally.fetch_ledger_cdc(company_name=company_name, last_alter_id=last_alter_id)

            if not xml:
                logger.info(f"[{company_name}][ledger] CDC: no new/changed ledgers")
                return

            rows = parse_ledgers(xml, company_name)
            if not rows:
                logger.info(f"[{company_name}][ledger] CDC: 0 rows (nothing changed since AlterID {last_alter_id})")
                return

            upsert_ledgers(rows, engine)
            new_max = _get_max_alter_id(rows)
            update_sync_state(company_name, 'ledger', new_max, engine, is_initial_done=True)
            logger.info(f"[{company_name}][ledger] CDC done | rows={len(rows)} | new max_alter_id={new_max}")
            return

        # -- SNAPSHOT MODE (first time only) --
        logger.info(f"[{company_name}][ledger] SNAPSHOT - fetching all ledgers for the first time")
        xml = tally.fetch_ledgers(company_name=company_name)

        if not xml:
            logger.warning(f"[{company_name}][ledger] No ledger data from Tally")
            return

        rows = parse_ledgers(xml, company_name)
        if not rows:
            logger.warning(f"[{company_name}][ledger] Snapshot parsed 0 rows")
            return

        upsert_ledgers(rows, engine)
        max_alter_id = _get_max_alter_id(rows)
        update_sync_state(company_name, 'ledger', max_alter_id, engine, is_initial_done=True)
        logger.info(
            f"[{company_name}][ledger] Snapshot done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id} | CDC enabled from next run"
        )

    except Exception:
        logger.exception(f"[{company_name}] Ledger sync failed")


# ─────────────────────────────────────────────────────────────────────────────
# SYNC – ONE VOUCHER TYPE
# This function is called once per entry in VOUCHER_CONFIG.
# It automatically decides snapshot vs CDC based on sync_state.
# ─────────────────────────────────────────────────────────────────────────────

def _sync_voucher(
    company_name: str,
    config:       dict,
    tally:        TallyConnector,
    engine,
    from_date:    str,
    to_date:      str,
):
    """
    Sync one voucher type for one company.

    Decision logic:
        if is_initial_done is False  →  run full snapshot (chunked by date)
        if is_initial_done is True   →  run CDC with last saved AlterID
    """
    voucher_type     = config['voucher_type']
    snapshot_fetch   = config['snapshot_fetch']    # e.g. 'fetch_sales'
    cdc_fetch        = config['cdc_fetch']         # e.g. 'fetch_sales_cdc'
    parser           = config['parser']
    upsert           = config['upsert']
    parser_type_name = config['parser_type_name']
    kind             = config['kind']

    logger.info(f"[{company_name}][{voucher_type}] Starting")

    try:
        # Read last sync state from DB
        state             = get_sync_state(company_name, voucher_type, engine)
        is_initial_done   = state.is_initial_done   if state else False
        last_alter_id     = state.last_alter_id     if state else 0
        last_synced_month = state.last_synced_month if state else None

        # ── CDC MODE ──────────────────────────────────────────────────────────
        if is_initial_done:
            logger.info(f"[{company_name}][{voucher_type}] CDC | last_alter_id={last_alter_id}")

            t0 = datetime.now()

            # Call e.g. tally.fetch_sales_cdc(company_name=..., last_alter_id=...)
            fetch_fn = getattr(tally, cdc_fetch)
            xml      = fetch_fn(company_name=company_name, last_alter_id=last_alter_id)

            fetch_ms = int((datetime.now() - t0).total_seconds() * 1000)

            if not xml:
                logger.warning(f"[{company_name}][{voucher_type}] CDC: no response from Tally ({fetch_ms}ms)")
                return

            rows = parser(xml, company_name, parser_type_name)

            if not rows:
                # Nothing changed in Tally for this voucher type — done in fetch_ms
                logger.info(
                    f"[{company_name}][{voucher_type}] CDC: nothing changed "
                    f"(fetch={fetch_ms}ms, 0 rows)"
                )
                return

            # New/changed vouchers found — save them
            t1 = datetime.now()
            upsert(rows, engine)
            upsert_ms = int((datetime.now() - t1).total_seconds() * 1000)

            new_max = _get_max_alter_id(rows)
            update_sync_state(company_name, voucher_type, new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][{voucher_type}] CDC done | "
                f"rows={len(rows)} | new max_alter_id={new_max} | "
                f"fetch={fetch_ms}ms upsert={upsert_ms}ms"
            )
            return

        # ── SNAPSHOT MODE (first time) ────────────────────────────────────────
        logger.info(
            f"[{company_name}][{voucher_type}] SNAPSHOT "
            f"({SNAPSHOT_CHUNK_MONTHS}-month chunks) | {from_date} → {to_date}"
        )

        # Choose the right DB model and upsert function based on voucher kind
        if kind == 'inventory':
            model_class = INVENTORY_MODEL_MAP[voucher_type]
            upsert_fn   = _upsert_inventory_voucher_in_session
        else:
            model_class = LEDGER_MODEL_MAP[voucher_type]
            upsert_fn   = _upsert_ledger_voucher_in_session

        # Call e.g. tally.fetch_sales(company_name=..., from_date=..., to_date=...)
        fetch_fn      = getattr(tally, snapshot_fetch)
        total_rows    = 0
        chunks_done   = 0
        all_alter_ids = []

        for chunk_from, chunk_to, month_str in _generate_chunks(from_date, to_date):

            # Skip chunks we already completed in a previous (interrupted) run
            if last_synced_month and month_str <= last_synced_month:
                logger.debug(f"[{company_name}][{voucher_type}] Skipping already-done chunk {month_str}")
                continue

            logger.info(f"[{company_name}][{voucher_type}] Chunk {month_str} | {chunk_from} → {chunk_to}")

            xml = fetch_fn(company_name=company_name, from_date=chunk_from, to_date=chunk_to)

            if not xml:
                # Tally returned nothing for this period; mark it done and move on
                logger.info(f"[{company_name}][{voucher_type}] Chunk {month_str}: empty response, advancing")
                _mark_chunk_done(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

            rows = parser(xml, company_name, parser_type_name)

            if not rows:
                logger.info(f"[{company_name}][{voucher_type}] Chunk {month_str}: 0 rows parsed, advancing")
                _mark_chunk_done(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

            # Save rows + advance the progress marker in one DB transaction
            upsert_and_advance_month(
                rows         = rows,
                model_class  = model_class,
                upsert_fn    = upsert_fn,
                company_name = company_name,
                voucher_type = voucher_type,
                month_str    = month_str,
                engine       = engine,
            )

            all_alter_ids.extend(int(r.get('alter_id', 0)) for r in rows)
            total_rows  += len(rows)
            chunks_done += 1

        # All chunks done — mark initial sync complete and save the max AlterID
        final_alter_id = max(all_alter_ids) if all_alter_ids else 0
        update_sync_state(
            company_name      = company_name,
            voucher_type      = voucher_type,
            last_alter_id     = final_alter_id,
            engine            = engine,
            last_synced_month = to_date[:6],    # e.g. '202503'
            is_initial_done   = True,
        )

        logger.info(
            f"[{company_name}][{voucher_type}] Snapshot complete | "
            f"chunks={chunks_done} | total_rows={total_rows} | max_alter_id={final_alter_id}"
        )

    except Exception:
        logger.exception(
            f"[{company_name}][{voucher_type}] Sync failed — "
            f"will resume from last committed chunk on next run"
        )


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def sync_company(
    company:          dict,
    tally:            TallyConnector,
    engine,
    to_date:          str,
    manual_from_date: str = None,
):
    """
    Sync all data for one company.

    1. Ledgers  (always full fetch, fast)
    2. Trial Balance
    3. All 8 voucher types in parallel threads

    Parameters
    ----------
    company          : dict from fetch_all_companies() — must have 'name' key
    tally            : connected TallyConnector instance
    engine           : SQLAlchemy engine
    to_date          : end of sync window, YYYYMMDD (usually today)
    manual_from_date : override start date; if None, uses company.starting_from
    """
    comp_name = company.get('name', '').strip()
    from_date = manual_from_date if manual_from_date else _resolve_from_date(company)

    logger.info('=' * 60)
    logger.info(f'Syncing company  : {comp_name}')
    logger.info(f'Date range       : {from_date} → {to_date}')
    logger.info(f'Chunk size       : {SNAPSHOT_CHUNK_MONTHS} months per API call')
    logger.info(f'Parallel workers : {VOUCHER_WORKERS} threads')
    logger.info('=' * 60)

    start_time = datetime.now()

    # Ledgers and trial balance first (sequential, fast)
    _sync_ledgers(comp_name, tally, engine)
    _sync_trial_balance(comp_name, tally, engine, from_date, to_date)

    # All voucher types in parallel
    logger.info(f"[{comp_name}] Launching {len(VOUCHER_CONFIG)} voucher syncs …")
    with ThreadPoolExecutor(max_workers=VOUCHER_WORKERS) as executor:
        futures = {
            executor.submit(
                _sync_voucher,
                company_name = comp_name,
                config       = config,
                tally        = tally,
                engine       = engine,
                from_date    = from_date,
                to_date      = to_date,
            ): config['voucher_type']
            for config in VOUCHER_CONFIG
        }

        for future in as_completed(futures):
            vt = futures[future]
            try:
                future.result()
                logger.info(f"[{comp_name}][{vt}] Thread finished ✓")
            except Exception:
                logger.error(f"[{comp_name}][{vt}] Thread raised an exception (other types continue)")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{comp_name}] Sync completed in {elapsed:.1f}s")


def sync_all_companies(
    companies:        list,
    tally:            TallyConnector,
    engine,
    to_date:          str,
    manual_from_date: str = None,
):
    """
    Sync every company returned by fetch_all_companies().
    Skips entries with missing or placeholder names.
    """
    if not companies:
        logger.warning("sync_all_companies: empty company list")
        return

    invalid_names = {'', 'N/A', 'NA', 'NONE'}
    valid = [c for c in companies if c.get('name', '').strip().upper() not in invalid_names]
    skipped = len(companies) - len(valid)
    logger.info(f"Syncing {len(valid)} companies (skipped {skipped} invalid entries)")

    for company in valid:
        sync_company(
            company          = company,
            tally            = tally,
            engine           = engine,
            to_date          = to_date,
            manual_from_date = manual_from_date,
        )