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

VOUCHER_WORKERS       = 8
SNAPSHOT_CHUNK_MONTHS = 3

VOUCHER_CONFIG = [
    {
        'voucher_type'    : 'sales',
        'snapshot_fetch'  : 'fetch_all_sales_vouchers',
        'cdc_fetch'       : 'fetch_sales_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_sales_vouchers,
        'parser_type_name': 'Sales Vouchers',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'purchase',
        'snapshot_fetch'  : 'fetch_all_purchase_vouchers',
        'cdc_fetch'       : 'fetch_purchase_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_purchase_vouchers,
        'parser_type_name': 'Purchase Vouchers',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'credit_note',
        'snapshot_fetch'  : 'fetch_all_sales_return',
        'cdc_fetch'       : 'fetch_credit_note_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_credit_notes,
        'parser_type_name': 'Credit Note',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'debit_note',
        'snapshot_fetch'  : 'fetch_all_purchase_return',
        'cdc_fetch'       : 'fetch_debit_note_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_debit_notes,
        'parser_type_name': 'Debit Note',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'receipt',
        'snapshot_fetch'  : 'fetch_all_receipt_vouchers',
        'cdc_fetch'       : 'fetch_receipt_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_receipt_vouchers,
        'parser_type_name': 'Receipt Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'payment',
        'snapshot_fetch'  : 'fetch_all_payment_vouchers',
        'cdc_fetch'       : 'fetch_payment_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_payment_vouchers,
        'parser_type_name': 'Payment Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'journal',
        'snapshot_fetch'  : 'fetch_all_journal_vouchers',
        'cdc_fetch'       : 'fetch_journal_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_journal_vouchers,
        'parser_type_name': 'Journal Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'contra',
        'snapshot_fetch'  : 'fetch_all_contra_vouchers',
        'cdc_fetch'       : 'fetch_contra_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_contra_vouchers,
        'parser_type_name': 'Contra Vouchers',
        'kind'            : 'ledger',
    },
]


def _get_max_alter_id(rows):
    if not rows:
        return 0
    return max(int(r.get('alter_id', 0)) for r in rows)


def _resolve_from_date(company: dict) -> str:
    starting_from = company.get('starting_from', '')
    if starting_from:
        cleaned = str(starting_from).strip().replace('-', '')
        if len(cleaned) == 8 and cleaned.isdigit():
            return cleaned
    fallback = '20240401'
    logger.warning(f"No valid starting_from for company '{company.get('name')}' - falling back to {fallback}")
    return fallback


def _generate_chunks(from_date_str: str, to_date_str: str, chunk_months: int = SNAPSHOT_CHUNK_MONTHS):
    start = datetime.strptime(from_date_str, '%Y%m%d').date()
    end   = datetime.strptime(to_date_str,   '%Y%m%d').date()

    chunk_start = start

    while chunk_start <= end:
        end_month = chunk_start.month + chunk_months - 1
        end_year  = chunk_start.year + (end_month - 1) // 12
        end_month = (end_month - 1) % 12 + 1
        last_day  = monthrange(end_year, end_month)[1]

        chunk_end  = min(date(end_year, end_month, last_day), end)
        month_str  = chunk_end.strftime('%Y%m')
        chunk_from = chunk_start.strftime('%Y%m%d')
        chunk_to   = chunk_end.strftime('%Y%m%d')

        yield chunk_from, chunk_to, month_str

        if chunk_end >= end:
            break

        next_month  = chunk_end.month + 1 if chunk_end.month < 12 else 1
        next_year   = chunk_end.year if chunk_end.month < 12 else chunk_end.year + 1
        chunk_start = date(next_year, next_month, 1)


def _advance_chunk_only(company_name, voucher_type, month_str, engine):
    db = _get_session(engine)
    try:
        state = db.query(SyncState).filter_by(
            company_name = company_name,
            voucher_type = voucher_type,
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
        logger.exception(f"[{company_name}] [{voucher_type}] Failed to advance chunk to {month_str}")
        raise
    finally:
        db.close()


def _sync_trial_balance(company_name, tally, engine, from_date, to_date):
    logger.info(f"[{company_name}] Syncing trial balance | {from_date} -> {to_date}")
    try:
        xml = tally.fetch_trial_balance(
            company_name = company_name,
            from_date    = from_date,
            to_date      = to_date,
        )
        if not xml:
            logger.warning(f"[{company_name}] No trial balance data returned from Tally")
            return

        rows = parse_trial_balance(xml, company_name, from_date, to_date)
        if not rows:
            logger.warning(f"[{company_name}] No trial balance rows parsed")
            return

        upsert_trial_balance(rows, engine)
        max_alter_id = _get_max_alter_id(rows)
        update_sync_state(company_name, 'trial_balance', max_alter_id, engine)
        logger.info(f"[{company_name}] Trial balance done | rows={len(rows)} | max_alter_id={max_alter_id}")

    except Exception:
        logger.exception(f"[{company_name}] Failed to sync trial balance")


def _sync_ledgers(company_name, tally, engine):
    logger.info(f"[{company_name}] Syncing ledgers")
    try:
        xml = tally.fetch_all_ledgers(company_name=company_name)
        if not xml:
            logger.warning(f"[{company_name}] No ledger data returned from Tally")
            return

        rows = parse_ledgers(xml, company_name)
        if not rows:
            logger.warning(f"[{company_name}] No ledger rows parsed")
            return

        upsert_ledgers(rows, engine)
        max_alter_id = _get_max_alter_id(rows)
        update_sync_state(company_name, 'ledger', max_alter_id, engine)
        logger.info(f"[{company_name}] Ledgers done | rows={len(rows)} | max_alter_id={max_alter_id}")

    except Exception:
        logger.exception(f"[{company_name}] Failed to sync ledgers")


def _sync_voucher(company_name, config, tally, engine, from_date, to_date):
    voucher_type     = config['voucher_type']
    snapshot_fetch   = config['snapshot_fetch']
    cdc_fetch        = config['cdc_fetch']
    parser           = config['parser']
    upsert           = config['upsert']
    parser_type_name = config['parser_type_name']
    kind             = config['kind']

    logger.info(f"[{company_name}] [{voucher_type}] Starting")

    try:
        state             = get_sync_state(company_name, voucher_type, engine)
        is_initial_done   = state.is_initial_done   if state else False
        last_alter_id     = state.last_alter_id     if state else 0
        last_synced_month = state.last_synced_month if state else None

        if is_initial_done:
            logger.info(f"[{company_name}] [{voucher_type}] CDC | last_alter_id={last_alter_id}")
            fetch_fn = getattr(tally, cdc_fetch)
            xml      = fetch_fn(company_name=company_name, last_alter_id=last_alter_id)

            if not xml:
                logger.warning(f"[{company_name}] [{voucher_type}] No CDC data returned")
                return

            rows = parser(xml, company_name, parser_type_name)
            if not rows:
                logger.info(f"[{company_name}] [{voucher_type}] No new/changed records (CDC)")
                return

            upsert(rows, engine)
            max_alter_id = _get_max_alter_id(rows)
            update_sync_state(company_name, voucher_type, max_alter_id, engine, is_initial_done=True)
            logger.info(f"[{company_name}] [{voucher_type}] CDC done | rows={len(rows)} | max_alter_id={max_alter_id}")
            return

        logger.info(
            f"[{company_name}] [{voucher_type}] INITIAL SNAPSHOT ({SNAPSHOT_CHUNK_MONTHS}-month chunks) | {from_date} -> {to_date}"
        )

        if kind == 'inventory':
            model_class = INVENTORY_MODEL_MAP[voucher_type]
            upsert_fn   = _upsert_inventory_voucher_in_session
        else:
            model_class = LEDGER_MODEL_MAP[voucher_type]
            upsert_fn   = _upsert_ledger_voucher_in_session

        fetch_fn      = getattr(tally, snapshot_fetch)
        total_rows    = 0
        chunks_done   = 0
        all_alter_ids = []

        for chunk_from, chunk_to, month_str in _generate_chunks(from_date, to_date):

            if last_synced_month and month_str <= last_synced_month:
                continue

            logger.info(f"[{company_name}] [{voucher_type}] Chunk {month_str} | {chunk_from} -> {chunk_to}")

            xml = fetch_fn(company_name=company_name, from_date=chunk_from, to_date=chunk_to)

            if not xml:
                logger.info(f"[{company_name}] [{voucher_type}] Chunk {month_str} empty, advancing")
                _advance_chunk_only(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

            rows = parser(xml, company_name, parser_type_name)

            if not rows:
                logger.info(f"[{company_name}] [{voucher_type}] Chunk {month_str} 0 rows, advancing")
                _advance_chunk_only(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

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

        final_alter_id = max(all_alter_ids) if all_alter_ids else 0
        update_sync_state(
            company_name      = company_name,
            voucher_type      = voucher_type,
            last_alter_id     = final_alter_id,
            engine            = engine,
            last_synced_month = to_date[:6],
            is_initial_done   = True,
        )

        logger.info(
            f"[{company_name}] [{voucher_type}] Initial snapshot complete | "
            f"chunks={chunks_done} | total_rows={total_rows} | max_alter_id={final_alter_id}"
        )

    except Exception:
        logger.exception(f"[{company_name}] [{voucher_type}] Sync failed - will resume from last committed chunk on next run")


def sync_company(company, tally, engine, to_date, manual_from_date=None):
    comp_name = company.get('name', '').strip()
    from_date = manual_from_date if manual_from_date else _resolve_from_date(company)

    logger.info(f"{'='*60}")
    logger.info(f"Starting sync  : {comp_name}")
    logger.info(f"Date range     : {from_date} -> {to_date}")
    logger.info(f"from_date      : {'MANUAL OVERRIDE' if manual_from_date else 'auto from company starting_from'}")
    logger.info(f"Chunk size     : {SNAPSHOT_CHUNK_MONTHS} months")
    logger.info(f"Voucher workers: {VOUCHER_WORKERS} (parallel)")
    logger.info(f"{'='*60}")

    start_time = datetime.now()

    _sync_ledgers(comp_name, tally, engine)
    _sync_trial_balance(comp_name, tally, engine, from_date, to_date)

    logger.info(f"[{comp_name}] Launching {len(VOUCHER_CONFIG)} voucher syncs with {VOUCHER_WORKERS} workers")

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
            voucher_type = futures[future]
            try:
                future.result()
                logger.info(f"[{comp_name}] [{voucher_type}] Thread finished OK")
            except Exception:
                logger.error(f"[{comp_name}] [{voucher_type}] Thread failed - other voucher types continue")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{comp_name}] Sync completed in {elapsed:.1f}s")


def sync_all_companies(companies, tally, engine, to_date, manual_from_date=None):
    if not companies:
        logger.warning("No companies provided")
        return

    valid = [
        c for c in companies
        if c.get('name', '').strip()
        and c.get('name', '').strip().upper() not in ['N/A', 'NA', 'NONE', '']
    ]

    logger.info(f"Syncing {len(valid)} valid companies (skipped {len(companies) - len(valid)} invalid)")

    for company in valid:
        sync_company(
            company          = company,
            tally            = tally,
            engine           = engine,
            to_date          = to_date,
            manual_from_date = manual_from_date,
        )