"""
tally_connector.py
==================
Connects to Tally and fetches data.

HOW THE TWO FETCH MODES WORK
─────────────────────────────────────────────────────────────────────
STEP 1 – FIRST TIME (Full Snapshot)
    We fetch EVERYTHING from Tally for a date range.
    Example: fetch_sales(company, from_date="20240401", to_date="20250331")
    After saving, we remember the MAX AlterID we saw (e.g. 9500).

STEP 2 – EVERY RUN AFTER THAT (CDC = Change Data Capture)
    We only ask Tally for records whose AlterID is GREATER than what we saved.
    Example: fetch_sales_cdc(company, last_alter_id=9500)
    Tally returns only new/changed vouchers since our last sync.
    We update our saved AlterID to the new max.

AlterID is a number Tally stamps on every record each time it is created or edited.
A higher AlterID means the record is newer or was recently changed.
─────────────────────────────────────────────────────────────────────

Every public method in this file follows this naming convention:
    fetch_<type>()       → Full snapshot (first time only, needs date range)
    fetch_<type>_cdc()   → CDC / incremental (every run after first time)
"""

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logging_config import logger


class TallyConnector:
    """
    Sends XML requests to Tally and returns the XML response as bytes.

    Usage:
        tally = TallyConnector(host='localhost', port=9000)
        xml_bytes = tally.fetch_sales("My Company", "20240401", "20241231")
        xml_bytes = tally.fetch_sales_cdc("My Company", last_alter_id=9500)
    """

    # Class-level cache so we parse each XML template file only once
    _xml_template_cache: Dict[str, ET.Element] = {}

    # ─────────────────────────────────────────────────────────────────
    # INITIALIZATION
    # ─────────────────────────────────────────────────────────────────

    def __init__(self, host='localhost', port=9000, timeout=(60, 1800), max_retries=3):
        self.host    = host
        self.port    = port
        self.url     = f'http://{host}:{port}'
        self.header  = {'Content-Type': 'text/xml; charset=utf-8'}
        self.status  = 'Disconnected'
        self.timeout = timeout
        self.session = self._create_session(max_retries)
        logger.info(f'Initializing TallyConnector → {self.url}')
        self.connect()

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create an HTTP session with automatic retry on server errors."""
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def connect(self) -> bool:
        """Ping Tally to check it is running. Sets self.status."""
        try:
            logger.info(f'Connecting to Tally at {self.url} …')
            response = self.session.post(url=self.url, headers=self.header, timeout=60)
            if response.status_code == 200:
                self.status = 'Connected'
                logger.info('Connected to Tally ✓')
                return True
            self.status = 'Disconnected'
            logger.warning(f'Tally returned status {response.status_code}')
            return False
        except Exception as e:
            self.status = 'Disconnected'
            logger.error(f'Cannot connect to Tally: {e}', exc_info=True)
            return False

    # ─────────────────────────────────────────────────────────────────
    # XML HELPERS (internal)
    # ─────────────────────────────────────────────────────────────────

    @classmethod
    def _load_xml_template(cls, template_path: str) -> ET.Element:
        """
        Read an XML file from disk and cache it in memory.
        Subsequent calls return a fresh copy of the cached tree (no disk I/O).
        """
        if template_path not in cls._xml_template_cache:
            tree = ET.parse(template_path)
            cls._xml_template_cache[template_path] = tree.getroot()
            logger.debug(f'Cached XML template: {template_path}')
        # Return a deep copy so callers can modify it without polluting the cache
        return ET.fromstring(ET.tostring(cls._xml_template_cache[template_path]))

    def _prepare_xml_request(
        self,
        template_path: str,
        company_name: str,
        from_date: Optional[str] = None,
        to_date:   Optional[str] = None,
        alter_id:  Optional[int] = None,
    ) -> bytes:
        """
        Fill in the XML template with company name, dates, and alter_id filter.

        alter_id=None  → snapshot mode   → filter becomes  AlterID > 0   (all records)
        alter_id=9500  → CDC mode        → filter becomes  AlterID > 9500 (only new ones)
        """
        root = self._load_xml_template(template_path)

        # Set company name
        for elem in root.iter('SVCURRENTCOMPANY'):
            elem.text = company_name

        # Set date range (only used in snapshot mode; CDC templates ignore these)
        if from_date:
            for elem in root.iter('SVFROMDATE'):
                elem.text = from_date
        if to_date:
            for elem in root.iter('SVTODATE'):
                elem.text = to_date

        xml_str = ET.tostring(root, encoding='unicode')

        # Replace the placeholder that CDC templates contain
        if alter_id is not None:
            filter_expr = f'$$Number:$AlterID > {alter_id}'
        else:
            filter_expr = '$$Number:$AlterID > 0'      # snapshot → all records

        xml_str = xml_str.replace('PLACEHOLDER_ALTER_ID', filter_expr)

        return xml_str.encode('utf-8')

    @staticmethod
    def sanitize_xml(xml_content) -> bytes:
        """
        Clean Tally's XML response so Python's XML parser can read it.
        Handles encoding issues and illegal characters (control characters, bare &).
        """
        if isinstance(xml_content, bytes):
            for encoding in ('utf-8', 'windows-1252', 'latin-1'):
                try:
                    xml_content = xml_content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue

        xml_content = str(xml_content)

        # Log any currency symbols found (useful for debugging foreign-currency entries)
        currency_map = {
            '$': 'USD', '£': 'GBP', '€': 'EUR', '¥': 'JPY/CNY',
            '₹': 'INR', '₨': 'INR/PKR', '₩': 'KRW', '₱': 'PHP',
            '₽': 'RUB', '₺': 'TRY', '₪': 'ILS', '₦': 'NGN',
            '฿': 'THB', '₫': 'VND', '?': 'Corrupted symbol',
        }
        found = [f"{sym}({cur})" for sym, cur in currency_map.items() if sym in xml_content]
        if found:
            logger.debug(f'Currency symbols in response: {", ".join(found)}')

        # Remove illegal XML control characters
        xml_content = re.sub(r'&#([0-8]|1[1-2]|1[4-9]|2[0-9]|3[0-1]);', '', xml_content)
        xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_content)
        # Escape bare & that are not already part of an XML entity
        xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)', '&amp;', xml_content)

        return xml_content.encode('utf-8')

    def _save_debug_file(self, content: bytes, prefix: str, company_name: str, suffix: str = 'xml') -> str:
        """Save raw XML to a file. Only used when debug=True."""
        safe_company = company_name.replace(' ', '_')
        timestamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename     = f"{prefix}_{safe_company}_{timestamp}.{suffix}"
        with open(filename, 'wb') as f:
            f.write(content)
        logger.info(f'Saved debug file: {filename}')
        return filename

    # ─────────────────────────────────────────────────────────────────
    # CORE FETCH (internal) – all public methods call this
    # ─────────────────────────────────────────────────────────────────

    def _fetch(
        self,
        template_path: str,
        data_type:     str,
        company_name:  str,
        from_date:     Optional[str] = None,
        to_date:       Optional[str] = None,
        alter_id:      Optional[int] = None,
        debug:         bool          = False,
    ) -> Optional[bytes]:
        """
        Internal worker used by every public fetch_* method.

        Returns sanitized XML bytes on success, or None on failure.

        Parameters
        ----------
        template_path : path to the XML request template under utils/
        data_type     : human-readable name for log messages
        company_name  : Tally company to query
        from_date     : start date YYYYMMDD (snapshot only)
        to_date       : end   date YYYYMMDD (snapshot only)
        alter_id      : last saved AlterID  (CDC only)
        debug         : if True, write request + response XML to disk
        """
        try:
            logger.info(f'[{company_name}] Fetching {data_type}')

            xml_payload = self._prepare_xml_request(
                template_path, company_name, from_date, to_date, alter_id
            )

            if from_date and to_date:
                logger.info(f'  Date range : {from_date} → {to_date}')
            if alter_id is not None:
                logger.info(f'  CDC mode   : AlterID > {alter_id}')

            # Optional: save the request XML to disk for inspection
            if debug:
                self._save_debug_file(xml_payload, f'req_{data_type.lower().replace(" ", "_")}', company_name)

            # Send the request to Tally
            t0 = datetime.now()
            try:
                response = self.session.post(
                    url     = self.url,
                    headers = self.header,
                    data    = xml_payload,
                    timeout = self.timeout,
                    stream  = False,
                )
            except requests.exceptions.Timeout:
                logger.error(f'[{company_name}] Timeout fetching {data_type}')
                return None

            if response.status_code != 200:
                logger.error(f'[{company_name}] HTTP {response.status_code} for {data_type}')
                return None

            elapsed = (datetime.now() - t0).total_seconds()
            logger.info(f'[{company_name}] Received {data_type} in {elapsed:.1f}s')

            # Optional: save raw + sanitized response to disk for inspection
            if debug:
                self._save_debug_file(response.content, f'resp_raw_{data_type.lower().replace(" ", "_")}', company_name)
                sanitized_debug = self.sanitize_xml(response.content)
                self._save_debug_file(sanitized_debug, f'resp_{data_type.lower().replace(" ", "_")}', company_name)

            # Debug: verify the CDC filter actually returned records with AlterID > our threshold
            if alter_id is not None:
                self._verify_alter_id_filter(response.content, alter_id, data_type)

            return self.sanitize_xml(response.content)

        except Exception as e:
            logger.error(f'[{company_name}] Unexpected error fetching {data_type}: {e}', exc_info=True)
            return None

    def _verify_alter_id_filter(self, raw_content: bytes, alter_id: int, data_type: str):
        """
        Parse the response and log the AlterID range we received.
        This helps confirm that Tally's CDC filter is working correctly.
        """
        try:
            sanitized = self.sanitize_xml(raw_content)
            root      = ET.fromstring(sanitized)
            ids       = [int(e.text) for e in root.iter('ALTERID') if e.text and e.text.strip().isdigit()]
            if ids:
                min_id, max_id = min(ids), max(ids)
                ok = '✓' if min_id > alter_id else '✗ WARNING: some records at/below threshold!'
                logger.debug(
                    f'[ALTER_ID CHECK] {data_type} | count={len(ids)} | '
                    f'range={min_id}→{max_id} | threshold={alter_id} | {ok}'
                )
            else:
                logger.debug(f'[ALTER_ID CHECK] {data_type} | no ALTERID tags in response (empty result)')
        except Exception as e:
            logger.debug(f'[ALTER_ID CHECK] {data_type} | could not inspect response: {e}')

    # ─────────────────────────────────────────────────────────────────
    # COMPANIES
    # ─────────────────────────────────────────────────────────────────

    def fetch_all_companies(self) -> list:
        """
        Returns a list of all companies open in Tally.
        Each item is a dict with keys: guid, name, formal_name, etc.
        """
        try:
            tree        = ET.parse('utils/company.xml')
            xml_payload = ET.tostring(tree.getroot(), encoding='utf-8')
            response    = self.session.post(url=self.url, headers=self.header, data=xml_payload, timeout=120)

            if response.status_code != 200:
                logger.error(f'fetch_all_companies: HTTP {response.status_code}')
                return []

            sanitized = self.sanitize_xml(response.content)
            root      = ET.fromstring(sanitized)
            companies = [self._parse_company(c) for c in root.findall('.//COMPANY')]
            logger.info(f'Found {len(companies)} companies in Tally')
            return companies

        except Exception as e:
            logger.error(f'fetch_all_companies error: {e}', exc_info=True)
            return []

    @staticmethod
    def _parse_company(company: ET.Element) -> Dict[str, Any]:
        return {
            'guid'            : company.findtext('GUID',                     ''),
            'name'            : company.findtext('NAME',                     ''),
            'formal_name'     : company.findtext('BASICCOMPANYFORMALNAME',   ''),
            'company_number'  : company.findtext('COMPANYNUMBER',            ''),
            'starting_from'   : company.findtext('STARTINGFROM',             ''),
            'books_from'      : company.findtext('BOOKSFROM',                ''),
            'audited_upto'    : company.findtext('AUDITEDUPTO',              ''),
        }

    # ─────────────────────────────────────────────────────────────────
    # LEDGERS & GROUPS
    # Ledgers/Groups don't use CDC (we always fetch all and upsert).
    # ─────────────────────────────────────────────────────────────────

    def fetch_ledgers(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """
        Full snapshot: fetch ALL ledgers from Tally.
        Called once on first run. After that, use fetch_ledger_cdc().
        """
        return self._fetch('utils/ledger.xml', 'Ledgers', company_name, from_date, to_date, debug=debug)

    def fetch_ledger_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """
        CDC: fetch only ledgers with AlterID > last_alter_id.

        Tally assigns a new AlterID every time a ledger is created or edited.
        By passing our last saved AlterID, we get only what changed since our
        last sync — much faster than fetching all ledgers every time.

        Uses: utils/cdc/ledger_cdc.xml  (has PLACEHOLDER_ALTER_ID in the filter)
        """
        return self._fetch(
            'utils/cdc/ledger_cdc.xml',
            'Ledgers CDC',
            company_name,
            alter_id=last_alter_id,
            debug=debug,
        )

    def fetch_groups(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Fetch ALL groups from Tally."""
        return self._fetch('utils/groups.xml', 'Groups', company_name, from_date, to_date, debug=debug)

    def fetch_groups_cdc(
        self,
        company_name: str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """Fetch groups that changed since last_alter_id."""
        return self._fetch('utils/cdc/groups.xml', 'Groups CDC', company_name, alter_id=last_alter_id, debug=debug)

    # ─────────────────────────────────────────────────────────────────
    # VOUCHERS – SNAPSHOT (first-time full fetch)
    # Call these ONCE to load all historical data.
    # Pass from_date and to_date to limit how much data you pull at once.
    # ─────────────────────────────────────────────────────────────────

    def fetch_sales(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL sales vouchers in the date range."""
        return self._fetch('utils/sales_vouchers.xml', 'Sales', company_name, from_date, to_date, debug=debug)

    def fetch_purchase(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL purchase vouchers in the date range."""
        return self._fetch('utils/purchase_vouchers.xml', 'Purchase', company_name, from_date, to_date, debug=debug)

    def fetch_receipt(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL receipt vouchers in the date range."""
        return self._fetch('utils/receipt_vouchers.xml', 'Receipt', company_name, from_date, to_date, debug=debug)

    def fetch_payment(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL payment vouchers in the date range."""
        return self._fetch('utils/payment_vouchers.xml', 'Payment', company_name, from_date, to_date, debug=debug)

    def fetch_journal(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL journal vouchers in the date range."""
        return self._fetch('utils/journal_vouchers.xml', 'Journal', company_name, from_date, to_date, debug=debug)

    def fetch_contra(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL contra vouchers in the date range."""
        return self._fetch('utils/contra_vouchers.xml', 'Contra', company_name, from_date, to_date, debug=debug)

    def fetch_credit_note(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL credit notes (sales returns) in the date range."""
        return self._fetch('utils/credit_note.xml', 'Credit Note', company_name, from_date, to_date, debug=debug)

    def fetch_debit_note(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """Full snapshot: fetch ALL debit notes (purchase returns) in the date range."""
        return self._fetch('utils/debit_note.xml', 'Debit Note', company_name, from_date, to_date, debug=debug)

    # ─────────────────────────────────────────────────────────────────
    # VOUCHERS – CDC (incremental, every run after first time)
    # Pass last_alter_id = the max AlterID you saved from the last sync.
    # Tally returns only records created/modified after that point.
    # ─────────────────────────────────────────────────────────────────

    def fetch_sales_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only sales vouchers with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/sales_cdc.xml', 'Sales CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_purchase_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only purchase vouchers with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/purchase_cdc.xml', 'Purchase CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_receipt_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only receipt vouchers with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/receipt_cdc.xml', 'Receipt CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_payment_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only payment vouchers with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/payment_cdc.xml', 'Payment CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_journal_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only journal vouchers with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/journal_cdc.xml', 'Journal CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_contra_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only contra vouchers with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/contra_cdc.xml', 'Contra CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_credit_note_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only credit notes with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/credit_cdc.xml', 'Credit Note CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_debit_note_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC: fetch only debit notes with AlterID > last_alter_id."""
        return self._fetch('utils/cdc/debit_cdc.xml', 'Debit Note CDC', company_name, alter_id=last_alter_id, debug=debug)

    # ─────────────────────────────────────────────────────────────────
    # REPORTS (no CDC – always fetched fresh)
    # ─────────────────────────────────────────────────────────────────

    def fetch_trial_balance(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/reports/tb.xml', 'Trial Balance', company_name, from_date, to_date, debug=debug)

    def fetch_balance_sheet(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/reports/balance_sheet.xml', 'Balance Sheet', company_name, from_date, to_date, debug=debug)

    def fetch_profit_loss(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/reports/profit_loss.xml', 'Profit & Loss', company_name, from_date, to_date, debug=debug)

    # ─────────────────────────────────────────────────────────────────
    # DATE UTILITIES
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def parse_tally_date(date_str: str) -> Optional[datetime]:
        """Convert Tally's YYYYMMDD string → datetime. Returns None on failure."""
        try:
            if date_str and date_str != 'N/A':
                return datetime.strptime(date_str, '%Y%m%d')
        except Exception:
            pass
        return None

    @staticmethod
    def format_tally_date(dt: datetime) -> Optional[str]:
        """Convert datetime → Tally's YYYYMMDD string."""
        return dt.strftime('%Y%m%d') if dt else None

    # ─────────────────────────────────────────────────────────────────
    # CLEANUP
    # ─────────────────────────────────────────────────────────────────

    def close(self):
        if hasattr(self, 'session'):
            self.session.close()
            logger.info('TallyConnector session closed')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()