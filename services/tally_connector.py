import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from typing import Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logging_config import logger


class TallyConnector:
    
    _xml_template_cache: Dict[str, ET.Element] = {}
    
    @staticmethod
    def sanitize_xml(xml_content):
        if isinstance(xml_content, bytes):
            try:
                xml_content = xml_content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    xml_content = xml_content.decode('windows-1252')
                except UnicodeDecodeError:
                    xml_content = xml_content.decode('latin-1')
        
        xml_content = str(xml_content)
        
        detected_symbols = []
        currency_checks = {
            '$': 'USD/AUD/CAD/etc',
            '£': 'GBP',
            '€': 'EUR',
            '¥': 'JPY/CNY',
            '₹': 'INR',
            '₨': 'INR/PKR',
            '₩': 'KRW',
            '₱': 'PHP',
            '₽': 'RUB',
            '₺': 'TRY',
            '₪': 'ILS',
            '₦': 'NGN',
            '฿': 'THB',
            '₫': 'VND',
            '?': 'Corrupted symbol'
        }
        
        for symbol, currency in currency_checks.items():
            if symbol in xml_content:
                detected_symbols.append(f"{symbol}({currency})")
        
        if detected_symbols:
            logger.debug(f"Currency symbols found: {', '.join(detected_symbols)}")
        
        xml_content = re.sub(r'&#([0-8]|1[1-2]|1[4-9]|2[0-9]|3[0-1]);', '', xml_content)
        xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_content)
        xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)', '&amp;', xml_content)
        
        return xml_content.encode('utf-8')
    
    def __init__(self, host='localhost', port=9000, timeout=(60, 1800), max_retries=3):
        self.host = host
        self.port = port
        self.url = f'http://{host}:{port}'
        self.header = {'Content-Type': 'text/xml; charset=utf-8'}
        self.status = 'Disconnected'
        self.timeout = timeout
        self.session = self._create_session(max_retries)
        logger.info(f'Initializing TallyConnector for {self.url}')
        self.connect()
    
    def _create_session(self, max_retries=3):
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def connect(self):
        try:
            logger.info(f'Attempting to connect to Tally at {self.url}...')
            response = self.session.post(
                url=self.url,
                headers=self.header,
                timeout=60
            )
            if response.status_code == 200:
                self.status = 'Connected'
                logger.info(f'Successfully connected to Tally at {self.url}')
                return True
            else:
                self.status = 'Disconnected'
                logger.warning(f'Unexpected response from Tally. Status: {response.status_code}')
                return False
        except Exception as e:
            self.status = 'Disconnected'
            logger.error(f'Error connecting to Tally: {e}', exc_info=True)
            return False
    
    @classmethod
    def _load_xml_template(cls, template_path: str) -> ET.Element:
        if template_path not in cls._xml_template_cache:
            tree = ET.parse(template_path)
            cls._xml_template_cache[template_path] = tree.getroot()
            logger.debug(f'Cached XML template: {template_path}')
        return ET.fromstring(ET.tostring(cls._xml_template_cache[template_path]))
    
    def _prepare_xml_request(
        self,
        template_path: str,
        company_name: str,
        from_date=None,
        to_date=None,
        alter_id=None
    ) -> bytes:
        root = self._load_xml_template(template_path)

        for sv_elem in root.iter('SVCURRENTCOMPANY'):
            sv_elem.text = company_name
        if from_date:
            for sv_elem in root.iter('SVFROMDATE'):
                sv_elem.text = from_date
        if to_date:
            for sv_elem in root.iter('SVTODATE'):
                sv_elem.text = to_date

        xml_str = ET.tostring(root, encoding='unicode')

        if alter_id is not None:
            xml_str = xml_str.replace(
                'PLACEHOLDER_ALTER_ID',
                f'$$Number:$AlterID > {alter_id}'
            )
        else:
            xml_str = xml_str.replace(
                'PLACEHOLDER_ALTER_ID',
                '$$Number:$AlterID > 0'
            )

        return xml_str.encode('utf-8')
    
    def _save_debug_file(self, content: bytes, prefix: str, company_name: str, suffix: str = 'xml'):
        safe_company = company_name.replace(' ', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{prefix}_{safe_company}_{timestamp}.{suffix}"
        with open(filename, 'wb') as f:
            f.write(content)
        logger.info(f'Saved debug file: {filename}')
        return filename
    
    def _fetch_data(
        self,
        template_path: str,
        data_type: str,
        company_name: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        alter_id: Optional[int] = None,
        debug: bool = False
    ) -> Optional[bytes]:
        try:
            logger.info(f'Fetching {data_type} for company: {company_name}')
            
            xml_payload = self._prepare_xml_request(
                template_path,
                company_name,
                from_date,
                to_date,
                alter_id
            )
            
            if from_date and to_date:
                logger.info(f'Date range: {from_date} to {to_date}')
            
            if alter_id is not None:
                logger.info(f'CDC mode — fetching records with AlterID > {alter_id}')
                # Verify the filter was correctly injected into the XML
                try:
                    payload_str = xml_payload.decode('utf-8') if isinstance(xml_payload, bytes) else xml_payload
                    import re as _re
                    filter_match = _re.search(r'NAME=["\']FilterByAlterID["\'][^>]*>(.*?)<', payload_str)
                    if filter_match:
                        logger.debug(f'[ALTER_ID CHECK] FilterByAlterID in XML = "{filter_match.group(1).strip()}"')
                    else:
                        logger.warning(f'[ALTER_ID CHECK] FilterByAlterID tag NOT FOUND in XML payload — filter may not be applied!')
                except Exception as _e:
                    logger.debug(f'[ALTER_ID CHECK] Could not inspect XML payload: {_e}')

            if debug:
                self._save_debug_file(
                    xml_payload,
                    f'debug_{data_type.lower().replace(" ", "_")}_request',
                    company_name
                )
            
            start_time = datetime.now()
            
            try:
                response = self.session.post(
                    url=self.url,
                    headers=self.header,
                    data=xml_payload,
                    timeout=self.timeout,
                    stream=False
                )
            except requests.exceptions.Timeout:
                logger.error(f'Timeout fetching {data_type} for {company_name}')
                return None
            
            if response.status_code != 200:
                logger.error(f'Failed to fetch {data_type}. Status: {response.status_code}')
                return None
            
            if debug:
                raw_file = self._save_debug_file(
                    response.content,
                    f'debug_{data_type.lower().replace(" ", "_")}_response_raw',
                    company_name
                )
                logger.info(f'Saved RAW XML: {raw_file}')
                
                sanitized_content = self.sanitize_xml(response.content)
                sanitized_file = self._save_debug_file(
                    sanitized_content,
                    f'debug_{data_type.lower().replace(" ", "_")}_response',
                    company_name
                )
                logger.info(f'Saved SANITIZED XML: {sanitized_file}')
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f'Fetched {data_type} for {company_name} in {total_time:.1f}s')

            # Debug: parse response and show alter_id range to verify filter worked
            if alter_id is not None:
                try:
                    sanitized = self.sanitize_xml(response.content)
                    import xml.etree.ElementTree as _ET
                    root_resp = _ET.fromstring(sanitized)
                    all_alter_ids = [
                        int(elem.text)
                        for elem in root_resp.iter('ALTERID')
                        if elem.text and elem.text.strip().isdigit()
                    ]
                    if all_alter_ids:
                        min_id = min(all_alter_ids)
                        max_id = max(all_alter_ids)
                        count  = len(all_alter_ids)
                        logger.debug(
                            f'[ALTER_ID CHECK] Response has {count} record(s) | '
                            f'AlterID range: {min_id} → {max_id} | '
                            f'Filter was: > {alter_id} | '
                            f'{"✓ All above threshold" if min_id > alter_id else "✗ WARNING: Some records AT OR BELOW threshold — filter may not be working!"}'
                        )
                    else:
                        logger.debug(f'[ALTER_ID CHECK] No ALTERID tags found in response (empty result or different tag name)')
                except Exception as _e:
                    logger.debug(f'[ALTER_ID CHECK] Could not inspect response: {_e}')

            return self.sanitize_xml(response.content)
            
        except Exception as e:
            logger.error(f'Unexpected error fetching {data_type} for {company_name}: {e}', exc_info=True)
            return None

    def fetch_all_companies(self):
        try:
            tree = ET.parse('utils/company.xml')
            xml_payload = ET.tostring(tree.getroot(), encoding='utf-8')
            response = self.session.post(
                url=self.url,
                headers=self.header,
                data=xml_payload,
                timeout=120
            )
            if response.status_code == 200:
                sanitized_content = self.sanitize_xml(response.content)
                root = ET.fromstring(sanitized_content)
                all_companies = []
                for company in root.findall(".//COMPANY"):
                    company_info = self._extract_company_details(company)
                    all_companies.append(company_info)
                logger.info(f"Found {len(all_companies)} companies")
                return all_companies
            else:
                logger.error(f"Failed to fetch companies. Status code: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error in fetch_all_companies: {e}", exc_info=True)
            return []

    def _extract_company_details(self, company) -> Dict[str, Any]:
        return {
            "guid": company.findtext("GUID", default=""),
            "name": company.findtext("NAME", default=""),
            "formal_name": company.findtext("BASICCOMPANYFORMALNAME", default=""),
            "company_number": company.findtext("COMPANYNUMBER", default=""),
            "starting_from": company.findtext("STARTINGFROM", default=""),
            "books_from": company.findtext("BOOKSFROM", default=""),
            "audited_upto": company.findtext("AUDITEDUPTO", default=""),
        }

    def fetch_all_ledgers(self, company_name: str, from_date: Optional[str] = None,
                          to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/ledger.xml', 'Ledgers', company_name, from_date, to_date, debug=debug)

    def fetch_all_groups(self, company_name: str, from_date: Optional[str] = None,
                         to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/groups.xml', 'Groups', company_name, from_date, to_date, debug=debug)

    def fetch_all_sales_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                  to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/sales_vouchers.xml', 'Sales Vouchers', company_name, from_date, to_date, debug=debug)

    def fetch_all_purchase_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                    to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/purchase_vouchers.xml', 'Purchase Vouchers', company_name, from_date, to_date, debug=debug)

    def fetch_all_receipt_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/receipt_vouchers.xml', 'Receipt Vouchers', company_name, from_date, to_date, debug=debug)

    def fetch_all_payment_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/payment_vouchers.xml', 'Payment Vouchers', company_name, from_date, to_date, debug=debug)

    def fetch_all_journal_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/journal_vouchers.xml', 'Journal Vouchers', company_name, from_date, to_date, debug=debug)

    def fetch_all_contra_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                  to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/contra_vouchers.xml', 'Contra Vouchers', company_name, from_date, to_date, debug=debug)

    def fetch_all_sales_return(self, company_name: str, from_date: Optional[str] = None,
                               to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/credit_note.xml', 'Sales Return', company_name, from_date, to_date, debug=debug)

    def fetch_all_purchase_return(self, company_name: str, from_date: Optional[str] = None,
                                  to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/debit_note.xml', 'Purchase Return', company_name, from_date, to_date, debug=debug)

    def fetch_trial_balance(self, company_name: str, from_date: Optional[str] = None,
                            to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/reports/tb.xml', 'Trial Balance', company_name, from_date, to_date, debug=debug)

    def fetch_balancesheet(self, company_name: str, from_date: Optional[str] = None,
                           to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/reports/balance_sheet.xml', 'Balance Sheet', company_name, from_date, to_date, debug=debug)

    def fetch_sales_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/sales_cdc.xml', 'Sales CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_purchase_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/purchase_cdc.xml', 'Purchase CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_receipt_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/receipt_cdc.xml', 'Receipt CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_payment_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/payment_cdc.xml', 'Payment CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_journal_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/journal_cdc.xml', 'Journal CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_contra_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/contra_cdc.xml', 'Contra CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_credit_note_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/credit_cdc.xml', 'Credit Note CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_debit_note_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/debit_cdc.xml', 'Debit Note CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_ledger_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/ledger.xml', 'Ledger CDC', company_name, alter_id=last_alter_id, debug=debug)

    def fetch_groups_cdc(self, company_name: str, last_alter_id: int, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data('utils/cdc/groups.xml', 'Groups CDC', company_name, alter_id=last_alter_id, debug=debug)

    @staticmethod
    def parse_tally_date(date_str: str) -> Optional[datetime]:
        try:
            if date_str and date_str != "N/A":
                return datetime.strptime(date_str, '%Y%m%d')
        except Exception:
            pass
        return None

    @staticmethod
    def format_tally_date(dt: datetime) -> Optional[str]:
        if dt:
            return dt.strftime('%Y%m%d')
        return None

    def close(self):
        if hasattr(self, 'session'):
            self.session.close()
            logger.info('Closed TallyConnector session')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()