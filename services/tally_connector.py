import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from functools import lru_cache
from typing import Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logging_config import logger


class TallyConnector:
    
    _xml_template_cache: Dict[str, ET.Element] = {}
    
    @staticmethod
    def sanitize_xml(xml_content):
        """
        Sanitize XML content while PRESERVING ALL CURRENCY SYMBOLS AND UNICODE CHARACTERS.
        
        This method is designed to handle ALL global currencies safely:
        - ASCII currencies: $ (USD)
        - Latin-1 currencies: £ (GBP), € (EUR in some encodings)
        - Windows-1252: € (EUR), £ (GBP)
        - UTF-8 Unicode: ₹ ₨ ¥ ₩ ₱ ₽ ₺ ₪ ₦ ₵ ₴ ₸ ₫ ฿ 元 ৳ (all Asian/Middle Eastern/African currencies)
        
        STRATEGY:
        1. Try UTF-8 first (handles all Unicode currencies correctly)
        2. If UTF-8 fails, try Windows-1252 (common for Tally on Windows, handles € £ well)
        3. If both fail, use Latin-1 (NEVER fails, preserves ALL bytes)
        4. Only remove XML-breaking control characters (0x00-0x1F, 0x7F)
        5. NEVER remove printable characters (all currency symbols are printable)
        
        SAFE for: USD, EUR, GBP, JPY, CNY, INR, CHF, CAD, AUD, NZD, KRW, SGD, HKD,
                  NOK, SEK, DKK, PLN, THB, MYR, IDR, PHP, MXN, BRL, ARS, CLP, COP,
                  ZAR, RUB, TRY, AED, SAR, QAR, KWD, ILS, EGP, PKR, BDT, LKR, NPR,
                  VND, KZT, UAH, NGN, KES, GHS, MAD, TWD, CZK, HUF, RON, BGN, HRK,
                  and ALL other currencies!
        """
        if isinstance(xml_content, bytes):
            # STEP 1: Try UTF-8 first (best for modern Unicode currencies)
            try:
                xml_content = xml_content.decode('utf-8')
                logger.debug("✓ Decoded XML as UTF-8 (best for Unicode currencies)")
                
            except UnicodeDecodeError:
                logger.debug("UTF-8 failed, trying Windows-1252...")
                
                # STEP 2: Try Windows-1252 (common for Tally on Windows)
                # This handles € (0x80), £ (0xA3) and other Western European symbols
                try:
                    xml_content = xml_content.decode('windows-1252')
                    logger.debug("✓ Decoded XML as Windows-1252 (good for €, £)")
                    
                except UnicodeDecodeError:
                    logger.debug("Windows-1252 failed, using Latin-1 fallback...")
                    
                    # STEP 3: Latin-1 fallback (NEVER fails - accepts all bytes)
                    # This preserves ALL bytes but may misinterpret some symbols
                    xml_content = xml_content.decode('latin-1')
                    logger.debug("✓ Decoded XML as Latin-1 (fallback - preserves all bytes)")
        
        # Ensure it's a string
        xml_content = str(xml_content)
        
        # Log detected currency symbols (for debugging)
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
        
        # CRITICAL: Only remove control characters that BREAK XML PARSING
        # These are non-printable control codes that cause XML parse errors
        # DO NOT REMOVE printable characters (includes ALL currency symbols)
        
        # Remove XML entity references for control characters (&#0; to &#31;)
        xml_content = re.sub(r'&#([0-8]|1[1-2]|1[4-9]|2[0-9]|3[0-1]);', '', xml_content)
        
        # Remove raw control characters (0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F)
        # These cause "not well-formed (invalid token)" XML errors
        # Note: We keep 0x09 (tab), 0x0A (newline), 0x0D (carriage return)
        xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_content)
        
        # Fix unescaped ampersands (but preserve existing XML entities)
        # This prevents "not well-formed" errors from unescaped &
        xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)', '&amp;', xml_content)
        
        # Encode back to UTF-8 for XML parsing
        # UTF-8 is the universal XML encoding that handles all Unicode
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
        from_date: Optional[str] = None, 
        to_date: Optional[str] = None
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
        
        return ET.tostring(root, encoding='utf-8')
    
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
        debug: bool = False
    ) -> Optional[bytes]:
        try:
            logger.info(f'Fetching {data_type} XML for company: {company_name}')
            
            xml_payload = self._prepare_xml_request(
                template_path, 
                company_name, 
                from_date, 
                to_date
            )
            
            if from_date and to_date:
                logger.info(f'Date range: {from_date} to {to_date}')
            
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
                # Save RAW response BEFORE any processing
                raw_file = self._save_debug_file(
                    response.content,
                    f'debug_{data_type.lower().replace(" ", "_")}_response_raw',
                    company_name
                )
                logger.info(f'Saved RAW XML (before sanitization): {raw_file}')
                
                # # Save sanitized response AFTER processing
                # sanitized_content = self.sanitize_xml(response.content)
                # sanitized_file = self._save_debug_file(
                #     sanitized_content,
                #     f'debug_{data_type.lower().replace(" ", "_")}_response',
                #     company_name
                # )
                # logger.info(f'Saved SANITIZED XML (after processing): {sanitized_file}')
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f'Successfully fetched {data_type} XML for {company_name} in {total_time:.1f}s')
            
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

                logger.info(f"Found {len(all_companies)} companies with detailed information")
                return all_companies
            else:
                logger.error(f"Failed to fetch companies. Status code: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error in fetch_all_companies: {e}", exc_info=True)
            return []

    def _extract_company_details(self, company) -> Dict[str, Any]:
        return {
            "name": company.findtext("NAME", default=""),
            "formal_name": company.findtext("BASICCOMPANYFORMALNAME", default=""),
            "guid": company.findtext("GUID", default=""),
            "company_number": company.findtext("COMPANYNUMBER", default=""),
            "starting_from": company.findtext("STARTINGFROM", default=""),
            "books_from": company.findtext("BOOKSFROM", default=""),
            "audited_upto": company.findtext("AUDITEDUPTO", default=""),
            "email": company.findtext("EMAIL", default=""),
            "phone_number": company.findtext("PHONENUMBER", default=""),
            "fax_number": company.findtext("FAXNUMBER", default=""),
            "website": company.findtext("WEBSITE", default=""),
            "contact_person": company.findtext("COMPANYCONTACTPERSON", default=""),
            "contact_number": company.findtext("COMPANYCONTACTNUMBER", default=""),
            "country": company.findtext("COUNTRYNAME", default=""),
            "state": company.findtext("STATENAME", default=""),
            "pincode": company.findtext("PINCODE", default=""),
            "gstin": company.findtext("GSTREGISTRATIONNUMBER", default=""),
            "gst_registration_type": company.findtext("GSTREGISTRATIONTYPE", default=""),
            "pan": company.findtext("INCOMETAXNUMBER", default=""),
            "tan": company.findtext("TANUMBER", default=""),
            "vat_tin": company.findtext("VATTINNUMBER", default=""),
            "sales_tax_number": company.findtext("SALESTAXNUMBER", default=""),
            "service_tax_reg": company.findtext("STREGNUMBER", default=""),
            "excise_reg": company.findtext("EXCISEREGN", default=""),
            "cin": company.findtext("XBRLCIN", default=""),
            "export_import_code": company.findtext("EXPORTIMPORTCODE", default=""),
            "pf_code": company.findtext("CMPPFCODE", default=""),
            "esi_code": company.findtext("CMPESICODE", default=""),
            "is_gst_on": company.findtext("ISGSTON", default="No") == "Yes",
            "is_accounting_on": company.findtext("ISACCOUNTINGON", default="No") == "Yes",
            "is_inventory_on": company.findtext("ISINVENTORYON", default="No") == "Yes",
            "is_integrated": company.findtext("ISINTEGRATED", default="No") == "Yes",
            "is_billwise_on": company.findtext("ISBILLWISEON", default="No") == "Yes",
            "is_cost_centres_on": company.findtext("ISCOSTCENTRESON", default="No") == "Yes",
            "is_security_on": company.findtext("ISSECURITYON", default="No") == "Yes",
            "is_payroll_on": company.findtext("ISPAYROLLON", default="No") == "Yes",
            "gst_applicable_date": company.findtext("GSTAPPLICABLEDATE", default=""),
            "type_of_supply": company.findtext("CMPTYPEOFSUPPLY", default=""),
            "is_vat_on": company.findtext("INDIANVAT", default="No") == "Yes",
            "vat_dealer_type": company.findtext("VATDEALERTYPE", default=""),
            "show_bank_details": company.findtext("SHOWBANKDETAILS", default=""),
            "is_ebanking_on": company.findtext("ISEBANKINGON", default="No") == "Yes",
        }

    def fetch_all_ledgers(self, company_name: str, from_date: Optional[str] = None, 
                         to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/ledger.xml',
            'Ledgers',
            company_name,
            from_date,
            to_date,
            debug
        )

    def fetch_all_sales_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                 to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/sales_vouchers.xml',
            'Sales Vouchers',
            company_name,
            from_date,
            to_date,
            debug
        )
    
    def fetch_all_receipt_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/receipt_vouchers.xml',
            'Receipt Vouchers',
            company_name,
            from_date,
            to_date,
            debug
        )

    def fetch_all_payment_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/payment_vouchers.xml',
            'Payment Vouchers',
            company_name,
            from_date,
            to_date,
            debug
        )

    def fetch_all_purchase_voucher(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/purchase_vouchers.xml',
            'Purchase Vouchers',
            company_name,
            from_date,
            to_date,
            debug
        )
    
    def fetch_all_journal_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                   to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/journal_vouchers.xml',
            'Journal Vouchers',
            company_name,
            from_date,
            to_date,
            debug
        )
    
    def fetch_all_contra_vouchers(self, company_name: str, from_date: Optional[str] = None,
                                  to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/contra_vouchers.xml',
            'Contra Vouchers',
            company_name,
            from_date,
            to_date,
            debug
        )
    
    def fetch_all_groups(self, company_name: str, from_date: Optional[str] = None,
                        to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/groups.xml',
            'Groups',
            company_name,
            from_date,
            to_date,
            debug
        )

    def fetch_all_sales_return(self, company_name: str, from_date: Optional[str] = None,
                               to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/credit_note.xml',
            'Sales Return',
            company_name,
            from_date,
            to_date,
            debug
        )

    def fetch_all_purchase_return(self, company_name: str, from_date: Optional[str] = None,
                                  to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/debit_note.xml',
            'Purchase Return',
            company_name,
            from_date,
            to_date,
            debug
        )
    
    def fetch_trial_balance(self, company_name: str, from_date: Optional[str] = None,
                        to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/reports/tb.xml',
            'Trial Balance',
            company_name,
            from_date,
            to_date,
            debug
        )
    

    def fetch_balancesheet(self, company_name: str, from_date: Optional[str] = None,
                        to_date: Optional[str] = None, debug: bool = False) -> Optional[bytes]:
        return self._fetch_data(
            'utils/reports/balance_sheet.xml',
            'Balance Sheet',
            company_name,
            from_date,
            to_date,
            debug
        )

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