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
        if isinstance(xml_content, bytes):
            xml_content = xml_content.decode('utf-8', errors='ignore')
        
        xml_content = re.sub(r'&#([0-8]|1[1-2]|1[4-9]|2[0-9]|3[0-1]);', '', xml_content)
        xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', xml_content)
        
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
                self._save_debug_file(
                    response.content,
                    f'debug_{data_type.lower().replace(" ", "_")}_response',
                    company_name
                )
            
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
                    company_data = self._extract_company_data(company)
                    
                    if company_data["name"] and company_data["name"].strip():
                        all_companies.append(company_data)
                
                logger.info(f'Found {len(all_companies)} companies with detailed information')
                return all_companies
                
            return []
            
        except Exception as e:
            logger.error(f'Error fetching company list: {e}', exc_info=True)
            return []
    
    @staticmethod
    def _extract_company_data(company: ET.Element) -> Dict[str, Any]:
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