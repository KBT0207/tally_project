import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from logging_config import logger


class TallyConnector:
    
    def __init__(self, host='localhost', port=9000):
        self.host = host
        self.port = port
        self.url = f'http://{host}:{port}'
        self.header = {'Content-Type': 'text/xml; charset=utf-8'}
        self.status = 'Disconnected'
        
        logger.info(f'Initializing TallyConnector for {self.url}')
        self.connect()

    def connect(self):
        try:
            logger.info(f'Attempting to connect to Tally at {self.url}...')
            response = requests.post(url=self.url, headers=self.header, timeout=60)
            
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

    def fetch_all_companies(self):
        try:
            tree = ET.parse('utils/company.xml')
            xml_payload = ET.tostring(tree.getroot(), encoding='utf-8')
            response = requests.post(url=self.url, headers=self.header, data=xml_payload, timeout=120)

            if response.status_code == 200:
                root = ET.fromstring(response.content)
                all_companies = []

                for company in root.findall(".//COMPANY"):
                    name = company.find("NAME").text if company.find("NAME") is not None else "N/A"
                    start = company.find("STARTINGFROM").text if company.find("STARTINGFROM") is not None else "N/A"
                    gst = company.find("GSTIN").text if company.find("GSTIN") is not None else "N/A"

                    if name != "N/A" and name.strip() != "":
                        all_companies.append({"name": name, "start_from": start, "gstin": gst})
                
                logger.info(f'Found {len(all_companies)} companies')
                return all_companies
            return []
        except Exception as e:
            logger.error(f'Error fetching company list: {e}', exc_info=True)
            return []

    def fetch_all_ledgers(self, company_name, from_date=None, to_date=None, debug=False):
        try:
            logger.info(f'Fetching ALL ledgers XML for company: {company_name}')
            
            tree = ET.parse('utils/ledger.xml')
            root = tree.getroot()
            
            for sv_elem in root.iter('SVCURRENTCOMPANY'):
                sv_elem.text = company_name
            
            if from_date:
                for sv_elem in root.iter('SVFROMDATE'):
                    sv_elem.text = from_date
            
            if to_date:
                for sv_elem in root.iter('SVTODATE'):
                    sv_elem.text = to_date
            
            logger.info(f'Date range: {from_date} to {to_date}')
            
            xml_payload = ET.tostring(root, encoding='utf-8')
            
            if debug:
                debug_file = f"debug_ledgers_request_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(debug_file, 'wb') as f:
                    f.write(xml_payload)
                logger.info(f'Saved request XML to {debug_file}')
            
            start_time = datetime.now()
            
            try:
                response = requests.post(url=self.url, headers=self.header, data=xml_payload, 
                                       timeout=(60, 1800), stream=False)
            except requests.exceptions.Timeout:
                logger.error(f'Timeout fetching ledgers for {company_name}')
                return None
            
            if response.status_code != 200:
                logger.error(f'Failed to fetch ledgers. Status: {response.status_code}')
                return None
            
            if debug:
                debug_response = f"debug_ledgers_response_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(debug_response, 'wb') as f:
                    f.write(response.content)
                logger.info(f'Saved response XML to {debug_response}')
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f'Successfully fetched ledgers XML for {company_name} in {total_time:.1f}s')
            
            return response.content
            
        except Exception as e:
            logger.error(f'Unexpected error fetching ledgers for {company_name}: {e}', exc_info=True)
            return None

    def fetch_all_groups(self, company_name, debug=False):
        try:
            logger.info(f'Fetching ALL Groups XML for company: {company_name}')
            
            tree = ET.parse('utils/groups.xml')
            root = tree.getroot()
            
            for sv_elem in root.iter('SVCURRENTCOMPANY'):
                sv_elem.text = company_name
            
            xml_payload = ET.tostring(root, encoding='utf-8')
            
            if debug:
                debug_file = f"debug_groups_request_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(debug_file, 'wb') as f:
                    f.write(xml_payload)
                logger.info(f'Saved request XML to {debug_file}')
            
            start_time = datetime.now()
            
            try:
                response = requests.post(url=self.url, headers=self.header, data=xml_payload, 
                                       timeout=(60, 1800), stream=False)
            except requests.exceptions.Timeout:
                logger.error(f'Timeout fetching groups for {company_name}')
                return None
            
            if response.status_code != 200:
                logger.error(f'Failed to fetch groups. Status: {response.status_code}')
                return None
            
            if debug:
                debug_response = f"debug_groups_response_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(debug_response, 'wb') as f:
                    f.write(response.content)
                logger.info(f'Saved response XML to {debug_response}')
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f'Successfully fetched groups XML for {company_name} in {total_time:.1f}s')
            
            return response.content
            
        except Exception as e:
            logger.error(f'Unexpected error fetching groups for {company_name}: {e}', exc_info=True)
            return None

    def fetch_all_sales_vouchers(self, company_name, from_date=None, to_date=None, debug=False):
        try:
            logger.info(f'Fetching Sales Vouchers XML for company: {company_name}')
            
            tree = ET.parse('utils/sales_vouchers.xml')
            root = tree.getroot()
            
            for sv_elem in root.iter('SVCURRENTCOMPANY'):
                sv_elem.text = company_name
            
            if from_date:
                for sv_elem in root.iter('SVFROMDATE'):
                    sv_elem.text = from_date
            
            if to_date:
                for sv_elem in root.iter('SVTODATE'):
                    sv_elem.text = to_date
            
            logger.info(f'Date range: {from_date} to {to_date}')
            
            xml_payload = ET.tostring(root, encoding='utf-8')
            
            if debug:
                debug_file = f"debug_sales_request_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(debug_file, 'wb') as f:
                    f.write(xml_payload)
                logger.info(f'Saved request XML to {debug_file}')
            
            start_time = datetime.now()
            
            try:
                response = requests.post(url=self.url, headers=self.header, data=xml_payload, 
                                       timeout=(60, 1800), stream=False)
            except requests.exceptions.Timeout:
                logger.error(f'Timeout fetching sales vouchers for {company_name}')
                return None
            
            if response.status_code != 200:
                logger.error(f'Failed to fetch sales vouchers. Status: {response.status_code}')
                return None
            
            if debug:
                debug_response = f"debug_sales_response_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
                with open(debug_response, 'wb') as f:
                    f.write(response.content)
                logger.info(f'Saved response XML to {debug_response}')
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f'Successfully fetched sales vouchers XML for {company_name} in {total_time:.1f}s')
            
            return response.content
            
        except Exception as e:
            logger.error(f'Unexpected error fetching sales vouchers for {company_name}: {e}', exc_info=True)
            return None

    def parse_tally_date(self, date_str):
        try:
            if date_str and date_str != "N/A":
                return datetime.strptime(date_str, '%Y%m%d')
        except:
            pass
        return None

    def format_tally_date(self, dt):
        if dt:
            return dt.strftime('%Y%m%d')
        return None