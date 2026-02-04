import requests
import json
import xml.etree.ElementTree as ET
import re
from datetime import datetime, date
from logging_config import logger
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment

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
            else:
                self.status = 'Disconnected'
                logger.warning(
                    f'Unexpected response from Tally at {self.url}. '
                    f'Status code: {response.status_code}, Response: {response.text}'
                )

        except requests.exceptions.ConnectionError as e:
            self.status = 'Disconnected'
            logger.error(f'ConnectionError while connecting to Tally at {self.url}: {e}', exc_info=True)
        except requests.exceptions.Timeout as e:
            self.status = 'Disconnected'
            logger.error(f'Timeout while connecting to Tally at {self.url}: {e}', exc_info=True)
        except Exception as e:
            self.status = 'Disconnected'
            logger.error(f'Unexpected error while connecting to Tally at {self.url}: {e}', exc_info=True)

    def get_company_list(self):
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
                        all_companies.append({
                            "name": name,
                            "start_from": start,
                            "gstin": gst,
                        })
                
                logger.info(f'Found {len(all_companies)} companies')
                return all_companies
            return []
        except Exception as e:
            logger.error(f'Error fetching company list: {e}', exc_info=True)
            return []

    def _get_text(self, element, tag, default=""):
        """Helper method to safely extract text from XML element"""
        if element is None:
            return default
        elem = element.find(tag)
        return elem.text.strip() if elem is not None and elem.text else default

    def get_ledger(self, company_name):
        try:
            logger.info(f'Fetching ALL ledgers for company: {company_name}')
            
            all_ledgers = []
            
            tree = ET.parse('utils/ledger.xml')
            xml_payload = ET.tostring(tree.getroot(), encoding='utf-8')
            
            start_time = datetime.now()
            
            try:
                response = requests.post(
                    url=self.url, 
                    headers=self.header, 
                    data=xml_payload,
                    timeout=900,
                    stream=False
                )
            except requests.exceptions.Timeout:
                logger.error(f'Timeout (900s) fetching ledgers for {company_name}')
                return []
            
            if response.status_code != 200:
                logger.error(f'Failed to fetch ledgers. Status: {response.status_code}')
                return []
            
            debug_file = f"tally_ledger_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
            with open(debug_file, 'wb') as f:
                f.write(response.content)
            logger.info(f'Saved XML response to {debug_file}')
            
            try:
                content = response.content.decode('utf-8', errors='ignore')
            except Exception as e:
                logger.error(f'Error decoding response: {e}')
                return []
            
            content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
            content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', content)
            
            try:
                root = ET.fromstring(content.encode('utf-8'))
            except ET.ParseError as e:
                logger.error(f'XML Parse Error: {e}')
                return []
            
            ledgers = root.findall(".//LEDGER")
            
            if not ledgers:
                logger.info(f'No ledgers found for company: {company_name}')
                return []
            
            for idx, ledger in enumerate(ledgers, 1):
                try:
                    ledger_data = {}
                    
                    # Dynamically extract ALL fields from the ledger element
                    for child in ledger:
                        tag = child.tag.replace('{TallyUDF}', 'UDF_')
                        
                        # Handle simple text fields
                        if len(child) == 0 and child.text:
                            ledger_data[tag] = child.text.strip()
                        elif len(child) == 0:
                            ledger_data[tag] = ""
                    
                    # Add ledger attributes
                    for attr_name, attr_value in ledger.attrib.items():
                        ledger_data[f"ATTR_{attr_name}"] = attr_value
                    
                    # Extract ADDRESS.LIST items
                    address_list = ledger.find(".//ADDRESS.LIST[@TYPE='String']")
                    if address_list is not None:
                        addresses = address_list.findall("ADDRESS")
                        for i, addr in enumerate(addresses, 1):
                            ledger_data[f"ADDRESS_LINE_{i}"] = addr.text.strip() if addr.text else ""
                    
                    # Extract MAILINGNAME.LIST
                    mailing_list = ledger.find(".//MAILINGNAME.LIST[@TYPE='String']")
                    if mailing_list is not None:
                        mailing_names = mailing_list.findall("MAILINGNAME")
                        for i, mn in enumerate(mailing_names, 1):
                            ledger_data[f"MAILINGNAME_{i}"] = mn.text.strip() if mn.text else ""
                    
                    # Extract LEDMULTIADDRESSLIST (Shipping Address)
                    ship_multi = ledger.find(".//LEDMULTIADDRESSLIST.LIST")
                    if ship_multi is not None:
                        for child in ship_multi:
                            if child.tag == "ADDRESS.LIST":
                                ship_addresses = child.findall("ADDRESS")
                                for i, addr in enumerate(ship_addresses, 1):
                                    ledger_data[f"SHIP_ADDRESS_LINE_{i}"] = addr.text.strip() if addr.text else ""
                            elif len(child) == 0 and child.text:
                                ledger_data[f"SHIP_{child.tag}"] = child.text.strip()
                            elif len(child) == 0:
                                ledger_data[f"SHIP_{child.tag}"] = ""
                    
                    # Extract PAYMENTDETAILS.LIST
                    payment_details = ledger.find(".//PAYMENTDETAILS.LIST")
                    if payment_details is not None:
                        for child in payment_details:
                            if len(child) == 0 and child.text:
                                ledger_data[f"PAYMENT_{child.tag}"] = child.text.strip()
                            elif len(child) == 0:
                                ledger_data[f"PAYMENT_{child.tag}"] = ""
                    
                    # Extract LANGUAGENAME.LIST (Alternative names)
                    lang_name = ledger.find(".//LANGUAGENAME.LIST")
                    if lang_name is not None:
                        # Get LANGUAGEID
                        lang_id = lang_name.find("LANGUAGEID")
                        if lang_id is not None and lang_id.text:
                            ledger_data["LANGUAGEID"] = lang_id.text.strip()
                        
                        # Get all alternative names
                        name_list = lang_name.find(".//NAME.LIST[@TYPE='String']")
                        if name_list is not None:
                            names = name_list.findall("NAME")
                            for i, name in enumerate(names, 1):
                                ledger_data[f"ALT_NAME_{i}"] = name.text.strip() if name.text else ""
                    
                    # Extract DEDUCTINSAMEVCHRULES.LIST
                    deduct_rules = ledger.find(".//DEDUCTINSAMEVCHRULES.LIST")
                    if deduct_rules is not None:
                        nature = deduct_rules.find("NATUREOFPAYMENT")
                        if nature is not None and nature.text:
                            ledger_data["DEDUCT_NATURE_OF_PAYMENT"] = nature.text.strip()
                    
                    # Extract OLD AUDIT ENTRY IDS
                    old_audit = ledger.find(".//OLDAUDITENTRYIDS.LIST[@TYPE='Number']")
                    if old_audit is not None:
                        audit_ids = old_audit.findall("OLDAUDITENTRYIDS")
                        for i, audit_id in enumerate(audit_ids, 1):
                            ledger_data[f"OLD_AUDIT_ENTRY_ID_{i}"] = audit_id.text.strip() if audit_id.text else ""
                    
                    if ledger_data.get('ATTR_NAME') or ledger_data.get('NAME'):
                        all_ledgers.append(ledger_data)
                
                except Exception as e:
                    logger.warning(f'Error processing ledger at index {idx}: {e}')
                    continue
            
            total_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f'Successfully fetched {len(all_ledgers)} ledgers for {company_name} in {total_time:.1f}s')
            
            return all_ledgers
            
        except Exception as e:
            logger.error(f'Unexpected error for {company_name}: {e}', exc_info=True)
            return []

    def save_ledgers_to_excel(self, ledgers, company_name=""):
        """Export ledgers to Excel with ALL fields dynamically"""
        try:
            if not ledgers:
                logger.warning('Attempted to export empty ledger list to Excel')
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_company_name = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_company_name = safe_company_name[:50]
            
            if safe_company_name:
                filename = f"ledgers_complete_{safe_company_name}_{timestamp}.xlsx"
            else:
                filename = f"ledgers_complete_{timestamp}.xlsx"
            
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Ledgers Complete"
            
            # Collect all unique keys across all ledgers
            all_keys = set()
            for ledger in ledgers:
                all_keys.update(ledger.keys())
            
            # Sort keys for consistent column order
            headers = sorted(list(all_keys))
            
            # Write headers with formatting
            for col_idx, header in enumerate(headers, 1):
                cell = sheet.cell(row=1, column=col_idx, value=header.replace('_', ' ').title())
                cell.font = Font(bold=True, color='FFFFFF', size=10)
                cell.fill = PatternFill(start_color='1F4E78', end_color='1F4E78', fill_type='solid')
                cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            
            # Write data
            for row_idx, ledger in enumerate(ledgers, 2):
                for col_idx, key in enumerate(headers, 1):
                    value = ledger.get(key, '')
                    cell = sheet.cell(row=row_idx, column=col_idx, value=value)
                    cell.alignment = Alignment(vertical='top', wrap_text=False)
            
            # Auto-adjust column widths
            for column in sheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                adjusted_width = min(max_length + 2, 50)
                sheet.column_dimensions[column_letter].width = max(adjusted_width, 12)
            
            # Freeze header row
            sheet.freeze_panes = 'A2'
            
            wb.save(filename)
            
            logger.info(f'Exported {len(ledgers)} ledgers with {len(headers)} columns to {filename}')
            print(f"\n📊 Excel file created with {len(headers)} columns!")
            
            return filename
            
        except Exception as e:
            logger.error(f'Error exporting to Excel: {e}', exc_info=True)
            return None