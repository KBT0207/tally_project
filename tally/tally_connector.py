import requests
import json
import xml.etree.ElementTree as ET
import re
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
            response = requests.post(url=self.url, headers=self.header, data=xml_payload)

            if response.status_code == 200:
                root = ET.fromstring(response.content)
                print(root.text)
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
                return all_companies
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []
        

    def get_ledger(self, company_name):
        try:
            logger.info(f'Fetching all ledger masters for company: {company_name}')
            print("⏳ Please wait... This may take 30-60 seconds for large data")
            
            tree = ET.parse('utils/ledger.xml')
            xml_payload = ET.tostring(tree.getroot(), encoding='utf-8')
            
            response = requests.post(
                url=self.url, 
                headers=self.header, 
                data=xml_payload,
                timeout=300
            )
            
            if response.status_code != 200:
                logger.error(f'Failed to fetch ledgers. Status: {response.status_code}')
                return []
            
            print("✓ Data received. Processing...")
            
            content = response.content.decode('utf-8', errors='ignore')
            content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
            content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', content)
            
            with open('ledger_response.xml', 'w', encoding='utf-8') as f:
                f.write(content)
            
            root = ET.fromstring(content.encode('utf-8'))
            ledgers = root.findall(".//LEDGER")
            
            print(f"✓ Found {len(ledgers)} ledgers. Extracting details...")
            
            all_ledgers = []
            
            for idx, ledger in enumerate(ledgers, 1):
                if idx % 100 == 0:
                    print(f"  Processing... {idx}/{len(ledgers)}")
                
                name = ledger.get('NAME', '')
                
                parent = ledger.find("PARENT")
                guid = ledger.find("GUID")
                opening_balance = ledger.find("OPENINGBALANCE")
                closing_balance = ledger.find("CLOSINGBALANCE")
                
                address_list = ledger.findall(".//ADDRESS")
                address = ", ".join([a.text for a in address_list if a.text]) if address_list else ""
                
                mailing_name_elem = ledger.find(".//MAILINGNAME")
                mobile = ledger.find("LEDGERPHONE") or ledger.find("PHONE")
                email = ledger.find("EMAIL") or ledger.find("LEDGEREMAIL")
                
                state = ledger.find("LEDSTATENAME") or ledger.find("STATENAME")
                country = ledger.find("COUNTRYNAME")
                pincode = ledger.find("PINCODE")
                
                gstin = ledger.find("PARTYGSTIN") or ledger.find("GSTIN")
                pan = ledger.find("INCOMETAXNUMBER") or ledger.find("PAN")
                gst_registration = ledger.find("GSTREGISTRATIONTYPE")
                
                bank_name = ledger.find("BANKNAME")
                bank_account = ledger.find("BANKACCHOLDERNAME") or ledger.find("ACCOUNTNUMBER")
                ifsc = ledger.find("IFSCODE") or ledger.find("BANKIFSCCODE")
                branch = ledger.find("BRANCHNAME")
                
                ledger_data = {
                    "name": name,
                    "parent": parent.text if parent is not None else "",
                    "guid": guid.text if guid is not None else "",
                    "opening_balance": opening_balance.text if opening_balance is not None else "0",
                    "closing_balance": closing_balance.text if closing_balance is not None else "0",
                    "address": address,
                    "mailing_name": mailing_name_elem.text if mailing_name_elem is not None else "",
                    "mobile": mobile.text if mobile is not None else "",
                    "email": email.text if email is not None else "",
                    "state": state.text if state is not None else "",
                    "country": country.text if country is not None else "",
                    "pincode": pincode.text if pincode is not None else "",
                    "gstin": gstin.text if gstin is not None else "",
                    "pan": pan.text if pan is not None else "",
                    "gst_registration_type": gst_registration.text if gst_registration is not None else "",
                    "bank_name": bank_name.text if bank_name is not None else "",
                    "bank_account": bank_account.text if bank_account is not None else "",
                    "ifsc_code": ifsc.text if ifsc is not None else "",
                    "branch": branch.text if branch is not None else ""
                }
                
                if ledger_data["name"]:
                    all_ledgers.append(ledger_data)
            
            print(f"\n✓ Successfully extracted {len(all_ledgers)} ledgers")
            logger.info(f'Successfully fetched {len(all_ledgers)} ledgers')
            
            return all_ledgers
            
        except Exception as e:
            logger.error(f'Error fetching ledgers: {e}', exc_info=True)
            return []


    def save_ledgers_to_csv(self, ledgers, filename='ledgers_export.csv'):
        try:
            import csv
            from datetime import datetime
            
            if not ledgers:
                print("No ledgers to export")
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ledgers_{timestamp}.csv"
            
            headers = [
                'Name', 'Parent Group', 'GUID', 'Opening Balance', 'Closing Balance',
                'Address', 'Mailing Name', 'Mobile', 'Email', 'State', 'Country',
                'Pincode', 'GSTIN', 'PAN', 'GST Registration Type',
                'Bank Name', 'Bank Account', 'IFSC Code', 'Branch'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                
                for ledger in ledgers:
                    row = [
                        ledger.get('name', ''),
                        ledger.get('parent', ''),
                        ledger.get('guid', ''),
                        ledger.get('opening_balance', '0'),
                        ledger.get('closing_balance', '0'),
                        ledger.get('address', ''),
                        ledger.get('mailing_name', ''),
                        ledger.get('mobile', ''),
                        ledger.get('email', ''),
                        ledger.get('state', ''),
                        ledger.get('country', ''),
                        ledger.get('pincode', ''),
                        ledger.get('gstin', ''),
                        ledger.get('pan', ''),
                        ledger.get('gst_registration_type', ''),
                        ledger.get('bank_name', ''),
                        ledger.get('bank_account', ''),
                        ledger.get('ifsc_code', ''),
                        ledger.get('branch', '')
                    ]
                    writer.writerow(row)
            
            print(f"\n✓ CSV exported successfully: {filename}")
            print(f"  Total records: {len(ledgers)}")
            logger.info(f'Exported {len(ledgers)} ledgers to {filename}')
            
            return filename
            
        except Exception as e:
            logger.error(f'Error exporting to CSV: {e}')
            return None


