import xml.etree.ElementTree as ET
import pandas as pd
import re
from datetime import datetime
from logging_config import logger
import time

try:
    from .currency_extractor import CurrencyExtractor
except ImportError:
    from currency_extractor import CurrencyExtractor

currency_extractor = CurrencyExtractor(default_currency='INR')

CURRENCY_NAMES = {
    'USD': 'US Dollar',
    'EUR': 'Euro',
    'GBP': 'British Pound',
    'JPY': 'Japanese Yen',
    'CNY': 'Chinese Yuan',
    'INR': 'Indian Rupee',
    'CHF': 'Swiss Franc',
    'CAD': 'Canadian Dollar',
    'AUD': 'Australian Dollar',
    'NZD': 'New Zealand Dollar',
    'KRW': 'South Korean Won',
    'SGD': 'Singapore Dollar',
    'HKD': 'Hong Kong Dollar',
    'NOK': 'Norwegian Krone',
    'SEK': 'Swedish Krona',
    'DKK': 'Danish Krone',
    'PLN': 'Polish Zloty',
    'THB': 'Thai Baht',
    'MYR': 'Malaysian Ringgit',
    'IDR': 'Indonesian Rupiah',
    'PHP': 'Philippine Peso',
    'MXN': 'Mexican Peso',
    'BRL': 'Brazilian Real',
    'ARS': 'Argentine Peso',
    'CLP': 'Chilean Peso',
    'COP': 'Colombian Peso',
    'ZAR': 'South African Rand',
    'RUB': 'Russian Ruble',
    'TRY': 'Turkish Lira',
    'AED': 'UAE Dirham',
    'SAR': 'Saudi Riyal',
    'QAR': 'Qatari Riyal',
    'KWD': 'Kuwaiti Dinar',
    'ILS': 'Israeli Shekel',
    'EGP': 'Egyptian Pound',
    'PKR': 'Pakistani Rupee',
    'BDT': 'Bangladeshi Taka',
    'LKR': 'Sri Lankan Rupee',
    'NPR': 'Nepalese Rupee',
    'VND': 'Vietnamese Dong',
    'KZT': 'Kazakhstani Tenge',
    'UAH': 'Ukrainian Hryvnia',
    'NGN': 'Nigerian Naira',
    'KES': 'Kenyan Shilling',
    'GHS': 'Ghanaian Cedi',
    'MAD': 'Moroccan Dirham',
    'TWD': 'New Taiwan Dollar',
    'CZK': 'Czech Koruna',
    'HUF': 'Hungarian Forint',
    'RON': 'Romanian Leu',
    'BGN': 'Bulgarian Lev',
    'HRK': 'Croatian Kuna',
}

class ProcessingTimer:
    def __init__(self, process_name):
        self.process_name = process_name
        self.start_time = None
        self.end_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"Started: {self.process_name}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time
        logger.info(f"Completed: {self.process_name} - Time taken: {elapsed:.2f} seconds")
        
    def get_elapsed_time(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

def get_currency_name(currency_code):
    return CURRENCY_NAMES.get(currency_code, currency_code)

def clean_text(text):
    if not text:
        return ""
    text = str(text)
    text = text.replace('&#13;&#10;', ' ')
    text = text.replace('&#13;', ' ')
    text = text.replace('&#10;', ' ')
    text = text.replace('\r\n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text

def sanitize_xml_content(content):
    if isinstance(content, bytes):
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                content = content.decode('latin-1')
            except UnicodeDecodeError:
                content = content.decode('utf-8', errors='ignore')
    
    content = content.replace('G�', 'G£')
    content = content.replace('\ufffd', '£')
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
    content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)', '&amp;', content)
    
    return content

def extract_numeric_amount(text):
    if not text:
        return "0"
    
    text = str(text)
    final_amount_match = re.search(r'=\s*[?]?\s*([-]?\d+\.?\d*)', text)
    if final_amount_match:
        return final_amount_match.group(1)
    
    numeric_match = re.search(r'([-]?\d+\.?\d*)', text)
    if numeric_match:
        return numeric_match.group(1)
    
    return "0"

def extract_rate_value(text):
    if not text:
        return "0"
    
    text = str(text)
    rate_match = re.search(r'=\s*[?]?\s*([-]?\d+\.?\d*)', text)
    if rate_match:
        return rate_match.group(1)
    
    numeric_match = re.search(r'([-]?\d+\.?\d*)', text)
    if numeric_match:
        return numeric_match.group(1)
    
    return "0"

def parse_tally_date(date_str):
    if not date_str or date_str.strip() == "":
        return None
    
    try:
        return datetime.strptime(date_str, '%Y%m%d').date()
    except:
        return None

def parse_tally_date_formatted(date_str):
    if not date_str or date_str.strip() == "":
        return None
    
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        return date_obj.strftime('%d-%m-%Y')
    except:
        return None

def convert_to_float(value):
    if value is None or value == "":
        return 0.0
    
    try:
        return float(value)
    except:
        return 0.0

def extract_currency_info(amount_text, voucher_level_currency=None):
    currency = currency_extractor.extract_currency(amount_text)
    
    if not currency or currency == 'UNKNOWN':
        currency = voucher_level_currency or 'INR'
    
    currency_name = get_currency_name(currency)
    
    return currency, currency_name

def determine_change_status(alter_id, master_id):
    try:
        alter = int(alter_id) if alter_id else 0
        master = int(master_id) if master_id else 0
        
        if alter == 0 or master == 0:
            return "Unknown"
        elif alter == 1 and master == 1:
            return "New"
        elif alter > 1:
            return "Modified"
        else:
            return "Unchanged"
    except:
        return "Unknown"

def process_simple_voucher(xml_content, voucher_type_name, output_filename):
    with ProcessingTimer(f"{voucher_type_name} voucher processing"):
        try:
            xml_content = sanitize_xml_content(xml_content)
            root = ET.fromstring(xml_content.encode('utf-8'))
            vouchers = root.findall(".//VOUCHER")
            logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")
            
            all_rows = []
            
            for voucher in vouchers:
                date_elem = voucher.find('DATE')
                voucher_no_elem = voucher.find('VOUCHERNUMBER')
                narration_elem = voucher.find('NARRATION')
                alter_id_elem = voucher.find('ALTERID')
                master_id_elem = voucher.find('MASTERID')
                guid_elem = voucher.find('GUID')
                
                date = date_elem.text if date_elem is not None and date_elem.text else ""
                voucher_no = clean_text(voucher_no_elem.text if voucher_no_elem is not None and voucher_no_elem.text else "")
                narration = clean_text(narration_elem.text if narration_elem is not None and narration_elem.text else "")
                alter_id = clean_text(alter_id_elem.text if alter_id_elem is not None and alter_id_elem.text else "0")
                master_id = clean_text(master_id_elem.text if master_id_elem is not None and master_id_elem.text else "0")
                guid = clean_text(guid_elem.text if guid_elem is not None and guid_elem.text else "")
                
                date_formatted = parse_tally_date_formatted(date)
                change_status = determine_change_status(alter_id, master_id)
                
                currency_name_elem = voucher.find('.//CURRENCYNAME')
                voucher_currency = "INR"
                if currency_name_elem is not None and currency_name_elem.text:
                    voucher_currency = currency_name_elem.text.strip()
                
                ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
                if not ledger_entries:
                    ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
                
                for ledger in ledger_entries:
                    ledger_name_elem = ledger.find('LEDGERNAME')
                    amount_elem = ledger.find('AMOUNT')
                    rate_elem = ledger.find('RATEOFEXCHANGE')
                    
                    ledger_name = clean_text(ledger_name_elem.text if ledger_name_elem is not None and ledger_name_elem.text else "")
                    amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else "0")
                    rate_text = clean_text(rate_elem.text if rate_elem is not None and rate_elem.text else "1")
                    
                    amount = convert_to_float(extract_numeric_amount(amount_text))
                    rate = convert_to_float(extract_rate_value(rate_text))
                    
                    currency, currency_name = extract_currency_info(amount_text, voucher_currency)
                    
                    if rate == 0.0:
                        rate = 1.0
                    
                    if currency == "INR":
                        inr_amount = abs(amount)
                        forex_amount = 0.0
                        fcy = "No"
                    else:
                        forex_amount = abs(amount) / rate if rate != 0 else abs(amount)
                        inr_amount = abs(amount)
                        fcy = "Yes"
                    
                    amount_type = "Credit" if amount < 0 else "Debit"
                    
                    row_data = {
                        'date': date_formatted,
                        'voucher_no': voucher_no,
                        'party_name': ledger_name,
                        'inr_amount': inr_amount,
                        'forex_amount': forex_amount,
                        'rate_of_exchange': rate,
                        'amount_type': amount_type,
                        'currency': currency,
                        'currency_name': currency_name,
                        'fcy': fcy,
                        'narration': narration,
                        'alter_id': alter_id,
                        'master_id': master_id,
                        'change_status': change_status,
                        'guid': guid
                    }
                    
                    all_rows.append(row_data)
            
            df = pd.DataFrame(all_rows)
            
            if len(df) > 0:
                column_order = ['date', 'voucher_no', 'party_name', 'inr_amount', 'forex_amount', 
                              'rate_of_exchange', 'amount_type', 'currency', 'currency_name', 'fcy', 
                              'narration', 'alter_id', 'master_id', 'change_status', 'guid']
                df = df[column_order]
            
            logger.info(f"Created {voucher_type_name} DataFrame with {len(df)} rows")
            if len(df) > 0:
                logger.info(f"Currency distribution: {df['currency'].value_counts().to_dict()}")
                logger.info(f"Change status: {df['change_status'].value_counts().to_dict()}")
            
            df.to_excel(output_filename, index=False)
            logger.info(f"Saved {voucher_type_name} to {output_filename}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error parsing {voucher_type_name} voucher: {e}", exc_info=True)
            return pd.DataFrame()

def journal_voucher(xml_content):
    return process_simple_voucher(xml_content, 'journal', 'journal.xlsx')

def receipt_voucher(xml_content):
    return process_simple_voucher(xml_content, 'receipt', 'receipt.xlsx')

def payment_voucher(xml_content):
    return process_simple_voucher(xml_content, 'payment', 'payment.xlsx')

def contra_voucher(xml_content):
    return process_simple_voucher(xml_content, 'contra', 'contra.xlsx')

def process_sales_purchase_voucher(xml_content, voucher_type_name, output_filename, is_return=False):
    with ProcessingTimer(f"{voucher_type_name} voucher processing"):
        try:
            xml_content = sanitize_xml_content(xml_content)
            root = ET.fromstring(xml_content.encode('utf-8'))
            vouchers = root.findall(".//VOUCHER")
            logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")
            
            all_rows = []
            
            for voucher in vouchers:
                voucher_key = clean_text(voucher.get('VCHKEY', ''))
                voucher_type = clean_text(voucher.get('VCHTYPE', ''))
                
                date = voucher.findtext('DATE', '').strip()
                voucher_no = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
                reference_no = clean_text(voucher.findtext('REFERENCE', ''))
                party_name = clean_text(voucher.findtext('PARTYNAME', ''))
                place_of_supply = clean_text(voucher.findtext('.//PLACEOFSUPPLY', ''))
                narration = clean_text(voucher.findtext('NARRATION', ''))
                entered_by = clean_text(voucher.findtext('ENTEREDBY', ''))
                guid = clean_text(voucher.findtext('GUID', ''))
                
                basic_buyer_name = clean_text(voucher.findtext('.//BASICBUYERNAME', ''))
                basic_ship_doc_no = clean_text(voucher.findtext('.//BASICSHIPPINGDOCUMENTNO', ''))
                party_gstin = clean_text(voucher.findtext('.//PARTYGSTIN', ''))
                alter_id = clean_text(voucher.findtext('ALTERID', '0'))
                master_id = clean_text(voucher.findtext('MASTERID', '0'))
                
                change_status = determine_change_status(alter_id, master_id)
                
                currency_name_elem = voucher.find('.//CURRENCYNAME')
                voucher_currency = "INR"
                if currency_name_elem is not None and currency_name_elem.text:
                    voucher_currency = currency_name_elem.text.strip()
                
                base_voucher_data = {
                    'voucher_key': voucher_key,
                    'voucher_type': voucher_type,
                    'date': parse_tally_date(date),
                    'voucher_number': voucher_no,
                    'reference': reference_no,
                    'party_name': party_name or basic_buyer_name,
                    'party_gstin': party_gstin,
                    'place_of_supply': place_of_supply,
                    'narration': narration,
                    'entered_by': entered_by,
                    'guid': guid,
                    'basic_ship_doc_no': basic_ship_doc_no,
                    'alter_id': alter_id,
                    'master_id': master_id,
                    'change_status': change_status,
                }
                
                ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
                if not ledger_entries:
                    ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
                
                tax_data = {
                    'cgst_amt': 0.0,
                    'sgst_amt': 0.0,
                    'igst_amt': 0.0,
                    'freight_amt': 0.0,
                    'dca_amt': 0.0,
                    'cf_amt': 0.0,
                    'other_amt': 0.0
                }
                
                for ledger in ledger_entries:
                    ledger_name = clean_text(ledger.find('LEDGERNAME').text if ledger.find('LEDGERNAME') is not None else "")
                    amount_elem = ledger.find('AMOUNT')
                    amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else "0")
                    
                    if voucher_currency == "INR":
                        currency, currency_name = extract_currency_info(amount_text)
                        if currency and currency != 'UNKNOWN' and currency != 'INR':
                            voucher_currency = currency
                    
                    try:
                        amount = float(extract_numeric_amount(amount_text))
                    except:
                        amount = 0.0
                    
                    ledger_name_lower = ledger_name.lower()
                    if re.search(r'cgst\s*output', ledger_name_lower):
                        tax_data['cgst_amt'] += abs(amount)
                    elif re.search(r'sgst\s*output', ledger_name_lower):
                        tax_data['sgst_amt'] += abs(amount)
                    elif re.search(r'igst\s*output', ledger_name_lower):
                        tax_data['igst_amt'] += abs(amount)
                    elif re.search(r'freight', ledger_name_lower):
                        tax_data['freight_amt'] += abs(amount)
                    elif re.search(r'dca', ledger_name_lower):
                        tax_data['dca_amt'] += abs(amount)
                    elif re.search(r'clearing\s*&?\s*forwarding', ledger_name_lower):
                        tax_data['cf_amt'] += abs(amount)
                    elif amount > 0 and ledger_name.strip() != "" and not re.search(r'sales|party|customer', ledger_name_lower):
                        tax_data['other_amt'] += abs(amount)
                
                inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
                if not inventory_entries:
                    inventory_entries = voucher.findall('.//INVENTORYENTRIES.LIST')
                
                if inventory_entries:
                    for inv in inventory_entries:
                        item_name = clean_text(inv.find('STOCKITEMNAME').text if inv.find('STOCKITEMNAME') is not None else "")
                        qty_elem = inv.find('ACTUALQTY')
                        rate_elem = inv.find('RATE')
                        amount_elem = inv.find('AMOUNT')
                        discount_elem = inv.find('DISCOUNT')
                        
                        qty = clean_text(qty_elem.text if qty_elem is not None and qty_elem.text else "0")
                        rate = clean_text(rate_elem.text if rate_elem is not None and rate_elem.text else "0")
                        amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else "0")
                        discount = clean_text(discount_elem.text if discount_elem is not None and discount_elem.text else "0")
                        
                        item_currency, item_currency_name = extract_currency_info(amount_text, voucher_currency)
                        
                        qty_numeric = extract_numeric_amount(qty)
                        rate_numeric = extract_rate_value(rate)
                        amount_numeric = extract_numeric_amount(amount_text)
                        discount_numeric = extract_numeric_amount(discount)
                        
                        row_data = {
                            **base_voucher_data,
                            'item_name': item_name,
                            'quantity': qty_numeric,
                            'rate': rate_numeric,
                            'amount': amount_numeric,
                            'discount': discount_numeric,
                            'currency': item_currency,
                            'currency_name': item_currency_name,
                            **tax_data
                        }
                        
                        all_rows.append(row_data)
                else:
                    currency_name = get_currency_name(voucher_currency)
                    row_data = {
                        **base_voucher_data,
                        'item_name': '',
                        'quantity': '0',
                        'rate': '0',
                        'amount': '0',
                        'discount': '0',
                        'currency': voucher_currency,
                        'currency_name': currency_name,
                        **tax_data
                    }
                    all_rows.append(row_data)
            
            df = pd.DataFrame(all_rows)
            
            logger.info(f"Created {voucher_type_name} DataFrame with {len(df)} rows")
            if len(df) > 0:
                logger.info(f"Currency distribution: {df['currency'].value_counts().to_dict()}")
                logger.info(f"Change status: {df['change_status'].value_counts().to_dict()}")
            df.to_excel(output_filename, index=False)
            
            return df
            
        except Exception as e:
            logger.error(f"Error parsing {voucher_type_name} voucher: {e}", exc_info=True)
            return pd.DataFrame()

def normalize_voucher(xml_content):
    return process_sales_purchase_voucher(xml_content, 'sales', 'sales.xlsx', is_return=False)

def sales_return_voucher(xml_content):
    return process_sales_purchase_voucher(xml_content, 'sales_return', 'sales_return.xlsx', is_return=True)

def purchase_voucher(xml_content):
    return process_sales_purchase_voucher(xml_content, 'purchase', 'purchase.xlsx', is_return=False)

def purchase_return_voucher(xml_content):
    return process_sales_purchase_voucher(xml_content, 'purchase_return', 'purchase_return.xlsx', is_return=True)

def trial_balance_to_xlsx(xml_content, company_name, start_date, end_date, output_filename='trial_balance.xlsx'):
    with ProcessingTimer(f"Trail balance processing for {company_name}"):
        try:
            xml_content = sanitize_xml_content(xml_content)
            root = ET.fromstring(xml_content.encode('utf-8'))
            
            ledgers = root.findall('.//COLLECTION/LEDGER')
            logger.info(f"Found {len(ledgers)} ledgers in trial balance")
            
            all_rows = []
            
            for ledger in ledgers:
                ledger_name = clean_text(ledger.get('NAME', ''))
                
                if not ledger_name:
                    continue
                
                opening_balance_text = ledger.findtext('OPENINGBALANCE', '0')
                closing_balance_text = ledger.findtext('CLOSINGBALANCE', '0')
                
                opening_balance = convert_to_float(opening_balance_text)
                closing_balance = convert_to_float(closing_balance_text)
                
                opening_balance = abs(opening_balance)
                closing_balance = abs(closing_balance)
                
                net_transactions = closing_balance - opening_balance
                
                if net_transactions != 0:
                    change_indicator = "Changed"
                elif opening_balance > 0:
                    change_indicator = "No Change"
                else:
                    change_indicator = "New"
                
                row_data = {
                    'particulars': ledger_name,
                    'opening_balance': opening_balance,
                    'net_transactions': net_transactions,
                    'closing_balance': closing_balance,
                    'change_indicator': change_indicator,
                    'material_centre': company_name,
                    'start_date': start_date,
                    'end_date': end_date
                }
                
                all_rows.append(row_data)
            
            df = pd.DataFrame(all_rows)
            
            if len(df) > 0:
                df = df.sort_values('particulars').reset_index(drop=True)
            
            logger.info(f"Created trial balance DataFrame with {len(df)} rows")
            if len(df) > 0:
                logger.info(f"Change indicators: {df['change_indicator'].value_counts().to_dict()}")
            
            df.to_excel(output_filename, index=False)
            logger.info(f"Saved trial balance to {output_filename}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing trail balance: {e}", exc_info=True)
            return pd.DataFrame()