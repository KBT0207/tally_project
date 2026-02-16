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
    """Sanitize XML content with proper None handling and preserve currency symbols"""
    if content is None:
        logger.error("XML content is None")
        return ""
    
    if isinstance(content, bytes):
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            # Use latin-1 which preserves all bytes (including £ symbol)
            content = content.decode('latin-1')
    
    content = str(content)
    
    # Remove control characters but keep printable unicode
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
    
    # Fix XML entities
    content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)', '&amp;', content)
    
    return content

def extract_numeric_amount(text):
    """Extract final numeric amount from text"""
    if not text:
        return "0"
    
    text = str(text)
    # Look for final amount after '='
    final_amount_match = re.search(r'=\s*[?]?\s*([-]?\d+\.?\d*)', text)
    if final_amount_match:
        return final_amount_match.group(1)
    
    # Otherwise get first number
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
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None

def parse_expiry_date(exp_date_text):
    """Parse expiry date from Tally format like '1-Jan-28' to YYYY-MM-DD"""
    if not exp_date_text or exp_date_text.strip() == "":
        return ""
    
    exp_date_text = str(exp_date_text).strip()
    
    try:
        # Try parsing format like "1-Jan-28" or "1-Jan-2028"
        for fmt in ['%d-%b-%y', '%d-%b-%Y']:
            try:
                date_obj = datetime.strptime(exp_date_text, fmt)
                return date_obj.strftime('%Y-%m-%d')
            except:
                continue
        
        # If parsing failed, return as-is (will be cleaned up later if needed)
        return exp_date_text
    except:
        return exp_date_text

def convert_to_float(value):
    if value is None or value == "":
        return 0.0
    
    try:
        return float(value)
    except:
        return 0.0

def extract_unit_from_rate(rate_text):
    """Extract unit from rate field (e.g., '390.00/Ltr' -> 'Ltr')"""
    if not rate_text:
        return ""
    
    rate_text = str(rate_text)
    # Look for pattern: number/unit
    match = re.search(r'/\s*(\w+)\s*$', rate_text)
    if match:
        return match.group(1)
    return ""

def parse_quantity_with_unit(qty_text):
    """Parse quantity with unit (e.g., ' 100.000 Ltr' -> (100.0, 'Ltr'))"""
    if not qty_text:
        return (0.0, "")
    
    qty_text = str(qty_text).strip()
    # Pattern: number followed by optional unit
    match = re.match(r'([-]?\d+\.?\d*)\s*(\w*)', qty_text)
    if match:
        qty = convert_to_float(match.group(1))
        unit = match.group(2) if match.group(2) else ""
        return (qty, unit)
    return (0.0, "")

def extract_currency_and_values(rate_text=None, amount_text=None, discount_text=None):
    """
    Extract currency and values in ORIGINAL CURRENCY (not converted to INR).
    
    Returns dict with:
        - currency: Currency code (e.g., 'INR', 'GBP', 'USD')
        - rate: Rate in original currency
        - amount: Amount in original currency
        - discount: Discount in original currency
        - exchange_rate: Exchange rate to INR (1.0 for INR)
    """
    result = {
        'currency': 'INR',
        'rate': 0.0,
        'amount': 0.0,
        'discount': 0.0,
        'exchange_rate': 1.0
    }
    
    # Detect currency from rate and amount fields
    detected_currency = 'INR'
    
    if rate_text:
        rate_currency = currency_extractor.extract_currency(rate_text)
        if rate_currency and rate_currency != 'INR':
            detected_currency = rate_currency
    
    if amount_text:
        amount_currency = currency_extractor.extract_currency(amount_text)
        if amount_currency and amount_currency != 'INR':
            detected_currency = amount_currency
    
    if detected_currency == 'INR':
        foreign_pattern = r'[?€$£¥]\s*@\s*[?€$£¥].*?/\s*[?€$£¥]\s*='
        if (amount_text and re.search(foreign_pattern, amount_text)) or \
           (rate_text and re.search(r'[?€$£¥]\s*=\s*[?€$£¥]', rate_text)):
            # Likely a foreign currency, check for specific symbols
            if '€' in str(amount_text) or '€' in str(rate_text) or 'EUR' in str(amount_text).upper():
                detected_currency = 'EUR'
            elif '$' in str(amount_text) or '$' in str(rate_text) or 'USD' in str(amount_text).upper():
                detected_currency = 'USD'
            elif '£' in str(amount_text) or '£' in str(rate_text) or 'GBP' in str(amount_text).upper():
                detected_currency = 'GBP'
            elif '?' in str(amount_text) or '?' in str(rate_text):
                detected_currency = 'EUR'
    
    result['currency'] = detected_currency
    
    # Extract values based on currency
    if detected_currency == 'INR':
        # For INR: just extract numeric values
        result['exchange_rate'] = 1.0
        
        if rate_text:
            result['rate'] = convert_to_float(extract_numeric_amount(rate_text))
        
        if amount_text:
            result['amount'] = convert_to_float(extract_numeric_amount(amount_text))
        
        if discount_text:
            result['discount'] = convert_to_float(extract_numeric_amount(discount_text))
    
    else:
        # For foreign currency: extract foreign amounts and exchange rate
        
        # Extract from AMOUNT field
        if amount_text:
            amount_details = currency_extractor.extract_foreign_currency_details(amount_text)
            
            # Amount in foreign currency
            if amount_details['foreign_amount']:
                result['amount'] = amount_details['foreign_amount']
            
            # Exchange rate
            if amount_details['exchange_rate']:
                result['exchange_rate'] = amount_details['exchange_rate']
            
            # Fallback: Try manual extraction if currency_extractor didn't find exchange rate
            if not result['exchange_rate'] or result['exchange_rate'] == 1.0:
                # Pattern: amount @ exchange_rate / currency = inr_amount
                # Example: "4900.00€ @ € 89.23/€ = € 437227.00" or "4900.00? @ ? 89.23/? = ? 437227.00"
                fallback_pattern = r'([-]?\d+\.?\d*)\s*[€$£¥?]\s*@\s*[€$£¥?]\s*([-]?\d+\.?\d*)\s*/\s*[€$£¥?]'
                fallback_match = re.search(fallback_pattern, amount_text)
                if fallback_match:
                    if not result['amount']:
                        result['amount'] = convert_to_float(fallback_match.group(1))
                    result['exchange_rate'] = convert_to_float(fallback_match.group(2))
        
        # Extract from RATE field
        if rate_text:
            rate_details = currency_extractor.extract_foreign_currency_details(rate_text)
            
            # Rate in foreign currency
            if rate_details['foreign_amount']:
                result['rate'] = rate_details['foreign_amount']
            
            # Fallback for rate extraction
            if not result['rate']:
                # Pattern: "14.00€ = € 1249.22/Box" or "14.00? = ? 1249.22/Box"
                rate_fallback_pattern = r'([-]?\d+\.?\d*)\s*[€$£¥?]\s*='
                rate_fallback_match = re.search(rate_fallback_pattern, rate_text)
                if rate_fallback_match:
                    result['rate'] = convert_to_float(rate_fallback_match.group(1))
            
            # If no exchange rate from amount, try to get from rate
            if not result['exchange_rate'] or result['exchange_rate'] == 1.0:
                if rate_details['foreign_amount'] and rate_details['base_amount']:
                    foreign_val = rate_details['foreign_amount']
                    inr_val = rate_details['base_amount']
                    if foreign_val and foreign_val != 0:
                        result['exchange_rate'] = inr_val / foreign_val
        
        # Extract discount in foreign currency
        if discount_text:
            discount_details = currency_extractor.extract_foreign_currency_details(discount_text)
            if discount_details['foreign_amount']:
                result['discount'] = discount_details['foreign_amount']
            else:
                result['discount'] = convert_to_float(extract_numeric_amount(discount_text))
    
    return result

def process_ledger_voucher_to_xlsx(xml_content, voucher_type_name='ledger', output_filename='ledger_vouchers.xlsx'):
    """
    Process ledger vouchers (Journal, Payment, Receipt, Contra) with enhanced CDC tracking
    """
    with ProcessingTimer(f"{voucher_type_name} processing"):
        try:
            if xml_content is None or (isinstance(xml_content, str) and xml_content.strip() == ""):
                logger.warning(f"Empty or None XML content for {voucher_type_name}")
                return pd.DataFrame()
            
            xml_content = sanitize_xml_content(xml_content)
            
            if not xml_content or xml_content.strip() == "":
                logger.warning(f"Empty XML content after sanitization for {voucher_type_name}")
                return pd.DataFrame()
            
            root = ET.fromstring(xml_content.encode('utf-8'))
            
            vouchers = root.findall('.//VOUCHER')
            logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")
            
            if len(vouchers) == 0:
                logger.warning(f"No {voucher_type_name} vouchers found in XML")
                return pd.DataFrame()
            
            all_rows = []
            
            for voucher in vouchers:
                # Extract voucher-level metadata
                guid = voucher.findtext('GUID', '')
                alter_id = voucher.findtext('ALTERID', '0')
                master_id = voucher.findtext('MASTERID', '')
                voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
                voucher_type = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
                date = clean_text(voucher.findtext('DATE', ''))
                reference = clean_text(voucher.findtext('REFERENCE', ''))
                narration = clean_text(voucher.findtext('NARRATION', ''))
                
                # Change tracking
                action = voucher.get('ACTION', 'Unknown')
                is_deleted = voucher.findtext('ISDELETED', 'No')
                change_status = 'Deleted' if is_deleted == 'Yes' else action
                
                # Process ledger entries
                ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
                if not ledger_entries:
                    ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
                
                # First pass: Extract exchange rate from any ledger entry in this voucher
                voucher_exchange_rate = 1.0
                voucher_currency = 'INR'
                
                for ledger in ledger_entries:
                    amount_text = clean_text(ledger.findtext('AMOUNT', '0'))
                    temp_currency_info = extract_currency_and_values(None, amount_text)
                    
                    # If we find a non-INR currency with exchange rate, use it for all entries
                    if temp_currency_info['currency'] != 'INR' and temp_currency_info['exchange_rate'] > 1.0:
                        voucher_exchange_rate = temp_currency_info['exchange_rate']
                        voucher_currency = temp_currency_info['currency']
                        break
                
                # Second pass: Process all ledger entries with the voucher-level exchange rate
                if ledger_entries:
                    for ledger in ledger_entries:
                        ledger_name = clean_text(ledger.findtext('LEDGERNAME', ''))
                        amount_text = clean_text(ledger.findtext('AMOUNT', '0'))
                        
                        # Extract currency info
                        currency_info = extract_currency_and_values(None, amount_text)
                        
                        # Use voucher-level exchange rate if this entry doesn't have one
                        if currency_info['exchange_rate'] == 1.0 and voucher_exchange_rate > 1.0:
                            currency_info['exchange_rate'] = voucher_exchange_rate
                            currency_info['currency'] = voucher_currency
                        
                        # Determine amount_type based on Tally convention:
                        # Negative amounts = Debit, Positive amounts = Credit
                        amount_type = 'Debit' if currency_info['amount'] < 0 else 'Credit'
                        
                        row_data = {
                            'guid': guid,
                            'alter_id': alter_id,
                            'master_id': master_id,
                            'voucher_type': voucher_type,
                            'date': parse_tally_date_formatted(date),
                            'voucher_number': voucher_number,
                            'reference': reference,
                            'change_status': change_status,
                            'ledger_name': ledger_name,
                            'amount': currency_info['amount'],
                            'amount_type': amount_type,
                            'currency': currency_info['currency'],
                            'exchange_rate': currency_info['exchange_rate'],
                            'narration': narration
                        }
                        
                        all_rows.append(row_data)
            
            df = pd.DataFrame(all_rows)
            
            df.to_excel(output_filename, index=False)
            
            return df
            
        except ET.ParseError as e:
            logger.error(f"XML Parse Error in {voucher_type_name}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error parsing {voucher_type_name} voucher: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

def process_inventory_voucher_to_xlsx(xml_content, voucher_type_name='inventory', output_filename='inventory_vouchers.xlsx'):
    """
    Process inventory vouchers with rate, amount, discount in ORIGINAL CURRENCY
    """
    with ProcessingTimer(f"{voucher_type_name} processing"):
        try:
            if xml_content is None or (isinstance(xml_content, str) and xml_content.strip() == ""):
                logger.warning(f"Empty or None XML content for {voucher_type_name}")
                return pd.DataFrame()
            
            xml_content = sanitize_xml_content(xml_content)
            
            if not xml_content or xml_content.strip() == "":
                logger.warning(f"Empty XML content after sanitization for {voucher_type_name}")
                return pd.DataFrame()
            
            root = ET.fromstring(xml_content.encode('utf-8'))
            
            vouchers = root.findall('.//VOUCHER')
            logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")
            
            if len(vouchers) == 0:
                logger.warning(f"No {voucher_type_name} vouchers found in XML")
                return pd.DataFrame()
            
            all_rows = []
            
            for voucher in vouchers:
                # Extract voucher-level metadata
                guid = voucher.findtext('GUID', '')
                alter_id = voucher.findtext('ALTERID', '0')
                master_id = voucher.findtext('MASTERID', '')
                voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
                voucher_type = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
                date = clean_text(voucher.findtext('DATE', ''))
                party_name = clean_text(voucher.findtext('PARTYNAME', ''))
                reference = clean_text(voucher.findtext('REFERENCE', ''))
                narration = clean_text(voucher.findtext('NARRATION', ''))
                
                # Extract voucher-level GST and e-invoice fields
                party_gstin = clean_text(voucher.findtext('PARTYGSTIN', ''))
                irn_number = clean_text(voucher.findtext('IRNACKNO', ''))
                eway_bill = clean_text(voucher.findtext('TEMPGSTEWAYBILLNUMBER', ''))
                
                # Change tracking
                action = voucher.get('ACTION', 'Unknown')
                is_deleted = voucher.findtext('ISDELETED', 'No')
                change_status = 'Deleted' if is_deleted == 'Yes' else action
                
                base_voucher_data = {
                    'guid': guid,
                    'alter_id': alter_id,
                    'master_id': master_id,
                    'voucher_type': voucher_type,
                    'date': parse_tally_date_formatted(date),
                    'voucher_number': voucher_number,
                    'reference': reference,
                    'party_name': party_name,
                    'narration': narration,
                    'change_status': change_status,
                    'gst_number': party_gstin,
                    'e_invoice_number': irn_number,
                    'eway_bill': eway_bill
                }
                
                # Initialize voucher-level charges (not item-specific)
                voucher_charges = {
                    'freight_amt': 0.0,
                    'dca_amt': 0.0,
                    'cf_amt': 0.0,
                    'other_amt': 0.0
                }
                
                # Extract tax amounts from ledger entries
                ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
                if not ledger_entries:
                    ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
                
                # First pass: Extract voucher-level exchange rate
                voucher_exchange_rate = 1.0
                voucher_currency = 'INR'
                
                # Check ledger entries for exchange rate
                for ledger in ledger_entries:
                    amount_text = clean_text(ledger.findtext('AMOUNT', '0'))
                    temp_currency_info = extract_currency_and_values(None, amount_text)
                    
                    if temp_currency_info['currency'] != 'INR' and temp_currency_info['exchange_rate'] > 1.0:
                        voucher_exchange_rate = temp_currency_info['exchange_rate']
                        voucher_currency = temp_currency_info['currency']
                        break
                
                # Also check inventory entries for exchange rate
                if voucher_exchange_rate == 1.0:
                    inventory_entries_temp = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
                    if not inventory_entries_temp:
                        inventory_entries_temp = voucher.findall('.//INVENTORYENTRIES.LIST')
                    
                    for inv in inventory_entries_temp:
                        rate_elem = inv.find('RATE')
                        amount_elem = inv.find('AMOUNT')
                        
                        rate_text = clean_text(rate_elem.text if rate_elem is not None and rate_elem.text else "0")
                        amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else "0")
                        
                        temp_currency_info = extract_currency_and_values(rate_text, amount_text, None)
                        
                        if temp_currency_info['currency'] != 'INR' and temp_currency_info['exchange_rate'] > 1.0:
                            voucher_exchange_rate = temp_currency_info['exchange_rate']
                            voucher_currency = temp_currency_info['currency']
                            break
                
                # Process ledger entries for GST and non-GST charges
                voucher_gst_data = {
                    'cgst_total': 0.0,
                    'sgst_total': 0.0,
                    'igst_total': 0.0,
                    'cgst_rate': 0.0,
                    'sgst_rate': 0.0,
                    'igst_rate': 0.0
                }
                
                for ledger in ledger_entries:
                    ledger_name = clean_text(ledger.findtext('LEDGERNAME', ''))
                    ledger_name_lower = ledger_name.lower()
                    
                    amount_text = clean_text(ledger.findtext('AMOUNT', '0'))
                    amount = convert_to_float(extract_numeric_amount(amount_text))
                    
                    if re.search(r'cgst|c\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                        voucher_gst_data['cgst_total'] += abs(amount)
                        rate_match = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name)
                        if rate_match and voucher_gst_data['cgst_rate'] == 0.0:
                            voucher_gst_data['cgst_rate'] = convert_to_float(rate_match.group(1))
                    elif re.search(r'sgst|s\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                        voucher_gst_data['sgst_total'] += abs(amount)
                        rate_match = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name)
                        if rate_match and voucher_gst_data['sgst_rate'] == 0.0:
                            voucher_gst_data['sgst_rate'] = convert_to_float(rate_match.group(1))
                    elif re.search(r'igst|i\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                        voucher_gst_data['igst_total'] += abs(amount)
                        rate_match = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name)
                        if rate_match and voucher_gst_data['igst_rate'] == 0.0:
                            voucher_gst_data['igst_rate'] = convert_to_float(rate_match.group(1))
                    elif re.search(r'freight', ledger_name_lower):
                        voucher_charges['freight_amt'] += abs(amount)
                    elif re.search(r'dca', ledger_name_lower):
                        voucher_charges['dca_amt'] += abs(amount)
                    elif re.search(r'clearing\s*&?\s*forwarding', ledger_name_lower):
                        voucher_charges['cf_amt'] += abs(amount)
                    elif (ledger_name.strip() != "" and 
                          ledger_name != party_name and
                          abs(amount) > 0.01 and
                          not re.search(r'round', ledger_name_lower)):
                        is_gst_ledger = (re.search(r'cgst|sgst|igst', ledger_name_lower) and 
                                        re.search(r'input|output', ledger_name_lower))
                        if not is_gst_ledger and not re.search(r'duty|cess', ledger_name_lower):
                            voucher_charges['other_amt'] += abs(amount)
                
                # Process inventory entries
                inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
                if not inventory_entries:
                    inventory_entries = voucher.findall('.//INVENTORYENTRIES.LIST')
                
                # Calculate total item amount for proportional GST distribution
                total_item_amount = 0.0
                temp_item_data = []
                
                # Check if inventory entries actually have data (not just empty XML elements)
                has_real_inventory = False
                if inventory_entries:
                    for inv in inventory_entries:
                        # Check if this inventory entry has an actual stock item
                        item_name = clean_text(inv.findtext('STOCKITEMNAME', ''))
                        amount_elem = inv.find('AMOUNT')
                        amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else "0")
                        amount = convert_to_float(extract_numeric_amount(amount_text))
                        
                        # Only consider it real inventory if it has a name AND a non-zero amount
                        if item_name and abs(amount) > 0.01:
                            has_real_inventory = True
                            break
                
                if has_real_inventory:
                    for inv in inventory_entries:
                        item_name = clean_text(inv.findtext('STOCKITEMNAME', ''))
                        qty_elem = inv.find('ACTUALQTY')
                        rate_elem = inv.find('RATE')
                        amount_elem = inv.find('AMOUNT')
                        discount_elem = inv.find('DISCOUNT')
                        billed_qty_elem = inv.find('BILLEDQTY')
                        
                        qty = clean_text(qty_elem.text if qty_elem is not None and qty_elem.text else "0")
                        rate_text = clean_text(rate_elem.text if rate_elem is not None and rate_elem.text else "0")
                        amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else "0")
                        discount_text = clean_text(discount_elem.text if discount_elem is not None and discount_elem.text else "0")
                        billed_qty_text = clean_text(billed_qty_elem.text if billed_qty_elem is not None and billed_qty_elem.text else "0")
                        
                        # Extract unit from rate
                        unit = extract_unit_from_rate(rate_text)
                        
                        # Extract alt_qty and alt_unit from billed quantity
                        alt_qty, alt_unit = parse_quantity_with_unit(billed_qty_text)
                        
                        # Extract batch information
                        batch_allocations = inv.findall('.//BATCHALLOCATIONS.LIST')
                        batch_no = ""
                        mfg_date = ""
                        exp_date = ""
                        
                        if batch_allocations:
                            # Take the first batch allocation
                            batch = batch_allocations[0]
                            batch_no = clean_text(batch.findtext('BATCHNAME', ''))
                            mfg_date_raw = clean_text(batch.findtext('MFDON', ''))
                            mfg_date = parse_tally_date_formatted(mfg_date_raw) if mfg_date_raw else ""
                            
                            # Extract expiry date - can be in different formats
                            exp_period_elem = batch.find('EXPIRYPERIOD')
                            if exp_period_elem is not None:
                                exp_date_text = exp_period_elem.text
                                if exp_date_text:
                                    # Parse date from format like "1-Jan-28" to YYYY-MM-DD
                                    exp_date = parse_expiry_date(exp_date_text)
                                # Also check for JD attribute which has numeric format
                                exp_jd = exp_period_elem.get('JD', '')
                                if exp_jd and not exp_date:
                                    exp_date = parse_tally_date_formatted(exp_jd) if exp_jd else ""
                        
                        # Extract HSN code from accounting allocations (item-specific)
                        hsn_code = ""
                        accounting_allocations = inv.findall('.//ACCOUNTINGALLOCATIONS.LIST')
                        if accounting_allocations:
                            for acc_alloc in accounting_allocations:
                                if not hsn_code:
                                    hsn_code = clean_text(acc_alloc.findtext('GSTHSNSACCODE', ''))
                                    break
                        
                        currency_data = extract_currency_and_values(rate_text, amount_text, discount_text)
                        
                        if currency_data['exchange_rate'] == 1.0 and voucher_exchange_rate > 1.0:
                            currency_data['exchange_rate'] = voucher_exchange_rate
                            currency_data['currency'] = voucher_currency
                        
                        qty_numeric = convert_to_float(extract_numeric_amount(qty))
                        
                        # For sales return vouchers, remove minus sign from amount
                        item_amount = currency_data['amount']
                        voucher_type_lower = voucher_type.lower()
                        if 'sales return' in voucher_type_lower or 'credit note' in voucher_type_lower:
                            item_amount = abs(item_amount)
                        
                        # Store temp item data
                        temp_item_data.append({
                            'item_name': item_name,
                            'quantity': qty_numeric,
                            'unit': unit,
                            'alt_qty': alt_qty,
                            'alt_unit': alt_unit,
                            'batch_no': batch_no,
                            'mfg_date': mfg_date,
                            'exp_date': exp_date,
                            'hsn_code': hsn_code,
                            'rate': currency_data['rate'],
                            'amount': item_amount,
                            'discount': currency_data['discount'],
                            'currency': currency_data['currency'],
                            'exchange_rate': currency_data['exchange_rate']
                        })
                        
                        total_item_amount += abs(item_amount)
                    
                    # Now distribute GST proportionally to each item
                    # Special case: if total_item_amount is 0 (empty items), treat as non-inventory voucher
                    if total_item_amount == 0 or len(temp_item_data) == 0:
                        # No real inventory - create a single row with all GST
                        gst_rate = voucher_gst_data['cgst_rate'] + voucher_gst_data['sgst_rate'] + voucher_gst_data['igst_rate']
                        
                        row_data = {
                            **base_voucher_data,
                            'item_name': '',
                            'quantity': 0.0,
                            'unit': '',
                            'alt_qty': 0.0,
                            'alt_unit': '',
                            'batch_no': '',
                            'mfg_date': '',
                            'exp_date': '',
                            'hsn_code': '',
                            'gst%': gst_rate,
                            'rate': 0.0,
                            'amount': 0.0,
                            'discount': 0.0,
                            'cgst_amt': voucher_gst_data['cgst_total'],
                            'sgst_amt': voucher_gst_data['sgst_total'],
                            'igst_amt': voucher_gst_data['igst_total'],
                            'currency': voucher_currency,
                            'exchange_rate': voucher_exchange_rate,
                            **voucher_charges
                        }
                        all_rows.append(row_data)
                    else:
                        # Real inventory with amounts - distribute GST proportionally
                        for item_data in temp_item_data:
                            item_amount = abs(item_data['amount'])
                            
                            # Calculate proportional GST for this item
                            proportion = item_amount / total_item_amount
                            cgst_amt = voucher_gst_data['cgst_total'] * proportion
                            sgst_amt = voucher_gst_data['sgst_total'] * proportion
                            igst_amt = voucher_gst_data['igst_total'] * proportion
                            
                            # Calculate total GST rate
                            gst_rate = voucher_gst_data['cgst_rate'] + voucher_gst_data['sgst_rate'] + voucher_gst_data['igst_rate']
                            
                            row_data = {
                                **base_voucher_data,
                                **item_data,
                                'gst%': gst_rate,
                                'cgst_amt': cgst_amt,
                                'sgst_amt': sgst_amt,
                                'igst_amt': igst_amt,
                                **voucher_charges
                            }
                            
                            all_rows.append(row_data)
                else:
                    # Voucher without inventory items - still has GST
                    gst_rate = voucher_gst_data['cgst_rate'] + voucher_gst_data['sgst_rate'] + voucher_gst_data['igst_rate']
                    
                    row_data = {
                        **base_voucher_data,
                        'item_name': '',
                        'quantity': 0.0,
                        'unit': '',
                        'alt_qty': 0.0,
                        'alt_unit': '',
                        'batch_no': '',
                        'mfg_date': '',
                        'exp_date': '',
                        'hsn_code': '',
                        'gst%': gst_rate,
                        'rate': 0.0,
                        'amount': 0.0,
                        'discount': 0.0,
                        'cgst_amt': voucher_gst_data['cgst_total'],
                        'sgst_amt': voucher_gst_data['sgst_total'],
                        'igst_amt': voucher_gst_data['igst_total'],
                        'currency': voucher_currency,
                        'exchange_rate': voucher_exchange_rate,
                        **voucher_charges
                    }
                    all_rows.append(row_data)
            
            df = pd.DataFrame(all_rows)
            
            if len(df) > 0:
                # First, calculate per-row totals
                df['row_total'] = (
                    df['amount'].fillna(0) + 
                    df['cgst_amt'].fillna(0) + 
                    df['sgst_amt'].fillna(0) + 
                    df['igst_amt'].fillna(0) + 
                    df['freight_amt'].fillna(0) + 
                    df['dca_amt'].fillna(0) + 
                    df['cf_amt'].fillna(0) + 
                    df['other_amt'].fillna(0)
                )
                
                # Group by voucher_number and calculate total for entire voucher
                voucher_totals = df.groupby('guid')['row_total'].sum().to_dict()
                
                # Mark first occurrence of each voucher
                df['is_first_row'] = ~df.duplicated(subset=['guid'], keep='first')
                
                # Set total_amt: show voucher total only for first row, 0 for others
                df['total_amt'] = df.apply(
                    lambda row: voucher_totals[row['guid']] if row['is_first_row'] else 0.0, 
                    axis=1
                )
                
                # Drop temporary columns
                df = df.drop(columns=['row_total', 'is_first_row'])
            
            columns = ["date", "voucher_number", "reference", "voucher_type", 
                    "party_name", "item_name", "quantity", "unit", "alt_qty", "alt_unit",
                    "batch_no", "mfg_date", "exp_date", "hsn_code", "gst%",
                    "rate", "amount", "discount", 
                    "cgst_amt", "sgst_amt", "igst_amt", 
                    "freight_amt", "dca_amt", "cf_amt", "other_amt", "total_amt", 
                    "currency", "exchange_rate", 
                    "gst_number", "e_invoice_number", "eway_bill",
                    "narration", "guid", "alter_id", "master_id", "change_status"]

            if len(df) > 0:
                df = df[columns]
            
            df.to_excel(output_filename, index=False)
            
            return df
            
        except ET.ParseError as e:
            logger.error(f"XML Parse Error in {voucher_type_name}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error parsing {voucher_type_name} voucher: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

def extract_all_ledgers_to_xlsx(xml_content, company_name='', output_filename='all_ledgers.xlsx'):
    """
    Extract all ledgers from Tally XML with comprehensive details
    """
    with ProcessingTimer("Ledger extraction"):
        try:
            if xml_content is None or (isinstance(xml_content, str) and xml_content.strip() == ""):
                logger.warning(f"Empty or None XML content for ledgers extraction")
                return pd.DataFrame()
            
            xml_content = sanitize_xml_content(xml_content)
            
            if not xml_content or xml_content.strip() == "":
                logger.warning(f"Empty XML content after sanitization for ledgers extraction")
                return pd.DataFrame()
            
            root = ET.fromstring(xml_content.encode('utf-8'))
            
            ledgers = root.findall('.//LEDGER')
            if len(ledgers) == 0:
                logger.warning(f"No ledgers found in XML")
                return pd.DataFrame()
            
            all_rows = []
            
            for ledger in ledgers:
                # Basic Information
                ledger_name = ledger.get('NAME', '')
                guid = clean_text(ledger.findtext('GUID', ''))
                parent = clean_text(ledger.findtext('PARENT', ''))
                
                # Identification
                alter_id = clean_text(ledger.findtext('ALTERID', '0'))
                
                # Dates
                created_date = clean_text(ledger.findtext('CREATEDDATE', ''))
                altered_on = clean_text(ledger.findtext('ALTEREDON', ''))
                
                # Contact Information
                email = clean_text(ledger.findtext('EMAIL', ''))
                website = clean_text(ledger.findtext('WEBSITE', ''))
                phone = clean_text(ledger.findtext('LEDGERPHONE', ''))
                mobile = clean_text(ledger.findtext('LEDGERMOBILE', ''))
                fax = clean_text(ledger.findtext('LEDGERFAX', ''))
                contact_person = clean_text(ledger.findtext('LEDGERCONTACT', ''))
                
                # Address Information
                pincode = clean_text(ledger.findtext('PINCODE', ''))
                state_name = clean_text(ledger.findtext('STATENAME', ''))
                country_name = clean_text(ledger.findtext('COUNTRYNAME', ''))
                
                # Financial Information
                opening_balance = clean_text(ledger.findtext('OPENINGBALANCE', '0'))
                currency_name = clean_text(ledger.findtext('CURRENCYNAME', ''))
                
                # Tax Information
                pan_number = clean_text(ledger.findtext('INCOMETAXNUMBER', ''))
                gstin = clean_text(ledger.findtext('PARTYGSTIN', ''))
                gst_registration_type = clean_text(ledger.findtext('GSTREGISTRATIONTYPE', ''))
                vat_tin_number = clean_text(ledger.findtext('VATTINNUMBER', ''))
                sales_tax_number = clean_text(ledger.findtext('SALESTAXNUMBER', ''))
                
                # Tax Applicability Flags
                is_gst_applicable = clean_text(ledger.findtext('ISGSTAPPLICABLE', 'No'))
                is_tds_applicable = clean_text(ledger.findtext('ISTDSAPPLICABLE', 'No'))
                is_tcs_applicable = clean_text(ledger.findtext('ISTCSAPPLICABLE', 'No'))
                
                # Banking Information
                bank_account_holder = clean_text(ledger.findtext('BANKACCHOLDERNAME', ''))
                ifsc_code = clean_text(ledger.findtext('IFSCODE', ''))
                bank_branch = clean_text(ledger.findtext('BRANCHNAME', ''))
                swift_code = clean_text(ledger.findtext('SWIFTCODE', ''))
                bank_iban = clean_text(ledger.findtext('BANKIBAN', ''))
                
                # Business Information
                export_import_code = clean_text(ledger.findtext('EXPORTIMPORTCODE', ''))
                msme_reg_number = clean_text(ledger.findtext('MSMEREGNUMBER', ''))
                
                # Account Settings
                is_bill_wise_on = clean_text(ledger.findtext('ISBILLWISEON', 'No'))
                is_cost_centres_on = clean_text(ledger.findtext('ISCOSTCENTRESON', 'No'))
                is_interest_on = clean_text(ledger.findtext('ISINTERESTON', 'No'))
                
                # Credit Management
                credit_limit = clean_text(ledger.findtext('CREDITLIMIT', '0'))
                bill_credit_period = clean_text(ledger.findtext('BILLCREDITPERIOD', ''))
                
                # Status Flags
                is_deleted = clean_text(ledger.findtext('ISDELETED', 'No'))
                
                # Narration
                narration = clean_text(ledger.findtext('NARRATION', ''))
                description = clean_text(ledger.findtext('DESCRIPTION', ''))
                
                # Mailing Details
                mailing_name_native = clean_text(ledger.findtext('MAILINGNAMENATIVE', ''))
                
                # Additional Tax Details
                tds_deductee_type = clean_text(ledger.findtext('TDSDEDUCTEETYPE', ''))
                tax_type = clean_text(ledger.findtext('TAXTYPE', ''))
                
                # Party Details
                relation_type = clean_text(ledger.findtext('RELATIONTYPE', ''))
                
                row_data = {
                    # Basic Info
                    'ledger_name': ledger_name,
                    'guid': guid,
                    'alter_id': alter_id,
                    'parent_group': parent,
                    'company_name': company_name,
                    
                    # Dates
                    'created_date': created_date,
                    'altered_on': altered_on,
                    
                    # Contact Info
                    'email': email,
                    'website': website,
                    'phone': phone,
                    'mobile': mobile,
                    'fax': fax,
                    'contact_person': contact_person,
                    
                    # Address
                    'pincode': pincode,
                    'state': state_name,
                    'country': country_name,
                    
                    # Financial
                    'opening_balance': opening_balance,
                    'currency': currency_name,
                    'credit_limit': credit_limit,
                    'bill_credit_period': bill_credit_period,
                    
                    # Tax Information
                    'pan': pan_number,
                    'gstin': gstin,
                    'gst_registration_type': gst_registration_type,
                    'vat_tin': vat_tin_number,
                    'sales_tax_number': sales_tax_number,
                    'is_gst_applicable': is_gst_applicable,
                    'is_tds_applicable': is_tds_applicable,
                    'is_tcs_applicable': is_tcs_applicable,
                    'tds_deductee_type': tds_deductee_type,
                    'tax_type': tax_type,
                    
                    # Banking
                    'bank_account_holder': bank_account_holder,
                    'ifsc_code': ifsc_code,
                    'bank_branch': bank_branch,
                    'swift_code': swift_code,
                    'bank_iban': bank_iban,
                    
                    # Business
                    'export_import_code': export_import_code,
                    'msme_reg_number': msme_reg_number,
                    
                    # Settings
                    'is_bill_wise_on': is_bill_wise_on,
                    'is_cost_centres_on': is_cost_centres_on,
                    'is_interest_on': is_interest_on,
                    
                    # Status
                    'is_deleted': is_deleted,
                    
                    # Additional
                    'narration': narration,
                    'description': description,
                    'mailing_name_native': mailing_name_native,
                    'relation_type': relation_type
                }
                
                all_rows.append(row_data)
            
            df = pd.DataFrame(all_rows)
            
            if len(df) > 0:
                df = df.sort_values('ledger_name').reset_index(drop=True)
            
            df.to_excel(output_filename, index=False)
            
            return df
            
        except ET.ParseError as e:
            logger.error(f"XML Parse Error in ledgers extraction: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting ledgers: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

def trial_balance_to_xlsx(xml_content, comp_name: str, start_date: str, end_date: str, output_filename='trial_balance.xlsx'):
    try:
        if not xml_content or not str(xml_content).strip():
            return pd.DataFrame()

        xml_content = sanitize_xml_content(xml_content)
        root = ET.fromstring(xml_content.encode('utf-8'))

        ledger_nodes = root.findall('.//LEDGER')
        rows = []

        for ledger in ledger_nodes:
            ledger_name = ledger.get('NAME', '')
            if not ledger_name:
                ledger_name = ledger.findtext('LEDGERNAME', '')
            ledger_name = clean_text(ledger_name)

            if not ledger_name:
                continue

            parent_group = clean_text(ledger.findtext('PARENT', ''))

            opening_text = clean_text(ledger.findtext('OPENINGBALANCE', '0'))
            closing_text = clean_text(ledger.findtext('CLOSINGBALANCE', '0'))

            opening_val = convert_to_float(extract_numeric_amount(str(opening_text)))
            closing_val = convert_to_float(extract_numeric_amount(str(closing_text)))
            net_transactions = closing_val - opening_val

            rows.append({
                'ledger_name': ledger_name,
                'parent_group': parent_group,
                'opening_balance': opening_val,
                'net_transactions': net_transactions,
                'closing_balance': closing_val,
                'start_date': start_date,
                'end_date': end_date,
                'company_name': comp_name,
            })

        df = pd.DataFrame(rows)

        if not df.empty:
            df = df.sort_values('ledger_name').reset_index(drop=True)

        df.to_excel(output_filename, index=False)
        return df

    except Exception:
        return pd.DataFrame()