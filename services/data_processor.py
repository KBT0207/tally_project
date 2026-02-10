import xml.etree.ElementTree as ET
import pandas as pd
import re
from datetime import datetime
from logging_config import logger

# Handle both relative and absolute imports
try:
    from .currency_extractor import CurrencyExtractor
except ImportError:
    from currency_extractor import CurrencyExtractor

currency_extractor = CurrencyExtractor(default_currency='INR')

# Currency name mapping for display
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

def get_currency_name(currency_code):
    """Get full currency name from currency code"""
    return CURRENCY_NAMES.get(currency_code, currency_code)

def sanitize_xml_content(content):
    """Clean and prepare XML content for parsing"""
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
    content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', content)
    
    return content

def extract_numeric_amount(text):
    """Extract numeric amount from text"""
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
    """Extract exchange rate value from text"""
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
    """Parse Tally date format (YYYYMMDD) to date object"""
    if not date_str or date_str.strip() == "":
        return None
    
    try:
        return datetime.strptime(date_str, '%Y%m%d').date()
    except:
        return None

def parse_tally_date_formatted(date_str):
    """Parse Tally date and return in DD-MM-YYYY format"""
    if not date_str or date_str.strip() == "":
        return None
    
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        return date_obj.strftime('%d-%m-%Y')
    except:
        return None

def convert_to_float(value):
    """Convert value to float, return 0.0 if conversion fails"""
    if value is None or value == "":
        return 0.0
    
    try:
        return float(value)
    except:
        return 0.0

def extract_currency_info(amount_text, voucher_level_currency=None):
    """
    Extract currency code and name from amount text
    
    Args:
        amount_text: Text containing currency symbol and amount
        voucher_level_currency: Currency extracted at voucher level (optional)
        
    Returns:
        tuple: (currency_code, currency_name)
    """
    currency = currency_extractor.extract_currency(amount_text)
    
    # If extraction returns UNKNOWN or empty, use voucher level currency
    if not currency or currency == 'UNKNOWN':
        currency = voucher_level_currency or 'INR'
    
    currency_name = get_currency_name(currency)
    
    return currency, currency_name

def process_simple_voucher(xml_content, voucher_type_name, output_filename):
    """
    Generic function to process simple vouchers (Journal, Receipt, Payment, Contra)
    
    Args:
        xml_content: XML content as string or bytes from Tally
        voucher_type_name: Name of voucher type for logging (e.g., 'journal', 'receipt')
        output_filename: Output Excel filename
        
    Returns:
        pandas.DataFrame with voucher data formatted with columns:
        - date: Date in DD-MM-YYYY format
        - voucher_no: Voucher number
        - party_name: Ledger name
        - inr_amount: Amount in INR
        - forex_amount: Amount in foreign currency (if applicable)
        - rate_of_exchange: Exchange rate (default 1 for INR)
        - amount_type: 'Credit' or 'Debit'
        - currency: Currency code (INR, USD, GBP, etc.)
        - currency_name: Full currency name
        - fcy: 'Yes' if foreign currency, 'No' if INR
        - narration: Voucher narration
    """
    try:
        xml_content = sanitize_xml_content(xml_content)
        root = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall(".//VOUCHER")
        logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")
        
        all_rows = []
        
        for voucher in vouchers:
            # Extract voucher-level information
            voucher_type = voucher.get('VCHTYPE', '')
            date = voucher.find('DATE').text if voucher.find('DATE') is not None else ""
            voucher_no = voucher.find('VOUCHERNUMBER').text if voucher.find('VOUCHERNUMBER') is not None else ""
            narration = voucher.find('NARRATION').text if voucher.find('NARRATION') is not None else ""
            
            # Format date to DD-MM-YYYY
            formatted_date = parse_tally_date_formatted(date)
            
            # Find all ledger entries
            ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
            if not ledger_entries:
                ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
            
            # Extract currency and exchange rate from the voucher
            voucher_currency = "INR"
            exchange_rate = 1.0
            
            # Check for EXCHGRATE field in voucher
            exchg_rate_elem = voucher.find('EXCHGRATE')
            if exchg_rate_elem is not None and exchg_rate_elem.text:
                try:
                    exchange_rate = float(extract_numeric_amount(exchg_rate_elem.text))
                except:
                    exchange_rate = 1.0
            
            # Check for CURRENCYNAME at voucher level
            currency_name_elem = voucher.find('CURRENCYNAME')
            if currency_name_elem is not None and currency_name_elem.text:
                voucher_currency = currency_name_elem.text.strip()
            
            # Process each ledger entry
            for ledger in ledger_entries:
                ledger_name = ledger.find('LEDGERNAME').text if ledger.find('LEDGERNAME') is not None else ""
                amount_elem = ledger.find('AMOUNT')
                amount_text = amount_elem.text if amount_elem is not None and amount_elem.text else "0"
                
                # Extract currency and currency name
                currency, currency_name = extract_currency_info(amount_text, voucher_currency)
                
                # Update voucher currency if foreign currency detected
                if currency and currency != 'UNKNOWN' and currency != "INR" and voucher_currency == "INR":
                    voucher_currency = currency
                
                # Extract numeric amount
                try:
                    amount = float(extract_numeric_amount(amount_text))
                except:
                    amount = 0.0
                
                # Determine if this is a credit (positive) or debit (negative) entry
                amount_type = "Debit" if amount < 0 else "Credit"
                abs_amount = abs(amount)
                
                # Skip zero amounts or empty ledger names
                if abs_amount == 0 or not ledger_name.strip():
                    continue
                
                # Calculate INR and forex amounts
                if currency != "INR" and exchange_rate != 1.0:
                    # If foreign currency, calculate INR amount
                    forex_amount = abs_amount
                    inr_amount = abs_amount * exchange_rate
                    fcy = "Yes"
                else:
                    # If INR, both amounts are the same
                    inr_amount = abs_amount
                    forex_amount = abs_amount
                    currency = "INR"
                    currency_name = "Indian Rupee"
                    fcy = "No"
                
                # Create row
                row_data = {
                    'date': formatted_date,
                    'voucher_no': voucher_no,
                    'party_name': ledger_name,
                    'inr_amount': round(inr_amount, 2),
                    'forex_amount': round(forex_amount, 2),
                    'rate_of_exchange': exchange_rate,
                    'amount_type': amount_type,
                    'currency': currency,
                    'currency_name': currency_name,
                    'fcy': fcy,
                    'narration': narration
                }
                
                all_rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(all_rows)
        
        # Reorder columns to match desired output
        column_order = [
            'date',
            'voucher_no',
            'party_name',
            'inr_amount',
            'forex_amount',
            'rate_of_exchange',
            'amount_type',
            'currency',
            'currency_name',
            'fcy',
            'narration'
        ]
        
        if len(df) > 0:
            df = df[column_order]
        
        logger.info(f"Created {voucher_type_name} voucher DataFrame with {len(df)} rows")
        logger.info(f"Currency distribution: {df['currency'].value_counts().to_dict() if len(df) > 0 else 'No data'}")
        df.to_excel(output_filename, index=False)
        
        return df
        
    except Exception as e:
        logger.error(f"Error parsing {voucher_type_name} voucher: {e}", exc_info=True)
        return pd.DataFrame()


# Wrapper functions for each voucher type
def journal_voucher(xml_content):
    """Process journal vouchers"""
    return process_simple_voucher(xml_content, 'journal', 'journal.xlsx')

def receipt_voucher(xml_content):
    """Process receipt vouchers"""
    return process_simple_voucher(xml_content, 'receipt', 'receipt.xlsx')

def payment_voucher(xml_content):
    """Process payment vouchers"""
    return process_simple_voucher(xml_content, 'payment', 'payment.xlsx')

def contra_voucher(xml_content):
    """Process contra vouchers"""
    return process_simple_voucher(xml_content, 'contra', 'contra.xlsx')


def process_sales_purchase_voucher(xml_content, voucher_type_name, output_filename, is_return=False):
    """
    Generic function to process sales/purchase vouchers
    
    Args:
        xml_content: XML content as string or bytes from Tally
        voucher_type_name: Name of voucher type ('sales', 'purchase', 'sales_return', 'purchase_return')
        output_filename: Output Excel filename
        is_return: True if this is a return voucher
        
    Returns:
        pandas.DataFrame with voucher data
    """
    try:
        xml_content = sanitize_xml_content(xml_content)
        root = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall(".//VOUCHER")
        logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")
        
        all_rows = []
        
        for voucher in vouchers:
            voucher_key = voucher.get('VCHKEY', '')
            voucher_type = voucher.get('VCHTYPE', '')
            date = voucher.find('DATE').text if voucher.find('DATE') is not None else ""
            voucher_no = voucher.find('VOUCHERNUMBER').text if voucher.find('VOUCHERNUMBER') is not None else ""
            reference_no = voucher.find('REFERENCE').text if voucher.find('REFERENCE') is not None else ""
            party_name = voucher.find('PARTYNAME').text if voucher.find('PARTYNAME') is not None else ""
            narration = voucher.find('NARRATION').text if voucher.find('NARRATION') is not None else ""
            party_gstin = voucher.find('PARTYGSTIN').text if voucher.find('PARTYGSTIN') is not None else ""
            place_of_supply = voucher.find('PLACEOFSUPPLY').text if voucher.find('PLACEOFSUPPLY') is not None else ""
            entered_by = voucher.find('ENTEREDBY').text if voucher.find('ENTEREDBY') is not None else ""
            guid = voucher.find('GUID').text if voucher.find('GUID') is not None else ""
            basic_ship_doc_no = voucher.find('BASICSHIPDOCUMENTNO').text if voucher.find('BASICSHIPDOCUMENTNO') is not None else ""
            alter_id = voucher.find('ALTERID').text if voucher.find('ALTERID') is not None else ""
            master_id = voucher.find('MASTERID').text if voucher.find('MASTERID') is not None else ""
            basic_buyer_name = voucher.find('BASICBUYERNAME').text if voucher.find('BASICBUYERNAME') is not None else ""
            
            # Extract voucher-level currency
            voucher_currency = "INR"
            currency_name_elem = voucher.find('CURRENCYNAME')
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
            
            # Extract currency from ledger entries if not at voucher level
            for ledger in ledger_entries:
                ledger_name = ledger.find('LEDGERNAME').text if ledger.find('LEDGERNAME') is not None else ""
                amount_elem = ledger.find('AMOUNT')
                amount_text = amount_elem.text if amount_elem is not None and amount_elem.text else "0"
                
                # Extract currency info
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
                    item_name = inv.find('STOCKITEMNAME').text if inv.find('STOCKITEMNAME') is not None else ""
                    qty_elem = inv.find('ACTUALQTY')
                    rate_elem = inv.find('RATE')
                    amount_elem = inv.find('AMOUNT')
                    discount_elem = inv.find('DISCOUNT')
                    
                    qty = qty_elem.text if qty_elem is not None and qty_elem.text else "0"
                    rate = rate_elem.text if rate_elem is not None and rate_elem.text else "0"
                    amount_text = amount_elem.text if amount_elem is not None and amount_elem.text else "0"
                    discount = discount_elem.text if discount_elem is not None and discount_elem.text else "0"
                    
                    # Extract currency from inventory amount
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
                # No inventory entries, create a row with voucher-level data
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
        df.to_excel(output_filename, index=False)
        
        return df
        
    except Exception as e:
        logger.error(f"Error parsing {voucher_type_name} voucher: {e}", exc_info=True)
        return pd.DataFrame()


# Wrapper functions for sales/purchase vouchers
def normalize_voucher(xml_content):
    """Process sales vouchers"""
    return process_sales_purchase_voucher(xml_content, 'sales', 'sales.xlsx', is_return=False)

def sales_return_voucher(xml_content):
    """Process sales return vouchers"""
    return process_sales_purchase_voucher(xml_content, 'sales_return', 'sales_return.xlsx', is_return=True)

def purchase_voucher(xml_content):
    """Process purchase vouchers"""
    return process_sales_purchase_voucher(xml_content, 'purchase', 'purchase.xlsx', is_return=False)

def purchase_return_voucher(xml_content):
    """Process purchase return vouchers"""
    return process_sales_purchase_voucher(xml_content, 'purchase_return', 'purchase_return.xlsx', is_return=True)