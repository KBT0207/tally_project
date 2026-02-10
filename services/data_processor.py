import xml.etree.ElementTree as ET
import pandas as pd
import re
from datetime import datetime
from logging_config import logger
from .currency_extractor import CurrencyExtractor

currency_extractor = CurrencyExtractor(default_currency='INR')

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
    content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', content)
    
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

def convert_to_float(value):
    if value is None or value == "":
        return 0.0
    
    try:
        return float(value)
    except:
        return 0.0

def normalize_voucher(xml_content):
    try:
        xml_content = sanitize_xml_content(xml_content)
        root = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall(".//VOUCHER")
        logger.info(f"Found {len(vouchers)} vouchers")
        
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
            
            voucher_currency = ""
            
            for ledger in ledger_entries:
                ledger_name = ledger.find('LEDGERNAME').text if ledger.find('LEDGERNAME') is not None else ""
                amount_elem = ledger.find('AMOUNT')
                amount_text = amount_elem.text if amount_elem is not None and amount_elem.text else "0"
                
                if not voucher_currency:
                    voucher_currency = currency_extractor.extract_currency(amount_text)
                    if voucher_currency:
                        logger.info(f"Extracted currency '{voucher_currency}' from ledger amount: {amount_text[:50]}")
                
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
                    rate_text = rate_elem.text if rate_elem is not None and rate_elem.text else "0"
                    amount_text = amount_elem.text if amount_elem is not None and amount_elem.text else "0"
                    discount = discount_elem.text if discount_elem is not None and discount_elem.text else "0"
                    
                    if not voucher_currency:
                        voucher_currency = currency_extractor.extract_currency(amount_text) or currency_extractor.extract_currency(rate_text)
                        if voucher_currency:
                            logger.info(f"Extracted currency '{voucher_currency}' from inventory item: {item_name}")
                    
                    qty_parts = qty.split()
                    qty_value = qty_parts[0] if len(qty_parts) > 0 else "0"
                    qty_unit = qty_parts[1] if len(qty_parts) > 1 else ""
                    
                    rate_value = extract_rate_value(rate_text)
                    
                    rate_unit_match = re.search(r'/(\w+)', rate_text)
                    rate_unit = rate_unit_match.group(1) if rate_unit_match else ""
                    
                    amount_value = extract_numeric_amount(amount_text)
                    
                    batch_allocations = inv.findall('.//BATCHALLOCATIONS.LIST')
                    
                    if batch_allocations:
                        for batch in batch_allocations:
                            batch_name = batch.find('BATCHNAME').text if batch.find('BATCHNAME') is not None else ""
                            mfg_date = batch.find('MFDON').text if batch.find('MFDON') is not None else ""
                            godown_name = batch.find('GODOWNNAME').text if batch.find('GODOWNNAME') is not None else ""
                            
                            batch_qty_elem = batch.find('ACTUALQTY')
                            if batch_qty_elem is not None and batch_qty_elem.text:
                                batch_qty_parts = batch_qty_elem.text.split()
                                batch_qty_value = batch_qty_parts[0] if len(batch_qty_parts) > 0 else qty_value
                            else:
                                batch_qty_value = qty_value
                            
                            row_data = base_voucher_data.copy()
                            row_data.update({
                                'item_name': item_name,
                                'qty': convert_to_float(batch_qty_value),
                                'qty_unit': qty_unit,
                                'rate': convert_to_float(rate_value),
                                'rate_unit': rate_unit,
                                'amount': convert_to_float(amount_value),
                                'discount': convert_to_float(extract_numeric_amount(discount)),
                                'batch_no': batch_name,
                                'mfg_date': parse_tally_date(mfg_date),
                                'godown': godown_name,
                                'currency': voucher_currency,
                                'cgst_amt': tax_data['cgst_amt'],
                                'sgst_amt': tax_data['sgst_amt'],
                                'igst_amt': tax_data['igst_amt'],
                                'freight_amt': tax_data['freight_amt'],
                                'dca_amt': tax_data['dca_amt'],
                                'cf_amt': tax_data['cf_amt'],
                                'other_amt': tax_data['other_amt']
                            })
                            all_rows.append(row_data)
                    else:
                        batch_name = inv.find('BATCHNAME').text if inv.find('BATCHNAME') is not None else ""
                        mfg_date = inv.find('MFDON').text if inv.find('MFDON') is not None else ""
                        godown_name = inv.find('GODOWNNAME').text if inv.find('GODOWNNAME') is not None else ""
                        
                        row_data = base_voucher_data.copy()
                        row_data.update({
                            'item_name': item_name,
                            'qty': convert_to_float(qty_value),
                            'qty_unit': qty_unit,
                            'rate': convert_to_float(rate_value),
                            'rate_unit': rate_unit,
                            'amount': convert_to_float(amount_value),
                            'discount': convert_to_float(extract_numeric_amount(discount)),
                            'batch_no': batch_name,
                            'mfg_date': parse_tally_date(mfg_date),
                            'godown': godown_name,
                            'currency': voucher_currency,
                            'cgst_amt': tax_data['cgst_amt'],
                            'sgst_amt': tax_data['sgst_amt'],
                            'igst_amt': tax_data['igst_amt'],
                            'freight_amt': tax_data['freight_amt'],
                            'dca_amt': tax_data['dca_amt'],
                            'cf_amt': tax_data['cf_amt'],
                            'other_amt': tax_data['other_amt']
                        })
                        all_rows.append(row_data)
            else:
                row_data = base_voucher_data.copy()
                row_data.update({
                    'item_name': 'NO Item',
                    'qty': 0.0,
                    'qty_unit': 'No Unit',
                    'rate': 0.0,
                    'rate_unit': 'No Unit',
                    'amount': 0.0,
                    'discount': 0.0,
                    'batch_no': '',
                    'mfg_date': None,
                    'godown': '',
                    'currency': voucher_currency,
                    'cgst_amt': tax_data['cgst_amt'],
                    'sgst_amt': tax_data['sgst_amt'],
                    'igst_amt': tax_data['igst_amt'],
                    'freight_amt': tax_data['freight_amt'],
                    'dca_amt': tax_data['dca_amt'],
                    'cf_amt': tax_data['cf_amt'],
                    'other_amt': tax_data['other_amt']
                })
                all_rows.append(row_data)
        
        df = pd.DataFrame(all_rows)
        
        column_order = [
            'date',
            'voucher_number',
            'voucher_type',
            'reference',
            'party_name',
            'party_gstin',
            'place_of_supply',
            'item_name',
            'qty',
            'qty_unit',
            'rate',
            'rate_unit',
            'amount',
            'discount',
            'currency',
            'batch_no',
            'mfg_date',
            'godown',
            'cgst_amt',
            'sgst_amt',
            'igst_amt',
            'freight_amt',
            'dca_amt',
            'cf_amt',
            'other_amt',
            'narration',
            'entered_by',
            'basic_ship_doc_no',
            'voucher_key',
            'guid',
            'alter_id',
            'master_id',
        ]
        
        existing_columns = [col for col in column_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in existing_columns]
        final_column_order = existing_columns + remaining_columns
        df = df[final_column_order]
        
        df.to_excel('purchase.xlsx', index=False)
        logger.info(f"Created DataFrame with {len(df)} rows (one per item)")
        logger.info(f"Currency extraction summary: {df['currency'].value_counts().to_dict()}")
        return df
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return pd.DataFrame()