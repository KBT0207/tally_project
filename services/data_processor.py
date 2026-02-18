import xml.etree.ElementTree as ET
import pandas as pd
import re
import traceback
import time
from datetime import datetime
from logging_config import logger

try:
    from .currency_extractor import CurrencyExtractor
except ImportError:
    from currency_extractor import CurrencyExtractor

currency_extractor = CurrencyExtractor(default_currency='INR')


class ProcessingTimer:
    def __init__(self, process_name):
        self.process_name = process_name
        self.start_time   = None
        self.end_time     = None

    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"Started: {self.process_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time
        logger.info(f"Completed: {self.process_name} - Time taken: {elapsed:.2f} seconds")


def clean_text(text):
    if not text:
        return ""
    text = str(text).replace('&#13;&#10;', ' ').replace('&#13;', ' ').replace('&#10;', ' ')
    text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    return re.sub(r'\s+', ' ', text).strip()


def sanitize_xml_content(content):
    if content is None:
        logger.error("XML content is None")
        return ""

    if isinstance(content, bytes):
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            content = content.decode('latin-1')

    content = str(content)
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
    content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)', '&amp;', content)

    return content


def extract_numeric_amount(text):
    if not text:
        return "0"

    text = str(text)
    final_amount_match = re.search(r'=\s*[?]?\s*[-]?(\d+\.?\d*)', text)
    if final_amount_match:
        return final_amount_match.group(1)

    numeric_match = re.search(r'[-]?(\d+\.?\d*)', text)
    if numeric_match:
        return numeric_match.group(1)

    return "0"


def parse_tally_date_formatted(date_str):
    if not date_str or date_str.strip() == "":
        return None
    try:
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    except Exception:
        return None


def parse_expiry_date(exp_date_text):
    if not exp_date_text or exp_date_text.strip() == "":
        return ""

    exp_date_text = str(exp_date_text).strip()

    try:
        for fmt in ['%d-%b-%y', '%d-%b-%Y']:
            try:
                return datetime.strptime(exp_date_text, fmt).strftime('%Y-%m-%d')
            except Exception:
                continue
        return exp_date_text
    except Exception:
        return exp_date_text


def convert_to_float(value):
    if value is None or value == "":
        return 0.0
    try:
        return abs(float(value))
    except Exception:
        return 0.0


def extract_unit_from_rate(rate_text):
    if not rate_text:
        return ""
    match = re.search(r'/\s*(\w+)\s*$', str(rate_text))
    return match.group(1) if match else ""


def parse_quantity_with_unit(qty_text):
    if not qty_text:
        return (0.0, "")

    qty_text = str(qty_text).strip()
    match = re.match(r'[-]?(\d+\.?\d*)\s*(\w*)', qty_text)
    if match:
        qty  = convert_to_float(match.group(1))
        unit = match.group(2) if match.group(2) else ""
        return (qty, unit)
    return (0.0, "")


def extract_currency_and_values(rate_text=None, amount_text=None, discount_text=None):
    result = {
        'currency'     : 'INR',
        'rate'         : 0.0,
        'amount'       : 0.0,
        'discount'     : 0.0,
        'exchange_rate': 1.0,
    }

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
            if '€' in str(amount_text) or '€' in str(rate_text) or 'EUR' in str(amount_text).upper():
                detected_currency = 'EUR'
            elif '$' in str(amount_text) or '$' in str(rate_text) or 'USD' in str(amount_text).upper():
                detected_currency = 'USD'
            elif '£' in str(amount_text) or '£' in str(rate_text) or 'GBP' in str(amount_text).upper():
                detected_currency = 'GBP'
            elif '?' in str(amount_text) or '?' in str(rate_text):
                detected_currency = 'EUR'

    result['currency'] = detected_currency

    if detected_currency == 'INR':
        result['exchange_rate'] = 1.0

        if rate_text:
            result['rate'] = convert_to_float(extract_numeric_amount(rate_text))

        if amount_text:
            result['amount'] = convert_to_float(extract_numeric_amount(amount_text))

        if discount_text:
            result['discount'] = convert_to_float(extract_numeric_amount(discount_text))

    else:
        if amount_text:
            amount_details = currency_extractor.extract_foreign_currency_details(amount_text)

            if amount_details['foreign_amount']:
                result['amount'] = abs(amount_details['foreign_amount'])

            if amount_details['exchange_rate']:
                result['exchange_rate'] = amount_details['exchange_rate']

            if not result['exchange_rate'] or result['exchange_rate'] == 1.0:
                fallback_pattern = r'[-]?(\d+\.?\d*)\s*[€$£¥?]\s*@\s*[€$£¥?]\s*[-]?(\d+\.?\d*)\s*/\s*[€$£¥?]'
                fallback_match = re.search(fallback_pattern, amount_text)
                if fallback_match:
                    if not result['amount']:
                        result['amount'] = convert_to_float(fallback_match.group(1))
                    result['exchange_rate'] = convert_to_float(fallback_match.group(2))

        if rate_text:
            rate_details = currency_extractor.extract_foreign_currency_details(rate_text)

            if rate_details['foreign_amount']:
                result['rate'] = abs(rate_details['foreign_amount'])

            if not result['rate']:
                rate_fallback_match = re.search(r'[-]?(\d+\.?\d*)\s*[€$£¥?]\s*=', rate_text)
                if rate_fallback_match:
                    result['rate'] = convert_to_float(rate_fallback_match.group(1))

            if not result['exchange_rate'] or result['exchange_rate'] == 1.0:
                if rate_details['foreign_amount'] and rate_details['base_amount']:
                    foreign_val = abs(rate_details['foreign_amount'])
                    inr_val     = abs(rate_details['base_amount'])
                    if foreign_val:
                        result['exchange_rate'] = inr_val / foreign_val

        if discount_text:
            discount_details = currency_extractor.extract_foreign_currency_details(discount_text)
            if discount_details['foreign_amount']:
                result['discount'] = abs(discount_details['foreign_amount'])
            else:
                result['discount'] = convert_to_float(extract_numeric_amount(discount_text))

    return result


def parse_ledger_voucher(xml_content, company_name: str, voucher_type_name: str = 'ledger') -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning(f"Empty or None XML content for {voucher_type_name}")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning(f"Empty XML after sanitization for {voucher_type_name}")
            return []

        root     = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall('.//VOUCHER')
        logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")

        if not vouchers:
            return []

        all_rows = []

        for voucher in vouchers:
            guid           = voucher.findtext('GUID', '')
            alter_id       = voucher.findtext('ALTERID', '0')
            master_id      = voucher.findtext('MASTERID', '')
            voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            voucher_type   = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
            date           = clean_text(voucher.findtext('DATE', ''))
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_text(voucher.findtext('NARRATION', ''))

            action          = voucher.get('ACTION', 'Unknown')
            is_deleted      = voucher.findtext('ISDELETED', 'No')
            change_status   = 'Deleted' if is_deleted == 'Yes' else action
            is_deleted_flag = 'Yes' if change_status in ('Deleted', 'Delete') else 'No'

            ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
            if not ledger_entries:
                ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')

            # Deleted vouchers from CDC have no entries — emit a stub row so
            # the upsert layer can mark the existing DB record as deleted.
            if not ledger_entries and is_deleted_flag == 'Yes':
                all_rows.append({
                    'company_name'  : company_name,
                    'date'          : parse_tally_date_formatted(date),
                    'voucher_type'  : voucher_type,
                    'voucher_number': voucher_number,
                    'reference'     : reference,
                    'ledger_name'   : '',
                    'amount'        : 0.0,
                    'amount_type'   : None,
                    'currency'      : 'INR',
                    'exchange_rate' : 1.0,
                    'narration'     : narration,
                    'guid'          : guid,
                    'alter_id'      : int(alter_id) if alter_id else 0,
                    'master_id'     : master_id,
                    'change_status' : change_status,
                    'is_deleted'    : 'Yes',
                })
                continue

            voucher_exchange_rate = 1.0
            voucher_currency      = 'INR'

            for ledger in ledger_entries:
                amount_text   = clean_text(ledger.findtext('AMOUNT', '0'))
                temp_currency = extract_currency_and_values(None, amount_text)
                if temp_currency['currency'] != 'INR' and temp_currency['exchange_rate'] > 1.0:
                    voucher_exchange_rate = temp_currency['exchange_rate']
                    voucher_currency      = temp_currency['currency']
                    break

            for ledger in ledger_entries:
                ledger_name   = clean_text(ledger.findtext('LEDGERNAME', ''))
                amount_text   = clean_text(ledger.findtext('AMOUNT', '0'))
                currency_info = extract_currency_and_values(None, amount_text)

                if currency_info['exchange_rate'] == 1.0 and voucher_exchange_rate > 1.0:
                    currency_info['exchange_rate'] = voucher_exchange_rate
                    currency_info['currency']      = voucher_currency

                amount_type     = 'Debit' if currency_info['amount'] < 0 else 'Credit'
                absolute_amount = abs(currency_info['amount'])

                all_rows.append({
                    'company_name'  : company_name,
                    'date'          : parse_tally_date_formatted(date),
                    'voucher_type'  : voucher_type,
                    'voucher_number': voucher_number,
                    'reference'     : reference,
                    'ledger_name'   : ledger_name,
                    'amount'        : absolute_amount,
                    'amount_type'   : amount_type,
                    'currency'      : currency_info['currency'],
                    'exchange_rate' : currency_info['exchange_rate'],
                    'narration'     : narration,
                    'guid'          : guid,
                    'alter_id'      : int(alter_id) if alter_id else 0,
                    'master_id'     : master_id,
                    'change_status' : change_status,
                    'is_deleted'    : is_deleted_flag,
                })

        logger.info(f"Parsed {len(all_rows)} rows for {voucher_type_name} [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in {voucher_type_name}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing {voucher_type_name}: {e}")
        logger.error(traceback.format_exc())
        return []


def parse_inventory_voucher(xml_content, company_name: str, voucher_type_name: str = 'inventory') -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning(f"Empty or None XML content for {voucher_type_name}")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning(f"Empty XML after sanitization for {voucher_type_name}")
            return []

        root     = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall('.//VOUCHER')
        logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")

        if not vouchers:
            return []

        all_rows = []

        for voucher in vouchers:
            guid           = voucher.findtext('GUID', '')
            alter_id       = voucher.findtext('ALTERID', '0')
            master_id      = voucher.findtext('MASTERID', '')
            voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            voucher_type   = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
            date           = clean_text(voucher.findtext('DATE', ''))
            party_name     = clean_text(voucher.findtext('PARTYNAME', ''))
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_text(voucher.findtext('NARRATION', ''))
            party_gstin    = clean_text(voucher.findtext('PARTYGSTIN', ''))
            irn_number     = clean_text(voucher.findtext('IRNACKNO', ''))
            eway_bill      = clean_text(voucher.findtext('TEMPGSTEWAYBILLNUMBER', ''))

            action          = voucher.get('ACTION', 'Unknown')
            is_deleted      = voucher.findtext('ISDELETED', 'No')
            change_status   = 'Deleted' if is_deleted == 'Yes' else action
            is_deleted_flag = 'Yes' if change_status in ('Deleted', 'Delete') else 'No'

            ledger_entries    = voucher.findall('.//ALLLEDGERENTRIES.LIST') or voucher.findall('.//LEDGERENTRIES.LIST')
            inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST') or voucher.findall('.//INVENTORYENTRIES.LIST')

            # Deleted vouchers from CDC arrive with no entries — emit a stub row
            # so the upsert layer can mark ALL existing DB rows for this guid deleted.
            if is_deleted_flag == 'Yes' and not ledger_entries and not inventory_entries:
                all_rows.append({
                    'company_name'    : company_name,
                    'date'            : parse_tally_date_formatted(date),
                    'voucher_number'  : voucher_number,
                    'reference'       : reference,
                    'voucher_type'    : voucher_type,
                    'party_name'      : party_name,
                    'gst_number'      : party_gstin,
                    'e_invoice_number': irn_number,
                    'eway_bill'       : eway_bill,
                    'item_name'       : '',
                    'quantity'        : 0.0,
                    'unit'            : '',
                    'alt_qty'         : 0.0,
                    'alt_unit'        : '',
                    'batch_no'        : '',
                    'mfg_date'        : '',
                    'exp_date'        : '',
                    'hsn_code'        : '',
                    'gst_rate'        : 0.0,
                    'rate'            : 0.0,
                    'amount'          : 0.0,
                    'discount'        : 0.0,
                    'cgst_amt'        : 0.0,
                    'sgst_amt'        : 0.0,
                    'igst_amt'        : 0.0,
                    'freight_amt'     : 0.0,
                    'dca_amt'         : 0.0,
                    'cf_amt'          : 0.0,
                    'other_amt'       : 0.0,
                    'total_amt'       : 0.0,
                    'currency'        : 'INR',
                    'exchange_rate'   : 1.0,
                    'narration'       : narration,
                    'guid'            : guid,
                    'alter_id'        : int(alter_id) if alter_id else 0,
                    'master_id'       : master_id,
                    'change_status'   : change_status,
                    'is_deleted'      : 'Yes',
                })
                continue

            voucher_exchange_rate = 1.0
            voucher_currency      = 'INR'

            for ledger in ledger_entries:
                amount_text   = clean_text(ledger.findtext('AMOUNT', '0'))
                temp_currency = extract_currency_and_values(None, amount_text)
                if temp_currency['currency'] != 'INR' and temp_currency['exchange_rate'] > 1.0:
                    voucher_exchange_rate = temp_currency['exchange_rate']
                    voucher_currency      = temp_currency['currency']
                    break

            if voucher_exchange_rate == 1.0:
                for inv in inventory_entries:
                    rate_elem   = inv.find('RATE')
                    amount_elem = inv.find('AMOUNT')
                    rate_text   = clean_text(rate_elem.text if rate_elem is not None and rate_elem.text else '0')
                    amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else '0')
                    temp_cur    = extract_currency_and_values(rate_text, amount_text, None)
                    if temp_cur['currency'] != 'INR' and temp_cur['exchange_rate'] > 1.0:
                        voucher_exchange_rate = temp_cur['exchange_rate']
                        voucher_currency      = temp_cur['currency']
                        break

            voucher_gst_data = {
                'cgst_total': 0.0, 'sgst_total': 0.0, 'igst_total': 0.0,
                'cgst_rate' : 0.0, 'sgst_rate' : 0.0, 'igst_rate' : 0.0,
            }
            voucher_charges = {
                'freight_amt': 0.0, 'dca_amt': 0.0,
                'cf_amt'     : 0.0, 'other_amt': 0.0,
            }

            for ledger in ledger_entries:
                ledger_name       = clean_text(ledger.findtext('LEDGERNAME', ''))
                ledger_name_lower = ledger_name.lower()
                amount_text       = clean_text(ledger.findtext('AMOUNT', '0'))
                amount            = convert_to_float(extract_numeric_amount(amount_text))

                if re.search(r'cgst|c\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                    voucher_gst_data['cgst_total'] += amount
                    rate_match = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name)
                    if rate_match and voucher_gst_data['cgst_rate'] == 0.0:
                        voucher_gst_data['cgst_rate'] = convert_to_float(rate_match.group(1))
                elif re.search(r'sgst|s\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                    voucher_gst_data['sgst_total'] += amount
                    rate_match = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name)
                    if rate_match and voucher_gst_data['sgst_rate'] == 0.0:
                        voucher_gst_data['sgst_rate'] = convert_to_float(rate_match.group(1))
                elif re.search(r'igst|i\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                    voucher_gst_data['igst_total'] += amount
                    rate_match = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name)
                    if rate_match and voucher_gst_data['igst_rate'] == 0.0:
                        voucher_gst_data['igst_rate'] = convert_to_float(rate_match.group(1))
                elif re.search(r'freight', ledger_name_lower):
                    voucher_charges['freight_amt'] += amount
                elif re.search(r'dca', ledger_name_lower):
                    voucher_charges['dca_amt'] += amount
                elif re.search(r'clearing\s*&?\s*forwarding', ledger_name_lower):
                    voucher_charges['cf_amt'] += amount
                elif (ledger_name.strip() and ledger_name != party_name and amount > 0.01
                      and not re.search(r'round', ledger_name_lower)):
                    is_gst = re.search(r'cgst|sgst|igst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower)
                    if not is_gst and not re.search(r'duty|cess', ledger_name_lower):
                        voucher_charges['other_amt'] += amount

            has_real_inventory = False
            if inventory_entries:
                for inv in inventory_entries:
                    item_name   = clean_text(inv.findtext('STOCKITEMNAME', ''))
                    amount_elem = inv.find('AMOUNT')
                    amount_text = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else '0')
                    if item_name and convert_to_float(extract_numeric_amount(amount_text)) > 0.01:
                        has_real_inventory = True
                        break

            temp_item_data    = []
            total_item_amount = 0.0

            if has_real_inventory:
                for inv in inventory_entries:
                    item_name       = clean_text(inv.findtext('STOCKITEMNAME', ''))
                    qty_elem        = inv.find('ACTUALQTY')
                    rate_elem       = inv.find('RATE')
                    amount_elem     = inv.find('AMOUNT')
                    discount_elem   = inv.find('DISCOUNT')
                    billed_qty_elem = inv.find('BILLEDQTY')

                    qty           = clean_text(qty_elem.text if qty_elem is not None and qty_elem.text else '0')
                    rate_text     = clean_text(rate_elem.text if rate_elem is not None and rate_elem.text else '0')
                    amount_text   = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else '0')
                    discount_text = clean_text(discount_elem.text if discount_elem is not None and discount_elem.text else '0')
                    billed_qty    = clean_text(billed_qty_elem.text if billed_qty_elem is not None and billed_qty_elem.text else '0')

                    unit              = extract_unit_from_rate(rate_text)
                    alt_qty, alt_unit = parse_quantity_with_unit(billed_qty)

                    batch_no = mfg_date = exp_date = ''
                    batch_allocations = inv.findall('.//BATCHALLOCATIONS.LIST')
                    if batch_allocations:
                        batch    = batch_allocations[0]
                        batch_no = clean_text(batch.findtext('BATCHNAME', ''))
                        mfg_raw  = clean_text(batch.findtext('MFDON', ''))
                        mfg_date = parse_tally_date_formatted(mfg_raw) if mfg_raw else ''
                        exp_elem = batch.find('EXPIRYPERIOD')
                        if exp_elem is not None:
                            if exp_elem.text:
                                exp_date = parse_expiry_date(exp_elem.text)
                            exp_jd = exp_elem.get('JD', '')
                            if exp_jd and not exp_date:
                                exp_date = parse_tally_date_formatted(exp_jd) or ''

                    hsn_code = ''
                    for acc_alloc in inv.findall('.//ACCOUNTINGALLOCATIONS.LIST'):
                        hsn_code = clean_text(acc_alloc.findtext('GSTHSNSACCODE', ''))
                        if hsn_code:
                            break

                    currency_data = extract_currency_and_values(rate_text, amount_text, discount_text)
                    if currency_data['exchange_rate'] == 1.0 and voucher_exchange_rate > 1.0:
                        currency_data['exchange_rate'] = voucher_exchange_rate
                        currency_data['currency']      = voucher_currency

                    qty_numeric        = convert_to_float(extract_numeric_amount(qty))
                    item_amount        = currency_data['amount']
                    total_item_amount += item_amount

                    temp_item_data.append({
                        'item_name'    : item_name,
                        'quantity'     : qty_numeric,
                        'unit'         : unit,
                        'alt_qty'      : alt_qty,
                        'alt_unit'     : alt_unit,
                        'batch_no'     : batch_no,
                        'mfg_date'     : mfg_date,
                        'exp_date'     : exp_date,
                        'hsn_code'     : hsn_code,
                        'rate'         : currency_data['rate'],
                        'amount'       : item_amount,
                        'discount'     : currency_data['discount'],
                        'currency'     : currency_data['currency'],
                        'exchange_rate': currency_data['exchange_rate'],
                    })

            gst_rate = voucher_gst_data['cgst_rate'] + voucher_gst_data['sgst_rate'] + voucher_gst_data['igst_rate']

            base = {
                'company_name'    : company_name,
                'date'            : parse_tally_date_formatted(date),
                'voucher_number'  : voucher_number,
                'reference'       : reference,
                'voucher_type'    : voucher_type,
                'party_name'      : party_name,
                'gst_number'      : party_gstin,
                'e_invoice_number': irn_number,
                'eway_bill'       : eway_bill,
                'change_status'   : change_status,
                'is_deleted'      : is_deleted_flag,
                'narration'       : narration,
                'guid'            : guid,
                'alter_id'        : int(alter_id) if alter_id else 0,
                'master_id'       : master_id,
            }

            if not temp_item_data or total_item_amount == 0:
                all_rows.append({
                    **base,
                    'item_name'    : 'No Item',
                    'quantity'     : 0.0,
                    'unit'         : 'No Unit',
                    'alt_qty'      : 0.0,
                    'alt_unit'     : '',
                    'batch_no'     : '',
                    'mfg_date'     : '',
                    'exp_date'     : '',
                    'hsn_code'     : '',
                    'gst_rate'     : gst_rate,
                    'rate'         : 0.0,
                    'amount'       : 0.0,
                    'discount'     : 0.0,
                    'cgst_amt'     : voucher_gst_data['cgst_total'],
                    'sgst_amt'     : voucher_gst_data['sgst_total'],
                    'igst_amt'     : voucher_gst_data['igst_total'],
                    'freight_amt'  : voucher_charges['freight_amt'],
                    'dca_amt'      : voucher_charges['dca_amt'],
                    'cf_amt'       : voucher_charges['cf_amt'],
                    'other_amt'    : voucher_charges['other_amt'],
                    'total_amt'    : (voucher_gst_data['cgst_total'] + voucher_gst_data['sgst_total'] +
                                      voucher_gst_data['igst_total'] + voucher_charges['freight_amt'] +
                                      voucher_charges['dca_amt'] + voucher_charges['cf_amt'] +
                                      voucher_charges['other_amt']),
                    'currency'     : voucher_currency,
                    'exchange_rate': voucher_exchange_rate,
                })
            else:
                voucher_total = (
                    total_item_amount +
                    voucher_gst_data['cgst_total'] + voucher_gst_data['sgst_total'] + voucher_gst_data['igst_total'] +
                    voucher_charges['freight_amt'] + voucher_charges['dca_amt'] +
                    voucher_charges['cf_amt'] + voucher_charges['other_amt']
                )
                for item in temp_item_data:
                    proportion = item['amount'] / total_item_amount if total_item_amount else 0
                    all_rows.append({
                        **base,
                        'item_name'    : item['item_name'],
                        'quantity'     : item['quantity'],
                        'unit'         : item['unit'],
                        'alt_qty'      : item['alt_qty'],
                        'alt_unit'     : item['alt_unit'],
                        'batch_no'     : item['batch_no'],
                        'mfg_date'     : item['mfg_date'],
                        'exp_date'     : item['exp_date'],
                        'hsn_code'     : item['hsn_code'],
                        'gst_rate'     : gst_rate,
                        'rate'         : item['rate'],
                        'amount'       : item['amount'],
                        'discount'     : item['discount'],
                        'cgst_amt'     : voucher_gst_data['cgst_total'] * proportion,
                        'sgst_amt'     : voucher_gst_data['sgst_total'] * proportion,
                        'igst_amt'     : voucher_gst_data['igst_total'] * proportion,
                        'freight_amt'  : voucher_charges['freight_amt'],
                        'dca_amt'      : voucher_charges['dca_amt'],
                        'cf_amt'       : voucher_charges['cf_amt'],
                        'other_amt'    : voucher_charges['other_amt'],
                        'total_amt'    : voucher_total,
                        'currency'     : item['currency'],
                        'exchange_rate': item['exchange_rate'],
                    })

        logger.info(f"Parsed {len(all_rows)} rows for {voucher_type_name} [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in {voucher_type_name}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing {voucher_type_name}: {e}")
        logger.error(traceback.format_exc())
        return []


def parse_ledgers(xml_content, company_name: str) -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning("Empty or None XML content for ledgers")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning("Empty XML after sanitization for ledgers")
            return []

        root    = ET.fromstring(xml_content.encode('utf-8'))
        ledgers = root.findall('.//LEDGER')

        if not ledgers:
            logger.warning("No ledgers found in XML")
            return []

        all_rows = []

        for ledger in ledgers:
            ledger_name    = ledger.get('NAME', '')
            guid           = clean_text(ledger.findtext('GUID', ''))
            alter_id       = clean_text(ledger.findtext('ALTERID', '0'))
            parent         = clean_text(ledger.findtext('PARENT', ''))
            created_date   = clean_text(ledger.findtext('CREATEDDATE', ''))
            altered_on     = clean_text(ledger.findtext('ALTEREDON', ''))
            email          = clean_text(ledger.findtext('EMAIL', ''))
            website        = clean_text(ledger.findtext('WEBSITE', ''))
            phone          = clean_text(ledger.findtext('LEDGERPHONE', ''))
            mobile         = clean_text(ledger.findtext('LEDGERMOBILE', ''))
            fax            = clean_text(ledger.findtext('LEDGERFAX', ''))
            contact_person = clean_text(ledger.findtext('LEDGERCONTACT', ''))

            aliases      = []
            direct_alias = clean_text(ledger.findtext('ALIAS', ''))
            if direct_alias and direct_alias != ledger_name:
                aliases.append(direct_alias)
            for lang_list in ledger.findall('.//LANGUAGENAME.LIST'):
                for name_list in lang_list.findall('.//NAME.LIST'):
                    for name in name_list.findall('NAME'):
                        alias_text = clean_text(name.text or '')
                        if alias_text and alias_text != ledger_name and alias_text not in aliases:
                            aliases.append(alias_text)

            address_lines = []
            for addr_list in ledger.findall('.//ADDRESS.LIST'):
                for address in addr_list.findall('ADDRESS'):
                    addr_text = clean_text(address.text or '')
                    if addr_text:
                        address_lines.append(addr_text)

            all_rows.append({
                'company_name'         : company_name,
                'ledger_name'          : ledger_name,
                'alias'                : aliases[0] if len(aliases) > 0 else '',
                'alias_2'              : aliases[1] if len(aliases) > 1 else '',
                'alias_3'              : aliases[2] if len(aliases) > 2 else '',
                'parent_group'         : parent,
                'contact_person'       : contact_person,
                'email'                : email,
                'phone'                : phone,
                'mobile'               : mobile,
                'fax'                  : fax,
                'website'              : website,
                'address_line_1'       : address_lines[0] if len(address_lines) > 0 else '',
                'address_line_2'       : address_lines[1] if len(address_lines) > 1 else '',
                'address_line_3'       : address_lines[2] if len(address_lines) > 2 else '',
                'pincode'              : clean_text(ledger.findtext('PINCODE', '')),
                'state'                : clean_text(ledger.findtext('STATENAME', '')),
                'country'              : clean_text(ledger.findtext('COUNTRYNAME', '')),
                'opening_balance'      : clean_text(ledger.findtext('OPENINGBALANCE', '0')),
                'credit_limit'         : clean_text(ledger.findtext('CREDITLIMIT', '0')),
                'bill_credit_period'   : clean_text(ledger.findtext('BILLCREDITPERIOD', '')),
                'pan'                  : clean_text(ledger.findtext('INCOMETAXNUMBER', '')),
                'gstin'                : clean_text(ledger.findtext('PARTYGSTIN', '')),
                'gst_registration_type': clean_text(ledger.findtext('GSTREGISTRATIONTYPE', '')),
                'vat_tin'              : clean_text(ledger.findtext('VATTINNUMBER', '')),
                'sales_tax_number'     : clean_text(ledger.findtext('SALESTAXNUMBER', '')),
                'bank_account_holder'  : clean_text(ledger.findtext('BANKACCHOLDERNAME', '')),
                'ifsc_code'            : clean_text(ledger.findtext('IFSCODE', '')),
                'bank_branch'          : clean_text(ledger.findtext('BRANCHNAME', '')),
                'swift_code'           : clean_text(ledger.findtext('SWIFTCODE', '')),
                'bank_iban'            : clean_text(ledger.findtext('BANKIBAN', '')),
                'export_import_code'   : clean_text(ledger.findtext('EXPORTIMPORTCODE', '')),
                'msme_reg_number'      : clean_text(ledger.findtext('MSMEREGNUMBER', '')),
                'is_bill_wise_on'      : clean_text(ledger.findtext('ISBILLWISEON', 'No')),
                'is_deleted'           : clean_text(ledger.findtext('ISDELETED', 'No')),
                'created_date'         : created_date,
                'altered_on'           : altered_on,
                'guid'                 : guid,
                'alter_id'             : int(alter_id) if alter_id else 0,
            })

        logger.info(f"Parsed {len(all_rows)} ledgers [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in ledgers: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing ledgers: {e}")
        logger.error(traceback.format_exc())
        return []



def parse_trial_balance(xml_content, company_name: str, start_date: str, end_date: str) -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning("Empty or None XML content for trial balance")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning("Empty XML after sanitization for trial balance")
            return []

        root         = ET.fromstring(xml_content.encode('utf-8'))
        ledger_nodes = root.findall('.//LEDGER')

        if not ledger_nodes:
            logger.warning("No ledger nodes found in trial balance XML")
            return []

        all_rows = []

        for ledger in ledger_nodes:
            ledger_name = ledger.get('NAME', '')
            if not ledger_name:
                ledger_name = ledger.findtext('LEDGERNAME', '')
            ledger_name = clean_text(ledger_name)

            if not ledger_name:
                continue

            guid         = clean_text(ledger.findtext('GUID',     ''))
            alter_id     = clean_text(ledger.findtext('ALTERID',  '0'))
            master_id    = clean_text(ledger.findtext('MASTERID', ''))
            parent_group = clean_text(ledger.findtext('PARENT',   ''))

            opening_text     = clean_text(ledger.findtext('OPENINGBALANCE', '0'))
            closing_text     = clean_text(ledger.findtext('CLOSINGBALANCE', '0'))
            opening_val      = convert_to_float(extract_numeric_amount(str(opening_text)))
            closing_val      = convert_to_float(extract_numeric_amount(str(closing_text)))
            net_transactions = closing_val - opening_val

            all_rows.append({
                'company_name'    : company_name,
                'ledger_name'     : ledger_name,
                'parent_group'    : parent_group,
                'opening_balance' : opening_val,
                'net_transactions': net_transactions,
                'closing_balance' : closing_val,
                'start_date'      : start_date,
                'end_date'        : end_date,
                'guid'            : guid,
                'alter_id'        : int(alter_id) if alter_id else 0,
                'master_id'       : master_id,
            })

        logger.info(f"Parsed {len(all_rows)} trial balance rows [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in trial balance: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing trial balance: {e}")
        logger.error(traceback.format_exc())
        return []