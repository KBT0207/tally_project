import pandas as pd
import os
import time
from datetime import datetime
from pathlib import Path

from services.tally_connector import TallyConnector
from services.data_processor import (
    process_inventory_voucher_to_xlsx,
    process_ledger_voucher_to_xlsx,
    extract_all_ledgers_to_xlsx,
    trial_balance_to_xlsx
)

FROM_DATE = '20250401'
TO_DATE = '20250430'

OUTPUT_DIR = f"tally_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

RETRY_ATTEMPTS = 3
RETRY_DELAY = 2
WAIT_BETWEEN_VOUCHERS = 1

def fetch_with_retry(fetch_function, *args, max_retries=RETRY_ATTEMPTS, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"    Attempt {attempt}/{max_retries}...", end=" ")
            result = fetch_function(*args, **kwargs)
            if result:
                print("✓")
                return result
            print("⚠ Empty response")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"✗ Error: {str(e)[:50]}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
            else:
                raise
    return None

tally = TallyConnector()

print("=" * 80)
print(f"TALLY DATA EXPORT - SEQUENTIAL MODE")
print(f"Date Range: {FROM_DATE} to {TO_DATE}")
print(f"Output Directory: {OUTPUT_DIR}")
print(f"Wait Between Vouchers: {WAIT_BETWEEN_VOUCHERS}s")
print("=" * 80)

companies = tally.fetch_all_companies()
print(f"\nFound {len(companies)} companies\n")

summary_report = []

for idx, company in enumerate(companies, 1):
    comp_name = company.get('name', '').strip()
    
    if not comp_name or comp_name.upper() in ['N/A', 'NA', 'NONE', '']:
        continue
    
    safe_comp_name = "".join(c for c in comp_name if c.isalnum() or c in (' ', '_', '-')).strip()
    company_dir = os.path.join(OUTPUT_DIR, safe_comp_name)
    Path(company_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"{'=' * 80}")
    print(f"[{idx}/{len(companies)}] COMPANY: {comp_name}")
    print(f"{'=' * 80}\n")
    
    company_summary = {
        'Company': comp_name,
        'Trial Balance': 0,
        'Ledgers': 0,
        'Sales': 0,
        'Purchase': 0,
        'Sales Return': 0,
        'Purchase Return': 0,
        'Receipt': 0,
        'Payment': 0,
        'Journal': 0,
        'Contra': 0,
        'Status': 'Processing'
    }
    
    try:
        print("1. TRIAL BALANCE")
        print("-" * 40)
        tb_data = fetch_with_retry(tally.fetch_trial_balance, company_name=comp_name, from_date=FROM_DATE, to_date=TO_DATE)
        if tb_data:
            tb_file = os.path.join(company_dir, "trial_balance.xlsx")
            tb_df = trial_balance_to_xlsx(tb_data, comp_name, FROM_DATE, TO_DATE, tb_file)
            company_summary['Trial Balance'] = len(tb_df)
            print(f"   Exported: {len(tb_df)} records → trial_balance.xlsx")
        else:
            print(f"   No data available")
        print()
        time.sleep(WAIT_BETWEEN_VOUCHERS)
    except Exception as e:
        print(f"   Failed: {e}\n")
    
    try:
        print("2. ALL LEDGERS")
        print("-" * 40)
        ledger_data = fetch_with_retry(tally.fetch_all_ledgers, company_name=comp_name)
        if ledger_data:
            ledger_file = os.path.join(company_dir, "all_ledgers.xlsx")
            ledger_df = extract_all_ledgers_to_xlsx(ledger_data, comp_name, ledger_file)
            company_summary['Ledgers'] = len(ledger_df)
            print(f"   Exported: {len(ledger_df)} records → all_ledgers.xlsx")
        else:
            print(f"   No data available")
        print()
        time.sleep(WAIT_BETWEEN_VOUCHERS)
    except Exception as e:
        print(f"   Failed: {e}\n")
    
    voucher_types = [
        ('Sales', 'fetch_all_sales_vouchers', process_inventory_voucher_to_xlsx, 'sales.xlsx'),
        ('Purchase', 'fetch_all_purchase_voucher', process_inventory_voucher_to_xlsx, 'purchase.xlsx'),
        ('Sales Return', 'fetch_all_sales_return', process_inventory_voucher_to_xlsx, 'sales_return.xlsx'),
        ('Purchase Return', 'fetch_all_purchase_return', process_inventory_voucher_to_xlsx, 'purchase_return.xlsx'),
        ('Receipt', 'fetch_all_receipt_vouchers', process_ledger_voucher_to_xlsx, 'receipt.xlsx'),
        ('Payment', 'fetch_all_payment_vouchers', process_ledger_voucher_to_xlsx, 'payment.xlsx'),
        ('Journal', 'fetch_all_journal_vouchers', process_ledger_voucher_to_xlsx, 'journal.xlsx'),
        ('Contra', 'fetch_all_contra_vouchers', process_ledger_voucher_to_xlsx, 'contra.xlsx'),
    ]
    
    for voucher_num, (voucher_name, fetch_method, process_function, filename) in enumerate(voucher_types, 3):
        try:
            print(f"{voucher_num}. {voucher_name.upper()}")
            print("-" * 40)
            
            fetch_func = getattr(tally, fetch_method)
            voucher_data = fetch_with_retry(fetch_func, company_name=comp_name, from_date=FROM_DATE, to_date=TO_DATE)
            
            if voucher_data:
                voucher_file = os.path.join(company_dir, filename)
                voucher_df = process_function(voucher_data, voucher_name, voucher_file)
                company_summary[voucher_name] = len(voucher_df)
                print(f"   Exported: {len(voucher_df)} records → {filename}")
            else:
                print(f"   No data available")
            
            print()
            time.sleep(WAIT_BETWEEN_VOUCHERS)
            
        except Exception as e:
            print(f"   Failed: {e}\n")
            time.sleep(WAIT_BETWEEN_VOUCHERS)
    
    company_summary['Status'] = 'Completed'
    summary_report.append(company_summary)
    print(f"✓ Company '{comp_name}' completed\n")

print("\n" + "=" * 80)
print("EXPORT SUMMARY")
print("=" * 80 + "\n")

summary_df = pd.DataFrame(summary_report)
summary_file = os.path.join(OUTPUT_DIR, "export_summary.xlsx")
summary_df.to_excel(summary_file, index=False)

print(summary_df.to_string(index=False))
print(f"\nSummary saved: {summary_file}")
print(f"All files saved in: {OUTPUT_DIR}")
print("=" * 80)