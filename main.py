"""
Comprehensive Tally Data Export Script
Exports: All Vouchers, Trial Balance, Inventory, Ledgers, Account Master
Date Range: 20240401 to 20250228
Includes: Sales, Purchase, Sales Return, Purchase Return, Journal, Payment, Receipt
"""

from services.tally_connector import TallyConnector
from services.data_processor import (
    process_inventory_voucher_to_xlsx,
    process_ledger_voucher_to_xlsx,
    extract_all_ledgers_to_xlsx,
    trial_balance_to_xlsx
)
import pandas as pd
import os
from datetime import datetime

# Configuration
FROM_DATE = '20240401'  # April 1, 2024
TO_DATE = '20240228'    # February 28, 2025
OUTPUT_DIR = 'tally_exports'  # Output directory

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Tally Connector
tally = TallyConnector()

# Fetch all companies
print("="*80)
print("TALLY COMPREHENSIVE DATA EXPORT")
print("="*80)
print(f"Date Range: {FROM_DATE} to {TO_DATE}")
print(f"Output Directory: {OUTPUT_DIR}")
print("="*80)

companies = tally.fetch_all_companies()
print(f"\nTotal companies found: {len(companies)}")

# Process each company
for idx, company in enumerate(companies, 1):
    comp_name = company.get('name', '').strip()
    
    # Skip empty, None, or N/A company names
    if not comp_name or comp_name.upper() in ['N/A', 'NA', 'NONE', '']:
        print(f"\n[{idx}/{len(companies)}] Skipping company with empty/invalid name")
        continue
    
    print(f"\n{'='*80}")
    print(f"[{idx}/{len(companies)}] Processing: {comp_name}")
    print(f"{'='*80}")
    
    # Create company-specific output directory
    comp_dir = os.path.join(OUTPUT_DIR, comp_name.replace('/', '_').replace('\\', '_'))
    os.makedirs(comp_dir, exist_ok=True)
    
    export_summary = {
        'company': comp_name,
        'exports': []
    }
    
    # ========================================================================
    # 1. ACCOUNT MASTER (All Ledgers)
    # ========================================================================
    print(f"\n[1/9] Extracting Account Master (All Ledgers)...")
    try:
        ledgers_xml = tally.fetch_all_ledgers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if ledgers_xml and ledgers_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Account_Master.xlsx')
            df_ledgers = extract_all_ledgers_to_xlsx(
                ledgers_xml,
                company_name=comp_name,
                output_filename=output_file
            )
            
            if len(df_ledgers) > 0:
                print(f"  ✓ Account Master: {len(df_ledgers)} ledgers exported")
                export_summary['exports'].append({
                    'type': 'Account Master',
                    'rows': len(df_ledgers),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No ledgers found")
        else:
            print(f"  ⚠ No ledger data available")
    except Exception as e:
        print(f"  ✗ Error extracting Account Master: {e}")
    
    # ========================================================================
    # 2. TRIAL BALANCE
    # ========================================================================
    print(f"\n[2/9] Extracting Trial Balance...")
    try:
        trial_xml = tally.fetch_trial_balance(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if trial_xml and trial_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Trial_Balance.xlsx')
            df_trial = trial_balance_to_xlsx(
                trial_xml,
                company_name=comp_name,
                start_date=FROM_DATE,
                end_date=TO_DATE,
                output_filename=output_file
            )
            
            if len(df_trial) > 0:
                print(f"  ✓ Trial Balance: {len(df_trial)} accounts exported")
                export_summary['exports'].append({
                    'type': 'Trial Balance',
                    'rows': len(df_trial),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No trial balance data")
        else:
            print(f"  ⚠ No trial balance data available")
    except Exception as e:
        print(f"  ✗ Error extracting Trial Balance: {e}")
    
    # ========================================================================
    # 3. JOURNAL VOUCHERS
    # ========================================================================
    print(f"\n[3/9] Extracting Journal Vouchers...")
    try:
        journal_xml = tally.fetch_all_journal_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if journal_xml and journal_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Journal_Vouchers.xlsx')
            df_journal = process_ledger_voucher_to_xlsx(
                journal_xml,
                voucher_type_name='journal',
                output_filename=output_file
            )
            
            if len(df_journal) > 0:
                print(f"  ✓ Journal Vouchers: {len(df_journal)} entries exported")
                # Show currency distribution
                if 'currency' in df_journal.columns:
                    currency_counts = df_journal['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Journal Vouchers',
                    'rows': len(df_journal),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No journal vouchers")
        else:
            print(f"  ⚠ No journal voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Journal Vouchers: {e}")
    
    # ========================================================================
    # 4. SALES VOUCHERS (Inventory)
    # ========================================================================
    print(f"\n[4/9] Extracting Sales Vouchers...")
    try:
        sales_xml = tally.fetch_all_sales_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if sales_xml and sales_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Sales_Vouchers.xlsx')
            df_sales = process_inventory_voucher_to_xlsx(
                sales_xml,
                voucher_type_name='sales',
                output_filename=output_file
            )
            
            if len(df_sales) > 0:
                print(f"  ✓ Sales Vouchers: {len(df_sales)} entries exported")
                # Show currency distribution
                if 'currency' in df_sales.columns:
                    currency_counts = df_sales['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Sales Vouchers',
                    'rows': len(df_sales),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No sales vouchers")
        else:
            print(f"  ⚠ No sales voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Sales Vouchers: {e}")
    
    # ========================================================================
    # 5. PURCHASE VOUCHERS (Inventory)
    # ========================================================================
    print(f"\n[5/9] Extracting Purchase Vouchers...")
    try:
        purchase_xml = tally.fetch_all_purchase_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if purchase_xml and purchase_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Purchase_Vouchers.xlsx')
            df_purchase = process_inventory_voucher_to_xlsx(
                purchase_xml,
                voucher_type_name='purchase',
                output_filename=output_file
            )
            
            if len(df_purchase) > 0:
                print(f"  ✓ Purchase Vouchers: {len(df_purchase)} entries exported")
                # Show currency distribution
                if 'currency' in df_purchase.columns:
                    currency_counts = df_purchase['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Purchase Vouchers',
                    'rows': len(df_purchase),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No purchase vouchers")
        else:
            print(f"  ⚠ No purchase voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Purchase Vouchers: {e}")
    
    # ========================================================================
    # 6. SALES RETURN / CREDIT NOTE VOUCHERS
    # ========================================================================
    print(f"\n[6/9] Extracting Sales Return / Credit Note Vouchers...")
    try:
        sales_return_xml = tally.fetch_all_credit_note_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if sales_return_xml and sales_return_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Sales_Return_Vouchers.xlsx')
            df_sales_return = process_inventory_voucher_to_xlsx(
                sales_return_xml,
                voucher_type_name='credit_note',
                output_filename=output_file
            )
            
            if len(df_sales_return) > 0:
                print(f"  ✓ Sales Return Vouchers: {len(df_sales_return)} entries exported")
                # Show currency distribution
                if 'currency' in df_sales_return.columns:
                    currency_counts = df_sales_return['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Sales Return Vouchers',
                    'rows': len(df_sales_return),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No sales return vouchers")
        else:
            print(f"  ⚠ No sales return voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Sales Return Vouchers: {e}")
    
    # ========================================================================
    # 7. PURCHASE RETURN / DEBIT NOTE VOUCHERS
    # ========================================================================
    print(f"\n[7/9] Extracting Purchase Return / Debit Note Vouchers...")
    try:
        purchase_return_xml = tally.fetch_all_debit_note_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if purchase_return_xml and purchase_return_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Purchase_Return_Vouchers.xlsx')
            df_purchase_return = process_inventory_voucher_to_xlsx(
                purchase_return_xml,
                voucher_type_name='debit_note',
                output_filename=output_file
            )
            
            if len(df_purchase_return) > 0:
                print(f"  ✓ Purchase Return Vouchers: {len(df_purchase_return)} entries exported")
                # Show currency distribution
                if 'currency' in df_purchase_return.columns:
                    currency_counts = df_purchase_return['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Purchase Return Vouchers',
                    'rows': len(df_purchase_return),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No purchase return vouchers")
        else:
            print(f"  ⚠ No purchase return voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Purchase Return Vouchers: {e}")
    
    # ========================================================================
    # 8. PAYMENT VOUCHERS
    # ========================================================================
    print(f"\n[8/9] Extracting Payment Vouchers...")
    try:
        payment_xml = tally.fetch_all_payment_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if payment_xml and payment_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Payment_Vouchers.xlsx')
            df_payment = process_ledger_voucher_to_xlsx(
                payment_xml,
                voucher_type_name='payment',
                output_filename=output_file
            )
            
            if len(df_payment) > 0:
                print(f"  ✓ Payment Vouchers: {len(df_payment)} entries exported")
                # Show currency distribution
                if 'currency' in df_payment.columns:
                    currency_counts = df_payment['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Payment Vouchers',
                    'rows': len(df_payment),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No payment vouchers")
        else:
            print(f"  ⚠ No payment voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Payment Vouchers: {e}")
    
    # ========================================================================
    # 9. RECEIPT VOUCHERS
    # ========================================================================
    print(f"\n[9/9] Extracting Receipt Vouchers...")
    try:
        receipt_xml = tally.fetch_all_receipt_vouchers(
            company_name=comp_name,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            debug=True
        )
        
        if receipt_xml and receipt_xml.strip():
            output_file = os.path.join(comp_dir, f'{comp_name}_Receipt_Vouchers.xlsx')
            df_receipt = process_ledger_voucher_to_xlsx(
                receipt_xml,
                voucher_type_name='receipt',
                output_filename=output_file
            )
            
            if len(df_receipt) > 0:
                print(f"  ✓ Receipt Vouchers: {len(df_receipt)} entries exported")
                # Show currency distribution
                if 'currency' in df_receipt.columns:
                    currency_counts = df_receipt['currency'].value_counts()
                    print(f"    Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
                export_summary['exports'].append({
                    'type': 'Receipt Vouchers',
                    'rows': len(df_receipt),
                    'file': output_file
                })
            else:
                print(f"  ⚠ No receipt vouchers")
        else:
            print(f"  ⚠ No receipt voucher data available")
    except Exception as e:
        print(f"  ✗ Error extracting Receipt Vouchers: {e}")
    
    # ========================================================================
    # SUMMARY FOR THIS COMPANY
    # ========================================================================
    print(f"\n{'─'*80}")
    print(f"Summary for {comp_name}:")
    print(f"{'─'*80}")
    
    if export_summary['exports']:
        total_rows = sum([e['rows'] for e in export_summary['exports']])
        print(f"  Total Exports: {len(export_summary['exports'])}")
        print(f"  Total Records: {total_rows:,}")
        print(f"\n  Files created:")
        for export in export_summary['exports']:
            print(f"    • {export['type']}: {export['rows']:,} rows → {os.path.basename(export['file'])}")
    else:
        print(f"  ⚠ No data exported for this company")

# ========================================================================
# FINAL SUMMARY
# ========================================================================
print(f"\n{'='*80}")
print("EXPORT COMPLETE!")
print(f"{'='*80}")
print(f"Date Range: {FROM_DATE} to {TO_DATE}")
print(f"Companies Processed: {len(companies)}")
print(f"Output Directory: {OUTPUT_DIR}")
print(f"\nAll files are organized by company name in separate folders.")
print(f"{'='*80}")
print(f"\nVoucher Types Exported:")
print(f"  1. Account Master (Ledgers)")
print(f"  2. Trial Balance")
print(f"  3. Journal Vouchers")
print(f"  4. Sales Vouchers")
print(f"  5. Purchase Vouchers")
print(f"  6. Sales Return / Credit Note Vouchers")
print(f"  7. Purchase Return / Debit Note Vouchers")
print(f"  8. Payment Vouchers")
print(f"  9. Receipt Vouchers")
print(f"{'='*80}")