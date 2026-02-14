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
FROM_DATE = '20250401'  # April 1, 2024
TO_DATE = '20250730'    # February 28, 2025
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
    
    data = tally.fetch_trial_balance(company_name=comp_name,from_date=FROM_DATE, to_date=TO_DATE, debug=True)
    d = trial_balance_to_xlsx(data, comp_name=comp_name, start_date=FROM_DATE, end_date=TO_DATE)