"""
Comprehensive Tally Data Export Script
Exports all voucher types for all companies to Excel files
"""

from services.tally_connector import TallyConnector
from services.data_processor import (
    normalize_voucher,          # Sales vouchers
    receipt_voucher,
    payment_voucher,
    journal_voucher,
    contra_voucher,
    sales_return_voucher,       # Sales return
    purchase_voucher,           # Purchase vouchers
    purchase_return_voucher     # Purchase return
)
from datetime import datetime
import pandas as pd
import os
from pathlib import Path

# # Configuration
# OUTPUT_DIR = "tally_exports"
# EXPORT_DATE = datetime.now().strftime('%Y%m%d_%H%M%S')

# # Create output directory
# Path(OUTPUT_DIR).mkdir(exist_ok=True)

# # Voucher type configuration
# VOUCHER_TYPES = {
#     'sales': {
#         'fetch_method': 'fetch_all_sales_vouchers',
#         'process_method': normalize_voucher,
#         'filename': 'sales_vouchers.xlsx'
#     },
#     'sales_return': {
#         'fetch_method': 'fetch_all_sales_return',
#         'process_method': sales_return_voucher,
#         'filename': 'sales_return_vouchers.xlsx'
#     },
    
#     'purchase': {
#         'fetch_method': 'fetch_all_purchase_voucher',
#         'process_method': purchase_voucher,
#         'filename': 'purchase_vouchers.xlsx'
#     },
#     'purchase_return': {
#         'fetch_method': 'fetch_all_purchase_return',
#         'process_method': purchase_return_voucher,
#         'filename': 'purchase_return_vouchers.xlsx'
#     },
#     'receipt': {
#         'fetch_method': 'fetch_all_receipt_vouchers',
#         'process_method': receipt_voucher,
#         'filename': 'receipt_vouchers.xlsx'
#     },
#     'payment': {
#         'fetch_method': 'fetch_all_payment_vouchers',
#         'process_method': payment_voucher,
#         'filename': 'payment_vouchers.xlsx'
#     },
#     'journal': {
#         'fetch_method': 'fetch_all_journal_vouchers',
#         'process_method': journal_voucher,
#         'filename': 'journal_vouchers.xlsx'
#     },
#     'contra': {
#         'fetch_method': 'fetch_all_contra_vouchers',
#         'process_method': contra_voucher,
#         'filename': 'contra_vouchers.xlsx'
#     }
# }


# def export_company_vouchers(tally, company_name, from_date, to_date, voucher_types_to_export=None):
#     """
#     Export all voucher types for a single company
    
#     Args:
#         tally: TallyConnector instance
#         company_name: Name of the company
#         from_date: Start date (YYYYMMDD format)
#         to_date: End date (YYYYMMDD format)
#         voucher_types_to_export: List of voucher types to export (None = all)
    
#     Returns:
#         dict: Summary of exported vouchers
#     """
#     print(f"\n{'='*80}")
#     print(f"Processing company: {company_name}")
#     print(f"Date range: {from_date} to {to_date}")
#     print(f"{'='*80}\n")
    
#     # Create company-specific directory
#     company_dir = os.path.join(OUTPUT_DIR, f"{company_name}_{EXPORT_DATE}")
#     Path(company_dir).mkdir(exist_ok=True)
    
#     export_summary = {
#         'company': company_name,
#         'from_date': from_date,
#         'to_date': to_date,
#         'vouchers': {}
#     }
    
#     # Determine which voucher types to export
#     types_to_process = voucher_types_to_export or VOUCHER_TYPES.keys()
    
#     # Process each voucher type
#     for voucher_type in types_to_process:
#         if voucher_type not in VOUCHER_TYPES:
#             print(f"⚠️  Unknown voucher type: {voucher_type}, skipping...")
#             continue
            
#         config = VOUCHER_TYPES[voucher_type]
        
#         print(f"📊 Processing {voucher_type.upper()} vouchers...")
        
#         try:
#             # Fetch data from Tally
#             fetch_method = getattr(tally, config['fetch_method'])
#             xml_data = fetch_method(
#                 company_name=company_name,
#                 from_date=from_date,
#                 to_date=to_date,
#                 debug=False  # Set to True for debugging
#             )
            
#             if not xml_data or xml_data.strip() == "":
#                 print(f"   ℹ️  No data returned for {voucher_type}")
#                 export_summary['vouchers'][voucher_type] = {
#                     'status': 'no_data',
#                     'count': 0
#                 }
#                 continue
            
#             # Process data
#             process_method = config['process_method']
#             df = process_method(xml_data)
            
#             if df is None or df.empty:
#                 print(f"   ℹ️  No {voucher_type} vouchers found")
#                 export_summary['vouchers'][voucher_type] = {
#                     'status': 'empty',
#                     'count': 0
#                 }
#                 continue
            
#             # Export to Excel
#             output_file = os.path.join(company_dir, config['filename'])
#             df.to_excel(output_file, index=False)
            
#             # Get currency distribution
#             currency_dist = df['currency'].value_counts().to_dict() if 'currency' in df.columns else {}
            
#             print(f"   ✅ Exported {len(df)} {voucher_type} vouchers")
#             print(f"      📁 File: {output_file}")
#             if currency_dist:
#                 print(f"      💱 Currencies: {currency_dist}")
            
#             export_summary['vouchers'][voucher_type] = {
#                 'status': 'success',
#                 'count': len(df),
#                 'file': output_file,
#                 'currencies': currency_dist
#             }
            
#         except Exception as e:
#             print(f"   ❌ Error processing {voucher_type}: {str(e)}")
#             export_summary['vouchers'][voucher_type] = {
#                 'status': 'error',
#                 'error': str(e),
#                 'count': 0
#             }
    
#     return export_summary


# def export_all_companies(from_date=None, to_date=None, voucher_types=None):
#     """
#     Export all vouchers for all companies
    
#     Args:
#         from_date: Start date (YYYYMMDD format, None = use company start date)
#         to_date: End date (YYYYMMDD format, None = today)
#         voucher_types: List of voucher types to export (None = all)
    
#     Returns:
#         list: Summary of all exports
#     """
#     # Initialize Tally connector
#     print("🔌 Connecting to Tally...")
#     tally = TallyConnector()
    
#     if tally.status != 'Connected':
#         print("❌ Failed to connect to Tally!")
#         return []
    
#     print("✅ Connected to Tally successfully\n")
    
#     # Get all companies
#     print("📋 Fetching company list...")
#     companies = tally.fetch_all_companies()
#     print(f"✅ Found {len(companies)} companies\n")
    
#     # Default to_date is today
#     if to_date is None:
#         to_date = datetime.now().strftime('%Y%m%d')
    
#     all_summaries = []
    
#     # Process each company
#     for idx, company in enumerate(companies, 1):
#         comp_name = company.get('name', '')
#         start_from = company.get('starting_from', '')
        
#         # Determine from_date
#         if from_date:
#             comp_from_date = from_date
#         elif start_from and start_from != "N/A":
#             comp_from_date = start_from
#         else:
#             comp_from_date = None
        
#         print(f"\n[{idx}/{len(companies)}] ", end="")
        
#         summary = export_company_vouchers(
#             tally=tally,
#             company_name=comp_name,
#             from_date=comp_from_date,
#             to_date=to_date,
#             voucher_types_to_export=voucher_types
#         )
        
#         all_summaries.append(summary)
    
#     return all_summaries


# def print_export_summary(summaries):
#     """Print a comprehensive summary of all exports"""
#     print(f"\n\n{'='*80}")
#     print("📊 EXPORT SUMMARY")
#     print(f"{'='*80}\n")
    
#     total_vouchers = 0
#     total_companies = len(summaries)
#     voucher_totals = {vtype: 0 for vtype in VOUCHER_TYPES.keys()}
    
#     for summary in summaries:
#         company_total = 0
#         print(f"\n🏢 {summary['company']}")
#         print(f"   📅 Period: {summary['from_date']} to {summary['to_date']}")
        
#         for vtype, vdata in summary['vouchers'].items():
#             count = vdata.get('count', 0)
#             company_total += count
#             voucher_totals[vtype] += count
            
#             if vdata['status'] == 'success' and count > 0:
#                 currencies = vdata.get('currencies', {})
#                 curr_str = ", ".join([f"{k}: {v}" for k, v in currencies.items()]) if currencies else "N/A"
#                 print(f"   ✅ {vtype.ljust(15)}: {count:6} vouchers ({curr_str})")
#             elif vdata['status'] == 'error':
#                 print(f"   ❌ {vtype.ljust(15)}: Error - {vdata.get('error', 'Unknown')}")
        
#         print(f"   📊 Total: {company_total} vouchers")
#         total_vouchers += company_total
    
#     print(f"\n{'='*80}")
#     print(f"🎯 OVERALL SUMMARY")
#     print(f"{'='*80}")
#     print(f"Total Companies: {total_companies}")
#     print(f"Total Vouchers: {total_vouchers}")
#     print(f"\nVoucher Type Breakdown:")
#     for vtype, count in voucher_totals.items():
#         if count > 0:
#             print(f"  • {vtype.ljust(20)}: {count:6} vouchers")
    
#     print(f"\n📁 Output Directory: {os.path.abspath(OUTPUT_DIR)}")
#     print(f"{'='*80}\n")


# def save_summary_report(summaries):
#     """Save export summary to Excel"""
#     report_data = []
    
#     for summary in summaries:
#         for vtype, vdata in summary['vouchers'].items():
#             report_data.append({
#                 'Company': summary['company'],
#                 'From Date': summary['from_date'],
#                 'To Date': summary['to_date'],
#                 'Voucher Type': vtype,
#                 'Status': vdata['status'],
#                 'Count': vdata.get('count', 0),
#                 'Currencies': str(vdata.get('currencies', {})),
#                 'File': vdata.get('file', ''),
#                 'Error': vdata.get('error', '')
#             })
    
#     df_report = pd.DataFrame(report_data)
#     report_file = os.path.join(OUTPUT_DIR, f"export_summary_{EXPORT_DATE}.xlsx")
#     df_report.to_excel(report_file, index=False)
    
#     print(f"📄 Summary report saved: {report_file}")


# # ==============================================================================
# # MAIN EXECUTION
# # ==============================================================================

# if __name__ == "__main__":
#     print("""
#     ╔════════════════════════════════════════════════════════════════╗
#     ║              TALLY COMPREHENSIVE VOUCHER EXPORT                ║
#     ╚════════════════════════════════════════════════════════════════╝
#     """)
    
#     # CONFIGURATION - Customize these as needed
#     # --------------------------------------------------------------------
    
#     # Option 1: Export ALL voucher types for ALL companies
#     # from_date = None  # Use company's starting date
#     # to_date = None    # Use today
#     # voucher_types = None  # Export all types
    
#     # Option 2: Export specific date range
#     from_date = "20240401"  # Start date
#     to_date = "20250331"     # End date
#     voucher_types = None     # Export all types
    
#     # Option 3: Export only specific voucher types
#     # voucher_types = ['sales', 'purchase', 'receipt', 'payment']
    
#     # Option 4: Current financial year (example for India: Apr-Mar)
#     # current_year = datetime.now().year
#     # if datetime.now().month < 4:  # Before April
#     #     from_date = f"{current_year-1}0401"
#     #     to_date = f"{current_year}0331"
#     # else:  # After April
#     #     from_date = f"{current_year}0401"
#     #     to_date = datetime.now().strftime('%Y%m%d')
#     # voucher_types = None
    
#     # --------------------------------------------------------------------
    
#     print(f"📅 Export Period: {from_date or 'Company Start'} to {to_date or 'Today'}")
#     print(f"📋 Voucher Types: {voucher_types or 'ALL'}")
#     print()
    
#     # Run export
#     summaries = export_all_companies(
#         from_date=from_date,
#         to_date=to_date,
#         voucher_types=voucher_types
#     )
    
#     # Print summary
#     print_export_summary(summaries)
    
#     # Save summary report
#     save_summary_report(summaries)
    
#     print("\n✅ Export completed successfully!")



# Initialize connector
tally = TallyConnector()

comp_list = tally.fetch_all_companies()

for i in comp_list:
    comp_name = i.get('name')

    # Fetch trial balance
    tb_data = tally.fetch_trial_balance(
        company_name=comp_name,
        from_date="202400401",
        to_date="202400401",
        debug=True  # Optional: saves request/response XML files
    )

    if tb_data:
        # Save or process the trial balance XML
        with open('trial_balance.xml', 'wb') as f:
            f.write(tb_data)