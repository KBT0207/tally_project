from services.tally_connector import TallyConnector
from services.data_processor import process_inventory_voucher_to_xlsx, process_ledger_voucher_to_xlsx
import pandas as pd

tally = TallyConnector()

comp = tally.fetch_all_companies()
print(f"\nTotal companies found: {len(comp)}")

for i in comp:
    comp_name = i.get('name', '').strip()
    
    # Skip empty, None, or N/A company names
    if not comp_name or comp_name.upper() in ['N/A', 'NA', 'NONE', '']:
        print(f"Skipping company with empty/invalid name")
        continue
    
    print(f"\nProcessing company: {comp_name}")
    
    jv = tally.fetch_all_sales_vouchers(
        company_name=comp_name, 
        from_date='20250401', 
        to_date='20250530', 
        debug=True
    )
    
    # Check if data was fetched
    if jv is None or jv.strip() == "":
        print(f"  → No sales voucher data for {comp_name}")
        continue
    
    print(f"  → Processing sales vouchers...")
    df = process_inventory_voucher_to_xlsx(
        jv, 
        voucher_type_name="sales", 
        output_filename=f'{comp_name}.xlsx'
    )
    
    if len(df) > 0:
        print(f"  ✓ Saved {len(df)} rows to {comp_name}.xlsx")
        
        # Show currency distribution
        currency_counts = df['currency'].value_counts()
        print(f"  ✓ Currencies: {', '.join([f'{curr}({count})' for curr, count in currency_counts.items()])}")
    else:
        print(f"  → No data to save for {comp_name}")

print("\n" + "="*80)
print("Processing complete!")
print("="*80)