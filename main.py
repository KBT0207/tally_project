from services.tally_connector import TallyConnector
from services.data_processor import process_inventory_voucher_to_xlsx, process_ledger_voucher_to_xlsx
import pandas as pd
tally = TallyConnector()

comp = tally.fetch_all_companies()
for i in comp:
    comp_name = i.get('name')
    print(f"Processing company: {comp_name}")
    
    jv = tally.fetch_all_sales_vouchers(company_name=comp_name, from_date='20240401', to_date='20240401', debug=True)
    
    # Check if data was fetched
    if jv is None or jv.strip() == "":
        print(f"No purchase return data for {comp_name}")
        continue
    
    df = process_inventory_voucher_to_xlsx(jv, voucher_type_name="purchase", output_filename=f'{comp_name}.xlsx')
