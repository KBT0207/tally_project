
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

FROM_DATE = '20250401'
TO_DATE = '20250430'


tally = TallyConnector()

companies = tally.fetch_all_companies()
for idx, company in enumerate(companies, 1):
    comp_name = company.get('name', '').strip()
    
    if not comp_name or comp_name.upper() in ['N/A', 'NA', 'NONE', '']:
        continue
    
    data = tally.fetch_all_sales_return(company_name=comp_name,from_date=FROM_DATE, to_date=TO_DATE, debug=True)
    d = process_inventory_voucher_to_xlsx(data)