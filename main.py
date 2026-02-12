from services.tally_connector import TallyConnector
from services.data_processor import process_sales_purchase_voucher, journal_voucher, trial_balance_to_xlsx



tally = TallyConnector()

comp = tally.fetch_all_companies()
for i in comp:
    comp_name = i.get('name')
    jv = tally.fetch_trial_balance(company_name=comp_name, from_date='20230401', to_date='20260401', debug=True)
    trial_balance_to_xlsx(jv, company_name=comp_name, start_date='20240401',end_date='20240401')