from services.tally_connector import TallyConnector
from services.data_processor import process_sales_purchase_voucher, journal_voucher



tally = TallyConnector()

comp = tally.fetch_all_companies()
for i in comp:
    comp_name = i.get('name')
    jv = tally.fetch_all_journal_vouchers(company_name=comp_name, from_date='20240401', to_date='20240401', debug=True)
    journal_voucher(jv)