from services.tally_connector import TallyConnector
from services.data_processor import normalize_voucher
from datetime import datetime

today = datetime.now().strftime('%Y%m%d')
tally = TallyConnector()

comp = tally.fetch_all_companies()


for i in comp:
    comp_name = i.get('name', '')
    start_from = i.get('starting_from', '')
    # start_from = "20240401"
    # today = "20250331"
    
#     from_date = start_from if start_from != "N/A" else None
    
#     print(f"Processing company: {comp_name}")
#     print(f"Date range: {from_date} to {today}")
    
#     data = tally.fetch_all_purchase_voucher(
#         company_name=comp_name, 
#         from_date=from_date,
#         to_date=today,
#         debug=True
#     )

#     data = tally.fetch_all_sales_vouchers(
#         company_name=comp_name, 
#         from_date=from_date,
#         to_date=today,
#         debug=True
#     )

#     df = normalize_voucher(data)