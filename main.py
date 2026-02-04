from tally.tally_connector import TallyConnector
from datetime import datetime


def main():
    tally_initalize = TallyConnector()
    compl_list = tally_initalize.get_company_list()

    for company in compl_list:
        name = company.get("name", "")
        start_from = company.get("start_from", "")
        if start_from:
            start_date = datetime.strptime(start_from, "%Y%m%d").date()
        else:
            start_date = None

        print(f"Company: {name}")
        print(f"Start Date: {start_date}")

        ledgers = tally_initalize.get_ledger(company_name=name)
        
        if ledgers:
            csv_file = tally_initalize.save_ledgers_to_csv(ledgers)
            if csv_file:
                print(f"  CSV: {csv_file}")

if __name__ == "__main__":
    main()