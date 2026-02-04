from tally.tally_connector import TallyConnector
from datetime import datetime
import time


def main():
    print("\n" + "="*70)
    print(" TALLY LEDGER EXPORT TOOL - EXCEL EDITION")
    print("="*70)
    
    tally_initalize = TallyConnector()
    
    if tally_initalize.status != 'Connected':
        print("\n❌ Failed to connect to Tally. Please check:")
        print("   1. Tally is running")
        print("   2. ODBC/HTTP server is enabled")
        print("   3. Port 9000 is accessible")
        return
    
    print(f"\n✅ Connected to Tally at {tally_initalize.url}")
    
    print("\n📋 Fetching company list...")
    compl_list = tally_initalize.get_company_list()

    if not compl_list:
        print("\n❌ No companies found!")
        return

    print(f"✅ Found {len(compl_list)} companies")
    print("\n" + "="*70)
    
    successful_exports = 0
    failed_exports = 0
    
    for idx, company in enumerate(compl_list, 1):
        name = company.get("name", "")
        start_from = company.get("start_from", "")
        
        if start_from:
            try:
                start_date = datetime.strptime(start_from, "%Y%m%d").date()
            except:
                start_date = "Unknown"
        else:
            start_date = "Unknown"

        print(f"\n{'='*70}")
        print(f"COMPANY {idx} of {len(compl_list)}")
        print(f"{'='*70}")
        print(f"📌 Name: {name}")
        print(f"📅 Start Date: {start_date}")
        print(f"{'='*70}")

        try:
            ledgers = tally_initalize.get_ledger(company_name=name)
            
            if ledgers and len(ledgers) > 0:
                print(f"\n📦 Processing {len(ledgers)} ledgers...")
                
                # Export to Excel instead of CSV
                xlsx_file = tally_initalize.save_ledgers_to_excel(ledgers, company_name=name)
                
                if xlsx_file:
                    successful_exports += 1
                    print(f"\n✅ EXCEL EXPORT SUCCESSFUL for '{name}'")
                    print(f"   📄 File: {xlsx_file}")
                else:
                    failed_exports += 1
                    print(f"\n⚠️  EXCEL EXPORT FAILED for '{name}'")
            else:
                print(f"\n⚠️  No ledgers found for '{name}'")
                failed_exports += 1
                
        except KeyboardInterrupt:
            print(f"\n\n⚠️  Export interrupted by user!")
            print(f"   Processed {idx-1} of {len(compl_list)} companies")
            break
            
        except Exception as e:
            failed_exports += 1
            print(f"\n❌ ERROR processing company '{name}'")
            print(f"   Error: {str(e)}")
            print(f"   Continuing with next company...")
            continue
        
        if idx < len(compl_list):
            print(f"\n⏳ Waiting 3 seconds before next company...")
            time.sleep(3)
    
    print(f"\n{'='*70}")
    print(" EXPORT SUMMARY")
    print(f"{'='*70}")
    print(f"✅ Successful: {successful_exports}")
    print(f"❌ Failed: {failed_exports}")
    print(f"📊 Total: {len(compl_list)}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user. Goodbye!")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        print("   Check logs for details")