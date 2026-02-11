"""
Main script to export all Tally data to Excel files
Fetches and processes:
- Company information
- Sales vouchers
- Sales return vouchers
- Purchase vouchers
- Purchase return vouchers
- Receipt vouchers
- Payment vouchers
- Journal vouchers
- Contra vouchers
- Trial balance
- Ledgers
- Groups
"""

import os
import sys
from datetime import datetime
import pandas as pd
from services.tally_connector import TallyConnector
from services.data_processor import (
    normalize_voucher,
    sales_return_voucher,
    purchase_voucher,
    purchase_return_voucher,
    trial_balance_to_xlsx,
    sanitize_xml_content,
    clean_text
)
import xml.etree.ElementTree as ET

# Configure logging with UTF-8 encoding to avoid Unicode errors
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tally_export.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TallyDataExporter:
    """Main class to export all Tally data to Excel files"""
    
    def __init__(self, host='localhost', port=9000, output_dir='tally_exports'):
        """
        Initialize Tally Data Exporter
        
        Args:
            host: Tally server host (default: localhost)
            port: Tally server port (default: 9000)
            output_dir: Directory to save exported files
        """
        self.output_dir = output_dir
        self.connector = TallyConnector(host=host, port=port)
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created export directory: {output_dir}")
    
    def _get_filename(self, company_name, data_type, extension='xlsx'):
        """Generate filename with timestamp"""
        safe_company = company_name.replace(' ', '_').replace('(', '').replace(')', '')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return os.path.join(self.output_dir, f"{safe_company}_{data_type}_{timestamp}.{extension}")
    
    def export_all_companies(self):
        """Export all companies information to Excel"""
        logger.info("=" * 80)
        logger.info("EXPORTING ALL COMPANIES DATA")
        logger.info("=" * 80)
        
        try:
            companies = self.connector.fetch_all_companies()
            
            if not companies:
                logger.warning("No companies found")
                return []
            
            logger.info(f"Found {len(companies)} companies")
            
            # Convert to DataFrame
            df = pd.DataFrame(companies)
            
            # Save to Excel
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(self.output_dir, f"ALL_COMPANIES_company_list_{timestamp}.xlsx")
            df.to_excel(filename, index=False)
            
            logger.info(f"[OK] Exported companies list to: {filename}")
            
            return companies
            
        except Exception as e:
            logger.error(f"Error exporting companies: {e}", exc_info=True)
            return []
    
    def export_sales_vouchers(self, company_name, from_date=None, to_date=None):
        """Export sales vouchers to Excel"""
        try:
            logger.info(f"Fetching sales vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_sales_vouchers(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No sales data received for {company_name}")
                return None
            
            # Process and save
            filename = self._get_filename(company_name, 'sales')
            df = normalize_voucher(xml_data)
            
            if len(df) > 0:
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} sales vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No sales vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting sales vouchers: {e}", exc_info=True)
            return None
    
    def export_sales_return_vouchers(self, company_name, from_date=None, to_date=None):
        """Export sales return vouchers to Excel"""
        try:
            logger.info(f"Fetching sales return vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_sales_return(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No sales return data received for {company_name}")
                return None
            
            # Process and save
            filename = self._get_filename(company_name, 'sales_return')
            df = sales_return_voucher(xml_data)
            
            if len(df) > 0:
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} sales return vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No sales return vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting sales return vouchers: {e}", exc_info=True)
            return None
    
    def export_purchase_vouchers(self, company_name, from_date=None, to_date=None):
        """Export purchase vouchers to Excel"""
        try:
            logger.info(f"Fetching purchase vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_purchase_voucher(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No purchase data received for {company_name}")
                return None
            
            # Process and save
            filename = self._get_filename(company_name, 'purchase')
            df = purchase_voucher(xml_data)
            
            if len(df) > 0:
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} purchase vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No purchase vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting purchase vouchers: {e}", exc_info=True)
            return None
    
    def export_purchase_return_vouchers(self, company_name, from_date=None, to_date=None):
        """Export purchase return vouchers to Excel"""
        try:
            logger.info(f"Fetching purchase return vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_purchase_return(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No purchase return data received for {company_name}")
                return None
            
            # Process and save
            filename = self._get_filename(company_name, 'purchase_return')
            df = purchase_return_voucher(xml_data)
            
            if len(df) > 0:
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} purchase return vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No purchase return vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting purchase return vouchers: {e}", exc_info=True)
            return None
    
    def export_receipt_vouchers(self, company_name, from_date=None, to_date=None):
        """Export receipt vouchers to Excel"""
        try:
            logger.info(f"Fetching receipt vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_receipt_vouchers(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No receipt data received for {company_name}")
                return None
            
            # Parse and save raw XML data
            filename = self._get_filename(company_name, 'receipt_vouchers')
            
            # Parse XML and extract vouchers
            xml_content = sanitize_xml_content(xml_data)
            root = ET.fromstring(xml_content.encode('utf-8'))
            vouchers = root.findall('.//VOUCHER')
            
            all_rows = []
            for voucher in vouchers:
                voucher_data = self._parse_generic_voucher(voucher, 'Receipt')
                if isinstance(voucher_data, list):
                    all_rows.extend(voucher_data)
                else:
                    all_rows.append(voucher_data)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} receipt vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No receipt vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting receipt vouchers: {e}", exc_info=True)
            return None
    
    def export_payment_vouchers(self, company_name, from_date=None, to_date=None):
        """Export payment vouchers to Excel"""
        try:
            logger.info(f"Fetching payment vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_payment_vouchers(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No payment data received for {company_name}")
                return None
            
            # Parse and save
            filename = self._get_filename(company_name, 'payment_vouchers')
            
            # Parse XML and extract vouchers
            xml_content = sanitize_xml_content(xml_data)
            root = ET.fromstring(xml_content.encode('utf-8'))
            vouchers = root.findall('.//VOUCHER')
            
            all_rows = []
            for voucher in vouchers:
                voucher_data = self._parse_generic_voucher(voucher, 'Payment')
                if isinstance(voucher_data, list):
                    all_rows.extend(voucher_data)
                else:
                    all_rows.append(voucher_data)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} payment vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No payment vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting payment vouchers: {e}", exc_info=True)
            return None
    
    def export_journal_vouchers(self, company_name, from_date=None, to_date=None):
        """Export journal vouchers to Excel"""
        try:
            logger.info(f"Fetching journal vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_journal_vouchers(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No journal data received for {company_name}")
                return None
            
            # Parse and save
            filename = self._get_filename(company_name, 'journal_vouchers')
            
            # Parse XML and extract vouchers
            xml_content = sanitize_xml_content(xml_data)
            root = ET.fromstring(xml_content.encode('utf-8'))
            vouchers = root.findall('.//VOUCHER')
            
            all_rows = []
            for voucher in vouchers:
                voucher_data = self._parse_generic_voucher(voucher, 'Journal')
                if isinstance(voucher_data, list):
                    all_rows.extend(voucher_data)
                else:
                    all_rows.append(voucher_data)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} journal vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No journal vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting journal vouchers: {e}", exc_info=True)
            return None
    
    def export_contra_vouchers(self, company_name, from_date=None, to_date=None):
        """Export contra vouchers to Excel"""
        try:
            logger.info(f"Fetching contra vouchers for: {company_name}")
            
            xml_data = self.connector.fetch_all_contra_vouchers(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No contra data received for {company_name}")
                return None
            
            # Parse and save
            filename = self._get_filename(company_name, 'contra_vouchers')
            
            # Parse XML and extract vouchers
            xml_content = sanitize_xml_content(xml_data)
            root = ET.fromstring(xml_content.encode('utf-8'))
            vouchers = root.findall('.//VOUCHER')
            
            all_rows = []
            for voucher in vouchers:
                voucher_data = self._parse_generic_voucher(voucher, 'Contra')
                if isinstance(voucher_data, list):
                    all_rows.extend(voucher_data)
                else:
                    all_rows.append(voucher_data)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} contra vouchers to: {filename}")
                return filename
            else:
                logger.warning(f"No contra vouchers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting contra vouchers: {e}", exc_info=True)
            return None
    
    def _parse_generic_voucher(self, voucher, voucher_type):
        """Parse generic voucher structure for Receipt, Payment, Journal, and Contra"""
        try:
            voucher_no = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            date = clean_text(voucher.findtext('DATE', ''))
            reference = clean_text(voucher.findtext('REFERENCE', ''))
            narration = clean_text(voucher.findtext('NARRATION', ''))
            guid = clean_text(voucher.findtext('GUID', ''))
            
            # Get ledger entries
            ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
            if not ledger_entries:
                ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
            
            # Create row for each ledger entry
            rows = []
            for ledger in ledger_entries:
                ledger_name = clean_text(ledger.findtext('LEDGERNAME', ''))
                amount = clean_text(ledger.findtext('AMOUNT', '0'))
                
                row = {
                    'voucher_type': voucher_type,
                    'voucher_number': voucher_no,
                    'date': date,
                    'reference': reference,
                    'ledger_name': ledger_name,
                    'amount': amount,
                    'narration': narration,
                    'guid': guid
                }
                rows.append(row)
            
            return rows if rows else [{
                'voucher_type': voucher_type,
                'voucher_number': voucher_no,
                'date': date,
                'reference': reference,
                'ledger_name': '',
                'amount': '0',
                'narration': narration,
                'guid': guid
            }]
            
        except Exception as e:
            logger.error(f"Error parsing {voucher_type} voucher: {e}")
            return [{
                'voucher_type': voucher_type,
                'voucher_number': '',
                'date': '',
                'reference': '',
                'ledger_name': '',
                'amount': '0',
                'narration': '',
                'guid': ''
            }]
    
    def export_trial_balance(self, company_name, from_date=None, to_date=None):
        """Export trial balance to Excel"""
        try:
            logger.info(f"Fetching trial balance for: {company_name}")
            
            xml_data = self.connector.fetch_trial_balance(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No trial balance data received for {company_name}")
                return None
            
            # Process and save
            filename = self._get_filename(company_name, 'trial_balance')
            df = trial_balance_to_xlsx(xml_data, company_name, from_date or '', to_date or '', filename)
            
            if len(df) > 0:
                logger.info(f"[OK] Exported trial balance with {len(df)} entries to: {filename}")
                return filename
            else:
                logger.warning(f"No trial balance data found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting trial balance: {e}", exc_info=True)
            return None
    
    def export_ledgers(self, company_name, from_date=None, to_date=None):
        """Export all ledgers to Excel"""
        try:
            logger.info(f"Fetching ledgers for: {company_name}")
            
            xml_data = self.connector.fetch_all_ledgers(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No ledgers data received for {company_name}")
                return None
            
            # Parse and save
            filename = self._get_filename(company_name, 'ledgers')
            
            xml_content = sanitize_xml_content(xml_data)
            root = ET.fromstring(xml_content.encode('utf-8'))
            ledgers = root.findall('.//LEDGER')
            
            all_rows = []
            for ledger in ledgers:
                ledger_data = {
                    'name': clean_text(ledger.get('NAME', '')),
                    'parent': clean_text(ledger.findtext('PARENT', '')),
                    'opening_balance': clean_text(ledger.findtext('OPENINGBALANCE', '0')),
                    'closing_balance': clean_text(ledger.findtext('CLOSINGBALANCE', '0')),
                    'guid': clean_text(ledger.findtext('GUID', ''))
                }
                all_rows.append(ledger_data)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} ledgers to: {filename}")
                return filename
            else:
                logger.warning(f"No ledgers found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting ledgers: {e}", exc_info=True)
            return None
    
    def export_groups(self, company_name, from_date=None, to_date=None):
        """Export all groups to Excel"""
        try:
            logger.info(f"Fetching groups for: {company_name}")
            
            xml_data = self.connector.fetch_all_groups(
                company_name, 
                from_date=from_date, 
                to_date=to_date
            )
            
            if not xml_data:
                logger.warning(f"No groups data received for {company_name}")
                return None
            
            # Parse and save
            filename = self._get_filename(company_name, 'groups')
            
            xml_content = sanitize_xml_content(xml_data)
            root = ET.fromstring(xml_content.encode('utf-8'))
            groups = root.findall('.//GROUP')
            
            all_rows = []
            for group in groups:
                group_data = {
                    'name': clean_text(group.get('NAME', '')),
                    'parent': clean_text(group.findtext('PARENT', '')),
                    'is_revenue': clean_text(group.findtext('ISREVENUE', '')),
                    'is_deemedpositive': clean_text(group.findtext('ISDEEMEDPOSITIVE', '')),
                    'guid': clean_text(group.findtext('GUID', ''))
                }
                all_rows.append(group_data)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_excel(filename, index=False)
                logger.info(f"[OK] Exported {len(df)} groups to: {filename}")
                return filename
            else:
                logger.warning(f"No groups found for {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting groups: {e}", exc_info=True)
            return None
    
    def export_all_vouchers(self, company_name, from_date=None, to_date=None):
        """Export all voucher types for a company"""
        logger.info("=" * 80)
        logger.info(f"EXPORTING ALL VOUCHERS FOR: {company_name}")
        logger.info("=" * 80)
        
        results = {}
        
        # Sales vouchers
        logger.info("\n--- Processing SALES ---")
        results['sales'] = self.export_sales_vouchers(company_name, from_date, to_date)
        
        # Sales Return vouchers
        logger.info("\n--- Processing SALES RETURN ---")
        results['sales_return'] = self.export_sales_return_vouchers(company_name, from_date, to_date)
        
        # Purchase vouchers
        logger.info("\n--- Processing PURCHASE ---")
        results['purchase'] = self.export_purchase_vouchers(company_name, from_date, to_date)
        
        # Purchase Return vouchers
        logger.info("\n--- Processing PURCHASE RETURN ---")
        results['purchase_return'] = self.export_purchase_return_vouchers(company_name, from_date, to_date)
        
        # Receipt vouchers
        logger.info("\n--- Processing RECEIPT ---")
        results['receipt'] = self.export_receipt_vouchers(company_name, from_date, to_date)
        
        # Payment vouchers
        logger.info("\n--- Processing PAYMENT ---")
        results['payment'] = self.export_payment_vouchers(company_name, from_date, to_date)
        
        # Journal vouchers
        logger.info("\n--- Processing JOURNAL ---")
        results['journal'] = self.export_journal_vouchers(company_name, from_date, to_date)
        
        # Contra vouchers
        logger.info("\n--- Processing CONTRA ---")
        results['contra'] = self.export_contra_vouchers(company_name, from_date, to_date)
        
        # Trial Balance
        logger.info("\n--- Processing TRIAL BALANCE ---")
        results['trial_balance'] = self.export_trial_balance(company_name, from_date, to_date)
        
        # Ledgers
        logger.info("\n--- Processing LEDGERS ---")
        results['ledgers'] = self.export_ledgers(company_name, from_date, to_date)
        
        # Groups
        logger.info("\n--- Processing GROUPS ---")
        results['groups'] = self.export_groups(company_name, from_date, to_date)
        
        return results
    
    def export_all_companies_data(self, from_date=None, to_date=None):
        """Export all data for all companies"""
        logger.info("\n" + "=" * 80)
        logger.info("STARTING COMPLETE TALLY DATA EXPORT")
        logger.info("=" * 80)
        
        # First export all companies list
        companies = self.export_all_companies()
        
        if not companies:
            logger.error("No companies found. Exiting.")
            return
        
        # Export data for each company
        for idx, company in enumerate(companies, 1):
            company_name = company.get('name', 'Unknown')
            
            logger.info("\n" + "=" * 80)
            logger.info(f"Processing Company {idx}/{len(companies)}: {company_name}")
            logger.info("=" * 80)
            
            try:
                # Export all vouchers and data
                self.export_all_vouchers(company_name, from_date, to_date)
                
            except Exception as e:
                logger.error(f"Error processing company {company_name}: {e}", exc_info=True)
                continue
        
        logger.info("\n" + "=" * 80)
        logger.info("COMPLETE TALLY DATA EXPORT FINISHED")
        logger.info("=" * 80)
        logger.info(f"All files exported to: {self.output_dir}")
    
    def close(self):
        """Close Tally connector"""
        if self.connector:
            self.connector.close()


def main():
    """Main function to run the export"""
    
    # Configuration
    TALLY_HOST = 'localhost'
    TALLY_PORT = 9000
    OUTPUT_DIR = 'tally_exports'
    
    # Date range (Format: YYYYMMDD) - Set to None for all data
    FROM_DATE = '20230401'  # April 1, 2023
    TO_DATE = '20260211'    # Today
    
    # You can set these to None to get all data:
    # FROM_DATE = None
    # TO_DATE = None
    
    try:
        # Create exporter
        exporter = TallyDataExporter(
            host=TALLY_HOST,
            port=TALLY_PORT,
            output_dir=OUTPUT_DIR
        )
        
        # Check connection
        if exporter.connector.status != 'Connected':
            logger.error(f"Failed to connect to Tally at {TALLY_HOST}:{TALLY_PORT}")
            logger.error("Please ensure Tally is running and the port is correct")
            return
        
        logger.info(f"[OK] Successfully connected to Tally at {TALLY_HOST}:{TALLY_PORT}")
        
        # Export all companies data
        exporter.export_all_companies_data(from_date=FROM_DATE, to_date=TO_DATE)
        
        # Close connection
        exporter.close()
        
        logger.info("\n" + "=" * 80)
        logger.info("EXPORT COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.info("\nExport interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error during export: {e}", exc_info=True)
    finally:
        logger.info("\nProgram finished")


if __name__ == "__main__":
    main()