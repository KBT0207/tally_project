import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()


class Config:
    """Application configuration"""
    
    # Database Configuration
    DB_USERNAME = os.getenv('DB_USERNAME', 'root')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    DB_NAME = os.getenv('DB_NAME', 'tally_cdc')
    
    # Tally Configuration
    TALLY_HOST = os.getenv('TALLY_HOST', 'localhost')
    TALLY_PORT = int(os.getenv('TALLY_PORT', 9000))
    TALLY_TIMEOUT = int(os.getenv('TALLY_TIMEOUT', 1800))
    
    # CDC Configuration
    CDC_BATCH_SIZE = int(os.getenv('CDC_BATCH_SIZE', 100))  # Records per batch for insert
    CDC_MAX_RETRIES = int(os.getenv('CDC_MAX_RETRIES', 3))  # Max retry attempts on failure
    CDC_RETRY_DELAY = int(os.getenv('CDC_RETRY_DELAY', 60))  # Seconds between retries
    
    # Sync Schedule Configuration
    SYNC_SCHEDULE_HOURLY = os.getenv('SYNC_SCHEDULE_HOURLY', 'False').lower() == 'true'
    SYNC_SCHEDULE_DAILY = os.getenv('SYNC_SCHEDULE_DAILY', 'True').lower() == 'true'
    SYNC_SCHEDULE_TIME = os.getenv('SYNC_SCHEDULE_TIME', '23:00')  # Daily sync time
    
    # Full Sync Configuration (safety mechanism)
    FULL_SYNC_INTERVAL_DAYS = int(os.getenv('FULL_SYNC_INTERVAL_DAYS', 30))  # Full sync every 30 days
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_DIR = Path(os.getenv('LOG_DIR', 'logs'))
    LOG_DIR.mkdir(exist_ok=True)
    
    # TDL Templates Directory
    TDL_TEMPLATES_DIR = Path(os.getenv('TDL_TEMPLATES_DIR', 'utils'))
    
    # Entity Types to Sync
    SYNC_ENTITIES = [
        'companies',
        'ledgers',
        'sales_vouchers',
        'purchase_vouchers',
        'receipt_vouchers',
        'payment_vouchers',
        'journal_vouchers',
        'contra_vouchers',
    ]
    
    @classmethod
    def get_entity_config(cls, entity_type: str) -> dict:
        """Get configuration for specific entity type"""
        entity_configs = {
            'companies': {
                'xml_template': 'company.xml',
                'model_class': 'Company',
                'table_name': 'companies',
                'supports_date_filter': False,
            },
            'ledgers': {
                'xml_template': 'ledger.xml',
                'model_class': 'Ledger',
                'table_name': 'ledgers',
                'supports_date_filter': True,
            },
            'sales_vouchers': {
                'xml_template': 'sales_vouchers.xml',
                'model_class': 'SalesVoucher',
                'table_name': 'sales_vouchers',
                'supports_date_filter': True,
            },
            'purchase_vouchers': {
                'xml_template': 'purchase_vouchers.xml',
                'model_class': 'PurchaseVoucher',
                'table_name': 'purchase_vouchers',
                'supports_date_filter': True,
            },
            'receipt_vouchers': {
                'xml_template': 'receipt_vouchers.xml',
                'model_class': 'ReceiptVoucher',
                'table_name': 'receipt_vouchers',
                'supports_date_filter': True,
            },
            'payment_vouchers': {
                'xml_template': 'payment_vouchers.xml',
                'model_class': 'PaymentVoucher',
                'table_name': 'payment_vouchers',
                'supports_date_filter': True,
            },
            'journal_vouchers': {
                'xml_template': 'journal_vouchers.xml',
                'model_class': 'JournalVoucher',
                'table_name': 'journal_vouchers',
                'supports_date_filter': True,
            },
            'contra_vouchers': {
                'xml_template': 'contra_vouchers.xml',
                'model_class': 'ContraVoucher',
                'table_name': 'contra_vouchers',
                'supports_date_filter': True,
            },
        }
        return entity_configs.get(entity_type, {})