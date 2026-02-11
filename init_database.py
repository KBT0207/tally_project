"""
Database Initialization Script for Tally CDC System

This script:
1. Creates the database if it doesn't exist
2. Creates all tables based on models
3. Verifies the setup
4. Shows table statistics
"""

import sys
from pathlib import Path

# Ensure imports work from root directory
sys.path.insert(0, str(Path(__file__).parent))

from database.db_connector import DatabaseConnector
from database.config.config import Config
from logging_config import logger


def initialize_database():
    """Initialize database and create all tables"""
    
    print("=" * 80)
    print("TALLY CDC SYSTEM - DATABASE INITIALIZATION")
    print("=" * 80)
    
    # Create database connector
    print(f"\n1. Connecting to MySQL server at {Config.DB_HOST}:{Config.DB_PORT}")
    db = DatabaseConnector(
        username=Config.DB_USERNAME,
        password=Config.DB_PASSWORD,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME
    )
    
    # Create database
    print(f"\n2. Creating database '{Config.DB_NAME}' if not exists...")
    try:
        db.create_database_if_not_exists()
        print("[OK] Database created/verified successfully")
        logger.info("Database created/verified successfully")
    except Exception as e:
        print(f"[ERROR] Failed to create database: {e}")
        logger.error(f"Failed to create database: {e}")
        return False
    
    # Test connection
    print(f"\n3. Testing database connection...")
    if db.test_connection():
        print("[OK] Database connection successful")
        logger.info("Database connection successful")
    else:
        print("[ERROR] Database connection failed")
        logger.error("Database connection failed")
        return False
    
    # Create tables
    print(f"\n4. Creating tables...")
    try:
        db.create_tables()
        print("[OK] All tables created successfully")
        logger.info("All tables created successfully")
    except Exception as e:
        print(f"[ERROR] Failed to create tables: {e}")
        logger.error(f"Failed to create tables: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Verify tables
    print(f"\n5. Verifying tables...")
    tables = [
        'sync_metadata',
        'companies',
        'ledgers',
        'sales_vouchers',
        'purchase_vouchers',
        'receipt_vouchers',
        'payment_vouchers',
        'journal_vouchers',
        'contra_vouchers',
    ]
    
    for table in tables:
        try:
            count = db.get_table_row_count(table)
            print(f"   [OK] {table:30s} - {count:6d} rows")
            logger.info(f"{table}: {count} rows")
        except Exception as e:
            print(f"   [ERROR] {table:30s} - Error: {e}")
            logger.error(f"{table}: Error - {e}")
    
    print("\n" + "=" * 80)
    print("DATABASE INITIALIZATION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\nNext Steps:")
    print("1. Verify .env file has correct database credentials")
    print("2. Check all tables created successfully above")
    print("3. Ready for Phase 2: CDC sync logic implementation")
    print("=" * 80)
    
    db.close()
    return True


def show_database_info():
    """Display current database information"""
    
    print("=" * 80)
    print("DATABASE INFORMATION")
    print("=" * 80)
    
    db = DatabaseConnector(
        username=Config.DB_USERNAME,
        password=Config.DB_PASSWORD,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME
    )
    
    if not db.test_connection():
        print("[ERROR] Cannot connect to database")
        logger.error("Cannot connect to database")
        return
    
    print(f"\nDatabase: {Config.DB_NAME}")
    print(f"Host: {Config.DB_HOST}:{Config.DB_PORT}")
    print(f"User: {Config.DB_USERNAME}\n")
    
    tables = [
        'sync_metadata',
        'companies',
        'ledgers',
        'sales_vouchers',
        'purchase_vouchers',
        'receipt_vouchers',
        'payment_vouchers',
        'journal_vouchers',
        'contra_vouchers',
    ]
    
    print("Table Statistics:")
    print("-" * 80)
    
    total_records = 0
    for table in tables:
        try:
            count = db.get_table_row_count(table)
            total_records += count
            print(f"{table:30s} : {count:>10,d} rows")
        except Exception as e:
            print(f"{table:30s} : Error - {e}")
            logger.warning(f"{table}: {e}")
    
    print("-" * 80)
    print(f"{'TOTAL':30s} : {total_records:>10,d} rows")
    print("=" * 80)
    
    db.close()


def reset_database():
    """Drop and recreate all tables (WARNING: Data loss!)"""
    
    print("=" * 80)
    print("WARNING: DATABASE RESET")
    print("This will DELETE ALL DATA!")
    print("=" * 80)
    
    confirmation = input("\nType 'RESET' to confirm database reset: ")
    
    if confirmation != 'RESET':
        print("Reset cancelled")
        logger.info("Database reset cancelled")
        return
    
    db = DatabaseConnector(
        username=Config.DB_USERNAME,
        password=Config.DB_PASSWORD,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME
    )
    
    print("\nDropping all tables...")
    try:
        db.drop_all_tables()
        print("[OK] All tables dropped")
        logger.info("All tables dropped")
    except Exception as e:
        print(f"[ERROR] Error dropping tables: {e}")
        logger.error(f"Error dropping tables: {e}")
        return
    
    print("\nRecreating tables...")
    try:
        db.create_tables()
        print("[OK] All tables recreated")
        logger.info("All tables recreated")
    except Exception as e:
        print(f"[ERROR] Error creating tables: {e}")
        logger.error(f"Error creating tables: {e}")
        return
    
    print("\nDatabase reset completed successfully")
    logger.info("Database reset completed successfully")
    db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Tally CDC Database Initialization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python init_database.py                  # Initialize database
  python init_database.py --info           # Show database info
  python init_database.py --reset          # Reset database (DANGER!)
        """
    )
    parser.add_argument('--reset', action='store_true', 
                       help='Reset database (drop and recreate all tables)')
    parser.add_argument('--info', action='store_true',
                       help='Show database information and statistics')
    
    args = parser.parse_args()
    
    try:
        if args.reset:
            reset_database()
        elif args.info:
            show_database_info()
        else:
            initialize_database()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        logger.info("Operation cancelled by user")
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()