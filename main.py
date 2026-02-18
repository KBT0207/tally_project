from datetime import date
from logging_config import logger
from services.tally_connector import TallyConnector
from services.sync_service import sync_all_companies
from database.db_connector import DatabaseConnector
from database.database_processor import company_import_db
from database.config.config import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

MANUAL_FROM_DATE = None       # auto per company  - MANUAL_FROM_DATE = '20240401' # manual override



def main():
    logger.info("Starting Tally Sync")

    db = DatabaseConnector(
        username = DB_USERNAME,
        password = DB_PASSWORD,
        host     = DB_HOST,
        port     = DB_PORT,
        database = DB_NAME,
    )

    db.create_database_if_not_exists()
    db.create_tables()

    if not db.test_connection():
        logger.error("Database connection failed. Aborting.")
        return

    engine = db.get_engine()

    with TallyConnector() as tally:
        if tally.status != 'Connected':
            logger.error("Tally connection failed. Aborting.")
            return

        logger.info("Fetching companies from Tally...")
        companies = tally.fetch_all_companies()

        if not companies:
            logger.warning("No companies found in Tally. Aborting.")
            return

        logger.info(f"Found {len(companies)} companies — importing to DB...")
        company_import_db(companies, engine)

        to_date = date.today().strftime('%Y%m%d')
        logger.info(f"to_date = {to_date}")

        if MANUAL_FROM_DATE:
            logger.info(f"from_date = {MANUAL_FROM_DATE} (manual override — applied to all companies)")
        else:
            logger.info("from_date = auto (using each company's starting_from)")

        sync_all_companies(
            companies        = companies,
            tally            = tally,
            engine           = engine,
            to_date          = to_date,
            manual_from_date = MANUAL_FROM_DATE,
        )

    logger.info("Tally Sync completed")


if __name__ == '__main__':
    main()