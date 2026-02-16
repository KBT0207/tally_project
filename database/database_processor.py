from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
from database.models.company import Company
import pandas as pd
from logging_config import logger


def company_import_db(data, engine):

    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    try:
        logger.info("Starting company import process")
        df = pd.DataFrame(data)
        df = df[df['name'].notna() & (df['name'].str.strip() != '')]
        logger.info(f"Records after name filtering: {len(df)}")

        date_cols = ['starting_from', 'books_from', 'audited_upto']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col],
                    format="%Y%m%d",
                    errors="coerce"
                ).dt.date

        inserted = 0
        updated = 0
        skipped = 0
        unchanged = 0

        fields = [
            "name",
            "formal_name",
            "company_number",
            "starting_from",
            "books_from",
            "audited_upto"
        ]

        for _, row in df.iterrows():

            if not row.get("guid"):
                skipped += 1
                logger.warning("Skipped record due to missing GUID")
                continue

            existing_company = db.query(Company).filter(
                Company.guid == row["guid"]
            ).first()

            if existing_company:

                is_changed = False

                for field in fields:
                    if getattr(existing_company, field) != row.get(field):
                        setattr(existing_company, field, row.get(field))
                        is_changed = True

                if is_changed:
                    updated += 1
                    logger.debug(f"Updated company: {row['guid']}")
                else:
                    unchanged += 1
                    logger.debug(f"No changes for company: {row['guid']}")

            else:
                new_company = Company(
                    guid=row["guid"],
                    name=row.get("name"),
                    formal_name=row.get("formal_name"),
                    company_number=row.get("company_number"),
                    starting_from=row.get("starting_from"),
                    books_from=row.get("books_from"),
                    audited_upto=row.get("audited_upto"),
                )
                db.add(new_company)
                inserted += 1
                logger.debug(f"Inserted company: {row['guid']}")

        db.commit()

        logger.info(
            f"Import completed | "
            f"Inserted: {inserted} | "
            f"Updated: {updated} | "
            f"Unchanged: {unchanged} | "
            f"Skipped: {skipped}"
        )

    except Exception:
        db.rollback()
        logger.exception("Error occurred during company import")
        raise

    finally:
        db.close()
        logger.info("Database session closed")
