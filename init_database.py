from database.models.company import Company
from database.models.base import Base
from database.db_connector import DatabaseConnector
from database.database_processor import company_import_db
from services.tally_connector import TallyConnector


db = DatabaseConnector(
    username='root',
    password='root',
    host='localhost',
    port=3306,
    database='tally_db'
)

# 1️⃣ Create database first
db.create_database_if_not_exists()

# 2️⃣ Then create engine
engine = db.get_engine()

# 3️⃣ Then create tables
Base.metadata.create_all(engine)


tally_connection = TallyConnector(
    port=9000,
    host='localhost',
    max_retries=3,
    timeout=(60, 1800)
)

# 4️⃣ Fetch companies
comp_list = tally_connection.fetch_all_companies()

# 5️⃣ Import into DB
company_import_db(comp_list, engine)
