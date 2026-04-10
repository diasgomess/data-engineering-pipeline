from src.database.connection import get_engine
from sqlalchemy import text

def create_schemas():
    engine = get_engine()

    schemas = ["bronze", "silver", "gold"]

    with engine.connect() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            print(f"Schema '{schema}' garantido.")

if __name__ == "__main__":
    create_schemas()