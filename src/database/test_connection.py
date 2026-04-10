from connection import get_engine
from sqlalchemy import text  # 👈 importar text

def test_connection():
    engine = get_engine()
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))  # 👈 envolver com text()
        print("CONEXÃO OK: ", result.scalar())
        
if __name__ == '__main__':
    test_connection()