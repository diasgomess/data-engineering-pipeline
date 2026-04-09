from connection import get_engine

def test_connection():
    engine = get_engine()
    
    with engine.connect() as conn:
        result = conn.execute("SELECT 1")
        print("CONEXÃO OK: ", result.scalar())
        
if __name__ == '__main__':
    test_connection()