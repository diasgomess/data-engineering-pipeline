import pandas as pd
from connection import get_engine

def insert_test():
    engine = get_engine()

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "nome": ["A", "B", "C"]
    })

    df.to_sql(
        name="teste_tabela",
        con=engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )

    print("Dados inseridos com sucesso!")

if __name__ == "__main__":
    insert_test()