from src.database.create_schema import create_schemas

def main():
    print("Iniciando pipeline...")

    create_schemas()

    print("Ambiente pronto!")

if __name__ == "__main__":
    main()