import os
from dotenv import load_dotenv
from conector.postgres import PostgresConector
from executor.schemas import SchemaFinder
from executor.table_snapshot import TableSnapshot

load_dotenv()

def listar_schemas_validos(db):
    finder = SchemaFinder(db)
    schemas = finder.listar_schemas_validos()
    print("Schemas válidos encontrados:")
    for schema in schemas:
        print(f"- {schema}")
    return schemas, finder


def processar_banco(db_config, env_name):
    with PostgresConector(**db_config) as db:
        schemas, _ = listar_schemas_validos(db)
        snapshot = TableSnapshot(db, env_name)
        snapshot.capturar(schemas)
        snapshot.imprimir()


def main():
    db_dev_config = {
        "host": os.getenv("DEV_DB_HOST"),
        "dbname": os.getenv("DEV_DB_NAME"),
        "user": os.getenv("DEV_DB_USER"),
        "password": os.getenv("DEV_DB_PASSWORD"),
        "port": int(os.getenv("DEV_DB_PORT")),
    }

    db_prod_config = {
        "host": os.getenv("PROD_DB_HOST"),
        "dbname": os.getenv("PROD_DB_NAME"),
        "user": os.getenv("PROD_DB_USER"),
        "password": os.getenv("PROD_DB_PASSWORD"),
        "port": int(os.getenv("PROD_DB_PORT")),
    }

    processar_banco(db_dev_config, "dev")
    processar_banco(db_prod_config, "prod")


if __name__ == "__main__":
    main()
