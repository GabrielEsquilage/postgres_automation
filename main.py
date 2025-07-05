import os
from dotenv import load_dotenv
from conector.postgres import PostgresConector
from executor.schemas import SchemaFinder
from executor.table_snapshot import TableSnapshot
from executor.comparador import ComparadorPessoaPorCpf
from executor.relacionamentos_tabela import RelacionamentosTabela

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

        # 🔹 Reutiliza o snapshot para obter lista de schema.tabela
        snapshot_tabelas = snapshot.get_snapshot()  # dict: {schema: [schema.tabela]}
        todas_tabelas = []
        for tabelas in snapshot_tabelas.values():
            todas_tabelas.extend(tabelas)

        # 🔹 Gera arquivos CSV com os relacionamentos
        total = len(todas_tabelas)
        print(f"\n📌 Gerando arquivos de relações para {total} tabelas...\n")

        for i, schema_tabela in enumerate(todas_tabelas, start=1):
            rel = RelacionamentosTabela(db, schema_tabela)
            caminho_csv = f"assets/relacoes/{schema_tabela.replace('.', '_')}.csv"
            rel.listar_relacoes(caminho_csv)

            print(f"[{i:>3}/{total}] ✅ {schema_tabela} → {caminho_csv}")


def comparar_pessoas_por_cpf(db_dev_config, db_prod_config, schema):
    with PostgresConector(**db_dev_config) as dbdev, PostgresConector(**db_prod_config) as dbprod:
        comparador = ComparadorPessoaPorCpf(
            db1=dbdev,
            db2=dbprod,
            schema=schema,
            ignorar_colunas=[
                "id", "criacao", "alteracao", "exclusao",
                "genero_id", "raca_id", "nascimento", "codigo_pessoa_wae"
            ]
        )
        comparador.exibir_iguais()
        comparador.exibir_diferencas()


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
    # processar_banco(db_prod_config, "prod")
    comparar_pessoas_por_cpf(db_dev_config, db_prod_config, schema="public")


if __name__ == "__main__":
    main()
