from conector.postgres import PostgresConector
from executor.schemas import SchemaFinder
from executor.count_tables import TableCounter
from utils.csv_writer import salvar_csv
from executor.indexes import IndexInspector


def main():
    db = PostgresConector(
        host="104.198.51.109",
        dbname="erp",
        user="erp_user",
        password="DPkCerDe4<Q9OZmC",
        port=5432
    )
    db.conectar()

    finder = SchemaFinder(db)
    contador = TableCounter(db)

    schemas = finder.listar_schemas_validos()
    print("Schemas válidos encontrados:")
    for schema in schemas:
        print(f"- {schema}")

    print("\nContando registros por tabela:")
    resultados_contagem = contador.contar_tabelas_em_schemas(schemas, finder)
    for r in resultados_contagem:
        print(f"{r['schema']}.{r['tabela']}: {r['total']} registros")
    
    # Salva os resultados da contagem de tabelas
    salvar_csv(resultados_contagem, "assets/contagem_tabelas.csv", ["schema", "tabela", "total"])


    print("\nListando índices por tabela:")
    inspetor = IndexInspector(db)
    indices = inspetor.listar_indices()
    for idx in indices:
        print(f"{idx['schema']}.{idx['tabela']} → {idx['indice']} ({idx['tipo']}) [{idx['colunas']}]")

    # Salva os índices em um novo arquivo CSV
    salvar_csv(indices, "assets/indices_erp.csv", ["schema", "tabela", "indice", "tipo", "colunas"])


    db.fechar()

if __name__ == "__main__":
    main()