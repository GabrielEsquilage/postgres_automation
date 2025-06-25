from conector.postgres import PostgresConector
from executor.schemas import SchemaFinder
from executor.count_tables import TableCounter
from executor.indexes import IndexInspector
from executor.index_depth import IndexDepthInspector
from utils.csv_writer import salvar_csv


def main():
    db = PostgresConector(
        host="",
        dbname="erp",
        user="postgres",
        password="",
        port=5432,
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

    salvar_csv(
        resultados_contagem,
        "assets/contagem_tabelas.csv",
        ["schema", "tabela", "total"],
    )

    print("\nListando índices por tabela:")
    inspetor = IndexInspector(db)
    indices = inspetor.listar_indices()
    for idx in indices:
        print(
            f"{idx['schema']}.{idx['tabela']} → {
                idx['indice']} ({idx['tipo']}) [{idx['colunas']}]"
        )

    salvar_csv(
        indices,
        "assets/indices_erp.csv",
        ["schema", "tabela", "indice", "tipo", "colunas"],
    )

    print("\nListando profundidade e tamanho dos índices:")
    profundidade_inspector = IndexDepthInspector(db)
    indices_profundidade = profundidade_inspector.get_index_depth()
    for idx in indices_profundidade:
        print(
            f"{idx['schema']}.{idx['tabela']} → {idx['indice']} | {
                idx['paginas']} páginas | {idx['tamanho_bytes']} bytes"
        )

    salvar_csv(
        indices_profundidade,
        "assets/indices_profundidade.csv",
        ["schema", "tabela", "indice", "tamanho_bytes", "paginas"],
    )

    db.fechar()


if __name__ == "__main__":
    main()

