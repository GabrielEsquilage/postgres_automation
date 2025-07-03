from conector.postgres import PostgresConector
from executor.schemas import SchemaFinder
from executor.count_tables import TableCounter
from executor.indexes import IndexInspector
from executor.index_depth import IndexDepthInspector
from utils.csv_writer import salvar_csv
from executor.table_snapshot import TableSnapshot


def listar_schemas_validos(db):
    finder = SchemaFinder(db)
    schemas = finder.listar_schemas_validos()
    print("Schemas válidos encontrados:")
    for schema in schemas:
        print(f"- {schema}")
    return schemas, finder


# def contar_registros_por_tabela(db, schemas, finder):
#     print("\nContando registros por tabela:")
#     contador = TableCounter(db)
#     resultados = contador.contar_tabelas_em_schemas(schemas, finder)
#     for r in resultados:
#         print(f"{r['schema']}.{r['tabela']}: {r['total']} registros")
#     salvar_csv(resultados, "assets/contagem_tabelas.csv", ["schema", "tabela", "total"])


# def listar_indices(db):
#     print("\nListando índices por tabela:")
#     inspetor = IndexInspector(db)
#     indices = inspetor.listar_indices()
#     for idx in indices:
#         print(f"{idx['schema']}.{idx['tabela']} → {idx['indice']} ({idx['tipo']}) [{idx['colunas']}]")
#     salvar_csv(indices, "assets/indices_erp.csv", ["schema", "tabela", "indice", "tipo", "colunas"])


# def listar_profundidade_indices(db):
#     print("\nListando profundidade e tamanho dos índices:")
#     profundidade_inspector = IndexDepthInspector(db)
#     profundidade = profundidade_inspector.get_index_depth()
#     for idx in profundidade:
#         print(f"{idx['schema']}.{idx['tabela']} → {idx['indice']} | {idx['paginas']} páginas | {idx['tamanho_bytes']} bytes")
#     salvar_csv(profundidade, "assets/indices_profundidade.csv", ["schema", "tabela", "indice", "tamanho_bytes", "paginas"])


def main():
    dbdev = PostgresConector(
        host="104.198.51.109",
        dbname="erp",
        user="erp_user",
        password="DPkCerDe4<Q9OZmC",
        port=5432,
    )
    dbdev.conectar()
    dbprod = PostgresConector(
        host="34.39.146.37",
        dbname="erp",
        user="postgres",
        password="Arwe173Vi07E73ykdlTh",
        port=5432,
    )
    dbprod.conectar()
    
    schemas_dev, finder_dev = listar_schemas_validos(dbdev)
    schemas_prod, finder_prod = listar_schemas_validos(dbprod)
    
    snap_dev = TableSnapshot(dbdev, "dev")
    snap_dev.capturar(schemas_dev)

    snap_prod = TableSnapshot(dbprod, "prod")
    snap_prod.capturar(schemas_prod)

    snap_dev.imprimir()
    snap_prod.imprimir()

    dbdev.fechar()
    dbprod.fechar()


    # contar_registros_por_tabela(db, schemas, finder)
    # listar_indices(db)
    # listar_profundidade_indices(db)

    dbdev.fechar()
    dbprod.fechar()


if __name__ == "__main__":
    main()
