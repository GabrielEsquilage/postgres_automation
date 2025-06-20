class IndexDepthInspector:
    def __init__(self, connector):
        self.cursor = connector.get_cursor()

    def get_index_depth(self):
        query = """
            SELECT
                sui.schemaname,
                sui.relname AS tablename,
                sui.indexrelname AS indexname,
                pg_relation_size(sui.indexrelid) AS index_size_bytes,
                (SELECT relpages FROM pg_class WHERE oid = sui.indexrelid) AS index_pages
            FROM
                pg_stat_user_indexes sui
            WHERE
                sui.schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY
                sui.schemaname, sui.relname, sui.indexrelname;
        """
        self.cursor.execute(query)
        dados = self.cursor.fetchall()

        resultados = []
        for schemaname, tablename, indexname, index_size_bytes, index_pages in dados:
            resultados.append(
                {
                    "schema": schemaname,
                    "tabela": tablename,
                    "indice": indexname,
                    "tamanho_bytes": index_size_bytes,
                    "paginas": index_pages,
                }
            )
        return resultados
