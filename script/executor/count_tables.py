from psycopg2 import sql

class TableCounter:
    def __init__(self, connector):
        self.connector = connector
        self.cursor = connector.get_cursor()

    def contar_registros(self, schema, tabela):
        try:
            query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(tabela)
            )
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Erro ao contar {schema}.{tabela}: {e}")
            return None

    def contar_tabelas_em_schemas(self, schemas, schema_finder):
        resultados = []
        for schema in schemas:
            tabelas = schema_finder.listar_tabelas_por_schema(schema)
            for tabela in tabelas:
                total = self.contar_registros(schema, tabela)
                resultados.append({
                    "schema": schema,
                    "tabela": tabela,
                    "total": total
                })
        return resultados
