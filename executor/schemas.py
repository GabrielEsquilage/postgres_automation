from .base_executor import BaseExecutor

class SchemaFinder(BaseExecutor):

    def listar_schemas_validos(self):
        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%';
        """
        self.cursor.execute(query)
        schemas = [row[0] for row in self.cursor.fetchall()]
        return schemas

    def listar_tabelas_por_schema(self, schema):
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE';
        """
        self.cursor.execute(query, (schema,))
        tabelas = [row[0] for row in self.cursor.fetchall()]
        return tabelas

    def listar_todas_tabelas(self):
        resultado = {}
        schemas = self.listar_schemas_validos()
        for schema in schemas:
            tabelas = self.listar_tabelas_por_schema(schema)
            resultado[schema] = tabelas
        return resultado
