from utils.csv_writer import salvar_csv

class RelacionamentosTabela:
    def __init__(self, db, schema_tabela):
        self.db = db
        self.schema, self.tabela = self._split_schema_tabela(schema_tabela)
        self.relacoes = []

    def _split_schema_tabela(self, schema_tabela):
        if "." not in schema_tabela:
            raise ValueError("Formato inválido. Use 'schema.tabela'")
        return schema_tabela.split(".", 1)

    def listar_relacoes(self, caminho_csv=None):
        self._coletar_relacoes()

        if caminho_csv:
            from os import makedirs
            from os.path import dirname
            makedirs(dirname(caminho_csv), exist_ok=True)

            salvar_csv(
                self.relacoes,
                caminho_csv,
                [
                    "schema_origem",
                    "tabela_origem",
                    "coluna_origem",
                    "schema_referenciado",
                    "tabela_referenciada",
                    "coluna_referenciada",
                ]
            )
            print(f"\n📄 Relações salvas em: {caminho_csv}")

    def _coletar_relacoes(self):
        cursor = self.db.get_cursor()
        query = """
            SELECT
                tc.table_schema AS schema_origem,
                tc.table_name AS tabela_origem,
                kcu.column_name AS coluna_origem,
                ccu.table_schema AS schema_referenciado,
                ccu.table_name AS tabela_referenciada,
                ccu.column_name AS coluna_referenciada
            FROM
                information_schema.table_constraints AS tc
            JOIN
                information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN
                information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE
                tc.constraint_type = 'FOREIGN KEY'
                AND (
                    (ccu.table_schema = %s AND ccu.table_name = %s)
                    OR (tc.table_schema = %s AND tc.table_name = %s)
                )
            ORDER BY
                tabela_origem, coluna_origem;
        """
        cursor.execute(query, (self.schema, self.tabela, self.schema, self.tabela))
        for row in cursor.fetchall():
            self.relacoes.append({
                "schema_origem": row[0],
                "tabela_origem": row[1],
                "coluna_origem": row[2],
                "schema_referenciado": row[3],
                "tabela_referenciada": row[4],
                "coluna_referenciada": row[5],
            })
