class IndexInspector:
    def __init__(self, connector):
        self.cursor = connector.get_cursor()

    def listar_indices(self):
        query = """
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
              AND schemaname NOT LIKE 'pg_temp_%';
        """
        self.cursor.execute(query)
        dados = self.cursor.fetchall()

        resultado = []
        for schema, tabela, indice, definicao in dados:
            tipo = self._extrair_tipo(definicao)
            colunas = self._extrair_colunas(definicao)
            resultado.append({
                "schema": schema,
                "tabela": tabela,
                "indice": indice,
                "tipo": tipo,
                "colunas": colunas
            })
        return resultado

    def _extrair_tipo(self, definicao):
        if "USING" in definicao:
            return definicao.split("USING")[1].split()[0].strip()
        return "desconhecido"

    def _extrair_colunas(self, definicao):
        if "(" in definicao and ")" in definicao:
            return definicao.split("(")[-1].split(")")[0].strip()
        return ""
