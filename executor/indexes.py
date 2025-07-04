from .base_executor import BaseExecutor
import re

class IndexInspector(BaseExecutor):
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
        match = re.search(r"USING (\w+)", definicao)
        return match.group(1) if match else "desconhecido"

    def _extrair_colunas(self, definicao):
        match = re.search(r"\((.*)\)", definicao)
        return match.group(1).strip() if match else ""
