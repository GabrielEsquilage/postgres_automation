from .schemas import SchemaFinder

class TableSnapshot:
    def __init__(self, db, nome_ambiente):
        self.db = db
        self.nome_ambiente = nome_ambiente
        self.schema_finder = SchemaFinder(db)
        self.snapshot = {}  # schema -> [tabelas]

    def capturar(self, schemas):
        for schema in schemas:
            print(f"[{self.nome_ambiente}] listando tabelas em {schema}...")
            tabelas = self.schema_finder.listar_tabelas_por_schema(schema)
            self.snapshot[schema] = [f"{schema}.{tabela}" for tabela in tabelas]

    def get_snapshot(self):
        return self.snapshot

    def imprimir(self):
        print(f"\nTabelas no ambiente {self.nome_ambiente.upper()}:")
        for schema, tabelas in self.snapshot.items():
            for tabela in tabelas:
                print(tabela)
