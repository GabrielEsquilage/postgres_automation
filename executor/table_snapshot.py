class TableSnapshot:
    def __init__(self, db, nome_ambiente):
        self.db = db
        self.nome_ambiente = nome_ambiente
        self.cursor = db.get_cursor()
        self.snapshot = {}  # schema -> [tabelas]

    def capturar(self, schemas):
        for schema in schemas:
            print(f"[{self.nome_ambiente}] listando tabelas em {schema}...")
            self.cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE';
            """, (schema,))
            tabelas = [f"{schema}.{row[0]}" for row in self.cursor.fetchall()]
            self.snapshot[schema] = tabelas

    def get_snapshot(self):
        return self.snapshot

    def imprimir(self):
        print(f"\nTabelas no ambiente {self.nome_ambiente.upper()}:")
        for schema, tabelas in self.snapshot.items():
            for tabela in tabelas:
                print(tabela)
