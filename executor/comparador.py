class ComparadorPessoaPorCpf:
    def __init__(self, db1, db2, schema, ignorar_colunas=None):
        self.db1 = db1
        self.db2 = db2
        self.schema = schema
        self.ignorar_colunas = ignorar_colunas if ignorar_colunas else []

    def carregar_dados_por_cpf(self, db):
        cursor = db.get_cursor()
        query = f"""
            SELECT p.*, d.numero AS cpf
            FROM {self.schema}.pessoa p
            JOIN {self.schema}.documento d ON d.pessoa_id = p.id
            WHERE d.tipo_documento_id = 1
              AND d.numero IS NOT NULL
        """
        cursor.execute(query)
        colunas = [desc[0] for desc in cursor.description]
        registros = {}
        for linha in cursor.fetchall():
            row_dict = dict(zip(colunas, linha))
            cpf = row_dict["cpf"]
            registros[cpf] = row_dict
        return registros

    def comparar_sem_colunas(self, d1, d2):
        f1 = {k: v for k, v in d1.items() if k not in self.ignorar_colunas}
        f2 = {k: v for k, v in d2.items() if k not in self.ignorar_colunas}
        return f1 == f2

    def exibir_iguais(self):
        print(f"\n🔍 Comparando pessoas do schema '{self.schema}' (por CPF tipo_documento_id = 1)...")

        dados_dev = self.carregar_dados_por_cpf(self.db1)
        dados_prod = self.carregar_dados_por_cpf(self.db2)

        chaves_em_ambos = set(dados_dev.keys()) & set(dados_prod.keys())
        total_iguais = 0

        for cpf in chaves_em_ambos:
            if self.comparar_sem_colunas(dados_dev[cpf], dados_prod[cpf]):
                nome = dados_dev[cpf].get("nome", "[sem nome]")
                print(f"✅ {nome} - CPF: {cpf}")
                total_iguais += 1

        print(f"\n✔️ Total de pessoas idênticas: {total_iguais}")

    def exibir_diferencas(self):
        print(f"\n❌ Verificando pessoas com mesmo CPF mas dados diferentes...")

        dados_dev = self.carregar_dados_por_cpf(self.db1)
        dados_prod = self.carregar_dados_por_cpf(self.db2)

        chaves_em_ambos = set(dados_dev.keys()) & set(dados_prod.keys())
        total_diferentes = 0

        for cpf in chaves_em_ambos:
            pessoa_dev = dados_dev[cpf]
            pessoa_prod = dados_prod[cpf]

            if not self.comparar_sem_colunas(pessoa_dev, pessoa_prod):
                nome = pessoa_dev.get("nome") or pessoa_prod.get("nome") or "[sem nome]"
                print(f"\n❌ {nome} - CPF: {cpf} - Diferenças encontradas:")

                for campo in pessoa_dev:
                    if campo in self.ignorar_colunas:
                        continue

                    val_dev = pessoa_dev.get(campo)
                    val_prod = pessoa_prod.get(campo)

                    if val_dev != val_prod:
                        print(f"  - Campo '{campo}': dev='{val_dev}' | prod='{val_prod}'")

                total_diferentes += 1

        print(f"\n⚠️ Total de pessoas com dados divergentes: {total_diferentes}")

