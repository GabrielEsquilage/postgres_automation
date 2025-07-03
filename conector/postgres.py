import psycopg2

class PostgresConector:
    def __init__(self, host, dbname, user, password, port=5432):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        self.conn = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                port=self.port
            )
            print(f"Conectado ao banco de dados {self.dbname} em {self.host}")
            return self
        except psycopg2.Error as e:
            print(f"Erro ao conectar: {e}")
            self.conn = None
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            print(f"Conexão com {self.dbname} em {self.host} encerrada.")

    def get_cursor(self):
        if self.conn:
            return self.conn.cursor()
        else:
            raise Exception("Falha na conexão: a conexão não está ativa.")
