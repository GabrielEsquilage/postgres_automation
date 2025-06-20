import psycopg2

class PostgresConector:
    def __init__(self, host, dbname, user, password, port=5432):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        self.conn = None

    def conectar(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                port=self.port
            )
            print("Conectado")
        except psycopg2.Error as e:
            print(f"erro ao conectar: {e}")
            self.conn = None
    
    def get_cursor(self):
        if self.conn:
            return self.conn.cursor()
        else:
            raise Exception("falha na conexão")

    def fechar(self):
        if self.conn:
            self.conn.close()
            print("Conexão encerrada.")
