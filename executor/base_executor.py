class BaseExecutor:
    def __init__(self, connector):
        self.connector = connector
        self.cursor = connector.get_cursor()
