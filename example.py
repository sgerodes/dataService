from models import *


@PostgresPersistenceModel(table="Dummy",
                          config={
                            "host": "localhost",
                            "database": "postgres",
                            "user": "postgres",
                            "password": "postgres"})
class DummyPostgresModel:
    def __init__(self):
        self.field1 = None
        self.field2 = 2

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"
