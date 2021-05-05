from data_errors import *


class InsertSql:
    def __init__(self):
        self.table_name = None
        self.columns = None
        self.values = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"


class PostgresPersistenceModel:
    TABLE_NAME_KEYWORD = "table"
    CONFIG_KEYWORD = "config"
    """
    kwargs
        config: mandatory, a dictionary that specifies host, database, user and password
        table: optionally, a string that specifies the table name, if omitted, the class name is used instead
    """

    def __init__(self, **kwargs):
        if PostgresPersistenceModel.CONFIG_KEYWORD not in kwargs:
            raise NoConfigException(
                f"The class {self.__class__.__name__} need to specify a kwarg {PostgresPersistenceModel.CONFIG_KEYWORD}")
        self.config = kwargs.get(PostgresPersistenceModel.CONFIG_KEYWORD)
        self.table = kwargs.get(PostgresPersistenceModel.TABLE_NAME_KEYWORD)

    def __call__(self, *args, **kwargs):
        if args:
            clazz = args[0]
            if PostgresPersistenceModel.TABLE_NAME_KEYWORD not in self.__dict__ or self.table is None:
                self.table = clazz.__name__

            clazz.config = self.config
            clazz.table = self.table
            return clazz

