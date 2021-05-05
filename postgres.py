import psycopg2
import logging
from data_models import *
from data_errors import *

logger = logging.getLogger(__name__)


CONNECTION = None
CURSOR = None


def get_InsertSql_object(obj: object):
    insert_sql = InsertSql()
    insert_sql.table_name = obj.__class__.__dict__[PostgresPersistenceModel.TABLE_NAME_KEYWORD]
    fields = vars(obj)
    insert_sql.columns = [f for f in fields.keys() if fields.get(f) is not None]
    insert_sql.values = [fields.get(c) for c in insert_sql.columns]
    logger.debug(f"created InsertSql: {insert_sql}")
    return insert_sql


def get_insert_sql(insert_sql: InsertSql):
    sql = f"INSERT INTO {insert_sql.table_name} ({','.join(insert_sql.columns)}) VALUES ({','.join(['%s'] * len(insert_sql.columns))});"
    logger.debug(f"created sql: {sql}")
    return sql


def insert(obj):
    try:
        db_configs = get_db_configs(obj)
        create_connection(db_configs)

        insert_sql_object = get_InsertSql_object(obj)
        sql = get_insert_sql(insert_sql_object)

        CURSOR.execute(sql, insert_sql_object.values)
        CONNECTION.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    finally:
        close_connection()


def get_db_configs(obj):
    clazz = obj.__class__
    if PostgresPersistenceModel.CONFIG_KEYWORD not in clazz.__dict__:
        raise NoConfigException(f"Class '{clazz}' does not contain the static configs field. This in necessary for a DB connection")
    return clazz.__dict__[PostgresPersistenceModel.CONFIG_KEYWORD]


def create_connection(db_configs):
    try:
        logger.debug('Connecting to the PostgreSQL database...')
        global CONNECTION
        global CURSOR
        CONNECTION = psycopg2.connect(**db_configs)
        CURSOR = CONNECTION.cursor()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)


def close_connection():
    if CURSOR is not None:
        CURSOR.close()
        logger.debug('Cursor closed.')
    if CONNECTION is not None:
        CONNECTION.close()
        logger.debug('Database connection closed.')
