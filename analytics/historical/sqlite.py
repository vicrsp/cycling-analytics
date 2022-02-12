import sys, getopt, os
import sqlite3
from sqlite3 import Error

MEMORY_DATABASE = "file::memory:?cache=shared"

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES |
                               sqlite3.PARSE_COLNAMES)
    except Error as e:
        print(e)
    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


# def clear_database(conn):
#     try:
#         sql_rto = '''delete from train;'''
#         sql_run = '''delete from train_fit_values;'''
#         sql_results = '''delete from result_variable_values;'''

#         queries = [sql_results, sql_run, sql_rto]
#         cur = conn.cursor()
#         for query in queries:        
#             cur.execute(query)
#             conn.commit()

#     except Error as e:
#       print(f'Error clearing database: {e}')
    


# def init_data(conn):

#     clear_database(conn)

#     sql_rto = '''INSERT INTO rto(id, name, type, model, date) VALUES (0, 'first RTO ever', 'oh', 'yeah',  NULL);'''
#     sql_run = '''INSERT INTO run(id, iteration, status, rto_id) VALUES (0, 0, 'none',0);'''

#     cur = conn.cursor()
#     cur.execute(sql_rto)
#     conn.commit()

#     cur.execute(sql_run)
#     conn.commit()


def create_db(database):
    sql_create_train_table = """ CREATE TABLE IF NOT EXISTS train (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        name text NOT NULL,
                                        type text NOT NULL,
                                        user text NOT NULL,
                                        date text NOT NULL
                                    ); """

    sql_create_fit_values_table = """CREATE TABLE IF NOT EXISTS train_fit_values (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    timestamp text NOT NULL,
                                    variable text NOT NULL,
                                    value real NULL,
                                    unit text NULL,
                                    train_id integer NOT NULL REFERENCES train (id) ON DELETE CASCADE,
                                    FOREIGN KEY (train_id) REFERENCES train (id)
                                );"""

   

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, sql_create_train_table)
        create_table(conn, sql_create_fit_values_table)
    else:
        print("Error! cannot create the database connection.")

