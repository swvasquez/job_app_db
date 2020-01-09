import sqlite3
import pathlib


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn


def create_table(conn, sql_command):
    try:
        c = conn.cursor()
        c.execute(sql_command)
    except sqlite3.Error as e:
        print(e)


if __name__ == '__main__':
    DB_PATH = pathlib.Path(__file__).parent / 'data' / \
              'job_application_db.sqlite3'

    table_sql = """
    CREATE TABLE job_applications (
        company text NOT NULL,
        job_id text NOT NULL,
        url text,
        filepath text,
        application_date text,
        response integer,
        PRIMARY KEY (company, job_id)
        );
    """

    conn = create_connection(DB_PATH.resolve().as_posix())
    create_table(conn, table_sql)
