import argparse
import pathlib
import sqlite3


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn


def add(conn, table, values):
    sql_command = """
    INSERT INTO {0}
    VALUES ({1});
    """
    try:
        c = conn.cursor()
        print("Executing:")
        print(sql_command.format(table, values))
        c.execute(sql_command.format(table, values))
        conn.commit()
    except sqlite3.Error as e:
        print(e)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('company', type=str)
    parser.add_argument('job_id', type=str)
    parser.add_argument('-u', '--url', type=str, default='NULL')
    parser.add_argument('-f', '--filepath', type=str, default='NULL'),
    parser.add_argument('-d', '--date', type=str, default='NULL')
    parser.add_argument('-r', '--response', type=str, default='no')

    args = parser.parse_args()
    values = ','.join((f"'{getattr(args, arg)}'" for arg in vars(args)))

    db_path = pathlib.Path(__file__).parent / 'data' / \
              'job_application_db.sqlite3'
    table = 'job_applications'

    conn = create_connection(db_path.resolve().as_posix())

    add(conn, table, values)
