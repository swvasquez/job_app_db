import argparse
import pathlib
import sqlite3
import yaml

if __name__ == '__main__':

    # Load configuration settings.
    config_path = pathlib.Path(__file__).parent / 'config.yaml'
    with config_path.open(mode='r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    DB_NAME = config['db_name']
    DB_DIR = pathlib.Path(__file__).parent / config['db_dir']
    TABLE_NAME = config['table_name']
    SCHEMA = config['schema']

    db_path = DB_DIR / f"{DB_NAME}.sqlite3"

    # Parse input values for entry to add.
    parser = argparse.ArgumentParser()
    parser.add_argument('company', type=str)
    parser.add_argument('job_id', type=str)
    parser.add_argument('-u', '--url', type=str, default='NULL')
    parser.add_argument('-f', '--filepath', type=str, default='NULL'),
    parser.add_argument('-d', '--date', type=str, default='NULL')
    parser.add_argument('-r', '--response', type=str, default='no')
    parser.add_argument('title', type=str)

    args = parser.parse_args()
    values = ','.join((f"'{getattr(args, arg)}'" for arg in vars(args)))

    # Connect to the database
    conn = sqlite3.connect(db_path.resolve().as_posix())
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")

    # If the database or table defined in the configuration file does not
    # exist, create it. If the table exists already, but the configuration
    # file has new columns, append them to the existing table.
    if TABLE_NAME in (table[0] for table in c.fetchall()):
        c.execute(f"PRAGMA table_info({TABLE_NAME})")
        existing_cols = {col[1] for col in c.fetchall()}
        new_cols = []
        for col in SCHEMA['columns']:
            column_name = list(col.keys())[0]
            type = col[column_name][0]
            if column_name not in existing_cols:
                new_cols.append(f"{column_name} {type.upper()}")
        if new_cols:
            append_sql = f"ALTER TABLE {TABLE_NAME} ADD {','.join(new_cols)};"
            print(append_sql)
            c.execute(append_sql)
            conn.commit()
    else:
        columns = SCHEMA['columns']
        column_defs = []

        for col in columns:
            column_name = list(col.keys())[0]
            type = col[column_name][0].upper()
            if col[column_name][1] == 'not_null':
                required = ' NOT NULL'
            else:
                required = ''
            column_defs.append(f"{column_name} {type}{required}")

        create_sql = f"CREATE TABLE {TABLE_NAME} ({','.join(column_defs)});"
        print(create_sql)
        c.execute(create_sql)
        conn.commit()

    # Adds the new data to the table.
    add_command = """
          INSERT INTO {0}
          VALUES ({1});
          """.format(TABLE_NAME, values)

    print("Executing:")
    print(add_command)

    c.execute(add_command)
    conn.commit()
