# A simple script to store job application progress using the Python SQLite
# wrapper. The table schema is located in the config.yaml file.

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
    COLUMNS = SCHEMA['columns']
    PKEY = SCHEMA['primary_key']

    db_path = DB_DIR / f"{DB_NAME}.sqlite3"

    # Creates parser rules for data entry.
    parser = argparse.ArgumentParser()
    for col in COLUMNS:
        field = col[0]
        default = None if col[2] == 'NOT_NULL' else 'NULL'
        if default:
            parser.add_argument(f"--{field}", type=str, default=default)
        else:
            parser.add_argument(field, type=str, default=default)

    args = parser.parse_args()
    values = ','.join((f"'{getattr(args, arg)}'" for arg in vars(args)))

    # Connect to the database
    conn = sqlite3.connect(db_path.resolve().as_posix())
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")

    # Check if schema in config file has changed. If it has, rename table
    # and create a new table with the updated schema. You will have to
    # manually move data from the old table to the updated one.
    new_schema = False
    no_table = True

    # First, check if table exists or schema has changed.
    existing_tables = [table[0] for table in c.fetchall()]
    if TABLE_NAME in existing_tables:
        no_table = False
        c.execute(f"PRAGMA table_info({TABLE_NAME})")
        existing_cols = [list(col[1:4]) for col in c.fetchall()]
        for col in existing_cols:
            col[2] = 'NULL' if col[2] == 0 else 1
        if existing_cols != SCHEMA:
            new_schema = True
            copies = {int(table_name[-1]) for table_name in existing_tables
                      if table_name.startswith(f"{TABLE_NAME}_copy_")}
            new = max(copies) + 1 if copies else 1
            new_name = f"{TABLE_NAME}_copy_{new}"
            rename_sql = f"""ALTER TABLE {TABLE_NAME} RENAME TO {new_name}"""

            print('Schema changed. Renaming table. Executing:')
            print(rename_sql)

            c.execute(rename_sql)

    # Then, if necessary, create table.
    if new_schema or no_table:
        columns = SCHEMA['columns']
        column_defs = []
        for col in columns:
            column_name = col[0]
            type = col[1].upper()
            required = 'NOT_NULL' if col[2] == 'NOT_NULL' else ''
            column_defs.append(f"{column_name} {type} {required}")
        if PKEY:
            new_sql = \
                f"""
                    CREATE TABLE {TABLE_NAME}({', '.join(column_defs)})
                    ADD CONSTRAINT {TABLE_NAME} PRIMARY KEY ({', '.join(PKEY)})
                """
        else:
            new_sql = \
                f""" CREATE TABLE {TABLE_NAME}({', '.join(column_defs)});"""

        print('Table does not exist. Creating new table. Executing:')
        print(new_sql)

        c.execute(new_sql)

    # Adds the new entry to the table.
    add_command = f"""
          INSERT INTO {TABLE_NAME}
          VALUES ({values});
          """

    print('Inserting entry into table. Executing:')
    print(add_command)
    c.execute(add_command)

    # Finalize modifications.
    conn.commit()
