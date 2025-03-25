import pandas as pd
from Testing.db_connect import query_execute

def create_bulk_table(file, table_name, connection):
    try:
        print("Converting data into DataFrame...")
        df = pd.read_csv(file) if isinstance(file, str) else pd.read_csv(file)
        df = pd.DataFrame(df)

        print("Creating table schema...")
        sql = f"DROP TABLE IF EXISTS {table_name};"
        sql += f"CREATE TABLE {table_name} (\n"

        for col, dtype in df.dtypes.items():
            col = f'"{col.strip()}"'  # Handle spaces in column names

            if pd.api.types.is_integer_dtype(dtype):
                sql += f"    {col} INTEGER,\n"
            elif pd.api.types.is_float_dtype(dtype):
                sql += f"    {col} FLOAT,\n"
            else:
                sql += f"    {col} TEXT,\n"  # Default to TEXT for mixed/unknown types

        sql = sql.rstrip(",\n") + "\n);"

        print(f"Executing SQL:\n{sql}")
        response = query_execute(connection, sql)
        print(response)

        # ✅ INSERT DATA INTO TABLE
        insert_data(df, table_name, connection)
        
        print("test",response)

        return response
    except Exception as e:
        return f"Error: {e}"

def insert_data(df, table_name, connection):
    cursor = connection.cursor()
    try:
        # Creating column names dynamically
        columns = ', '.join([f'"{col}"' for col in df.columns])
        values_placeholder = ', '.join(['%s'] * len(df.columns))

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"
        print(f"Executing INSERT query: {insert_query}")

        # Convert DataFrame to list of tuples
        print("converting data into tuples")
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
        print("Inseting data into table")
        cursor.executemany(insert_query, data_tuples)  # ✅ Bulk insert
        connection.commit()
        print("✅ Data Inserted Successfully")
    except Exception as e:
        connection.rollback()
        print(f"❌ Error inserting data: {e}")
    finally:
        cursor.close()
