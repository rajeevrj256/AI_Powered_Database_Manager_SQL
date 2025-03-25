import psycopg2
from dotenv import load_dotenv
from prettytable import PrettyTable
import pandas as pd






# Connect to the PostgreSQL database


def connect_db(USER, PASSWORD, HOST, PORT, DBNAME):
    
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        print(" Connection successful!")

        return connection

    except Exception as e:
        error_message = f"Database connection failed: {str(e)}"
        print(f"Failed to connect: {e}")
        #return error_message

def stucture_table(result):
    columns = [desc[0] for desc in result.description]
    table = PrettyTable(columns)
    for row in result:
        table.add_row(row)
    return table

def query_execute(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        
        if query.strip().lower().startswith('select'):
            result = stucture_table(cursor)
            return result
        else:
            
            connection.commit()
            return "Query executed successfully."
    except Exception as e:
        connection.rollback()
        return f"Error: {e}"    
        
def table_name(connection):
    
    query="""
                   SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
"""
    response=query_execute(connection, query)
    if isinstance(response, PrettyTable):  # If the response is a PrettyTable object
        response_str = str(response)
        rows = response_str.strip().split("\n")[3:-1]  # Skip headers and last line
        table_data = []
        for row in rows:
            parts = row.strip().split("|")[1:-1]  # Extract content between '|' delimiters
            table_name = parts[0].strip()           # Clean extra spaces
            
            table_data.append(f"{table_name}")
    else:
        table_data = [f"{row[0]}:{row[1]}" for row in response] if isinstance(response, list) else []

    print("Table names with an array:")
    print(table_data)
    return table_data
       

def create_bulk_table(file,table_name,connection):
    try:
        print("converting data into dataframe")
        df = pd.read_csv(file) if isinstance(file, str) else pd.read_csv(file)
        df=pd.DataFrame(df)
        print("creating table")
        print(file)
        sql= f"DROP TABLE IF EXISTS {table_name};"
        sql += f"CREATE TABLE {table_name} (\n"
        for col, dtype in df.dtypes.items():
            col = f'"{col.strip()}"'
            if "int" in str(dtype):
                sql += f"    {col} INTEGER,\n"
            elif "float" in str(dtype):
                sql += f"    {col} FLOAT,\n"
            else:
                sql += f"    {col} TEXT,\n"
        sql = sql.rstrip(",\n") + "\n);"
        print(sql)
        response=query_execute(connection, sql)
        print(response)
        return response
    except Exception as e:
        return f"Error: {e}"
    
def coloumns_name(table_name,connection):

    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}';
   

    """
    print("Executing the query...")
    response = query_execute(connection, query)
    if isinstance(response, PrettyTable):  # If the response is a PrettyTable object
        response_str = str(response)
        rows = response_str.strip().split("\n")[3:-1]  # Skip headers and last line
        column_data = []
        for row in rows:
            parts = row.strip().split("|")[1:-1]  # Extract content between '|' delimiters
            col_name = parts[0].strip()           # Clean extra spaces
            data_type = parts[1].strip()
            column_data.append(f"{col_name}:{data_type}")
    else:
        column_data = [f"{row[0]}:{row[1]}" for row in response] if isinstance(response, list) else []

    print("Column names with data types as an array:")
    print(column_data)
    return column_data
def close_connection(connection):
    connection.close()
    print("Connection closed.")
    
    
    
def main(query,connection):
    
    print("Executing the query...")
    response = query_execute(connection, query)
    print(response)
    return response
    

    
if __name__ == "__main__":
    main()