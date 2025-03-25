import psycopg2
from dotenv import load_dotenv
from prettytable import PrettyTable
import pandas as pd


def stucture_table(result):
    columns = result.fetchall()
    rows = [dict(zip(columns, row)) for row in result.fetchall()]
    table_html = "<table border='1'><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"
    table_html += "".join("<tr>" + "".join(f"<td>{val}</td>" for val in row) + "</tr>" for row in rows)
    table_html += "</table>"
    
    return table_html

def query_execute(connection, query,limit=10, offset=10):
    cursor = connection.cursor()
    try:
        paginated_query = f"{query}"
        cursor.execute(paginated_query)
        
        if query.strip().lower().startswith('select'):
            columns = [desc[0] for desc in cursor.description]  # Extract column names
            rows = cursor.fetchall()  # Fetch data
            
            result = [dict(zip(columns, row)) for row in rows]  # Convert to list of dicts
            return {"data": result}  # Al
        else:
            
            connection.commit()
            return {"message": "Query executed successfully."}
    except Exception as e:
        connection.rollback()
        return {"error": str(e)}    
        
def table_name(connection):
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """
    cursor = connection.cursor()
    cursor.execute(query)
    response = cursor.fetchall()
    if response:
        response = [row[0] for row in response]
        return response
    else:
        return []
    
    
def coloumns_name(connection,table_name):
    
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}';
   

    """
    print("Executing the query...")
    cursor = connection.cursor()
    cursor.execute(query)
    response = cursor.fetchall()
    
    if response:
        response = [row[0] for row in response]
        return response
    else:
        return []