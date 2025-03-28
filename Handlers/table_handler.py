import psycopg2
from dotenv import load_dotenv
from prettytable import PrettyTable
import pandas as pd
import numpy as np

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
        print("Executing the query...")
        paginated_query = f"{query}"
        cursor.execute(paginated_query)
        print("Executing the query2...")
        if query.strip().lower().startswith('select'):
            # Extract column names from cursor description
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            result = []
            for row in rows:
                row_dict = {}
                for col, val in zip(columns, row):
                    # Replace NaN with None
                    if isinstance(val, float) and np.isnan(val):
                        row_dict[col] = None
                    else:
                        row_dict[col] = val
                result.append(row_dict)
            return {"data": result}
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
    print("Executing the column query...")
    cursor = connection.cursor()
    cursor.execute(query)
    response = cursor.fetchall()
    
    if response:
        response = [row[0] for row in response]
        return response
    else:
        return []