
from agents import define_complexity, generate_query, generate_query_with_complex,create_new_table



from db_connect import main,coloumns_name




    
def pipeline(user_request, table_name, column_names,connection):
    
    
    if connection is None:
        print("Error: Database connection is not established. Please connect to the database and try again.")
        return
    
    complexity = define_complexity(user_request)
    print("Complexity of the query:", complexity)
    query=""
    table_query = [ "schema","create a new table","create a table","create a new table with the following schema","create table"]
    
    if any(word in user_request for word in table_query):

        print("Generating a new table query...")
        query = create_new_table(user_request)
        
    elif  "complex"  in complexity:
        print("Generating a complex query...")
        query = generate_query(user_request, table_name, column_names)
    else:
        print("Generating a simple query...")   
        query = generate_query(user_request, table_name, column_names)
    print("Generated SQL Query:\n", query)
    
    if "Error:" not in query and query.strip():  # Check for error or empty query
        generated_query = main(query,connection)
       
        return generated_query
    else:
        print("Query generation failed. Please review the input and try again.")
    
    
    
# if __name__ == "__main__":
    
#     while True:
        
#         user_request = input("Enter the SQL query request: ")
#         if(user_request=="exit"):
#             break      
         
#         column=coloumns_name("bank_accounts")
        
         
#         if not column:  
#             print("Error: Invalid column names. Please check the table name and try again.")
#         pipeline(user_request, "bank_accounts", column)
#     # 