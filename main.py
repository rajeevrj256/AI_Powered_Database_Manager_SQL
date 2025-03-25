from flask import Flask, request, jsonify
from flask_cors import CORS
from Handlers.table_handler import  query_execute, table_name,coloumns_name 
from Handlers.db_handler import create_connection_pool, get_connection, close_connection, close_idle_connection
from Handlers.query_handler import pipeline

app = Flask(__name__)

CORS(app)
db_connection=None
@app.route('/db_connect',methods=['POST'])
def db_connect():
    try:
        data=request.get_json()
        user,password,host,port,database=data['user'],data['password'],data['host'],data['port'],data['database']
        
        if not all([user,password,host,port,database]):
            return jsonify({"status":"failed","message":"All fields are required"}),400
        
        conn_pool= create_connection_pool(user,password,host,port,database)
        pool_key=f"{user}:{host}/{database}"
        if isinstance(conn_pool,str):
            return jsonify({"status":"failed","message":conn_pool}),500
        
        
        return jsonify({"status":"success","message":"Database connected successfully","conn_pool":pool_key}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
   
@app.route('/table_name',methods=['POST'])
def get_table_name():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        if not conn_pool:
            return jsonify({"status":"failed","message":"Connection pool is required"}),400
        
        conn=get_connection(conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400
        
        result=table_name(conn)
        return jsonify({"status":"success","message":"Table names fetched successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500   
    
@app.route('/columns',methods=['POST']) 
def get_columns():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool'] 
        table_name=data['table_name']
        if not conn_pool or not table_name:
            return jsonify({"status":"failed","message":"Connection pool and table name is required"}),400
        
        conn=get_connection(conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400        
        result=coloumns_name(conn,table_name)
        return jsonify({"status":"success","message":"Columns fetched successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
@app.route('/query_generate',methods=['POST'])
def query_generate_endpoint():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        table_name=data['table_name']
        column_names=data['column_names']
        prompt=data['prompt']
        if not conn_pool or not table_name or not column_names or not prompt:
            return jsonify({"status":"failed","message":"Connection pool,table name,column names and prompt is required"}),400  
        conn=get_connection(conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400
        result=pipeline(prompt,table_name,column_names,conn)
        return jsonify({"status":"success","message":"Query generated successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
@app.route('/query_execute',methods=['POST'])
def execute_query():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        query=data['query']
        
        if not conn_pool:
            return jsonify({"status":"failed","message":"Connection pool is required"}),400
        if not query:
            return jsonify({"status":"failed","message":"Query is required"}),400
       
        conn=get_connection(conn_pool)
        
        if conn:
            result=query_execute(conn,query)
            return jsonify({"status":"success","message":"Query executed successfully","result":result}),200
        else:
            return jsonify({"status":"failed","message":"Database not connected"}),400
    except Exception as e:        
        return jsonify({"status":"failed","message":str(e)}),500
    
    

@app.route('/db_disconnect',methods=['Post'])
def db_disconnect():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        if not conn_pool:
            return jsonify({"status":"failed","message":"Connection pool is required"}),400
        
        if close_connection(conn_pool):
            return jsonify({"status":"success","message":"Database disconnected successfully"}),200
        else:
            return jsonify({"status":"failed","message":"Database not connected"}),400
       
        
    except Exception as e:
        print("Error in db_disconnect:", str(e))
        return jsonify({"status":"failed","message":str(e)}),500

if __name__ == '__main__':
    app.run(debug=True)
