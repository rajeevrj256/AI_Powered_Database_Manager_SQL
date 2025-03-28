from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_caching import Cache
from Handlers.table_handler import  query_execute, table_name,coloumns_name 
from Handlers.db_handler import create_connection_pool, get_connection, close_connection, close_idle_connection
from Handlers.query_handler import pipeline
from Handlers.file_handler import create_bulk_table
from dotenv import load_dotenv
import asyncio
import hashlib
import json
import os

app = Flask(__name__)
CORS(app)
load_dotenv()
app.config['CACHE_TYPE'] = 'redis'
app.config['CACHE_REDIS_URL'] = os.getenv('CACHE_REDIS_URL')
app.config['CACHE_DEFAULT_TIMEOUT'] = 300  # Cache for 5 minutes

cache=Cache(app)
db_connection=None
@app.route('/postgresql/db_connect',methods=['POST'])
async def db_connect():
    try:
        data=request.get_json()
        user,password,host,port,database=data['user'],data['password'],data['host'],data['port'],data['database']
        
        if not all([user,password,host,port,database]):
            return jsonify({"status":"failed","message":"All fields are required"}),400
        
        conn_pool = await asyncio.to_thread(create_connection_pool,user, password, host, port, database)
        pool_key=f"{user}:{host}/{database}"
        if isinstance(conn_pool,str):
            return jsonify({"status":"failed","message":conn_pool}),500
        
        
        return jsonify({"status":"success","message":"Database connected successfully","conn_pool":pool_key}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
   
@app.route('/postgresql/table_name',methods=['POST'])
async def get_table_name():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        if not conn_pool:
            return jsonify({"status":"failed","message":"Connection pool is required"}),400
        
        conn = await asyncio.to_thread(get_connection, conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400
        
        result = await asyncio.to_thread(table_name, conn)
        return jsonify({"status":"success","message":"Table names fetched successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500   
    
@app.route('/postgresql/columns',methods=['POST']) 
async def get_columns():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool'] 
        table_name=data['table_name']
        if not conn_pool or not table_name:
            return jsonify({"status":"failed","message":"Connection pool and table name is required"}),400
        
        conn = await asyncio.to_thread(get_connection, conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400        
        result = await asyncio.to_thread(coloumns_name, conn, table_name)
        return jsonify({"status":"success","message":"Columns fetched successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
@app.route('/postgresql/query_generate',methods=['POST'])
async def query_generate_endpoint():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        table_name=data['table_name']
        column_names=data['column_names']
        prompt=data['prompt']
        if not conn_pool or not table_name or not column_names or not prompt:
            return jsonify({"status":"failed","message":"Connection pool,table name,column names and prompt is required"}),400  
        conn = await asyncio.to_thread(get_connection, conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400
        result = await asyncio.to_thread(pipeline, prompt, table_name, column_names, conn)
        return jsonify({"status":"success","message":"Query generated successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
@app.route('/postgresql/query_execute',methods=['POST'])
async def execute_query():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        query=data['query']
        
        if not conn_pool:
            return jsonify({"status":"failed","message":"Connection pool is required"}),400
        if not query:
            return jsonify({"status":"failed","message":"Query is required"}),400
       
        query_hash = hashlib.sha256(query.encode()).hexdigest()
        cache_key = f"query_result:{query_hash}"
        cached_result = cache.get(cache_key)
        if cached_result:
            return jsonify({"status": "success", "message": "Cached Query Result", "result": json.loads(cached_result)}), 200
        
        
        conn = await asyncio.to_thread(get_connection, conn_pool)
        
        if conn:
            result = await asyncio.to_thread(query_execute, conn, query)
            cache.set(cache_key, json.dumps(result), timeout=300)  # Cache for 5 minutes
            return jsonify({"status":"success","message":"Query executed successfully","result":result}),200
        else:
            return jsonify({"status":"failed","message":"Database not connected"}),400
    except Exception as e:        
        return jsonify({"status":"failed","message":str(e)}),500
    
@app.route('/postgresql/create_bulk_table',methods=['POST'])
async def create_bulk_table_endpoint():
    
    try:
        
        conn_pool = request.form.get('conn_pool')
        table_name = request.form.get('table_name')
        file = request.files.get('file')
        
        
        conn=await asyncio.to_thread(get_connection, conn_pool)
        if not conn:
            return jsonify({"status":"failed","message":"Database not connected"}),400
        
        if file is None:
            return jsonify({"status":"failed","message":"File is required"}),400
        
        if table_name is None:
            return jsonify({"status":"failed","message":"Table name is required"}),400
        
        result = await asyncio.to_thread(create_bulk_table, file,table_name,conn)
        
        return jsonify({"status":"success","message":"Bulk table created successfully","result":result}),200
    except Exception as e:
        return jsonify({"status":"failed","message":str(e)}),500
    
@app.route('/postgresql/db_disconnect',methods=['Post'])
async def db_disconnect():
    try:
        data=request.get_json()
        conn_pool=data['conn_pool']
        if not conn_pool:
            return jsonify({"status":"failed","message":"Connection pool is required"}),400
        result = await asyncio.to_thread(close_connection, conn_pool)
        if result:
            return jsonify({"status":"success","message":"Database disconnected successfully"}),200
        else:
            return jsonify({"status":"failed","message":"Database not connected"}),400
       
        
    except Exception as e:
        print("Error in db_disconnect:", str(e))
        return jsonify({"status":"failed","message":str(e)}),500

if __name__ == '__main__':
    import asyncio
    import uvicorn
    asyncio.run(uvicorn.run(app, host="0.0.0.0", port=5000))
