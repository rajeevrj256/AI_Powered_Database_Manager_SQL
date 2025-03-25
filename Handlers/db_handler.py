import psycopg2
from psycopg2 import pool
import threading
import time

Connection_pools={}

active_connections={}

connection_lock=threading.Lock()

connection_timeout=300

def create_connection_pool(user,password,host,port,database,min_conn=1,max_conn=20):
    pool_key=f"{user}:{host}/{database}"
    
    with connection_lock:
        if pool_key not in Connection_pools:
            try:
                Connection_pools[pool_key]=psycopg2.pool.SimpleConnectionPool(min_conn,
                                                                              max_conn,
                                                                              user=user,
                                                                              password=password,
                                                                              host=host,port=port,
                                                                              database=database
                                                                        )
                active_connections[pool_key]=time.time()
            except Exception as e:
                return str(e)
    return Connection_pools[pool_key]


def get_connection(pool_key):
   
    with connection_lock:
        if pool_key  in Connection_pools:
            connection=Connection_pools[pool_key].getconn()
            active_connections[pool_key]=time.time()
            return connection
        return None
    
def close_connection(pool_key):
   
    with connection_lock:
        if pool_key in Connection_pools:
            Connection_pools[pool_key].closeall()
            del Connection_pools[pool_key]
            del active_connections[pool_key]
            return True
        return False
            
            
def close_idle_connection():
    while True:
        
        time.sleep(60)
        with connection_lock:
            
            for pool_key in list(active_connections.keys()):
                last_active_time=active_connections[pool_key]
                if time.time()-last_active_time>connection_timeout:
                    Connection_pools[pool_key].closeall()
                    del Connection_pools[pool_key]
                    del active_connections[pool_key]
                    print(f"Connection pool with key {pool_key} is removed")
                    
                    
cleanup_thread=threading.Thread(target=close_idle_connection,daemon=True)
cleanup_thread.start()
        