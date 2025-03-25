import streamlit as st
import psycopg2
from urllib.parse import urlparse
from Testing.db_connect import connect_db, table_name, coloumns_name, close_connection
from Testing.table_creation import create_bulk_table
from Testing.chatbot import pipeline

def main():
    st.set_page_config(page_title="Database Query Assistant", layout="wide")

    # Top Navigation
    st.markdown(
        "<h1 style='text-align: center;'>Database Query Assistant 💬</h1>", 
        unsafe_allow_html=True
    )

    # Database Connection Panel (Sidebar)
    with st.sidebar:
        st.header("🔗 Database Connection")
        
        
        
        URL = st.text_input("URL:Transaction pooler")
        if URL:
            parse_url = urlparse(URL)
            USER= parse_url.username
            PASSWORD= parse_url.password
            
            HOST= parse_url.hostname
            PORT= parse_url.port
            DBNAME= parse_url.path[1:]
        
        st.header("OR")
        
        USER = st.text_input("Username")
        PASSWORD = st.text_input("Password", type="password")
        HOST = st.text_input("Host")
        PORT = st.number_input("Port", value=5432)
        DBNAME = st.text_input("Database Name")   
        
        
        connect_btn = st.button("✅ Connect to DB")
        disconnect_btn = st.button("❌ Disconnect")

        if connect_btn:
            connection = connect_db(USER, PASSWORD, HOST, PORT, DBNAME) 
            if connection:
                st.success("✅ Database connected!")
                st.session_state["connection"] = connection
            else:
                st.error("❌ Database connection failed!")

        if disconnect_btn and "connection" in st.session_state:
            close_connection(st.session_state["connection"])
            del st.session_state["connection"]
            st.warning("⚠️ Disconnected from database.")
            
        if "connection" in st.session_state:
            st.subheader("📊 Select a Table")
            table_list = table_name(st.session_state["connection"])
            selected_table = st.selectbox("Choose a table:", table_list, key="table_selection")
           
            
        else:
            selected_table = None
            st.warning("⚠️ No database connected")
            
            
        if "connection" in st.session_state:
            
            st.subheader("📂 Upload File & Create Table")
            uploaded_file = st.file_uploader("Upload a CSV or PDF file", type=["csv", "pdf"])
            table_name_input = st.text_input("Enter Table Name")
            if uploaded_file and table_name_input:
                if st.button("📌 Create Table"):
                    if uploaded_file.type == "text/csv":
                        
                        response=create_bulk_table(uploaded_file,table_name_input, st.session_state["connection"])
                        print("crate jnfjkewfnkerjfn",response)
                        if response:
                            st.success(f"✅ Table '{table_name_input}' created successfully!")
                            table_list = table_name(st.session_state["connection"])
                            st.session_state["new_table"] = table_name_input
                            
                    elif uploaded_file.type == "application/pdf":
                        st.error("🚧 PDF processing is not supported yet.")
    # Ensure a connection exists
    if "connection" not in st.session_state:
        st.warning("❗ Please connect to the database.")
        return

    



    # Fetch table columns

    columns = coloumns_name(selected_table, st.session_state["connection"])

    # Chat UI
    st.subheader("💬 Chat with SQL Assistant")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []  # Store chat history

    # Display previous chat messages
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # User Input
    user_query = st.chat_input("Enter your SQL query...")

    if user_query:
        # Store and display user message
        st.session_state.messages.append({"role": "user", "content": user_query})
        with st.chat_message("user"):
            st.markdown(user_query)

        # Process query
        with st.spinner("Generating SQL query..."):
            try:
                query = pipeline(user_query, selected_table, columns, st.session_state["connection"])

            # Store and display generated SQL query
                st.session_state.messages.append({"role": "assistant", "content": f"**Generated Query:**\n```sql\n{query}\n```"})
                with st.chat_message("assistant"):
                    st.code(query, language="sql")
                    
                    
                    
                
            except Exception as e:
                st.error(f"Error executing query: {e}")
        

if __name__ == "__main__":
    main()