import streamlit as st
import json
import os
from dotenv import load_dotenv
import re
import pandas as pd
from scripts.vector_storage import VertexAIEmbeddingFunction, ChromaDBHandler
from scripts.sql_script_generator2 import SQLScriptGenerator
from google.oauth2 import service_account


# Load environment variables from .env file
load_dotenv()

# Streamlit UI
st.title("Data Migration Logic Generation")

# Load the JSON data files
@st.cache_data
def load_json_data():
    try:
        with open("Legacy_tables.json", "r") as f:
            legacy_tables = json.load(f)
        with open("Target_tables.json", "r") as f:
            target_tables = json.load(f)
        return legacy_tables, target_tables
    except Exception as e:
        st.error(f"Error loading JSON files: {str(e)}")
        return [], []

# Function to extract SQL query from response
def extract_sql_query(response):
    query = re.search(r"```sql(.*?)```", response, re.DOTALL)
    if query is None:
        return response
    else:
        return query.group(1).strip()

# Function to get legacy and dependent tables
def get_legacy_and_dependent_tables(target_schema, target_tables, legacy_tables, vector_storage):
    """
    Retrieves legacy tables from a retriever and finds dependent tables based on foreign keys.
    """
    # Extract foreign keys and initialize result lists
    foreign_keys = target_schema.get("foreign_keys", [])
    dependent_tables = [
        table for table in target_tables if table["table_name"] in foreign_keys
    ]
    
    target_schema_query = {
        target_schema["table_name"]: [column[0] for column in target_schema["columns"]]
    }
    
    # Create retriever
    retriever = vector_storage.as_retriever(
        search_type="mmr", search_kwargs={"k": 10, "fetch_k": 50}
    )
    
    # Retrieve and parse legacy table data
    retrieved_data = retriever.invoke(str(target_schema_query))
    retrieved_tables = [eval(doc.page_content) for doc in retrieved_data]

    # Extract matching legacy tables based on table names
    table_names = [list(item.keys())[0] for item in retrieved_tables]
    retrieved_tables = [
        next(table for table in legacy_tables if table["table_name"] == table_name)
        for table_name in table_names
        if any(table["table_name"] == table_name for table in legacy_tables)
    ]

    return retrieved_tables, dependent_tables

# Function to initialize vector storage
@st.cache_resource
def initialize_vector_storage():
    with st.spinner("Initializing vector storage..."):
        legacy_tables, _ = load_json_data()

        
        
        # Format data for embedding
        data = [
            {table["table_name"]: [column[0] for column in table["columns"]]}
            for table in legacy_tables
        ]
        service_account_info = json.loads(st.secrets["LUMBER_POC_CREDENTIALS_PATH"])
        credentials = service_account.Credentials.from_service_account_info(service_account_info)


        # Initialize embedding function and ChromaDB handler
        embeddings = VertexAIEmbeddingFunction(
            model_name="text-embedding-004",
            credentials=credentials,
            project_id="lumbar-poc",
        )

        chromadb_handler = ChromaDBHandler(embedding_function=embeddings)
        collection = chromadb_handler.get_or_create_collection("data_migration")
        chromadb_handler.add_to_collection(collection, data)
        
        return chromadb_handler.load_vector_storage()

# Function to get target schema from file name
def get_target_schema_by_filename(target_tables, filename):
    for table in target_tables:
        if table.get("file_name") == filename:
            return table
    return None

# Main application logic
try:
    # Load data and initialize vector storage
    legacy_tables, target_tables = load_json_data()
    
    if not legacy_tables or not target_tables:
        st.warning("Please make sure Legacy_tables.json and Target_tables.json files exist in the application directory.")
    else:
        # Get list of all target SQL files for selection
        all_sql_files = []
        for table in target_tables:
            if "file_name" in table:
                all_sql_files.append(table["file_name"])
                
        
        # # Group files by table name for better organization
        # grouped_files = {}
        # for file in all_sql_files:
        #     table_name = file.split('.')[1]  # Extract table name from file name
        #     if table_name not in grouped_files:
        #         grouped_files[table_name] = []
        #     grouped_files[table_name].append(file)
        
        # Select Table
        tables = all_sql_files
        table_name = st.selectbox("Select a Table:", tables)
        st.write("Total no target files in drop down:",len(tables))

        
        # # Dynamically update SQL file options based on table selection
        # sql_files = grouped_files.get(table_name, [])
        # sql_file = st.selectbox("Select an SQL File:", sql_files) if sql_files else None
        
        
        # Initialize vector storage when needed
        if table_name and st.button("Generate Transformation Logic"):
            with st.spinner("Processing... This may take a moment."):
                vector_storage = initialize_vector_storage()
                
                # Get target schema for selected file
                target_schema = get_target_schema_by_filename(target_tables, table_name)
                
                if target_schema:
                    # Get legacy and dependent tables
                    retrieved_tables, dependent_tables = get_legacy_and_dependent_tables(
                        target_schema, target_tables, legacy_tables, vector_storage
                    )
                    
                    
                    # Display retrieved tables and dependencies
                    st.subheader("Legacy Tables Retrieved")
                    for table in retrieved_tables:
                        st.write(f"- {table['table_name']}")
                    
                    st.subheader("Dependent Tables")
                    for table in dependent_tables:
                        st.write(f"- {table['table_name']}")
                    
                    # Generate SQL transformation logic
                    sql_writer = SQLScriptGenerator()
                    response = sql_writer.get_sql_query(
                        legacy_tables=legacy_tables,
                        target_schema=target_schema,
                        dependent_tables=dependent_tables,
                    )
                    
                    # Extract SQL query
                    query = extract_sql_query(response=response)
                    
                    # Display the transformation logic
                    st.subheader("Transformation Logic")
                    st.code(query, language="sql")
                    
                    # Provide option to download the SQL script
                    st.download_button(
                        label="Download SQL Script",
                        data=query,
                        file_name=f"transform_{table_name}",
                        mime="text/plain"
                    )
                    
                    # # Optional: Save to CSV
                    # if st.checkbox("Save to CSV"):
                    #     legacy_schemas_str = ", ".join([table["table_name"] for table in retrieved_tables])
                    #     target_schema_str = target_schema["table_name"]
                    #     dependent_schemas_str = ", ".join([table["table_name"] for table in dependent_tables])
                        
                    #     csv_data = pd.DataFrame({
                    #         "legacy_schemas": [legacy_schemas_str],
                    #         "target_schema": [target_schema_str],
                    #         "dependent_schemas": [dependent_schemas_str],
                    #         "query": [query]
                    #     })
                        
                    #     csv_file = "encounter_queries_claude.csv"
                    #     file_exists = os.path.isfile(csv_file)
                        
                    #     csv_data.to_csv(csv_file, mode='a', header=not file_exists, index=False)
                    #     st.success(f"Query saved to {csv_file}")
                else:
                    st.error(f"Could not find target schema for file: {table_name}")
except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.error("Please check your JSON files and make sure all dependencies are installed.")