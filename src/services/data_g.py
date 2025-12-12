# Install required packages first
# pip install databricks-sdk faker pandas

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
from databricks.sdk.service import sql
import pandas as pd
from faker import Faker
import random
from datetime import datetime
import time

fake = Faker()

# ============================================
# STEP 1: Authentication & Connection
# ============================================
print("🔐 Connecting to Databricks...")

import os 
from dotenv import load_dotenv
load_dotenv()
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Initialize Databricks client with token
w = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN
)

print("✅ Connected to Databricks")

# Configuration
CATALOG_NAME = "workspace"  # We'll create this
SCHEMA_NAME = "sample_data"
TABLE_1 = "sales_transactions"
TABLE_2 = "customer_feedback"


# ============================================
# STEP 2: List and Create Catalog
# ============================================
print("\n📋 Checking available catalogs...")

try:
    # List all catalogs
    catalogs = list(w.catalogs.list())
    print(f"✅ Found {len(catalogs)} catalog(s):")
    for cat in catalogs:
        print(f"   - {cat.name}")
    
    # Check if our catalog exists
    catalog_exists = any(cat.name == CATALOG_NAME for cat in catalogs)
    
    if not catalog_exists:
        print(f"\n🔨 Creating catalog: {CATALOG_NAME}")
        w.catalogs.create(
            name=CATALOG_NAME,
            comment="POC Catalog for Knowledge Graph demo"
        )
        print(f"✅ Catalog created: {CATALOG_NAME}")
    else:
        print(f"ℹ️  Catalog already exists: {CATALOG_NAME}")
        
except Exception as e:
    print(f"❌ Error with catalogs: {str(e)}")
    print("\n💡 Trying to use 'hive_metastore' as fallback...")
    CATALOG_NAME = "hive_metastore"

# ============================================
# STEP 3: Create Schema
# ============================================
print(f"\n🔨 Creating schema: {CATALOG_NAME}.{SCHEMA_NAME}")

try:
    w.schemas.create(
        catalog_name=CATALOG_NAME,
        name=SCHEMA_NAME,
        comment="Sample schema for Knowledge Graph POC"
    )
    print(f"✅ Schema created: {CATALOG_NAME}.{SCHEMA_NAME}")
except Exception as e:
    print(f"ℹ️  Schema might already exist: {str(e)}")

# ============================================
# STEP 4: Get SQL Warehouse
# ============================================
print("\n🔍 Finding SQL Warehouse...")

warehouses = list(w.warehouses.list())
if not warehouses:
    raise Exception("❌ No SQL Warehouses found. Please create one in your Databricks workspace.")

warehouse_id = warehouses[0].id
print(f"✅ Using warehouse: {warehouses[0].name} (ID: {warehouse_id})")

# ============================================
# STEP 5: Generate Sample Data - Table 1
# ============================================
print("\n📊 Generating sales transaction data...")

sales_data = {
    'transaction_id': [f'TXN-{str(i).zfill(5)}' for i in range(1, 151)],
    'customer_id': [f'CUST-{random.randint(1000, 9999)}' for _ in range(150)],
    'product_name': [fake.catch_phrase() for _ in range(150)],
    'product_category': [random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books']) for _ in range(150)],
    'order_date': [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(150)],
    'quantity': [random.randint(1, 10) for _ in range(150)],
    'unit_price': [round(random.uniform(10, 500), 2) for _ in range(150)],
    'total_amount': [],
    'region': [random.choice(['North America', 'Europe', 'Asia Pacific', 'Latin America']) for _ in range(150)],
    'sales_channel': [random.choice(['Online', 'In-Store', 'Mobile App', 'Phone']) for _ in range(150)],
    'payment_method': [random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']) for _ in range(150)],
    'created_by': ['sales_team'] * 150,
    'created_at': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 150
}

sales_data['total_amount'] = [
    round(sales_data['quantity'][i] * sales_data['unit_price'][i], 2) 
    for i in range(150)
]

df_sales = pd.DataFrame(sales_data)
print(f"✅ Generated {len(df_sales)} sales records")

# ============================================
# STEP 6: Generate Sample Data - Table 2
# ============================================
print("📊 Generating customer feedback data...")

feedback_data = {
    'feedback_id': [f'FB-{str(i).zfill(5)}' for i in range(1, 101)],
    'customer_id': [f'CUST-{random.randint(1000, 9999)}' for _ in range(100)],
    'transaction_id': [random.choice(sales_data['transaction_id']) for _ in range(100)],
    'rating': [random.randint(1, 5) for _ in range(100)],
    'feedback_text': [fake.sentence(nb_words=15) for _ in range(100)],
    'feedback_category': [random.choice(['Product Quality', 'Delivery', 'Customer Service', 'Pricing', 'Overall Experience']) for _ in range(100)],
    'sentiment': [random.choice(['Positive', 'Neutral', 'Negative']) for _ in range(100)],
    'submitted_date': [fake.date_between(start_date='-6m', end_date='today').strftime('%Y-%m-%d') for _ in range(100)],
    'response_status': [random.choice(['Pending', 'Acknowledged', 'Resolved', 'Closed']) for _ in range(100)],
    'assigned_to': [random.choice(['support_team', 'cx_team', 'product_team']) for _ in range(100)],
    'created_by': ['customer_experience_team'] * 100,
    'created_at': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 100
}

df_feedback = pd.DataFrame(feedback_data)
print(f"✅ Generated {len(df_feedback)} feedback records")

# ============================================
# STEP 7: Create Tables with SQL
# ============================================
print(f"\n💾 Creating tables in {CATALOG_NAME}.{SCHEMA_NAME}...")

# Helper function to execute SQL
def execute_sql(sql_query, description):
    print(f"\n📝 {description}")
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql_query,
            wait_timeout="30s"
        )
        
        # Wait for completion
        if response.status.state == sql.StatementState.SUCCEEDED:
            print(f"✅ Success")
            return response
        else:
            print(f"⏳ Status: {response.status.state}")
            return response
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None

# Create Table 1 with schema and descriptions
table_1_path = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_1}"

create_table1_sql = f"""
CREATE TABLE IF NOT EXISTS {table_1_path} (
    transaction_id STRING COMMENT 'Unique identifier for each sales transaction',
    customer_id STRING COMMENT 'Unique customer identifier for linking to customer records',
    product_name STRING COMMENT 'Name of the product purchased in the transaction',
    product_category STRING COMMENT 'Category classification of the product (Electronics, Clothing, etc.)',
    order_date STRING COMMENT 'Date when the order was placed by the customer',
    quantity INT COMMENT 'Number of units purchased in this transaction',
    unit_price DOUBLE COMMENT 'Price per unit of the product in USD',
    total_amount DOUBLE COMMENT 'Total transaction amount (quantity × unit_price) in USD',
    region STRING COMMENT 'Geographic region where the sale occurred',
    sales_channel STRING COMMENT 'Channel through which the sale was made (Online, In-Store, etc.)',
    payment_method STRING COMMENT 'Payment method used by customer for the transaction',
    created_by STRING COMMENT 'Team or user who created this record',
    created_at STRING COMMENT 'Timestamp when this record was created in the system'
)
USING DELTA
COMMENT 'Sales transaction data containing all customer purchases across multiple channels and regions. Owner: Sales Team | Tags: Sales, Revenue, Transactions'
"""

execute_sql(create_table1_sql, f"Creating table: {table_1_path}")

# Create Table 2 with schema and descriptions
table_2_path = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_2}"

create_table2_sql = f"""
CREATE TABLE IF NOT EXISTS {table_2_path} (
    feedback_id STRING COMMENT 'Unique identifier for each customer feedback entry',
    customer_id STRING COMMENT 'Customer identifier linking to the customer who provided feedback',
    transaction_id STRING COMMENT 'Transaction identifier linking feedback to specific purchase',
    rating INT COMMENT 'Customer satisfaction rating on a scale of 1-5 stars',
    feedback_text STRING COMMENT 'Detailed customer feedback comments and suggestions',
    feedback_category STRING COMMENT 'Category of feedback (Product Quality, Delivery, Service, etc.)',
    sentiment STRING COMMENT 'Sentiment analysis classification (Positive, Neutral, Negative)',
    submitted_date STRING COMMENT 'Date when the customer submitted the feedback',
    response_status STRING COMMENT 'Current status of feedback response (Pending, Resolved, etc.)',
    assigned_to STRING COMMENT 'Team assigned to handle and respond to this feedback',
    created_by STRING COMMENT 'Team or user who ingested this feedback record',
    created_at STRING COMMENT 'Timestamp when this feedback record was created in the system'
)
USING DELTA
COMMENT 'Customer feedback and satisfaction data linked to transactions for quality monitoring and improvement. Owner: Customer Experience Team | Tags: Feedback, Customer Satisfaction, Quality'
"""

execute_sql(create_table2_sql, f"Creating table: {table_2_path}")

# ============================================
# STEP 8: Insert Data
# ============================================
print("\n📥 Inserting data into tables...")

# Function to create INSERT statements
def create_insert_sql(df, table_path, batch_size=50):
    inserts = []
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        values = []
        for _, row in batch.iterrows():
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append('NULL')
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped_val = val.replace("'", "''")
                    row_values.append(f"'{escaped_val}'")
                else:
                    row_values.append(str(val))
            values.append(f"({', '.join(row_values)})")
        
        columns = ', '.join(df.columns)
        values_str = ',\n'.join(values)
        inserts.append(f"INSERT INTO {table_path} ({columns}) VALUES\n{values_str}")
    
    return inserts

# Insert sales data
print(f"\n📥 Inserting data into {TABLE_1}...")
sales_inserts = create_insert_sql(df_sales, table_1_path, batch_size=50)
for i, insert_sql in enumerate(sales_inserts):
    execute_sql(insert_sql, f"Inserting batch {i+1}/{len(sales_inserts)}")

# Insert feedback data
print(f"\n📥 Inserting data into {TABLE_2}...")
feedback_inserts = create_insert_sql(df_feedback, table_2_path, batch_size=50)
for i, insert_sql in enumerate(feedback_inserts):
    execute_sql(insert_sql, f"Inserting batch {i+1}/{len(feedback_inserts)}")

# ============================================
# STEP 9: Create Metadata Table
# ============================================
print("\n🔗 Creating metadata table for Knowledge Graph...")

metadata_table_path = f"{CATALOG_NAME}.{SCHEMA_NAME}.metadata_catalog"

create_metadata_sql = f"""
CREATE TABLE IF NOT EXISTS {metadata_table_path} (
    table_catalog STRING COMMENT 'Catalog name where the table resides',
    table_schema STRING COMMENT 'Schema name where the table resides',
    table_name STRING COMMENT 'Name of the data table',
    column_name STRING COMMENT 'Name of the column',
    column_description STRING COMMENT 'Detailed description of the column',
    data_type STRING COMMENT 'Data type of the column',
    owner_team STRING COMMENT 'Team that owns this data asset',
    tags STRING COMMENT 'Comma-separated tags for categorization',
    sensitivity_level STRING COMMENT 'Data sensitivity classification (Low, Medium, High)',
    created_at STRING COMMENT 'Timestamp when metadata was captured'
)
USING DELTA
COMMENT 'Metadata catalog containing all column descriptions and ownership information for Knowledge Graph'
"""

execute_sql(create_metadata_sql, f"Creating metadata table: {metadata_table_path}")

# Create metadata records
metadata_records = []

# Column descriptions for Table 1
sales_columns_desc = {
    'transaction_id': ('Unique identifier for each sales transaction', 'STRING', 'Low'),
    'customer_id': ('Unique customer identifier for linking to customer records', 'STRING', 'Medium'),
    'product_name': ('Name of the product purchased in the transaction', 'STRING', 'Low'),
    'product_category': ('Category classification of the product (Electronics, Clothing, etc.)', 'STRING', 'Low'),
    'order_date': ('Date when the order was placed by the customer', 'STRING', 'Low'),
    'quantity': ('Number of units purchased in this transaction', 'INT', 'Low'),
    'unit_price': ('Price per unit of the product in USD', 'DOUBLE', 'Low'),
    'total_amount': ('Total transaction amount (quantity × unit_price) in USD', 'DOUBLE', 'Medium'),
    'region': ('Geographic region where the sale occurred', 'STRING', 'Low'),
    'sales_channel': ('Channel through which the sale was made (Online, In-Store, etc.)', 'STRING', 'Low'),
    'payment_method': ('Payment method used by customer for the transaction', 'STRING', 'Medium'),
    'created_by': ('Team or user who created this record', 'STRING', 'Low'),
    'created_at': ('Timestamp when this record was created in the system', 'STRING', 'Low')
}

# Column descriptions for Table 2
feedback_columns_desc = {
    'feedback_id': ('Unique identifier for each customer feedback entry', 'STRING', 'Low'),
    'customer_id': ('Customer identifier linking to the customer who provided feedback', 'STRING', 'High'),
    'transaction_id': ('Transaction identifier linking feedback to specific purchase', 'STRING', 'Medium'),
    'rating': ('Customer satisfaction rating on a scale of 1-5 stars', 'INT', 'Low'),
    'feedback_text': ('Detailed customer feedback comments and suggestions', 'STRING', 'Medium'),
    'feedback_category': ('Category of feedback (Product Quality, Delivery, Service, etc.)', 'STRING', 'Low'),
    'sentiment': ('Sentiment analysis classification (Positive, Neutral, Negative)', 'STRING', 'Low'),
    'submitted_date': ('Date when the customer submitted the feedback', 'STRING', 'Low'),
    'response_status': ('Current status of feedback response (Pending, Resolved, etc.)', 'STRING', 'Low'),
    'assigned_to': ('Team assigned to handle and respond to this feedback', 'STRING', 'Low'),
    'created_by': ('Team or user who ingested this feedback record', 'STRING', 'Low'),
    'created_at': ('Timestamp when this feedback record was created in the system', 'STRING', 'Low')
}

# Add metadata for Table 1
for column, (description, dtype, sensitivity) in sales_columns_desc.items():
    metadata_records.append(
        f"('{CATALOG_NAME}', '{SCHEMA_NAME}', '{TABLE_1}', '{column}', '{description}', "
        f"'{dtype}', 'sales_team', 'Sales,Revenue,Transactions', '{sensitivity}', "
        f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')"
    )

# Add metadata for Table 2
for column, (description, dtype, sensitivity) in feedback_columns_desc.items():
    metadata_records.append(
        f"('{CATALOG_NAME}', '{SCHEMA_NAME}', '{TABLE_2}', '{column}', '{description}', "
        f"'{dtype}', 'customer_experience_team', 'Feedback,CustomerSatisfaction,Quality', '{sensitivity}', "
        f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')"
    )

# Insert metadata in batches
print(f"\n📥 Inserting {len(metadata_records)} metadata records...")
batch_size = 25
for i in range(0, len(metadata_records), batch_size):
    batch = metadata_records[i:i+batch_size]
    insert_metadata_sql = f"""
    INSERT INTO {metadata_table_path} VALUES
    {', '.join(batch)}
    """
    execute_sql(insert_metadata_sql, f"Inserting metadata batch {i//batch_size + 1}")

# ============================================
# STEP 10: Verify Results
# ============================================
print("\n" + "="*60)
print("📊 VERIFICATION")
print("="*60)

# Count records in each table
count_queries = [
    (f"SELECT COUNT(*) as count FROM {table_1_path}", TABLE_1),
    (f"SELECT COUNT(*) as count FROM {table_2_path}", TABLE_2),
    (f"SELECT COUNT(*) as count FROM {metadata_table_path}", "metadata_catalog")
]

for query, table_name in count_queries:
    print(f"\n📊 Counting records in {table_name}...")
    result = execute_sql(query, f"Counting {table_name}")
    if result and result.result:
        print(f"   Columns: {result.result.data_array}")

# Show sample data
print(f"\n📋 Sample data from {TABLE_1}:")
execute_sql(f"SELECT * FROM {table_1_path} LIMIT 3", f"Fetching sample from {TABLE_1}")

print(f"\n📋 Sample data from {TABLE_2}:")
execute_sql(f"SELECT * FROM {table_2_path} LIMIT 3", f"Fetching sample from {TABLE_2}")

# Show table descriptions
print(f"\n📝 Table schema for {TABLE_1}:")
execute_sql(f"DESCRIBE TABLE EXTENDED {table_1_path}", f"Describing {TABLE_1}")

# ============================================
# FINAL OUTPUT
# ============================================
print("\n" + "="*60)
print("✨ DATA INGESTION COMPLETE!")
print("="*60)
print(f"\n📦 Tables Created:")
print(f"   1. {table_1_path} (150 rows)")
print(f"   2. {table_2_path} (100 rows)")
print(f"   3. {metadata_table_path} (25 metadata records)")
print(f"\n✅ All columns have detailed descriptions")
print(f"✅ Metadata catalog ready for Knowledge Graph ingestion")
print(f"\n🔗 Connection Details:")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schema: {SCHEMA_NAME}")
print(f"   Warehouse: {warehouse_id}")
print("\n🎯 Next Steps:")
print("   - Extract metadata from metadata_catalog table")
print("   - Build Neo4j Knowledge Graph nodes and relationships")
print("   - Implement Graph RAG for natural language queries")