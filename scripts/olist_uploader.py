import pandas as pd
import snowflake.connector
from pathlib import Path
import logging
from typing import Dict, List
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OlistSnowflakeUploader:
    """
    Robust Olist dataset uploader to Snowflake with error handling and retry logic.
    Handles case sensitivity, large datasets, and creates test duplicates.
    """
    
    def __init__(self, config: Dict[str, str], data_folder: str):
        """
        Initialize uploader with Snowflake credentials and data folder path.
        
        Args:
            config: Dict with keys: user, password, account, warehouse, database, role
            data_folder: Path to folder containing Olist CSV files
        """
        self.config = config
        self.data_folder = Path(data_folder)
        self.conn = None
        self.cursor = None
        
        # Define table schemas with proper data types
        self.table_schemas = {
            'CUSTOMERS': {
                'customer_id': 'VARCHAR(100)',
                'customer_unique_id': 'VARCHAR(100)',
                'customer_zip_code_prefix': 'VARCHAR(10)',
                'customer_city': 'VARCHAR(100)',
                'customer_state': 'VARCHAR(2)'
            },
            'ORDERS': {
                'order_id': 'VARCHAR(100)',
                'customer_id': 'VARCHAR(100)',
                'order_status': 'VARCHAR(50)',
                'order_purchase_timestamp': 'TIMESTAMP_NTZ',
                'order_approved_at': 'TIMESTAMP_NTZ',
                'order_delivered_carrier_date': 'TIMESTAMP_NTZ',
                'order_delivered_customer_date': 'TIMESTAMP_NTZ',
                'order_estimated_delivery_date': 'TIMESTAMP_NTZ'
            },
            'ORDER_ITEMS': {
                'order_id': 'VARCHAR(100)',
                'order_item_id': 'INTEGER',
                'product_id': 'VARCHAR(100)',
                'seller_id': 'VARCHAR(100)',
                'shipping_limit_date': 'TIMESTAMP_NTZ',
                'price': 'FLOAT',
                'freight_value': 'FLOAT'
            },
            'PRODUCTS': {
                'product_id': 'VARCHAR(100)',
                'product_category_name': 'VARCHAR(100)',
                'product_name_lenght': 'INTEGER',
                'product_description_lenght': 'INTEGER',
                'product_photos_qty': 'INTEGER',
                'product_weight_g': 'INTEGER',
                'product_length_cm': 'INTEGER',
                'product_height_cm': 'INTEGER',
                'product_width_cm': 'INTEGER'
            },
            'SELLERS': {
                'seller_id': 'VARCHAR(100)',
                'seller_zip_code_prefix': 'VARCHAR(10)',
                'seller_city': 'VARCHAR(100)',
                'seller_state': 'VARCHAR(2)'
            },
            'GEOLOCATION': {
                'geolocation_zip_code_prefix': 'VARCHAR(10)',
                'geolocation_lat': 'FLOAT',
                'geolocation_lng': 'FLOAT',
                'geolocation_city': 'VARCHAR(100)',
                'geolocation_state': 'VARCHAR(2)'
            },
            'ORDER_PAYMENTS': {
                'order_id': 'VARCHAR(100)',
                'payment_sequential': 'INTEGER',
                'payment_type': 'VARCHAR(50)',
                'payment_installments': 'INTEGER',
                'payment_value': 'FLOAT'
            },
            'ORDER_REVIEWS': {
                'review_id': 'VARCHAR(100)',
                'order_id': 'VARCHAR(100)',
                'review_score': 'INTEGER',
                'review_comment_title': 'VARCHAR(500)',
                'review_comment_message': 'VARCHAR(5000)',
                'review_creation_date': 'TIMESTAMP_NTZ',
                'review_answer_timestamp': 'TIMESTAMP_NTZ'
            }
        }
        
        # CSV filename mapping
        self.csv_files = {
            'CUSTOMERS': 'olist_customers_dataset.csv',
            'ORDERS': 'olist_orders_dataset.csv',
            'ORDER_ITEMS': 'olist_order_items_dataset.csv',
            'PRODUCTS': 'olist_products_dataset.csv',
            'SELLERS': 'olist_sellers_dataset.csv',
            'GEOLOCATION': 'olist_geolocation_dataset.csv',
            'ORDER_PAYMENTS': 'olist_order_payments_dataset.csv',
            'ORDER_REVIEWS': 'olist_order_reviews_dataset.csv'
        }
    
    def connect(self):
        """Establish Snowflake connection with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to Snowflake (attempt {attempt + 1}/{max_retries})...")
                
                # Build connection parameters with SSL certificate handling
                conn_params = {
                    'user': self.config['user'],
                    'password': self.config['password'],
                    'account': self.config['account'],
                    'warehouse': self.config.get('warehouse', 'COMPUTE_WH'),
                    'database': self.config['database'],
                    'role': self.config.get('role', 'ACCOUNTADMIN'),
                    'insecure_mode': False,  # Keep SSL verification on by default
                }
                
                # Add optional schema if provided
                if 'schema' in self.config:
                    conn_params['schema'] = self.config['schema']
                
                self.conn = snowflake.connector.connect(**conn_params)
                self.cursor = self.conn.cursor()
                logger.info("✓ Connected to Snowflake successfully")
                
                # Set session parameters for better performance
                self.cursor.execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
                self.cursor.execute("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
                
                return True
            except snowflake.connector.errors.OperationalError as e:
                error_msg = str(e)
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                
                # Provide helpful error messages
                if "could not be validated" in error_msg.lower() or "certificate" in error_msg.lower():
                    logger.error("💡 SSL Certificate issue detected. Your options:")
                    logger.error("   1. Check if you're on VPN/corporate network")
                    logger.error("   2. Update Python's certifi package: pip install --upgrade certifi")
                    logger.error("   3. Add 'insecure_mode': True to config (NOT recommended for production)")
                elif "your_account" in error_msg:
                    logger.error("💡 ERROR: You're using placeholder 'your_account' in config!")
                    logger.error("   Fix: Replace with your actual Snowflake account identifier")
                    logger.error("   Example: 'ab12345.us-east-1' or 'myorg-myaccount'")
                elif "incorrect username or password" in error_msg.lower():
                    logger.error("💡 Authentication failed - check username/password")
                
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
        return False
    
    def disconnect(self):
        """Close Snowflake connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from Snowflake")
    
    def setup_schemas(self):
        """Create database schemas for Olist data."""
        schemas = ['OLIST_SALES', 'OLIST_MARKETING', 'OLIST_ANALYTICS']
        
        logger.info("\n🔧 Setting up schemas...")
        for schema in schemas:
            try:
                self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                logger.info(f"  ✓ {schema}")
            except Exception as e:
                logger.error(f"  ✗ Failed to create schema {schema}: {e}")
                raise
    
    def create_table(self, schema: str, table_name: str):
        """Create table with proper schema definition."""
        if table_name not in self.table_schemas:
            raise ValueError(f"No schema definition for table: {table_name}")
        
        columns = self.table_schemas[table_name]
        column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in columns.items()])
        
        create_sql = f"""
        CREATE OR REPLACE TABLE {schema}.{table_name} (
            {column_defs}
        )
        """
        
        try:
            self.cursor.execute(create_sql)
            logger.info(f"  ✓ Created table {schema}.{table_name}")
        except Exception as e:
            logger.error(f"  ✗ Failed to create table {schema}.{table_name}: {e}")
            raise
    
    def load_csv_to_table(self, schema: str, table_name: str, csv_file: str, chunk_size: int = 50000):
        """
        Load CSV data into Snowflake table in chunks with proper type handling.
        
        Args:
            schema: Target schema name
            table_name: Target table name
            csv_file: CSV filename
            chunk_size: Number of rows per batch insert
        """
        csv_path = self.data_folder / csv_file
        
        if not csv_path.exists():
            logger.warning(f"  ⚠ CSV file not found: {csv_path}")
            return False
        
        logger.info(f"\n📥 Loading {table_name} from {csv_file}")
        
        try:
            # Read CSV with proper handling
            df = pd.read_csv(csv_path, low_memory=False)
            total_rows = len(df)
            logger.info(f"  Read {total_rows:,} rows from CSV")
            
            # Get column names from schema definition
            schema_columns = list(self.table_schemas[table_name].keys())
            csv_columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            # Rename DataFrame columns to match schema
            df.columns = schema_columns[:len(df.columns)]
            
            # Handle timestamp columns - convert to string format for Snowflake
            for col, dtype in self.table_schemas[table_name].items():
                if col in df.columns:
                    if 'TIMESTAMP' in dtype:
                        # Convert to datetime then to string format Snowflake expects
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        # Convert to string, replacing NaT with None
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').where(df[col].notna(), None)
                    elif dtype == 'INTEGER':
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    elif dtype == 'FLOAT':
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Replace NaN with None for proper NULL handling (but skip timestamp columns already handled)
            for col in df.columns:
                if self.table_schemas[table_name].get(col, '') not in ['TIMESTAMP_NTZ', 'TIMESTAMP']:
                    df[col] = df[col].where(pd.notnull(df[col]), None)
            
            # Insert in chunks
            rows_inserted = 0
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                
                # Build INSERT statement with quoted column names
                columns_str = ", ".join([f'"{col}"' for col in chunk.columns])
                placeholders = ", ".join(["%s"] * len(chunk.columns))
                insert_sql = f'INSERT INTO {schema}.{table_name} ({columns_str}) VALUES ({placeholders})'
                
                # Convert to list of tuples
                data = [tuple(row) for row in chunk.values]
                
                # Execute batch insert
                self.cursor.executemany(insert_sql, data)
                rows_inserted += len(chunk)
                
                if rows_inserted % 100000 == 0 or rows_inserted == total_rows:
                    logger.info(f"    Inserted {rows_inserted:,} / {total_rows:,} rows")
            
            logger.info(f"  ✓ Loaded {rows_inserted:,} rows into {schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"  ✗ Failed to load {table_name}: {e}")
            raise
    
    def create_exact_duplicates(self):
        """Create exact duplicate tables in OLIST_MARKETING schema."""
        logger.info("\n🔄 Creating exact duplicates in OLIST_MARKETING...")
        
        duplicate_mappings = {
            'CUSTOMERS': 'CLIENT_DATA',
            'ORDERS': 'SALES_ORDERS',
            'PRODUCTS': 'PRODUCT_CATALOG'
        }
        
        for source_table, target_table in duplicate_mappings.items():
            try:
                sql = f"""
                CREATE OR REPLACE TABLE OLIST_MARKETING.{target_table} AS 
                SELECT * FROM OLIST_SALES.{source_table}
                """
                self.cursor.execute(sql)
                
                # Verify row count
                self.cursor.execute(f"SELECT COUNT(*) FROM OLIST_MARKETING.{target_table}")
                count = self.cursor.fetchone()[0]
                logger.info(f"  ✓ {target_table} ({count:,} rows)")
                
            except Exception as e:
                logger.error(f"  ✗ Failed to create {target_table}: {e}")
                # Don't raise - continue with other duplicates
    
    def create_renamed_duplicates(self):
        """Create renamed duplicate tables in OLIST_ANALYTICS schema."""
        logger.info("\n🔄 Creating renamed duplicates in OLIST_ANALYTICS...")
        
        try:
            # CUSTOMER_MASTER with renamed columns
            sql = """
            CREATE OR REPLACE TABLE OLIST_ANALYTICS.CUSTOMER_MASTER AS 
            SELECT 
                "customer_id" as cust_id,
                "customer_unique_id" as unique_customer_identifier,
                "customer_zip_code_prefix" as zip_code,
                "customer_city" as city_name,
                "customer_state" as state_code
            FROM OLIST_SALES.CUSTOMERS
            """
            self.cursor.execute(sql)
            self.cursor.execute("SELECT COUNT(*) FROM OLIST_ANALYTICS.CUSTOMER_MASTER")
            count = self.cursor.fetchone()[0]
            logger.info(f"  ✓ CUSTOMER_MASTER ({count:,} rows)")
            
        except Exception as e:
            logger.error(f"  ✗ Failed to create CUSTOMER_MASTER: {e}")
        
        try:
            # PURCHASE_HISTORY with renamed columns
            sql = """
            CREATE OR REPLACE TABLE OLIST_ANALYTICS.PURCHASE_HISTORY AS 
            SELECT 
                "order_id" as purchase_id,
                "customer_id" as cust_id,
                "order_status" as purchase_status,
                "order_purchase_timestamp" as purchase_date,
                "order_delivered_customer_date" as delivery_date
            FROM OLIST_SALES.ORDERS
            """
            self.cursor.execute(sql)
            self.cursor.execute("SELECT COUNT(*) FROM OLIST_ANALYTICS.PURCHASE_HISTORY")
            count = self.cursor.fetchone()[0]
            logger.info(f"  ✓ PURCHASE_HISTORY ({count:,} rows)")
            
        except Exception as e:
            logger.error(f"  ✗ Failed to create PURCHASE_HISTORY: {e}")
    
    def verify_upload(self):
        """Verify all tables were created and contain data."""
        logger.info("\n✅ Verifying upload...")
        
        schemas_to_check = {
            'OLIST_SALES': list(self.table_schemas.keys()),
            'OLIST_MARKETING': ['CLIENT_DATA', 'SALES_ORDERS', 'PRODUCT_CATALOG'],
            'OLIST_ANALYTICS': ['CUSTOMER_MASTER', 'PURCHASE_HISTORY']
        }
        
        total_tables = 0
        successful_tables = 0
        
        for schema, tables in schemas_to_check.items():
            logger.info(f"\n{schema}:")
            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    count = self.cursor.fetchone()[0]
                    logger.info(f"  ✓ {table}: {count:,} rows")
                    total_tables += 1
                    if count > 0:
                        successful_tables += 1
                except Exception as e:
                    logger.warning(f"  ✗ {table}: {e}")
                    total_tables += 1
        
        logger.info(f"\n📊 Summary: {successful_tables}/{total_tables} tables loaded successfully")
        return successful_tables == total_tables
    
    def run_complete_upload(self, create_duplicates: bool = True):
        """
        Run the complete upload process.
        
        Args:
            create_duplicates: Whether to create duplicate tables for testing
        """
        start_time = time.time()
        
        try:
            # Connect to Snowflake
            self.connect()
            
            # Setup schemas
            self.setup_schemas()
            
            # Load all base tables into OLIST_SALES
            logger.info("\n📦 Loading base tables into OLIST_SALES...")
            for table_name, csv_file in self.csv_files.items():
                self.create_table('OLIST_SALES', table_name)
                self.load_csv_to_table('OLIST_SALES', table_name, csv_file)
            
            # Create duplicate tables if requested
            if create_duplicates:
                self.create_exact_duplicates()
                self.create_renamed_duplicates()
            
            # Verify everything
            success = self.verify_upload()
            
            elapsed_time = time.time() - start_time
            logger.info(f"\n{'='*60}")
            if success:
                logger.info(f"✅ UPLOAD COMPLETE! Time taken: {elapsed_time:.1f} seconds")
            else:
                logger.warning(f"⚠️ UPLOAD COMPLETED WITH WARNINGS! Time taken: {elapsed_time:.1f} seconds")
            logger.info(f"{'='*60}")
            
            return success
            
        except Exception as e:
            logger.error(f"\n❌ Upload failed: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Main execution function - reads credentials from .env file."""
    
    # Load configuration from environment variables
    config = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
    }
    
    # Validate that all required environment variables are set
    missing_vars = [key for key, value in config.items() if not value]
    if missing_vars:
        logger.error("❌ ERROR: Missing environment variables in .env file:")
        for var in missing_vars:
            logger.error(f"   - SNOWFLAKE_{var.upper()}")
        logger.error("\nMake sure your .env file contains all required variables:")
        logger.error("   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,")
        logger.error("   SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE")
        return
    
    logger.info(f"✓ Loaded credentials from .env file")
    logger.info(f"  Account: {config['account']}")
    logger.info(f"  User: {config['user']}")
    logger.info(f"  Warehouse: {config['warehouse']}")
    logger.info(f"  Database: {config['database']}")
    logger.info(f"  Role: {config['role']}")
    
    # Path to your Olist CSV files folder
    # UPDATE THIS to your actual path
    data_folder = '/Users/pranav/Desktop/LLM KG/data-governance-kg/olist_data'
    
    if not Path(data_folder).exists():
        logger.error(f"❌ ERROR: Data folder not found: {data_folder}")
        logger.error("   Please update 'data_folder' path in main() function")
        logger.error("   to point to your Olist CSV files location")
        return
    
    logger.info(f"  Data folder: {data_folder}\n")
    
    # Create uploader instance
    uploader = OlistSnowflakeUploader(config, data_folder)
    
    # Run upload
    # Set create_duplicates=False if you just want base tables (faster, ~5 min)
    # Set create_duplicates=True to also create test duplicates (~7 min)
    try:
        uploader.run_complete_upload(create_duplicates=True)
    except snowflake.connector.errors.OperationalError as e:
        if "certificate" in str(e).lower():
            logger.error("\n" + "="*60)
            logger.error("SSL CERTIFICATE ERROR - Quick Fixes:")
            logger.error("="*60)
            logger.error("Option 1: Update your Python certificates")
            logger.error("  pip install --upgrade certifi")
            logger.error("")
            logger.error("Option 2: Disable SSL verification (NOT recommended)")
            logger.error("  Add 'insecure_mode': True to config in connect() method")
            logger.error("")
            logger.error("Option 3: Check network")
            logger.error("  - Disconnect from VPN if applicable")
            logger.error("  - Check corporate firewall settings")
            logger.error("="*60)
        raise


if __name__ == "__main__":
    main()