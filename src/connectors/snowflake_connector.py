import snowflake.connector
from snowflake.connector import DictCursor
import os
from dotenv import load_dotenv
import logging

load_dotenv()

class SnowflakeConnector:
    def __init__(self):
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self):
        """Establish connection to Snowflake using PAT as password"""
        try:
            connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),  # PAT token
                'role': os.getenv('SNOWFLAKE_ROLE', 'TRAINING_ROLE'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'ANIMAL_TASK_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'TRAINING_DB'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'WEATHER')
            }
            
            self.logger.info(f"Connecting to Snowflake...")
            self.logger.info(f"  Account: {connection_params['account']}")
            self.logger.info(f"  User: {connection_params['user']}")
            self.logger.info(f"  Role: {connection_params['role']}")
            self.logger.info(f"  Database: {connection_params['database']}")
            
            self.connection = snowflake.connector.connect(**connection_params)
            self.logger.info("✅ Connected to Snowflake successfully!")
            
            return self.connection
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def get_databases(self):
        """Get all databases"""
        cursor = self.connection.cursor(DictCursor)
        cursor.execute("SHOW DATABASES")
        return cursor.fetchall()
    
    def get_schemas(self, database):
        """Fetch all schemas in database"""
        cursor = self.connection.cursor(DictCursor)
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return cursor.fetchall()
    
    def get_tables(self, database, schema):
        """Fetch all tables in schema"""
        cursor = self.connection.cursor(DictCursor)
        cursor.execute(f"SHOW TABLES IN {database}.{schema}")
        return cursor.fetchall()
    
    def get_table_metadata(self, database, schema, table):
        """Get detailed table information"""
        cursor = self.connection.cursor(DictCursor)
        
        # Get columns
        cursor.execute(f"SHOW COLUMNS IN {database}.{schema}.{table}")
        columns = cursor.fetchall()
        
        # Get row count
        try:
            cursor.execute(f"SELECT COUNT(*) as row_count FROM {database}.{schema}.{table}")
            row_count = cursor.fetchone()['ROW_COUNT']
        except Exception as e:
            self.logger.warning(f"Could not get row count for {table}: {e}")
            row_count = -1
        
        # Get sample data
        try:
            cursor.execute(f"SELECT * FROM {database}.{schema}.{table} LIMIT 100")
            sample_data = cursor.fetchall()
        except Exception as e:
            self.logger.warning(f"Could not get sample data for {table}: {e}")
            sample_data = []
        
        return {
            'columns': columns,
            'row_count': row_count,
            'sample_data': sample_data
        }
    
    def get_table_lineage(self, database, schema, table):
        """Get table dependencies - simplified for training account"""
        # Training accounts might not have access to account_usage schema
        return []
    
    def get_access_history(self, database, schema, table):
        """Get access history - simplified for training account"""
        # Training accounts might not have access to account_usage schema
        return []
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from Snowflake")