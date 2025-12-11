from connectors.snowflake_connector import SnowflakeConnector
import hashlib
import json
import logging

class SnowflakeMetadataExtractor:
    def __init__(self):
        self.connector = SnowflakeConnector()
        self.metadata_catalog = {}
        self.logger = logging.getLogger(__name__)
        
    def extract_all_metadata(self):
        """Main extraction pipeline"""
        self.connector.connect()
        
        try:
            # Get all databases
            databases = self.connector.get_databases()
            
            # Filter to only databases we have access to
            accessible_databases = ['TRAINING_DB', 'SNOWFLAKE_SAMPLE_DATA']
            
            for db in databases:
                db_name = db['name']
                
                # Skip system databases and ones we can't access
                if db_name.startswith('SNOWFLAKE') and db_name != 'SNOWFLAKE_SAMPLE_DATA':
                    self.logger.info(f"Skipping system database: {db_name}")
                    continue
                    
                if db_name not in accessible_databases:
                    self.logger.info(f"Skipping restricted database: {db_name}")
                    continue
                
                self.logger.info(f"Processing database: {db_name}")
                self.metadata_catalog[db_name] = {}
                
                try:
                    # Get schemas in database
                    schemas = self.connector.get_schemas(db_name)
                    
                    for schema in schemas:
                        schema_name = schema['name']
                        
                        # Skip system schemas
                        if schema_name in ['INFORMATION_SCHEMA', 'PUBLIC']:
                            continue
                        
                        self.logger.info(f"  Processing schema: {db_name}.{schema_name}")
                        self.metadata_catalog[db_name][schema_name] = {}
                        
                        try:
                            # Get tables in schema
                            tables = self.connector.get_tables(db_name, schema_name)
                            
                            for table in tables:
                                table_name = table['name']
                                self.logger.info(f"    Extracting metadata for: {table_name}")
                                
                                # Extract comprehensive metadata
                                table_metadata = self.extract_table_metadata(
                                    db_name, schema_name, table_name
                                )
                                
                                self.metadata_catalog[db_name][schema_name][table_name] = table_metadata
                        
                        except Exception as e:
                            self.logger.warning(f"    Could not access tables in {db_name}.{schema_name}: {e}")
                            continue
                            
                except Exception as e:
                    self.logger.warning(f"  Could not access schemas in {db_name}: {e}")
                    continue
            
            return self.metadata_catalog
            
        finally:
            self.connector.close()
    
    def extract_table_metadata(self, database, schema, table):
        """Extract detailed metadata for a single table"""
        try:
            # Get basic metadata
            metadata = self.connector.get_table_metadata(database, schema, table)
            
            # Create table fingerprint for duplicate detection
            fingerprint = self.create_table_fingerprint(metadata)
            
            # Get lineage information (might fail for training account)
            try:
                lineage = self.connector.get_table_lineage(database, schema, table)
            except:
                lineage = []
            
            # Get access patterns (might fail for training account)
            try:
                access_history = self.connector.get_access_history(database, schema, table)
            except:
                access_history = []
            
            return {
                'full_name': f"{database}.{schema}.{table}",
                'database': database,
                'schema': schema,
                'table': table,
                'columns': metadata['columns'],
                'row_count': metadata['row_count'],
                'fingerprint': fingerprint,
                'lineage': lineage,
                'access_history': access_history,
                'profile': self.profile_data(metadata['sample_data'])
            }
        except Exception as e:
            self.logger.error(f"Error extracting metadata for {database}.{schema}.{table}: {e}")
            return None
    
    def create_table_fingerprint(self, metadata):
        """Create a unique fingerprint for duplicate detection"""
        
        # Create signature based on column names and types
        column_signature = []
        for col in metadata['columns']:
            column_name = col.get('column_name', col.get('COLUMN_NAME', ''))
            data_type = col.get('data_type', col.get('DATA_TYPE', ''))
            column_signature.append(f"{column_name}:{data_type}")
        
        signature_string = '|'.join(sorted(column_signature))
        
        # Create hash
        fingerprint = hashlib.md5(signature_string.encode()).hexdigest()
        
        return {
            'column_signature': fingerprint,
            'column_count': len(metadata['columns']),
            'row_count': metadata['row_count']
        }
    
    def profile_data(self, sample_data):
        """Basic profiling of data patterns"""
        if not sample_data:
            return {}
        
        return {
            'sample_size': len(sample_data),
            'has_data': len(sample_data) > 0
        }