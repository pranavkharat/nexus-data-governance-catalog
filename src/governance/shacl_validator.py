# src/governance/shacl_validator.py

"""
SHACL-Inspired Data Governance Validator
Cypher-based validation for Neo4j knowledge graph

Addresses RQ3: What constraints optimize governance coverage vs curator overhead?

Now includes:
- Snowflake validation (Shapes 1-10)
- Federated/Databricks validation (Shapes 11-20)
- Cross-source consistency checks
"""

from neo4j import GraphDatabase
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime
from enum import Enum
import os
from dotenv import load_dotenv

load_dotenv()


class Severity(Enum):
    """Violation severity levels"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Violation:
    """Represents a single constraint violation"""
    shape_name: str
    node_id: str
    node_label: str
    message: str
    severity: Severity
    property_path: Optional[str] = None
    actual_value: Optional[str] = None
    expected_value: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'shape': self.shape_name,
            'node': self.node_id,
            'label': self.node_label,
            'message': self.message,
            'severity': self.severity.value,
            'property': self.property_path,
            'actual': self.actual_value,
            'expected': self.expected_value
        }


@dataclass
class ValidationReport:
    """Complete validation report"""
    timestamp: datetime
    total_nodes_checked: int
    shapes_evaluated: int
    violations: List[Violation] = field(default_factory=list)
    scope: str = "all"  # "all", "snowflake", "databricks", "federated"
    
    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0
    
    @property
    def critical_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == Severity.CRITICAL)
    
    @property
    def warning_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == Severity.WARNING)
    
    @property
    def info_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == Severity.INFO)
    
    def summary(self) -> Dict:
        return {
            'timestamp': self.timestamp.isoformat(),
            'scope': self.scope,
            'is_valid': self.is_valid,
            'nodes_checked': self.total_nodes_checked,
            'shapes_evaluated': self.shapes_evaluated,
            'total_violations': len(self.violations),
            'critical': self.critical_count,
            'warnings': self.warning_count,
            'info': self.info_count
        }


class SHACLValidator:
    """
    SHACL-Inspired Validator using Cypher
    
    Supports:
    - Snowflake-only validation (OlistData nodes)
    - Databricks-only validation (FederatedTable where source='databricks')
    - Federated validation (all FederatedTable nodes)
    - Cross-source consistency checks
    """
    
    def __init__(self):
        """Initialize Neo4j connection"""
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", neo4j_password)
        )
        
        with self.driver.session() as session:
            session.run("RETURN 1")
        
        print("✅ SHACL Validator connected to Neo4j")
        
        # Define all shapes
        self.snowflake_shapes = self._define_snowflake_shapes()
        self.federated_shapes = self._define_federated_shapes()
        self.shapes = self.snowflake_shapes + self.federated_shapes
    
    def _define_snowflake_shapes(self) -> List[Dict]:
        """Original Snowflake/OlistData shapes (1-10)"""
        return [
            {
                'name': 'LineageCompletenessShape',
                'description': 'Derived tables must have DERIVES_FROM lineage',
                'severity': Severity.CRITICAL,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t:OlistData)
                    WHERE t.schema IN ['OLIST_MARKETING', 'OLIST_ANALYTICS']
                    AND NOT EXISTS {
                        MATCH (t)-[:DERIVES_FROM]->(:OlistData)
                    }
                    RETURN t.schema + '.' + t.name AS node_id,
                           'OlistData' AS node_label,
                           'Derived table missing DERIVES_FROM lineage' AS message,
                           t.schema AS property_path,
                           'No lineage' AS actual_value,
                           'At least 1 DERIVES_FROM edge' AS expected_value
                """
            },
            {
                'name': 'SourceTableShape',
                'description': 'Source tables should not have upstream lineage',
                'severity': Severity.WARNING,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t:OlistData)
                    WHERE t.schema = 'OLIST_SALES'
                    AND EXISTS {
                        MATCH (t)-[:DERIVES_FROM]->(:OlistData)
                    }
                    RETURN t.schema + '.' + t.name AS node_id,
                           'OlistData' AS node_label,
                           'Source table has unexpected upstream lineage' AS message,
                           'DERIVES_FROM' AS property_path,
                           'Has upstream' AS actual_value,
                           'No upstream lineage' AS expected_value
                """
            },
            {
                'name': 'RowCountQualityShape',
                'description': 'Every table must have row_count > 0',
                'severity': Severity.CRITICAL,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t:OlistData)
                    WHERE t.row_count IS NULL OR t.row_count <= 0
                    RETURN t.schema + '.' + t.name AS node_id,
                           'OlistData' AS node_label,
                           'Table has invalid row count' AS message,
                           'row_count' AS property_path,
                           COALESCE(toString(t.row_count), 'NULL') AS actual_value,
                           '> 0' AS expected_value
                """
            },
            {
                'name': 'ColumnCompletenessShape',
                'description': 'Every table must have at least 1 column defined',
                'severity': Severity.CRITICAL,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t:OlistData)
                    WHERE NOT EXISTS {
                        MATCH (t)-[:HAS_COLUMN]->(:OlistColumn)
                    }
                    RETURN t.schema + '.' + t.name AS node_id,
                           'OlistData' AS node_label,
                           'Table has no column definitions' AS message,
                           'HAS_COLUMN' AS property_path,
                           '0 columns' AS actual_value,
                           '>= 1 column' AS expected_value
                """
            },
            {
                'name': 'DuplicateConsistencyShape',
                'description': 'Exact duplicate pairs should have matching row counts',
                'severity': Severity.WARNING,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData)
                    WHERE d.match_type = 'EXACT_SCHEMA'
                    AND t1.row_count <> t2.row_count
                    RETURN t1.schema + '.' + t1.name + ' <-> ' + t2.schema + '.' + t2.name AS node_id,
                           'OLIST_DUPLICATE' AS node_label,
                           'Exact duplicates have mismatched row counts' AS message,
                           'row_count' AS property_path,
                           toString(t1.row_count) + ' vs ' + toString(t2.row_count) AS actual_value,
                           'Equal row counts' AS expected_value
                """
            },
            {
                'name': 'DuplicateConfidenceShape',
                'description': 'Duplicate relationships should have confidence >= 0.5',
                'severity': Severity.INFO,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t1:OlistData)-[d:OLIST_DUPLICATE]->(t2:OlistData)
                    WHERE d.confidence < 0.5
                    RETURN t1.schema + '.' + t1.name + ' <-> ' + t2.schema + '.' + t2.name AS node_id,
                           'OLIST_DUPLICATE' AS node_label,
                           'Low confidence duplicate - may be false positive' AS message,
                           'confidence' AS property_path,
                           toString(d.confidence) AS actual_value,
                           '>= 0.5' AS expected_value
                """
            },
            {
                'name': 'LineageConfidenceShape',
                'description': 'Lineage relationships should have confidence >= 0.6',
                'severity': Severity.INFO,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t1:OlistData)-[r:DERIVES_FROM]->(t2:OlistData)
                    WHERE r.confidence < 0.6
                    RETURN t1.schema + '.' + t1.name + ' <- ' + t2.schema + '.' + t2.name AS node_id,
                           'DERIVES_FROM' AS node_label,
                           'Low confidence lineage - needs verification' AS message,
                           'confidence' AS property_path,
                           toString(r.confidence) AS actual_value,
                           '>= 0.6' AS expected_value
                """
            },
            {
                'name': 'SampleDataIntegrityShape',
                'description': 'Every Order must be linked to a Customer',
                'severity': Severity.CRITICAL,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (o:Order)
                    WHERE NOT EXISTS {
                        MATCH (c:Customer)-[:PLACED]->(o)
                    }
                    RETURN o.order_id AS node_id,
                           'Order' AS node_label,
                           'Order has no associated Customer' AS message,
                           'PLACED' AS property_path,
                           'No customer' AS actual_value,
                           'Exactly 1 customer' AS expected_value
                """
            },
            {
                'name': 'SchemaNameConventionShape',
                'description': 'Schema names should follow OLIST_* pattern',
                'severity': Severity.INFO,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t:OlistData)
                    WHERE NOT t.schema STARTS WITH 'OLIST_'
                    RETURN t.schema + '.' + t.name AS node_id,
                           'OlistData' AS node_label,
                           'Schema name does not follow OLIST_* convention' AS message,
                           'schema' AS property_path,
                           t.schema AS actual_value,
                           'OLIST_*' AS expected_value
                """
            },
            {
                'name': 'TableNameShape',
                'description': 'Table name must not be empty or null',
                'severity': Severity.CRITICAL,
                'scope': 'snowflake',
                'validation_query': """
                    MATCH (t:OlistData)
                    WHERE t.name IS NULL OR trim(t.name) = ''
                    RETURN coalesce(t.schema, 'UNKNOWN') + '.' + coalesce(t.name, 'NULL') AS node_id,
                           'OlistData' AS node_label,
                           'Table has empty or null name' AS message,
                           'name' AS property_path,
                           coalesce(t.name, 'NULL') AS actual_value,
                           'Non-empty string' AS expected_value
                """
            }
        ]
    
    def _define_federated_shapes(self) -> List[Dict]:
        """Federated/Databricks shapes (11-20)"""
        return [
            {
                'name': 'FederatedOwnershipShape',
                'description': 'All federated tables must have an owner assigned',
                'severity': Severity.CRITICAL,
                'scope': 'federated',
                'validation_query': """
                    MATCH (t:FederatedTable)
                    WHERE t.owner IS NULL OR trim(t.owner) = ''
                    RETURN t.full_name AS node_id,
                           'FederatedTable' AS node_label,
                           'Federated table missing owner assignment' AS message,
                           'owner' AS property_path,
                           coalesce(t.owner, 'NULL') AS actual_value,
                           'Non-empty owner string' AS expected_value
                """
            },
            {
                'name': 'DatabricksColumnCompletenessShape',
                'description': 'Databricks tables must have column definitions',
                'severity': Severity.CRITICAL,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks'
                    AND NOT EXISTS {
                        MATCH (t)-[:HAS_COLUMN]->(:FederatedColumn)
                    }
                    RETURN t.full_name AS node_id,
                           'FederatedTable' AS node_label,
                           'Databricks table has no column definitions' AS message,
                           'HAS_COLUMN' AS property_path,
                           '0 columns' AS actual_value,
                           '>= 1 column' AS expected_value
                """
            },
            {
                'name': 'SensitivityClassificationShape',
                'description': 'All Databricks columns must have sensitivity classification',
                'severity': Severity.WARNING,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn)
                    WHERE t.source = 'databricks'
                    AND (c.sensitivity IS NULL OR trim(c.sensitivity) = '')
                    RETURN t.full_name + '.' + c.name AS node_id,
                           'FederatedColumn' AS node_label,
                           'Column missing sensitivity classification' AS message,
                           'sensitivity' AS property_path,
                           coalesce(c.sensitivity, 'NULL') AS actual_value,
                           'Low/Medium/High/Critical' AS expected_value
                """
            },
            {
                'name': 'PIIDetectionShape',
                'description': 'Columns with PII indicators should be marked High/Critical',
                'severity': Severity.WARNING,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn)
                    WHERE t.source = 'databricks'
                    AND (
                        toLower(c.name) CONTAINS 'email' OR
                        toLower(c.name) CONTAINS 'phone' OR
                        toLower(c.name) CONTAINS 'ssn' OR
                        toLower(c.name) CONTAINS 'address' OR
                        toLower(c.name) CONTAINS 'credit_card' OR
                        toLower(c.name) CONTAINS 'password'
                    )
                    AND NOT c.sensitivity IN ['High', 'Critical', 'high', 'critical']
                    RETURN t.full_name + '.' + c.name AS node_id,
                           'FederatedColumn' AS node_label,
                           'Potential PII column not marked High/Critical sensitivity' AS message,
                           'sensitivity' AS property_path,
                           coalesce(c.sensitivity, 'NULL') AS actual_value,
                           'High or Critical' AS expected_value
                """
            },
            {
                'name': 'CrossSourceConfidenceShape',
                'description': 'Cross-source SIMILAR_TO matches below 0.25 need review',
                'severity': Severity.INFO,
                'scope': 'cross-source',
                'validation_query': """
                    MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                    WHERE r.score < 0.25
                    RETURN db.full_name + ' <-> ' + sf.schema + '.' + sf.name AS node_id,
                           'SIMILAR_TO' AS node_label,
                           'Very low confidence cross-source match' AS message,
                           'score' AS property_path,
                           toString(r.score) AS actual_value,
                           '>= 0.25' AS expected_value
                """
            },
            {
                'name': 'FederatedRowCountShape',
                'description': 'Databricks tables must have valid row counts',
                'severity': Severity.CRITICAL,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks'
                    AND (t.row_count IS NULL OR t.row_count <= 0)
                    RETURN t.full_name AS node_id,
                           'FederatedTable' AS node_label,
                           'Databricks table has invalid row count' AS message,
                           'row_count' AS property_path,
                           coalesce(toString(t.row_count), 'NULL') AS actual_value,
                           '> 0' AS expected_value
                """
            },
            {
                'name': 'DatabricksSourceLinkageShape',
                'description': 'Databricks tables must be linked to DataSource node',
                'severity': Severity.WARNING,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks'
                    AND NOT EXISTS {
                        MATCH (t)-[:FROM_SOURCE]->(:DataSource)
                    }
                    RETURN t.full_name AS node_id,
                           'FederatedTable' AS node_label,
                           'Databricks table not linked to DataSource node' AS message,
                           'FROM_SOURCE' AS property_path,
                           'No linkage' AS actual_value,
                           'FROM_SOURCE -> DataSource' AS expected_value
                """
            },
            {
                'name': 'ColumnDataTypeShape',
                'description': 'Column data types should be properly formatted',
                'severity': Severity.INFO,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)-[:HAS_COLUMN]->(c:FederatedColumn)
                    WHERE t.source = 'databricks'
                    AND c.data_type IS NOT NULL
                    AND NOT (
                        c.data_type STARTS WITH 'ColumnTypeName.' OR
                        c.data_type IN ['STRING', 'INT', 'LONG', 'DOUBLE', 'BOOLEAN', 'DATE', 'TIMESTAMP']
                    )
                    RETURN t.full_name + '.' + c.name AS node_id,
                           'FederatedColumn' AS node_label,
                           'Non-standard data type format' AS message,
                           'data_type' AS property_path,
                           c.data_type AS actual_value,
                           'ColumnTypeName.* or standard SQL type' AS expected_value
                """
            },
            {
                'name': 'FederatedLineageShape',
                'description': 'Databricks tables should have lineage when relationships exist',
                'severity': Severity.INFO,
                'scope': 'databricks',
                'validation_query': """
                    MATCH (t:FederatedTable)
                    WHERE t.source = 'databricks'
                    AND t.full_name CONTAINS 'feedback'
                    AND NOT EXISTS {
                        MATCH (t)-[:DERIVES_FROM]->(:FederatedTable)
                    }
                    RETURN t.full_name AS node_id,
                           'FederatedTable' AS node_label,
                           'Feedback table may derive from transactions - lineage not captured' AS message,
                           'DERIVES_FROM' AS property_path,
                           'No lineage' AS actual_value,
                           'DERIVES_FROM relationship if applicable' AS expected_value
                """
            },
            {
                'name': 'CrossPlatformConsistencyShape',
                'description': 'High-confidence matches should have similar row counts',
                'severity': Severity.WARNING,
                'scope': 'cross-source',
                'validation_query': """
                    MATCH (db:FederatedTable)-[r:SIMILAR_TO]->(sf:OlistData)
                    WHERE r.score >= 0.35
                    AND db.row_count IS NOT NULL AND sf.row_count IS NOT NULL
                    AND abs(toFloat(db.row_count) - toFloat(sf.row_count)) > (toFloat(sf.row_count) * 0.5)
                    RETURN db.full_name + ' <-> ' + sf.schema + '.' + sf.name AS node_id,
                           'SIMILAR_TO' AS node_label,
                           'High-confidence match has >50% row count difference' AS message,
                           'row_count' AS property_path,
                           toString(db.row_count) + ' vs ' + toString(sf.row_count) AS actual_value,
                           'Within 50% of each other' AS expected_value
                """
            }
        ]
    
    def validate_all(self) -> ValidationReport:
        """Run ALL validation shapes (Snowflake + Federated)"""
        return self._run_validation(self.shapes, scope="all")
    
    def validate_snowflake(self) -> ValidationReport:
        """Run only Snowflake/OlistData shapes"""
        return self._run_validation(self.snowflake_shapes, scope="snowflake")
    
    def validate_databricks(self) -> ValidationReport:
        """Run only Databricks-specific shapes"""
        databricks_shapes = [s for s in self.federated_shapes 
                           if s.get('scope') in ['databricks', 'cross-source']]
        return self._run_validation(databricks_shapes, scope="databricks")
    
    def validate_federated(self) -> ValidationReport:
        """Run all federated shapes (includes Snowflake FederatedTable + Databricks)"""
        return self._run_validation(self.federated_shapes, scope="federated")
    
    def validate_cross_source(self) -> ValidationReport:
        """Run only cross-source consistency shapes"""
        cross_shapes = [s for s in self.federated_shapes 
                       if s.get('scope') == 'cross-source']
        return self._run_validation(cross_shapes, scope="cross-source")
    
    def _run_validation(self, shapes: List[Dict], scope: str) -> ValidationReport:
        """Execute validation for given shapes"""
        print(f"\n{'='*70}")
        print(f"🔍 SHACL VALIDATION - Scope: {scope.upper()}")
        print(f"{'='*70}")
        
        violations = []
        nodes_checked = 0
        
        with self.driver.session() as session:
            # Count nodes based on scope
            if scope in ["all", "snowflake"]:
                result = session.run("MATCH (n:OlistData) RETURN count(n) as c")
                nodes_checked += result.single()['c']
            if scope in ["all", "federated", "databricks", "cross-source"]:
                result = session.run("MATCH (n:FederatedTable) RETURN count(n) as c")
                nodes_checked += result.single()['c']
            
            for shape in shapes:
                shape_violations = self._validate_shape(session, shape)
                violations.extend(shape_violations)
                
                status = "✅" if len(shape_violations) == 0 else "❌"
                print(f"  {status} {shape['name']}: {len(shape_violations)} violations")
        
        report = ValidationReport(
            timestamp=datetime.now(),
            total_nodes_checked=nodes_checked,
            shapes_evaluated=len(shapes),
            violations=violations,
            scope=scope
        )
        
        print(f"{'-'*70}")
        print(f"📊 SUMMARY ({scope}): {len(violations)} total violations")
        print(f"   🔴 Critical: {report.critical_count}")
        print(f"   🟡 Warning: {report.warning_count}")
        print(f"   🔵 Info: {report.info_count}")
        print(f"{'='*70}")
        
        return report
    
    def _validate_shape(self, session, shape: Dict) -> List[Violation]:
        """Execute a single shape validation query"""
        violations = []
        
        try:
            result = session.run(shape['validation_query'])
            
            for record in result:
                violation = Violation(
                    shape_name=shape['name'],
                    node_id=record['node_id'],
                    node_label=record['node_label'],
                    message=record['message'],
                    severity=shape['severity'],
                    property_path=record.get('property_path'),
                    actual_value=record.get('actual_value'),
                    expected_value=record.get('expected_value')
                )
                violations.append(violation)
                
        except Exception as e:
            print(f"  ⚠️ Error in {shape['name']}: {e}")
        
        return violations
    
    def validate_shape(self, shape_name: str) -> ValidationReport:
        """Run a single validation shape by name"""
        shape = next((s for s in self.shapes if s['name'] == shape_name), None)
        
        if not shape:
            raise ValueError(f"Unknown shape: {shape_name}")
        
        return self._run_validation([shape], scope=shape.get('scope', 'unknown'))
    
    def get_shape_info(self, scope: str = None) -> List[Dict]:
        """Get information about defined shapes, optionally filtered by scope"""
        shapes_to_show = self.shapes
        if scope:
            shapes_to_show = [s for s in self.shapes if s.get('scope') == scope]
        
        return [
            {
                'name': s['name'],
                'description': s['description'],
                'severity': s['severity'].value,
                'scope': s.get('scope', 'unknown')
            }
            for s in shapes_to_show
        ]
    
    def get_stats(self) -> Dict:
        """Get validation statistics"""
        with self.driver.session() as session:
            stats = {}
            
            # OlistData count
            result = session.run("MATCH (n:OlistData) RETURN count(n) as c")
            stats['olist_tables'] = result.single()['c']
            
            # FederatedTable counts
            result = session.run("""
                MATCH (n:FederatedTable)
                RETURN n.source as source, count(n) as c
            """)
            for record in result:
                stats[f'federated_{record["source"]}'] = record['c']
            
            # Column counts
            result = session.run("MATCH (n:OlistColumn) RETURN count(n) as c")
            stats['olist_columns'] = result.single()['c']
            
            result = session.run("MATCH (n:FederatedColumn) RETURN count(n) as c")
            stats['federated_columns'] = result.single()['c']
            
            # Relationship counts
            result = session.run("MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as c")
            stats['cross_source_matches'] = result.single()['c']
            
            stats['total_shapes'] = len(self.shapes)
            stats['snowflake_shapes'] = len(self.snowflake_shapes)
            stats['federated_shapes'] = len(self.federated_shapes)
            
        return stats
    
    def generate_report_html(self, report: ValidationReport) -> str:
        """Generate HTML report for Gradio display"""
        
        scope_colors = {
            'all': '#8b5cf6',
            'snowflake': '#3b82f6',
            'databricks': '#f97316',
            'federated': '#10b981',
            'cross-source': '#ec4899'
        }
        scope_color = scope_colors.get(report.scope, '#6b7280')
        
        if report.is_valid:
            return f"""
<div style="background: rgba(16, 185, 129, 0.15); border-radius: 16px; padding: 24px; border: 1px solid rgba(16, 185, 129, 0.3);">
    <div style="text-align: center;">
        <div style="font-size: 48px; margin-bottom: 16px;">✅</div>
        <div style="font-size: 24px; font-weight: 700; color: #6ee7b7;">All Validations Passed!</div>
        <div style="font-size: 14px; color: #a0a0b0; margin-top: 8px;">
            Scope: <span style="color: {scope_color}; font-weight: 600;">{report.scope.upper()}</span> | 
            {report.total_nodes_checked} nodes checked | {report.shapes_evaluated} shapes
        </div>
    </div>
</div>
"""
        
        html = f"""
<div style="background: rgba(20, 20, 30, 0.8); border-radius: 16px; padding: 24px; border: 1px solid rgba(255,255,255,0.08);">
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 20px;">
        <span style="font-size: 28px;">🛡️</span>
        <span style="font-size: 20px; font-weight: 600; color: #f0f0f5;">Governance Report</span>
        <span style="background: {scope_color}; padding: 4px 12px; border-radius: 12px; font-size: 12px; color: white;">{report.scope.upper()}</span>
    </div>
    
    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px;">
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 16px; text-align: center;">
            <div style="font-size: 28px; font-weight: 700; color: #f0f0f5;">{report.total_nodes_checked}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Nodes</div>
        </div>
        <div style="background: rgba(239, 68, 68, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(239, 68, 68, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #fca5a5;">{report.critical_count}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Critical</div>
        </div>
        <div style="background: rgba(245, 158, 11, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(245, 158, 11, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #fcd34d;">{report.warning_count}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Warning</div>
        </div>
        <div style="background: rgba(59, 130, 246, 0.15); border-radius: 12px; padding: 16px; text-align: center; border: 1px solid rgba(59, 130, 246, 0.3);">
            <div style="font-size: 28px; font-weight: 700; color: #93c5fd;">{report.info_count}</div>
            <div style="font-size: 12px; color: #a0a0b0;">Info</div>
        </div>
    </div>
    
    <div style="font-size: 16px; font-weight: 600; color: #f0f0f5; margin-bottom: 16px;">Violations</div>
    <div style="display: grid; gap: 12px;">
"""
        
        sorted_violations = sorted(
            report.violations, 
            key=lambda v: ['critical', 'warning', 'info'].index(v.severity.value)
        )
        
        for v in sorted_violations:
            if v.severity == Severity.CRITICAL:
                border_color = "#ef4444"
                icon = "🔴"
            elif v.severity == Severity.WARNING:
                border_color = "#f59e0b"
                icon = "🟡"
            else:
                border_color = "#3b82f6"
                icon = "🔵"
            
            html += f"""
        <div style="background: rgba(30, 30, 45, 0.9); border-radius: 12px; padding: 16px; border-left: 4px solid {border_color};">
            <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 8px;">
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span>{icon}</span>
                    <span style="font-weight: 600; color: #f0f0f5; font-size: 13px;">{v.node_id}</span>
                </div>
                <span style="background: rgba(99, 102, 241, 0.3); padding: 4px 10px; border-radius: 10px; font-size: 10px; color: #a5b4fc;">{v.shape_name}</span>
            </div>
            <div style="font-size: 13px; color: #a0a0b0; margin-bottom: 8px;">{v.message}</div>
            <div style="display: flex; gap: 16px; font-size: 11px; color: #606070;">
                <span>Expected: <span style="color: #6ee7b7;">{v.expected_value}</span></span>
                <span>Actual: <span style="color: #fca5a5;">{v.actual_value}</span></span>
            </div>
        </div>
"""
        
        html += """
    </div>
</div>
"""
        return html
    
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
        print("✅ SHACL Validator connection closed")


# ============================================
# TESTING
# ============================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🧪 TESTING SHACL VALIDATOR (Snowflake + Databricks)")
    print("="*70)
    
    validator = SHACLValidator()
    
    # Show stats
    print("\n📊 GRAPH STATISTICS:")
    stats = validator.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Show all shapes
    print("\n📋 ALL GOVERNANCE SHAPES:")
    print("-"*70)
    for shape in validator.get_shape_info():
        severity_icon = "🔴" if shape['severity'] == 'critical' else "🟡" if shape['severity'] == 'warning' else "🔵"
        scope_tag = f"[{shape['scope']}]"
        print(f"  {severity_icon} {shape['name']} {scope_tag}")
        print(f"     {shape['description']}")
    
    # Run validations by scope
    print("\n" + "="*70)
    print("RUNNING VALIDATIONS BY SCOPE")
    print("="*70)
    
    # Databricks-only validation
    print("\n🟠 DATABRICKS VALIDATION:")
    db_report = validator.validate_databricks()
    
    # Cross-source validation
    print("\n🩷 CROSS-SOURCE VALIDATION:")
    cs_report = validator.validate_cross_source()
    
    # Full validation
    print("\n🟣 FULL VALIDATION (ALL SCOPES):")
    full_report = validator.validate_all()
    
    # Show sample violations
    if full_report.violations:
        print("\n🔍 SAMPLE VIOLATIONS (first 5):")
        print("-"*70)
        for v in full_report.violations[:5]:
            print(f"  [{v.severity.value.upper()}] {v.node_id}")
            print(f"     Shape: {v.shape_name}")
            print(f"     {v.message}")
            print()
    
    validator.close()