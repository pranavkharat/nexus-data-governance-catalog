"""
NEXUS Cross-Source Duplicate Detection
Implements SANTOS-inspired semantic similarity detection across Snowflake and Databricks

Based on: SANTOS (SIGMOD 2023) - Relationship-based Semantic Table Union Search
Adaptation: Metadata-level matching without direct value access

Author: Pranav Kharat, Northeastern University
File: src/federation/cross_source_duplicate_detector.py
"""

import os
import json
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()


@dataclass
class TableSignature:
    """Represents a table's semantic signature for SANTOS matching"""
    table_id: str
    source: str  # 'snowflake' or 'databricks'
    schema: str
    name: str
    row_count: int
    column_count: int
    columns: List[Dict]  # [{name, type, ordinal}]
    column_embedding: Optional[np.ndarray] = None
    type_signature: str = ""
    name_signature: str = ""


@dataclass
class SimilarityScore:
    """SANTOS-style similarity breakdown"""
    source_table: str
    target_table: str
    source_platform: str
    target_platform: str
    column_semantic_score: float
    type_overlap_score: float
    name_overlap_score: float
    statistical_score: float
    relationship_score: float
    total_score: float
    confidence: str
    matching_columns: List[Tuple[str, str, float]]


class CrossSourceDuplicateDetector:
    """
    SANTOS-inspired cross-source duplicate detection
    
    Weighting:
    - 40% Column Semantic Similarity (embeddings)
    - 25% Schema Overlap (type + name Jaccard)
    - 20% Statistical Fingerprint (row/column counts)
    - 15% Relationship Context (FK patterns)
    """
    
    WEIGHT_SEMANTIC = 0.40
    WEIGHT_SCHEMA = 0.25
    WEIGHT_STATISTICAL = 0.20
    WEIGHT_RELATIONSHIP = 0.15
    
    HIGH_CONFIDENCE_THRESHOLD = 0.75
    MEDIUM_CONFIDENCE_THRESHOLD = 0.50
    MIN_SIMILARITY_THRESHOLD = 0.30
    
    def __init__(self, neo4j_uri: str = "bolt://localhost:7687",
                 neo4j_user: str = "neo4j",
                 neo4j_password: str = None):
        self.neo4j_password = neo4j_password or os.getenv("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_user, self.neo4j_password)
        )
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.snowflake_tables: Dict[str, TableSignature] = {}
        self.databricks_tables: Dict[str, TableSignature] = {}
        
    def close(self):
        self.driver.close()
    
    def _parse_snowflake_type(self, type_str: str) -> str:
        """Parse Snowflake JSON data_type to simple type name"""
        if not type_str:
            return 'UNKNOWN'
        try:
            if type_str.startswith('{'):
                type_obj = json.loads(type_str)
                return type_obj.get('type', 'UNKNOWN').upper()
            return type_str.upper()
        except (json.JSONDecodeError, AttributeError):
            return type_str.upper() if type_str else 'UNKNOWN'
        
    # =========================================================================
    # PHASE A: Building Signatures (SANTOS Pre-processing)
    # =========================================================================
    
    def extract_snowflake_signatures(self) -> Dict[str, TableSignature]:
        """Extract table signatures from OlistData nodes"""
        query = """
        MATCH (t:OlistData)
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:OlistColumn)
        WITH t, collect({
            name: c.name,
            type: c.data_type,
            ordinal: c.ordinal_position
        }) as columns
        RETURN t.name as name,
               t.schema as schema,
               t.row_count as row_count,
               t.column_count as column_count,
               columns
        ORDER BY t.schema, t.name
        """
        
        with self.driver.session() as session:
            results = session.run(query)
            for record in results:
                table_id = f"{record['schema']}.{record['name']}".lower()
                
                # Parse columns and extract type from JSON
                columns = []
                for c in record['columns']:
                    if c['name']:
                        columns.append({
                            'name': c['name'],
                            'type': self._parse_snowflake_type(c['type']),
                            'ordinal': c['ordinal']
                        })
                
                sig = TableSignature(
                    table_id=table_id,
                    source='snowflake',
                    schema=record['schema'],
                    name=record['name'],
                    row_count=record['row_count'] or 0,
                    column_count=record['column_count'] or len(columns),
                    columns=columns,
                    type_signature=self._compute_type_signature(columns),
                    name_signature=self._compute_name_signature(columns)
                )
                
                col_text = " ".join([c['name'] for c in columns])
                if col_text.strip():
                    sig.column_embedding = self.embedding_model.encode(col_text)
                else:
                    sig.column_embedding = None
                
                self.snowflake_tables[table_id] = sig
                
        print(f"✅ Extracted {len(self.snowflake_tables)} Snowflake signatures")
        for table_id, sig in list(self.snowflake_tables.items())[:3]:
            print(f"   DEBUG: {table_id} -> {len(sig.columns)} columns: {[c['name'] for c in sig.columns[:3]]}")
        return self.snowflake_tables
    
    def extract_databricks_signatures(self) -> Dict[str, TableSignature]:
        """Extract table signatures from FederatedTable nodes"""
        query = """
        MATCH (t:FederatedTable)
        WHERE t.source = 'databricks'
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:FederatedColumn)
        WITH t, collect({
            name: c.name,
            type: replace(coalesce(c.data_type, 'UNKNOWN'), 'ColumnTypeName.', ''),
            ordinal: c.position
        }) as columns
        ORDER BY t.full_name
        RETURN t.full_name as full_name,
               t.column_count as column_count,
               t.row_count as row_count,
               columns
        """
        
        with self.driver.session() as session:
            results = session.run(query)
            for record in results:
                table_id = record['full_name']
                columns = [c for c in record['columns'] if c['name']]
                
                sig = TableSignature(
                    table_id=table_id,
                    source='databricks',
                    schema='workspace.sample_data',
                    name=table_id.split('.')[-1] if '.' in table_id else table_id,
                    row_count=record['row_count'] or 0,
                    column_count=record['column_count'] or len(columns),
                    columns=columns,
                    type_signature=self._compute_type_signature(columns),
                    name_signature=self._compute_name_signature(columns)
                )
                
                col_text = " ".join([c['name'] for c in columns])
                sig.column_embedding = self.embedding_model.encode(col_text)
                
                self.databricks_tables[table_id] = sig
                
        print(f"✅ Extracted {len(self.databricks_tables)} Databricks signatures")
        for table_id, sig in self.databricks_tables.items():
            print(f"   DEBUG: {table_id} -> {len(sig.columns)} columns: {[c['name'] for c in sig.columns[:5]]}")
        return self.databricks_tables
    
    def _compute_type_signature(self, columns: List[Dict]) -> str:
        """Create sorted type signature with cross-platform normalization"""
        type_map = {
            'TEXT': 'STRING', 'VARCHAR': 'STRING', 'CHAR': 'STRING', 
            'STRING': 'STRING', 'NVARCHAR': 'STRING',
            'NUMBER': 'NUMERIC', 'INT': 'NUMERIC', 'INTEGER': 'NUMERIC',
            'FLOAT': 'NUMERIC', 'DOUBLE': 'NUMERIC', 'DECIMAL': 'NUMERIC',
            'BIGINT': 'NUMERIC', 'SMALLINT': 'NUMERIC', 'LONG': 'NUMERIC',
            'DATE': 'DATETIME', 'TIMESTAMP': 'DATETIME', 'DATETIME': 'DATETIME',
            'TIMESTAMP_NTZ': 'DATETIME', 'TIMESTAMP_LTZ': 'DATETIME',
            'BOOLEAN': 'BOOLEAN', 'BOOL': 'BOOLEAN',
        }
        
        types = []
        for c in columns:
            raw_type = c.get('type', 'UNKNOWN').upper()
            normalized = type_map.get(raw_type, raw_type)
            types.append(normalized)
        
        return ",".join(sorted(types))
    
    def _compute_name_signature(self, columns: List[Dict]) -> str:
        """Create sorted name signature (normalized)"""
        names = sorted([c.get('name', '').lower().replace('_', '') for c in columns])
        return ",".join(names)
    
    # =========================================================================
    # PHASE B: Matching Algorithm (SANTOS Querying)
    # =========================================================================
    
    def compute_similarity(self, src: TableSignature, tgt: TableSignature) -> SimilarityScore:
        """Compute SANTOS-style similarity between two tables"""
        
        semantic_score = self._cosine_similarity(
            src.column_embedding, 
            tgt.column_embedding
        )
        
        type_overlap = self._jaccard_similarity(
            set(src.type_signature.split(',')),
            set(tgt.type_signature.split(','))
        )
        
        name_overlap = self._jaccard_similarity(
            set(src.name_signature.split(',')),
            set(tgt.name_signature.split(','))
        )
        
        schema_score = (type_overlap + name_overlap) / 2
        statistical_score = self._statistical_similarity(src, tgt)
        relationship_score = self._relationship_similarity(src, tgt)
        matching_columns = self._find_column_matches(src, tgt)
        
        total_score = (
            self.WEIGHT_SEMANTIC * semantic_score +
            self.WEIGHT_SCHEMA * schema_score +
            self.WEIGHT_STATISTICAL * statistical_score +
            self.WEIGHT_RELATIONSHIP * relationship_score
        )
        
        if total_score >= self.HIGH_CONFIDENCE_THRESHOLD:
            confidence = 'high'
        elif total_score >= self.MEDIUM_CONFIDENCE_THRESHOLD:
            confidence = 'medium'
        else:
            confidence = 'low'
            
        return SimilarityScore(
            source_table=src.table_id,
            target_table=tgt.table_id,
            source_platform=src.source,
            target_platform=tgt.source,
            column_semantic_score=semantic_score,
            type_overlap_score=type_overlap,
            name_overlap_score=name_overlap,
            statistical_score=statistical_score,
            relationship_score=relationship_score,
            total_score=total_score,
            confidence=confidence,
            matching_columns=matching_columns
        )
    
    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        if a is None or b is None:
            return 0.0
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
    
    def _jaccard_similarity(self, set_a: set, set_b: set) -> float:
        if not set_a or not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union > 0 else 0.0
    
    def _statistical_similarity(self, src: TableSignature, tgt: TableSignature) -> float:
        """Compare statistical fingerprints using log-scaled similarity"""
        if src.row_count > 0 and tgt.row_count > 0:
            log_ratio = abs(np.log10(src.row_count + 1) - np.log10(tgt.row_count + 1))
            row_sim = max(0, 1 - log_ratio / 3)
        else:
            row_sim = 0.5
            
        max_cols = max(src.column_count, tgt.column_count)
        min_cols = min(src.column_count, tgt.column_count)
        col_sim = min_cols / max_cols if max_cols > 0 else 0
        
        return (row_sim + col_sim) / 2
    
    def _relationship_similarity(self, src: TableSignature, tgt: TableSignature) -> float:
        """RS_CONF equivalent: Compare FK patterns"""
        def get_fk_pattern(cols: List[Dict]) -> set:
            patterns = set()
            for c in cols:
                name = c.get('name', '').lower()
                if name.endswith('_id') or name.endswith('_key'):
                    entity = name.rsplit('_', 1)[0]
                    patterns.add(entity)
            return patterns
        
        src_patterns = get_fk_pattern(src.columns)
        tgt_patterns = get_fk_pattern(tgt.columns)
        
        return self._jaccard_similarity(src_patterns, tgt_patterns)
    
    def _find_column_matches(self, src: TableSignature, tgt: TableSignature, 
                             threshold: float = 0.7) -> List[Tuple[str, str, float]]:
        """Find individual column matches using embedding similarity"""
        matches = []
        
        for src_col in src.columns:
            src_name = src_col.get('name', '')
            if not src_name:
                continue
            src_emb = self.embedding_model.encode(src_name)
            
            best_match = None
            best_sim = 0
            
            for tgt_col in tgt.columns:
                tgt_name = tgt_col.get('name', '')
                if not tgt_name:
                    continue
                tgt_emb = self.embedding_model.encode(tgt_name)
                sim = self._cosine_similarity(src_emb, tgt_emb)
                
                if sim > best_sim and sim >= threshold:
                    best_sim = sim
                    best_match = tgt_name
                    
            if best_match:
                matches.append((src_name, best_match, round(best_sim, 3)))
                
        return matches
    
    # =========================================================================
    # Cross-Source Detection
    # =========================================================================
    
    def detect_cross_source_duplicates(self, min_threshold: float = None) -> List[SimilarityScore]:
        """Main entry point: Detect duplicates between Snowflake and Databricks"""
        threshold = min_threshold or self.MIN_SIMILARITY_THRESHOLD
        
        if not self.snowflake_tables:
            self.extract_snowflake_signatures()
        if not self.databricks_tables:
            self.extract_databricks_signatures()
            
        results = []
        all_scores = []
        
        print(f"\n🔍 Comparing {len(self.snowflake_tables)} Snowflake tables "
              f"with {len(self.databricks_tables)} Databricks tables...")
        
        for db_id, db_sig in self.databricks_tables.items():
            for sf_id, sf_sig in self.snowflake_tables.items():
                score = self.compute_similarity(db_sig, sf_sig)
                all_scores.append(score)
                
                if score.total_score >= threshold:
                    results.append(score)
        
        all_scores.sort(key=lambda x: x.total_score, reverse=True)
        print(f"\n   DEBUG: Top 5 scores (regardless of threshold):")
        for s in all_scores[:5]:
            print(f"      {s.source_table} ↔ {s.target_table}: {s.total_score:.2%}")
            print(f"         sem={s.column_semantic_score:.2%}, type={s.type_overlap_score:.2%}, "
                  f"name={s.name_overlap_score:.2%}, stat={s.statistical_score:.2%}, rel={s.relationship_score:.2%}")
                    
        results.sort(key=lambda x: x.total_score, reverse=True)
        
        print(f"\n✅ Found {len(results)} potential matches above threshold {threshold}")
        return results
    
    def create_similarity_edges(self, results: List[SimilarityScore] = None,
                                min_threshold: float = None) -> int:
        """Create [:SIMILAR_TO] relationships in Neo4j"""
        if results is None:
            results = self.detect_cross_source_duplicates(min_threshold)
            
        threshold = min_threshold or self.MIN_SIMILARITY_THRESHOLD
        edges_created = 0
        
        create_query = """
        MATCH (db:FederatedTable {full_name: $databricks_table})
        MATCH (sf:OlistData)
        WHERE toLower(sf.schema + '.' + sf.name) = toLower($snowflake_table)
        MERGE (db)-[r:SIMILAR_TO]->(sf)
        SET r.score = $score,
            r.confidence = $confidence,
            r.semantic_score = $semantic_score,
            r.type_overlap = $type_overlap,
            r.name_overlap = $name_overlap,
            r.statistical_score = $statistical_score,
            r.relationship_score = $relationship_score,
            r.matching_columns = $matching_columns,
            r.algorithm = 'SANTOS-adapted',
            r.detected_at = datetime()
        RETURN r
        """
        
        with self.driver.session() as session:
            for score in results:
                if score.total_score < threshold:
                    continue
                    
                db_table = score.source_table
                sf_table = score.target_table
                
                matching_cols_str = "; ".join([
                    f"{src}->{tgt} ({sim})" 
                    for src, tgt, sim in score.matching_columns
                ])
                
                result = session.run(create_query, {
                    'databricks_table': db_table,
                    'snowflake_table': sf_table,
                    'score': round(score.total_score, 4),
                    'confidence': score.confidence,
                    'semantic_score': round(score.column_semantic_score, 4),
                    'type_overlap': round(score.type_overlap_score, 4),
                    'name_overlap': round(score.name_overlap_score, 4),
                    'statistical_score': round(score.statistical_score, 4),
                    'relationship_score': round(score.relationship_score, 4),
                    'matching_columns': matching_cols_str
                })
                
                if result.single():
                    edges_created += 1
                    print(f"  ✅ {db_table} --[SIMILAR_TO {score.total_score:.2%}]--> {sf_table}")
                    
        print(f"\n✅ Created {edges_created} SIMILAR_TO edges in Neo4j")
        return edges_created
    
    def get_similarity_report(self, results: List[SimilarityScore] = None) -> str:
        """Generate human-readable report"""
        if results is None:
            results = self.detect_cross_source_duplicates()
            
        report = []
        report.append("=" * 70)
        report.append("NEXUS Cross-Source Duplicate Detection Report")
        report.append("Algorithm: SANTOS-adapted (Metadata-level)")
        report.append("=" * 70)
        report.append("")
        
        high = [r for r in results if r.confidence == 'high']
        medium = [r for r in results if r.confidence == 'medium']
        low = [r for r in results if r.confidence == 'low']
        
        report.append(f"Summary: {len(high)} high, {len(medium)} medium, {len(low)} low confidence matches")
        report.append("")
        
        for score in results:
            report.append(f"{'─' * 60}")
            report.append(f"📊 {score.source_table} ({score.source_platform})")
            report.append(f"   ↔ {score.target_table} ({score.target_platform})")
            report.append(f"   Total Score: {score.total_score:.2%} ({score.confidence.upper()})")
            report.append(f"   ├─ Semantic (embeddings): {score.column_semantic_score:.2%}")
            report.append(f"   ├─ Type overlap:         {score.type_overlap_score:.2%}")
            report.append(f"   ├─ Name overlap:         {score.name_overlap_score:.2%}")
            report.append(f"   ├─ Statistical:          {score.statistical_score:.2%}")
            report.append(f"   └─ Relationship:         {score.relationship_score:.2%}")
            
            if score.matching_columns:
                report.append(f"   Column Matches:")
                for src, tgt, sim in score.matching_columns[:5]:
                    report.append(f"      • {src} → {tgt} ({sim:.0%})")
            report.append("")
            
        return "\n".join(report)


# =============================================================================
# CLI / Testing
# =============================================================================

if __name__ == "__main__":
    print("🚀 NEXUS Cross-Source Duplicate Detection")
    print("   Based on SANTOS (SIGMOD 2023)")
    print()
    
    detector = CrossSourceDuplicateDetector()
    
    try:
        results = detector.detect_cross_source_duplicates(min_threshold=0.25)
        print(detector.get_similarity_report(results))
        
        if results:
            print("\n📝 Creating SIMILAR_TO edges in Neo4j...")
            edges = detector.create_similarity_edges(results, min_threshold=0.30)
            print(f"   Created {edges} edges")
            
    finally:
        detector.close()