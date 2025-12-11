# src/federation/__init__.py

"""
NEXUS Federated Knowledge Graph Module

Enables multi-source data catalog federation with privacy-preserving fingerprints.
Supports: Snowflake + Databricks (extensible to other sources)

Design Principles:
1. Privacy-First: Only metadata fingerprints shared, never raw data
2. Source Attribution: Every node tagged with origin
3. Cross-Source Discovery: Find similar tables across platforms
4. Unified Schema: Single node type for all sources

Components:
- DatabricksMetadataExtractor: Extract metadata from Databricks Unity Catalog
- FederatedKGBuilder: Build unified Neo4j graph from multiple sources
- TableFingerprint: Privacy-safe table representation

Usage:
    from src.federation import (
        DatabricksMetadataExtractor,
        FederatedKGBuilder,
        build_federated_graph
    )
    
    # Quick start: Build complete federated graph
    stats = build_federated_graph()
    
    # Or step by step:
    extractor = DatabricksMetadataExtractor()
    fingerprints = extractor.extract_all_fingerprints('workspace', 'sample_data')
    
    builder = FederatedKGBuilder()
    builder.add_databricks_tables(fingerprints)
    builder.add_snowflake_tables_as_federated()
    builder.compute_cross_source_similarities()
"""

from .databricks_metadata_extractor import (
    DatabricksMetadataExtractor,
    TableFingerprint,
    ColumnFingerprint
)

from .federated_kg_builder import (
    FederatedKGBuilder,
    build_federated_graph
)

__all__ = [
    'DatabricksMetadataExtractor',
    'FederatedKGBuilder',
    'TableFingerprint',
    'ColumnFingerprint',
    'build_federated_graph'
]