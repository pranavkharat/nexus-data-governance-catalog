# src/lineage/__init__.py

"""
NEXUS Data Lineage Module

Extracts and manages data lineage relationships for the knowledge graph.
Addresses RQ2: How much lineage can be inferred from Snowflake query history?

Components:
- SnowflakeLineageExtractor: Extracts lineage from Snowflake
- LineageGraphBuilder: Creates DERIVES_FROM edges in Neo4j

Usage:
    from src.lineage import SnowflakeLineageExtractor, LineageGraphBuilder
    
    # Extract lineage
    extractor = SnowflakeLineageExtractor()
    edges = extractor.extract_all_lineage()
    
    # Build graph
    builder = LineageGraphBuilder()
    builder.build_lineage_graph(edges)
    
    # Query lineage
    upstream = builder.get_upstream_lineage('OLIST_MARKETING', 'CLIENT_DATA')
    downstream = builder.get_downstream_lineage('OLIST_SALES', 'CUSTOMERS')
"""

from .snowflake_lineage_extractor import SnowflakeLineageExtractor, LineageEdge
from .lineage_graph_builder import LineageGraphBuilder

__all__ = [
    'SnowflakeLineageExtractor',
    'LineageGraphBuilder', 
    'LineageEdge'
]