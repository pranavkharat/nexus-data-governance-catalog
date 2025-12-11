# src/governance/__init__.py

"""
NEXUS Data Governance Module

SHACL-Inspired validation for knowledge graph data quality.
Addresses RQ3: What constraints optimize governance coverage vs curator overhead?

Components:
- SHACLValidator: Cypher-based constraint validation
- Severity: Violation severity levels (CRITICAL, WARNING, INFO)
- Violation: Individual constraint violation
- ValidationReport: Complete validation results

Usage:
    from src.governance import SHACLValidator, Severity
    
    # Run validation
    validator = SHACLValidator()
    report = validator.validate_all()
    
    # Check results
    print(f"Valid: {report.is_valid}")
    print(f"Critical violations: {report.critical_count}")
    
    # Generate HTML report
    html = validator.generate_report_html(report)
"""

from .shacl_validator import (
    SHACLValidator,
    Severity,
    Violation,
    ValidationReport
)

__all__ = [
    'SHACLValidator',
    'Severity',
    'Violation',
    'ValidationReport'
]