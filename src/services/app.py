"""
Knowledge Graph + Databricks Genie Data Catalog
Routes questions to appropriate Genie Space based on identified tables
Features:
- Sales Genie Space: workspace.sample_data.sales_transactions
- Feedback Genie Space: workspace.sample_data.customer_feedback
- LLM-powered Vega-Lite chart generation (3 chart types)
"""

import streamlit as st
from neo4j import GraphDatabase
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal, Tuple
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import logging
import traceback
from io import StringIO
import json

from databricks.sdk import WorkspaceClient

load_dotenv()

# ============================================
# Configuration
# ============================================

CATALOG_NAME = "workspace"
SCHEMA_NAME = "sample_data"
TABLE_SALES = "sales_transactions"
TABLE_FEEDBACK = "customer_feedback"

SALES_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_SALES}"
FEEDBACK_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_FEEDBACK}"

GENIE_SPACE_SALES = os.getenv("GENIE_SPACE_ID_SALES")
GENIE_SPACE_FEEDBACK = os.getenv("GENIE_SPACE_ID_FEEDBACK")

MAX_PREVIEW_ROWS = int(os.getenv("GENIE_PREVIEW_ROWS", "50"))

# ============================================
# Logging Setup
# ============================================

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class StringLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_buffer = StringIO()

    def emit(self, record):
        msg = self.format(record)
        self.log_buffer.write(msg + '\n')

    def get_logs(self):
        return self.log_buffer.getvalue()

    def clear(self):
        self.log_buffer = StringIO()

ui_log_handler = StringLogHandler()
ui_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ui_log_handler)

# ============================================
# Page Configuration
# ============================================

st.set_page_config(
    page_title="Knowledge Graph + Genie Data Catalog",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# Initialize Session State
# ============================================

def init_session_state():
    """Initialize all session state variables."""
    if "genie_conv_sales" not in st.session_state:
        st.session_state.genie_conv_sales = None
    if "genie_conv_feedback" not in st.session_state:
        st.session_state.genie_conv_feedback = None
    if "last_query_result" not in st.session_state:
        st.session_state.last_query_result = None
    if "genie_question" not in st.session_state:
        st.session_state.genie_question = ""
    if "generated_charts" not in st.session_state:
        st.session_state.generated_charts = {}
    if "debug_logs" not in st.session_state:
        st.session_state.debug_logs = ""

init_session_state()

# ============================================
# Pydantic Models
# ============================================

class DataAssetInfo(BaseModel):
    name: str = Field(description="Name of the table or column")
    type: str = Field(description="Type (Table, Column, etc.)")
    description: Optional[str] = Field(default=None)
    owner: Optional[str] = Field(default=None)
    tags: List[str] = Field(default_factory=list)
    sensitivity: Optional[str] = Field(default=None)
    full_name: Optional[str] = Field(default=None)


class TableIdentification(BaseModel):
    tables: List[str] = Field(description="List of relevant table names")
    columns: List[str] = Field(description="List of relevant column names")
    reasoning: str = Field(description="Why these tables/columns are relevant")
    confidence: float = Field(description="Confidence score 0-1")
    primary_domain: Literal["sales", "feedback", "both"] = Field(
        description="Primary domain: 'sales', 'feedback', or 'both'"
    )


class GenieResponse(BaseModel):
    text: str = Field(default="")
    sql: Optional[str] = Field(default=None)
    data: Optional[List[Dict[str, Any]]] = Field(default=None)
    conversation_id: Optional[str] = Field(default=None)
    message_id: Optional[str] = Field(default=None)
    error: Optional[str] = Field(default=None)
    source_space: Optional[str] = Field(default=None)


class AgenticQueryResult(BaseModel):
    question: str
    identified_tables: List[str] = Field(default_factory=list)
    identified_columns: List[str] = Field(default_factory=list)
    identification_reasoning: Optional[str] = None
    confidence: float = 0.0
    primary_domain: str = "sales"
    genie_responses: List[GenieResponse] = Field(default_factory=list)
    combined_response: str = ""
    assets: List[DataAssetInfo] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    error: Optional[str] = None


class VegaLiteSpec(BaseModel):
    """Vega-Lite chart specification generated by LLM"""
    title: str = Field(description="Chart title")
    description: str = Field(description="Brief description of what the chart shows")
    mark: Literal["bar", "line", "point"] = Field(description="Chart type: bar, line, or point")
    x_field: str = Field(description="Column name for x-axis")
    x_type: Literal["quantitative", "nominal", "ordinal", "temporal"] = Field(description="X-axis data type")
    y_field: str = Field(description="Column name for y-axis")
    y_type: Literal["quantitative", "nominal", "ordinal", "temporal"] = Field(description="Y-axis data type")
    y_aggregate: Optional[Literal["sum", "mean", "count", "min", "max"]] = Field(default=None, description="Y-axis aggregation")
    color_field: Optional[str] = Field(default=None, description="Column for color encoding")
    color_type: Optional[Literal["quantitative", "nominal", "ordinal"]] = Field(default=None, description="Color data type")


# ============================================
# Table Schema Definition
# ============================================

SCHEMA_DEFINITION = {
    TABLE_SALES: {
        "full_name": SALES_TABLE_PATH,
        "description": "Sales transaction data containing all customer purchases across multiple channels and regions",
        "owner": "sales_team",
        "tags": ["Sales", "Revenue", "Transactions"],
        "columns": {
            "transaction_id": {"type": "STRING", "description": "Unique identifier for each sales transaction", "sensitivity": "Low"},
            "customer_id": {"type": "STRING", "description": "Unique customer identifier", "sensitivity": "Medium"},
            "product_name": {"type": "STRING", "description": "Name of the product purchased", "sensitivity": "Low"},
            "product_category": {"type": "STRING", "description": "Category classification of the product", "sensitivity": "Low"},
            "order_date": {"type": "STRING", "description": "Date when the order was placed", "sensitivity": "Low"},
            "quantity": {"type": "INT", "description": "Number of units purchased", "sensitivity": "Low"},
            "unit_price": {"type": "DOUBLE", "description": "Price per unit in USD", "sensitivity": "Low"},
            "total_amount": {"type": "DOUBLE", "description": "Total transaction amount in USD", "sensitivity": "Medium"},
            "region": {"type": "STRING", "description": "Geographic region where the sale occurred", "sensitivity": "Low"},
            "sales_channel": {"type": "STRING", "description": "Channel through which the sale was made", "sensitivity": "Low"},
            "payment_method": {"type": "STRING", "description": "Payment method used by customer", "sensitivity": "Medium"},
            "created_by": {"type": "STRING", "description": "Team or user who created this record", "sensitivity": "Low"},
            "created_at": {"type": "STRING", "description": "Timestamp when this record was created", "sensitivity": "Low"}
        }
    },
    TABLE_FEEDBACK: {
        "full_name": FEEDBACK_TABLE_PATH,
        "description": "Customer feedback and satisfaction data linked to transactions",
        "owner": "customer_experience_team",
        "tags": ["Feedback", "Customer Satisfaction", "Quality"],
        "columns": {
            "feedback_id": {"type": "STRING", "description": "Unique identifier for each feedback entry", "sensitivity": "Low"},
            "customer_id": {"type": "STRING", "description": "Customer identifier", "sensitivity": "High"},
            "transaction_id": {"type": "STRING", "description": "Transaction identifier linking feedback to purchase", "sensitivity": "Medium"},
            "rating": {"type": "INT", "description": "Customer satisfaction rating 1-5 stars", "sensitivity": "Low"},
            "feedback_text": {"type": "STRING", "description": "Customer feedback comments", "sensitivity": "Medium"},
            "feedback_category": {"type": "STRING", "description": "Category of feedback", "sensitivity": "Low"},
            "sentiment": {"type": "STRING", "description": "Sentiment classification", "sensitivity": "Low"},
            "submitted_date": {"type": "STRING", "description": "Date when feedback was submitted", "sensitivity": "Low"},
            "response_status": {"type": "STRING", "description": "Status of feedback response", "sensitivity": "Low"},
            "assigned_to": {"type": "STRING", "description": "Team assigned to handle feedback", "sensitivity": "Low"},
            "created_by": {"type": "STRING", "description": "Team or user who ingested this record", "sensitivity": "Low"},
            "created_at": {"type": "STRING", "description": "Timestamp when this record was created", "sensitivity": "Low"}
        }
    }
}

# ============================================
# Cached Resources
# ============================================

@st.cache_resource
def get_neo4j_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD"))
    )


@st.cache_resource
def get_instructor_client():
    return instructor.from_openai(OpenAI(api_key=os.getenv("OPENAI_API_KEY")))


@st.cache_resource
def get_databricks_client():
    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )

# ============================================
# Schema Helper Functions
# ============================================

def get_schema_as_string() -> str:
    lines = []
    for table_name, table_info in SCHEMA_DEFINITION.items():
        cols = ", ".join([
            f"{col_name} ({col_info['type']})"
            for col_name, col_info in table_info['columns'].items()
        ])
        lines.append(
            f"Table: {table_name} (Full: {table_info['full_name']}, Owner: {table_info['owner']})\n"
            f"  Description: {table_info['description']}\n"
            f"  Tags: {', '.join(table_info['tags'])}\n"
            f"  Columns: {cols}"
        )
    return "\n\n".join(lines)


def get_table_info(table_name: str) -> Optional[Dict]:
    return SCHEMA_DEFINITION.get(table_name)


def get_statistics() -> Dict[str, int]:
    total_tables = len(SCHEMA_DEFINITION)
    total_columns = sum(len(t['columns']) for t in SCHEMA_DEFINITION.values())
    total_tags = len(set(tag for t in SCHEMA_DEFINITION.values() for tag in t['tags']))
    return {
        "tables": total_tables,
        "columns": total_columns,
        "tags": total_tags,
        "teams": len(set(t['owner'] for t in SCHEMA_DEFINITION.values()))
    }

# ============================================
# Table Identification (Knowledge Graph Logic)
# ============================================

def identify_relevant_tables(question: str) -> TableIdentification:
    client = get_instructor_client()
    schema_str = get_schema_as_string()

    response = client.chat.completions.create(
        model="gpt-4o",
        response_model=TableIdentification,
        messages=[
            {
                "role": "system",
                "content": f"""You are a data catalog expert. Given a user question and the available schema,
identify which tables and columns are most relevant to answer the question.

Available Schema:
{schema_str}

IMPORTANT RULES:
1. If the question is about sales, revenue, orders, products, transactions, or purchases → use "sales_transactions" table, domain="sales"
2. If the question is about feedback, ratings, sentiment, customer satisfaction, or reviews → use "customer_feedback" table, domain="feedback"
3. If the question needs BOTH tables (e.g., "feedback for high-value orders") → use both tables, domain="both"
4. The tables are linked by customer_id and transaction_id columns

Be precise and set the primary_domain correctly for routing to the appropriate Genie space."""
            },
            {"role": "user", "content": question}
        ]
    )
    return response


def get_assets_for_tables(table_names: List[str]) -> List[DataAssetInfo]:
    assets = []
    for table_name in table_names:
        table_info = get_table_info(table_name)
        if table_info:
            assets.append(DataAssetInfo(
                name=table_name,
                type="Table",
                description=table_info['description'],
                owner=table_info['owner'],
                tags=table_info['tags'],
                full_name=table_info['full_name']
            ))
    return assets

# ============================================
# Vega-Lite Chart Generation (LLM-Powered)
# ============================================

def generate_vega_lite_spec(data: List[Dict[str, Any]], chart_type: str) -> Optional[VegaLiteSpec]:
    """Use LLM to generate a Vega-Lite specification for the given data and chart type."""
    if not data:
        return None
    
    client = get_instructor_client()
    
    sample_row = data[0] if data else {}
    columns_info = []
    for col, val in sample_row.items():
        if isinstance(val, (int, float)):
            vega_type = "quantitative"
        elif any(x in col.lower() for x in ['date', 'time', 'created', 'updated']):
            vega_type = "temporal"
        else:
            vega_type = "nominal"
        columns_info.append(f"- {col}: {vega_type}")
    
    columns_str = "\n".join(columns_info)
    sample_str = json.dumps(data[:3], indent=2, default=str)
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            response_model=VegaLiteSpec,
            messages=[
                {
                    "role": "system",
                    "content": f"""You are a data visualization expert. Generate a Vega-Lite specification.

Available columns:
{columns_str}

Sample data:
{sample_str}

RULES:
1. Chart type MUST be: {chart_type}
2. Use exact column names from the data
3. Choose the best x and y fields for this chart type
4. For bar charts: use nominal/ordinal for x, quantitative for y with aggregation
5. For line charts: use temporal/ordinal for x, quantitative for y
6. For point/scatter charts: use quantitative for both x and y
7. Add color encoding if it makes the chart more informative
8. Use aggregation (sum, mean, count) when appropriate for the y-axis"""
                },
                {"role": "user", "content": f"Create a {chart_type} chart for this data"}
            ]
        )
        return response
    except Exception as e:
        logger.error(f"Error generating Vega-Lite spec: {e}")
        return None


def build_vega_lite_json(spec: VegaLiteSpec) -> Dict:
    """Build the complete Vega-Lite JSON specification."""
    encoding = {
        "x": {"field": spec.x_field, "type": spec.x_type},
        "y": {"field": spec.y_field, "type": spec.y_type}
    }
    
    if spec.y_aggregate:
        encoding["y"]["aggregate"] = spec.y_aggregate
    
    if spec.color_field and spec.color_type:
        encoding["color"] = {"field": spec.color_field, "type": spec.color_type}
    
    vega_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": spec.title,
        "description": spec.description,
        "width": 600,
        "height": 400,
        "mark": {"type": spec.mark, "tooltip": True},
        "encoding": encoding
    }
    
    return vega_spec


def generate_chart(data: List[Dict[str, Any]], chart_type: str, chart_key: str):
    """Generate chart and store in session state."""
    with st.spinner(f"🤖 Generating {chart_type} chart with AI..."):
        spec = generate_vega_lite_spec(data, chart_type)
        
        if spec:
            vega_json = build_vega_lite_json(spec)
            st.session_state.generated_charts[chart_key] = {
                "spec": spec,
                "vega_json": vega_json,
                "data": data,
                "chart_type": chart_type
            }
        else:
            st.session_state.generated_charts[chart_key] = {"error": "Failed to generate chart"}


def render_chart_section(data: List[Dict[str, Any]], source_name: str, key_prefix: str):
    """Render chart buttons and display generated charts."""
    if not data:
        return
    
    st.markdown(f"#### 📊 Create Chart")
    
    col1, col2, col3 = st.columns(3)
    
    bar_key = f"{key_prefix}_bar"
    line_key = f"{key_prefix}_line"
    scatter_key = f"{key_prefix}_scatter"
    
    with col1:
        if st.button("📊 Bar Chart", key=f"btn_{bar_key}", use_container_width=True):
            generate_chart(data, "bar", bar_key)
    
    with col2:
        if st.button("📈 Line Chart", key=f"btn_{line_key}", use_container_width=True):
            generate_chart(data, "line", line_key)
    
    with col3:
        if st.button("⚫ Scatter Plot", key=f"btn_{scatter_key}", use_container_width=True):
            generate_chart(data, "point", scatter_key)
    
    # Display any generated charts for this data source
    for chart_key in [bar_key, line_key, scatter_key]:
        if chart_key in st.session_state.generated_charts:
            chart_data = st.session_state.generated_charts[chart_key]
            
            if "error" in chart_data:
                st.error(chart_data["error"])
            else:
                spec = chart_data["spec"]
                vega_json = chart_data["vega_json"]
                df = pd.DataFrame(chart_data["data"])
                
                st.markdown(f"**{spec.title}**")
                st.caption(spec.description)
                st.vega_lite_chart(df, vega_json, use_container_width=True)
                
                with st.expander("📋 View Vega-Lite Spec"):
                    st.code(json.dumps(vega_json, indent=2), language="json")

# ============================================
# Genie Query Functions
# ============================================

def _cell_to_python(cell: Any) -> Any:
    if isinstance(cell, dict) and len(cell) == 1:
        return next(iter(cell.values()))
    return cell


def _statement_response_to_records(
    statement_response: Any, 
    max_rows: int = 50
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[List[str]]]:
    try:
        d = statement_response
        if hasattr(statement_response, "as_dict"):
            d = statement_response.as_dict()

        if not isinstance(d, dict):
            return None, None

        manifest = d.get("manifest") or {}
        schema = (manifest.get("schema") or {})
        columns = schema.get("columns") or []
        col_names = [c.get("name") for c in columns if isinstance(c, dict) and c.get("name")]

        result = d.get("result") or {}
        rows = result.get("data_array") or result.get("data_typed_array") or []

        if not isinstance(rows, list) or not col_names:
            return None, col_names or None

        records: List[Dict[str, Any]] = []
        for row in rows[:max_rows]:
            if not isinstance(row, list):
                continue
            values = [_cell_to_python(c) for c in row]
            if len(values) < len(col_names):
                values = values + ([None] * (len(col_names) - len(values)))
            if len(values) > len(col_names):
                values = values[:len(col_names)]
            records.append(dict(zip(col_names, values)))

        return records, col_names
    except Exception:
        return None, None


def query_genie_sdk(
    question: str,
    space_id: str,
    table_context: str,
    space_name: str,
    conversation_id: Optional[str] = None
) -> GenieResponse:
    """Query Databricks Genie using the Official SDK."""
    try:
        logger.info(f"=== Genie Query: {space_name} ===")
        logger.info(f"Question: {question}")

        if not space_id:
            return GenieResponse(text="", error="Missing space_id", source_space=space_name)

        client = get_databricks_client()
        enhanced_question = f"Context: Querying {table_context}. Question: {question}"

        if conversation_id:
            msg = client.genie.create_message_and_wait(
                space_id=space_id,
                conversation_id=conversation_id,
                content=enhanced_question
            )
        else:
            msg = client.genie.start_conversation_and_wait(
                space_id=space_id,
                content=enhanced_question
            )

        conv_id = getattr(msg, "conversation_id", None) or conversation_id
        message_id = getattr(msg, "message_id", None) or getattr(msg, "id", None)

        if getattr(msg, "error", None):
            return GenieResponse(
                text="", error=str(msg.error),
                conversation_id=conv_id, message_id=message_id, source_space=space_name
            )

        attachments = getattr(msg, "attachments", None) or []
        response_text_parts: List[str] = []
        executed_sql: Optional[str] = None
        data_records: Optional[List[Dict[str, Any]]] = None
        query_attachment_id: Optional[str] = None

        for att in attachments:
            text_att = getattr(att, "text", None)
            if text_att and getattr(text_att, "content", None):
                response_text_parts.append(str(text_att.content))

            q_att = getattr(att, "query", None)
            if q_att:
                if getattr(q_att, "query", None):
                    executed_sql = str(q_att.query)
                if getattr(q_att, "description", None):
                    response_text_parts.append(str(q_att.description))
                query_attachment_id = getattr(att, "attachment_id", None) or query_attachment_id

        response_text = "\n\n".join([p for p in response_text_parts if p]).strip()

        if query_attachment_id and conv_id and message_id:
            try:
                qr = client.genie.get_message_attachment_query_result(
                    space_id=space_id,
                    conversation_id=conv_id,
                    message_id=message_id,
                    attachment_id=query_attachment_id
                )
                stmt_resp = getattr(qr, "statement_response", None)
                if stmt_resp:
                    records, _ = _statement_response_to_records(stmt_resp, max_rows=MAX_PREVIEW_ROWS)
                    if records:
                        data_records = records
            except Exception as e:
                logger.warning(f"Could not fetch query results: {e}")

        if not response_text:
            response_text = "Genie responded successfully."

        return GenieResponse(
            text=response_text, sql=executed_sql, data=data_records,
            conversation_id=conv_id, message_id=message_id, source_space=space_name
        )

    except Exception as e:
        logger.error(f"Genie Query FAILED: {type(e).__name__}: {str(e)}")
        return GenieResponse(
            text="", error=f"{type(e).__name__}: {str(e)}",
            conversation_id=conversation_id, source_space=space_name
        )

# ============================================
# Main Agentic Query Function
# ============================================

def run_agentic_query(question: str, progress_callback=None) -> AgenticQueryResult:
    """Run the full agentic query."""
    try:
        if progress_callback:
            progress_callback("🔍 Step 1: Identifying relevant tables...")

        identification = identify_relevant_tables(question)
        assets = get_assets_for_tables(identification.tables)
        
        for col in identification.columns[:5]:
            assets.append(DataAssetInfo(name=col, type="Column"))

        if progress_callback:
            progress_callback(f"✅ Found tables: {', '.join(identification.tables)}")
            progress_callback(f"📊 Primary domain: {identification.primary_domain}")

        genie_responses: List[GenieResponse] = []

        if identification.primary_domain in ["sales", "both"]:
            if GENIE_SPACE_SALES:
                if progress_callback:
                    progress_callback("📊 Step 2a: Querying Sales Genie Space...")

                sales_response = query_genie_sdk(
                    question=question,
                    space_id=GENIE_SPACE_SALES,
                    table_context=SALES_TABLE_PATH,
                    space_name="Sales",
                    conversation_id=st.session_state.genie_conv_sales
                )
                if sales_response.conversation_id:
                    st.session_state.genie_conv_sales = sales_response.conversation_id
                genie_responses.append(sales_response)
            else:
                genie_responses.append(GenieResponse(
                    text="Sales Genie Space not configured",
                    error="GENIE_SPACE_ID_SALES not set",
                    source_space="Sales"
                ))

        if identification.primary_domain in ["feedback", "both"]:
            if GENIE_SPACE_FEEDBACK:
                if progress_callback:
                    progress_callback("📝 Step 2b: Querying Feedback Genie Space...")

                feedback_response = query_genie_sdk(
                    question=question,
                    space_id=GENIE_SPACE_FEEDBACK,
                    table_context=FEEDBACK_TABLE_PATH,
                    space_name="Feedback",
                    conversation_id=st.session_state.genie_conv_feedback
                )
                if feedback_response.conversation_id:
                    st.session_state.genie_conv_feedback = feedback_response.conversation_id
                genie_responses.append(feedback_response)
            else:
                genie_responses.append(GenieResponse(
                    text="Feedback Genie Space not configured",
                    error="GENIE_SPACE_ID_FEEDBACK not set",
                    source_space="Feedback"
                ))

        combined_parts = []
        for resp in genie_responses:
            if resp.text and not resp.error:
                combined_parts.append(f"**[{resp.source_space}]**: {resp.text}")
            elif resp.error:
                combined_parts.append(f"**[{resp.source_space}]**: Error - {resp.error}")

        combined_response = "\n\n".join(combined_parts) if combined_parts else "No response from Genie"

        suggestions = []
        if identification.primary_domain == "sales":
            suggestions = ["What is the average order value?", "Show sales by region", "Which products sell the most?"]
        elif identification.primary_domain == "feedback":
            suggestions = ["What is the average customer rating?", "Show feedback sentiment distribution", "Which categories have most complaints?"]
        else:
            suggestions = ["Show feedback for top-selling products", "Compare ratings across regions"]

        return AgenticQueryResult(
            question=question,
            identified_tables=identification.tables,
            identified_columns=identification.columns,
            identification_reasoning=identification.reasoning,
            confidence=identification.confidence,
            primary_domain=identification.primary_domain,
            genie_responses=genie_responses,
            combined_response=combined_response,
            assets=assets,
            suggestions=suggestions
        )

    except Exception as e:
        return AgenticQueryResult(question=question, combined_response="", error=str(e))

# ============================================
# Streamlit UI Components
# ============================================

def render_sidebar():
    with st.sidebar:
        st.header("📊 Data Catalog Stats")
        stats = get_statistics()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Tables", stats['tables'])
            st.metric("Teams", stats['teams'])
        with col2:
            st.metric("Columns", stats['columns'])
            st.metric("Tags", stats['tags'])

        st.markdown("---")
        st.header("🧞 Genie Spaces")

        if GENIE_SPACE_SALES:
            st.success(f"✅ Sales: `{GENIE_SPACE_SALES[:8]}...`")
        else:
            st.error("❌ Sales: Not configured")

        if GENIE_SPACE_FEEDBACK:
            st.success(f"✅ Feedback: `{GENIE_SPACE_FEEDBACK[:8]}...`")
        else:
            st.error("❌ Feedback: Not configured")

        st.markdown("---")
        st.header("🔧 Settings")
        
        mode = st.radio(
            "Query Mode:",
            ["Knowledge Graph Only", "Agentic Genie Mode"],
            index=1 if (GENIE_SPACE_SALES or GENIE_SPACE_FEEDBACK) else 0
        )

        st.markdown("---")
        st.header("💬 Conversation")
        
        if st.button("Reset Sales conversation"):
            st.session_state.genie_conv_sales = None
            st.success("Sales conversation reset!")
            
        if st.button("Reset Feedback conversation"):
            st.session_state.genie_conv_feedback = None
            st.success("Feedback conversation reset!")
        
        if st.button("Clear Results & Charts"):
            st.session_state.last_query_result = None
            st.session_state.generated_charts = {}
            st.session_state.debug_logs = ""
            st.rerun()

        return mode


def display_query_results(result: AgenticQueryResult):
    """Display query results from session state."""
    st.markdown("---")

    col1, col2, col3 = st.columns(3)
    domain_emoji = {"sales": "📊", "feedback": "📝", "both": "🔗"}
    with col1:
        st.info(f"{domain_emoji.get(result.primary_domain, '📋')} **Domain:** {result.primary_domain.upper()}")
    with col2:
        st.info(f"📦 **Tables:** {', '.join(result.identified_tables)}")
    with col3:
        st.info(f"🎯 **Confidence:** {result.confidence:.0%}")

    st.success("### 📊 Genie Response")
    st.markdown(result.combined_response)

    for idx, genie_resp in enumerate(result.genie_responses):
        with st.expander(f"🧞 {genie_resp.source_space} Genie Details", expanded=True):
            if genie_resp.error:
                st.error(f"Error: {genie_resp.error}")
            else:
                st.write(genie_resp.text)

                if genie_resp.sql:
                    st.markdown("**Generated SQL:**")
                    st.code(genie_resp.sql, language="sql")

                if genie_resp.data:
                    st.markdown("**Data Preview:**")
                    st.dataframe(pd.DataFrame(genie_resp.data), use_container_width=True)
                    
                    st.markdown("---")
                    render_chart_section(
                        data=genie_resp.data,
                        source_name=genie_resp.source_space,
                        key_prefix=f"chart_{genie_resp.source_space}_{idx}"
                    )

                if genie_resp.conversation_id:
                    st.caption(f"conversation_id: {genie_resp.conversation_id}")

    st.markdown("### 🏷️ Identified Assets")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Tables:**")
        for asset in result.assets:
            if asset.type == "Table":
                with st.expander(f"📊 {asset.name}"):
                    st.write(f"**Full Name:** `{asset.full_name}`")
                    st.write(f"**Description:** {asset.description}")
                    st.write(f"**Owner:** {asset.owner}")

    with col2:
        st.markdown("**Relevant Columns:**")
        for col in result.identified_columns[:10]:
            st.text(f"  • {col}")
        st.markdown("**Reasoning:**")
        st.caption(result.identification_reasoning)

    if result.suggestions:
        st.markdown("### 💡 Follow-up Suggestions")
        cols = st.columns(len(result.suggestions))
        for i, suggestion in enumerate(result.suggestions):
            with cols[i]:
                if st.button(suggestion, key=f"suggest_{i}"):
                    st.session_state.genie_question = suggestion
                    st.session_state.last_query_result = None
                    st.session_state.generated_charts = {}
                    st.rerun()

    with st.expander("🐛 Debug Logs"):
        st.code(st.session_state.debug_logs if st.session_state.debug_logs else "No debug logs", language="text")


def render_agentic_tab():
    st.header("🤖 Agentic Genie Search")

    st.markdown("""
    **How it works:**
    1. 🔍 **Identifies** relevant tables from your question
    2. 🎯 **Routes** to the appropriate Genie Space
    3. 📊 **Returns** results with SQL and data
    4. 📈 **Create Charts** with one click (Bar, Line, Scatter)
    """)

    with st.expander("💡 Example Questions", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**📊 Sales Questions:**")
            for ex in ["What were total sales last month?", "Show sales by product category", 
                      "Which region has highest revenue?", "Top 10 products by quantity sold"]:
                if st.button(ex, key=f"sales_{ex}"):
                    st.session_state.genie_question = ex
                    st.session_state.last_query_result = None
                    st.session_state.generated_charts = {}

        with col2:
            st.markdown("**📝 Feedback Questions:**")
            for ex in ["What is the average customer rating?", "Show sentiment distribution",
                      "Which feedback category has most issues?", "List negative feedback"]:
                if st.button(ex, key=f"feedback_{ex}"):
                    st.session_state.genie_question = ex
                    st.session_state.last_query_result = None
                    st.session_state.generated_charts = {}

    question = st.text_input(
        "Your question:",
        value=st.session_state.genie_question,
        placeholder="e.g., What were total sales by region last quarter?",
        key="question_input"
    )
    
    # Update session state when input changes
    if question != st.session_state.genie_question:
        st.session_state.genie_question = question

    search_clicked = st.button("🚀 Search", type="primary")

    if search_clicked and question:
        # Clear previous results and charts when new search
        st.session_state.generated_charts = {}
        ui_log_handler.clear()
        
        status_container = st.status("🔄 Processing query...", expanded=True)

        def update_status(message):
            status_container.write(message)

        result = run_agentic_query(question, update_status)
        
        # Store debug logs
        st.session_state.debug_logs = ui_log_handler.get_logs()

        if result.error:
            status_container.update(label="❌ Error", state="error")
            st.error(result.error)
            return

        status_container.update(label="✅ Query Complete!", state="complete")
        
        # Store result in session state
        st.session_state.last_query_result = result

    elif search_clicked:
        st.warning("Please enter a question")

    # Always display results if they exist in session state
    if st.session_state.last_query_result:
        display_query_results(st.session_state.last_query_result)


def render_kg_only_tab():
    st.header("🔍 Knowledge Graph Search")
    
    question = st.text_input(
        "Your question:",
        placeholder="e.g., What tables contain customer information?",
        key="kg_question"
    )

    if st.button("🔍 Search KG", type="primary") and question:
        identification = identify_relevant_tables(question)

        col1, col2 = st.columns(2)
        with col1:
            st.info(f"📊 **Domain:** {identification.primary_domain.upper()}")
        with col2:
            st.info(f"🎯 **Confidence:** {identification.confidence:.0%}")

        st.markdown("### 📋 Identified Tables")
        for table_name in identification.tables:
            table_info = get_table_info(table_name)
            if table_info:
                with st.expander(f"📊 {table_name}", expanded=True):
                    st.write(f"**Full Name:** `{table_info['full_name']}`")
                    st.write(f"**Description:** {table_info['description']}")
                    st.write(f"**Columns:** {', '.join(table_info['columns'].keys())}")

        st.markdown("### 💡 Reasoning")
        st.caption(identification.identification_reasoning)


def render_browse_tab():
    st.header("📚 Browse Tables")

    selected_table = st.selectbox(
        "Select a table:",
        options=list(SCHEMA_DEFINITION.keys()),
        format_func=lambda x: f"{x} ({SCHEMA_DEFINITION[x]['owner']})"
    )

    if selected_table:
        table_info = SCHEMA_DEFINITION[selected_table]

        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown(f"### 📊 {selected_table}")
            st.write(f"**Full Name:** `{table_info['full_name']}`")
            st.write(f"**Description:** {table_info['description']}")
            st.write(f"**Owner:** {table_info['owner']}")
            st.write(f"**Tags:** {', '.join(table_info['tags'])}")

        with col2:
            space_id = GENIE_SPACE_SALES if selected_table == TABLE_SALES else GENIE_SPACE_FEEDBACK
            if space_id:
                st.success("🧞 Genie Space: Configured")
                st.caption(f"ID: {space_id[:12]}...")
            else:
                st.warning("🧞 Genie Space: Not configured")

        st.markdown("### 📋 Columns")
        columns_data = [
            {"Column": k, "Type": v['type'], "Sensitivity": v['sensitivity'], "Description": v['description']}
            for k, v in table_info['columns'].items()
        ]
        st.dataframe(pd.DataFrame(columns_data), hide_index=True, use_container_width=True)


def render_analytics_tab():
    st.header("📈 Schema Analytics")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📊 Columns by Table")
        chart_data = pd.DataFrame({
            "Table": list(SCHEMA_DEFINITION.keys()),
            "Columns": [len(t['columns']) for t in SCHEMA_DEFINITION.values()]
        })
        st.bar_chart(chart_data.set_index("Table"))

    with col2:
        st.markdown("### 🔒 Sensitivity Distribution")
        sensitivity_counts = {"Low": 0, "Medium": 0, "High": 0}
        for table_info in SCHEMA_DEFINITION.values():
            for col_info in table_info['columns'].values():
                sens = col_info.get('sensitivity', 'Low')
                sensitivity_counts[sens] = sensitivity_counts.get(sens, 0) + 1

        sens_df = pd.DataFrame({
            "Sensitivity": list(sensitivity_counts.keys()),
            "Count": list(sensitivity_counts.values())
        })
        st.bar_chart(sens_df.set_index("Sensitivity"))

# ============================================
# Main Application
# ============================================

def main():
    st.title("🌐 Knowledge Graph + Genie Data Catalog")
    st.markdown(f"**Catalog:** `{CATALOG_NAME}.{SCHEMA_NAME}` | **Tables:** {TABLE_SALES}, {TABLE_FEEDBACK}")

    mode = render_sidebar()

    if mode == "Agentic Genie Mode":
        tab1, tab2, tab3 = st.tabs(["🤖 Agentic Search", "📚 Browse Tables", "📈 Analytics"])
        with tab1:
            render_agentic_tab()
        with tab2:
            render_browse_tab()
        with tab3:
            render_analytics_tab()
    else:
        tab1, tab2, tab3 = st.tabs(["🔍 KG Search", "📚 Browse Tables", "📈 Analytics"])
        with tab1:
            render_kg_only_tab()
        with tab2:
            render_browse_tab()
        with tab3:
            render_analytics_tab()

    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Mode: {mode}")


if __name__ == "__main__":
    main()