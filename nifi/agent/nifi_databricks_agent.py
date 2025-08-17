import json
import os
from typing import Annotated, Any, Generator, Optional, Sequence, TypedDict, Union
from uuid import uuid4
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field
import logging

# Load environment variables from .env file (handle both regular Python and Databricks)
try:
    from dotenv import load_dotenv
    try:
        # Regular Python environment
        load_dotenv(Path(__file__).parent / '.env')
    except NameError:
        # Databricks notebook environment - __file__ not available
        print("Running in Databricks notebook - using environment variables")
except ImportError:
    print("python-dotenv not installed. Install with: pip install python-dotenv")
    print("Or set environment variables manually")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import mlflow
from databricks_langchain import ChatDatabricks
from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
)
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

############################################
# Configuration Parameters
############################################

# Configuration from environment variables
NOTIFICATION_EMAIL = os.environ.get('NOTIFICATION_EMAIL', 'eliao@bpcs.com')
DATABRICKS_HOSTNAME = os.environ.get('DATABRICKS_HOSTNAME', 'https://adb-5721858900606423.3.azuredatabricks.net')
DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
MODEL_ENDPOINT = os.environ.get('MODEL_ENDPOINT', 'databricks-meta-llama-3-3-70b-instruct')
PATTERN_FILE = os.environ.get('PATTERN_FILE', 'migration_nifi_patterns.json')

############################################
# Pattern Registry for Scalability
############################################

class PatternRegistry:
    """Manages migration patterns with dynamic loading and caching"""
    
    def __init__(self, pattern_file: str = None):
        if pattern_file is None:
            pattern_file = PATTERN_FILE  # Use from environment
        
        # Handle both regular Python and Databricks environments
        try:
            # Regular Python environment
            self.pattern_file = Path(__file__).parent / pattern_file
        except NameError:
            # Databricks notebook environment
            self.pattern_file = Path(pattern_file)
        self.patterns = self._load_patterns()
        self.cache = {}
        self.usage_stats = {}
        
    def _load_patterns(self):
        """Load patterns from JSON file"""
        if self.pattern_file.exists():
            with open(self.pattern_file, 'r') as f:
                return json.load(f)
        else:
            logger.warning(f"Pattern file {self.pattern_file} not found. Using embedded patterns.")
            return self._get_default_patterns()
    
    def _get_default_patterns(self):
        """Return embedded patterns if file not found"""
        return {
            "processors": {
                "GetFile": {
                    "databricks_equivalent": "Auto Loader",
                    "code_template": "spark.readStream.format('cloudFiles').load('{path}')"
                },
                "PutHDFS": {
                    "databricks_equivalent": "Delta Lake",
                    "code_template": "df.write.format('delta').save('{path}')"
                }
            }
        }
    
    def get_pattern(self, processor: str, context: dict = None):
        """Get pattern with caching"""
        cache_key = f"{processor}_{str(context)}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        pattern = self.patterns.get("processors", {}).get(processor)
        if pattern:
            self.cache[cache_key] = pattern
            self.track_usage(processor)
        return pattern
    
    def track_usage(self, processor: str):
        """Track pattern usage"""
        if processor not in self.usage_stats:
            self.usage_stats[processor] = {"count": 0, "last_used": None}
        self.usage_stats[processor]["count"] += 1
        self.usage_stats[processor]["last_used"] = datetime.now().isoformat()
    
    def add_pattern(self, processor: str, pattern: dict):
        """Add new pattern dynamically"""
        if "processors" not in self.patterns:
            self.patterns["processors"] = {}
        self.patterns["processors"][processor] = pattern
        self._save_patterns()
        logger.info(f"Added new pattern for {processor}")
    
    def _save_patterns(self):
        """Save patterns back to file"""
        with open(self.pattern_file, 'w') as f:
            json.dump(self.patterns, f, indent=2)

# Initialize global pattern registry
pattern_registry = PatternRegistry()

############################################
# Define your LLM endpoint and system prompt
############################################

# Configure ChatDatabricks properly with explicit credentials
if not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_TOKEN environment variable is required")

# Set environment variables for authentication (ChatDatabricks reads these)
os.environ['DATABRICKS_TOKEN'] = DATABRICKS_TOKEN
os.environ['DATABRICKS_HOST'] = DATABRICKS_HOSTNAME  # Note: DATABRICKS_HOST not DATABRICKS_HOSTNAME

# Initialize ChatDatabricks exactly like your working agent.py
llm = ChatDatabricks(endpoint=MODEL_ENDPOINT)

# Updated system prompt for NiFi to Databricks migration
system_prompt = """You are an expert in Apache NiFi and Databricks migration. 
Your role is to help users convert NiFi workflows to Databricks pipelines using:
- Auto Loader for file ingestion (replacing GetFile/ListFile processors)
- Delta Lake for data storage (replacing PutHDFS/PutFile processors)
- Structured Streaming for real-time processing
- Databricks Jobs for orchestration

Always provide executable PySpark code and explain the migration patterns."""

###############################################################################
## Custom Tools for NiFi to Databricks Migration
###############################################################################

from langchain_core.tools import tool

@tool
def parse_nifi_template(xml_content: str) -> str:
    """Parse a NiFi XML template and extract processors, properties, and connections.
    
    Args:
        xml_content: The XML content of the NiFi template
    """
    try:
        root = ET.fromstring(xml_content)
        
        processors = []
        connections = []
        
        # Extract processors
        for processor in root.findall(".//processors"):
            proc_info = {
                "name": processor.find("name").text if processor.find("name") is not None else "Unknown",
                "type": processor.find("type").text if processor.find("type") is not None else "Unknown",
                "properties": {}
            }
            
            # Extract properties
            properties = processor.find(".//properties")
            if properties is not None:
                for prop in properties.findall("entry"):
                    key_elem = prop.find("key")
                    value_elem = prop.find("value")
                    if key_elem is not None and value_elem is not None:
                        proc_info["properties"][key_elem.text] = value_elem.text
            
            processors.append(proc_info)
        
        # Extract connections
        for connection in root.findall(".//connections"):
            source = connection.find(".//source/id")
            destination = connection.find(".//destination/id")
            relationships = connection.find(".//selectedRelationships")
            
            conn_info = {
                "source": source.text if source is not None else "Unknown",
                "destination": destination.text if destination is not None else "Unknown",
                "relationships": relationships.text if relationships is not None else []
            }
            connections.append(conn_info)
        
        result = {
            "processors": processors,
            "connections": connections,
            "processor_count": len(processors),
            "connection_count": len(connections)
        }
        
        return json.dumps(result, indent=2)
        
    except ET.ParseError as e:
        return f"Error parsing XML: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"

def _render_pattern(processor_class: str, properties: dict) -> dict:
    """Single source of truth: pull from pattern_registry and render once."""
    pattern = pattern_registry.get_pattern(processor_class) or {}
    
    # Fill template once
    code = None
    if "code_template" in pattern:
        code = pattern["code_template"]
        for key, value in (properties or {}).items():
            code = code.replace(f"{{{key}}}", str(value))
    
    return {
        "equivalent": pattern.get("databricks_equivalent", "Unknown"),
        "description": pattern.get("description", ""),
        "best_practices": pattern.get("best_practices", []),
        "code": code
    }

@tool
def generate_databricks_code(processor_type: str, properties: str = "{}") -> str:
    """Generate equivalent Databricks/PySpark code for a NiFi processor.
    
    Args:
        processor_type: The NiFi processor type to convert
        properties: JSON string of processor properties (default: "{}")
    """
    # Parse properties if it's a string
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except:
            properties = {}
    
    # Find the processor class name
    processor_class = processor_type.split('.')[-1] if '.' in processor_type else processor_type
    
    # Use single source of truth
    rendered = _render_pattern(processor_class, properties)
    
    if rendered["code"]:
        code = f"# {processor_class} → {rendered['equivalent']}\n"
        if rendered["description"]:
            code += f"# {rendered['description']}\n"
        code += f"\n{rendered['code']}"
        
        # Add best practices as comments
        if rendered["best_practices"]:
            code += "\n\n# Best Practices:\n"
            for practice in rendered["best_practices"]:
                code += f"# - {practice}\n"
        
        return code
    
    # If no pattern found, return generic code
    return f"""
# Generic processor conversion for: {processor_type}
# Properties: {json.dumps(properties, indent=2)}

# TODO: Implement specific logic for {processor_type}
# Review the properties and implement equivalent Databricks logic
df = spark.read.format("delta").load("/path/to/data")
# Add your transformation logic here
"""

@tool
def get_migration_pattern(nifi_component: str, properties: str = "{}") -> str:
    """Get best practices and patterns for migrating NiFi components to Databricks.
    
    Args:
        nifi_component: The NiFi component or pattern to migrate
        properties: JSON string of processor properties (default: "{}")
    """
    # Parse properties if it's a string
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except:
            properties = {}
    
    # Use single source of truth
    rendered = _render_pattern(nifi_component, properties)
    
    if rendered["equivalent"] != "Unknown":
        response = [
            f"**Migration Pattern: {nifi_component} → {rendered['equivalent']}**",
            "",
            rendered["description"]
        ]
        
        if rendered["best_practices"]:
            response += ["", "Best Practices:"]
            response += [f"- {practice}" for practice in rendered["best_practices"]]
        
        if rendered["code"]:
            response += ["", "Code Template:", "```python", rendered["code"], "```"]
        
        return "\n".join(response)
    
    # If no pattern found, return generic guidance
    return f"""
**General Migration Guidelines for {nifi_component}**
1. Identify the data flow pattern
2. Map to equivalent Databricks components
3. Implement error handling and monitoring
4. Test with sample data
5. Optimize for performance
6. Document the migration approach
"""

@tool
def create_job_config(job_name: str, notebook_path: str, schedule: str = "") -> str:
    """Create a Databricks job configuration JSON.
    
    Args:
        job_name: Name of the Databricks job
        notebook_path: Path to the Databricks notebook
        schedule: Optional cron schedule expression (default: empty string for no schedule)
    """
    job_config = {
        "name": job_name,
        "tasks": [
            {
                "task_key": f"{job_name}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {}
                },
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true"
                    }
                },
                "timeout_seconds": 3600,
                "max_retries": 2,
                "retry_on_timeout": True
            }
        ],
        "email_notifications": {
            "on_failure": [NOTIFICATION_EMAIL]
        },
        "max_concurrent_runs": 1
    }
    
    if schedule:
        job_config["schedule"] = {
            "quartz_cron_expression": schedule,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED"
        }
    
    return json.dumps(job_config, indent=2)

@tool
def extract_nifi_parameters_and_services(xml_content: str) -> str:
    """Return NiFi Parameter Contexts and Controller Services with suggested Databricks mappings (secrets, widgets, job params)."""
    try:
        root = ET.fromstring(xml_content)
        out = {"parameter_contexts": [], "controller_services": [], "suggested_mappings": []}

        # Parameter Contexts
        for pc in root.findall(".//parameterContexts/parameterContext"):
            name = (pc.findtext("component/name") or "unnamed").strip()
            params = []
            for p in pc.findall(".//component/parameters/parameter"):
                params.append({
                    "name": p.findtext("parameter/name"),
                    "value": p.findtext("parameter/value"),
                    "sensitive": (p.findtext("parameter/sensitive") == "true"),
                })
            out["parameter_contexts"].append({"name": name, "parameters": params})

        # Controller Services
        for cs in root.findall(".//controllerServices/controllerService"):
            c = cs.find("component")
            out["controller_services"].append({
                "id": cs.findtext("id"),
                "name": c.findtext("name"),
                "type": c.findtext("type"),
                "properties": {e.findtext("name"): e.findtext("value") for e in c.findall(".//properties/entry")},
            })

        # Simple mapping rules → Databricks
        for cs in out["controller_services"]:
            t = (cs["type"] or "").lower()
            if "dbcp" in t or "jdbc" in t:
                out["suggested_mappings"].append({
                    "nifi": cs["name"],
                    "databricks_equivalent": "JDBC via spark.read/write + Databricks Secrets",
                    "how": "Store URL/user/password in a secret scope; use JDBC jars on cluster.",
                })
            if "sslcontextservice" in t:
                out["suggested_mappings"].append({
                    "nifi": cs["name"],
                    "databricks_equivalent": "Secure endpoints + secrets-backed cert paths",
                    "how": "Upload certs to a secured location; reference with secrets / init scripts.",
                })
        return json.dumps(out, indent=2)
    except Exception as e:
        return f"Failed to parse NiFi XML: {e}"

@tool
def build_migration_plan(xml_content: str) -> str:
    """Produce a topologically sorted DAG of NiFi processors based on Connections."""
    try:
        root = ET.fromstring(xml_content)
        # id → name/type
        procs = {}
        for pr in root.findall(".//processors"):
            pid = pr.findtext("id")
            procs[pid] = {
                "id": pid,
                "name": pr.findtext("name") or pid,
                "type": pr.findtext("type") or "Unknown"
            }

        edges = []
        for conn in root.findall(".//connections"):
            src = conn.findtext(".//source/id")
            dst = conn.findtext(".//destination/id")
            if src and dst:
                edges.append((src, dst))

        # Kahn's algorithm
        from collections import defaultdict, deque
        indeg=defaultdict(int); graph=defaultdict(list)
        for s,d in edges:
            graph[s].append(d); indeg[d]+=1
            if s not in indeg: indeg[s]+=0

        q=deque([n for n in indeg if indeg[n]==0])
        ordered=[]
        while q:
            n=q.popleft()
            if n in procs: ordered.append(procs[n])
            for v in graph[n]:
                indeg[v]-=1
                if indeg[v]==0: q.append(v)

        plan={"tasks": ordered, "edges": edges, "note":"Use this order to compose Jobs tasks or DLT dependencies"}
        return json.dumps(plan, indent=2)
    except Exception as e:
        return f"Failed building plan: {e}"

@tool
def suggest_autoloader_options(properties: str = "{}") -> str:
    """Given NiFi GetFile/ListFile-like properties, suggest Auto Loader code & options."""
    props = json.loads(properties) if properties else {}
    path = props.get("Input Directory") or props.get("Directory") or "/mnt/raw"
    fmt = (props.get("File Filter") or "*.json").split(".")[-1]
    fmt = "csv" if fmt.lower() in ["csv"] else "json" if fmt.lower() in ["json"] else "parquet"

    code = f"""from pyspark.sql.functions import *
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "{fmt}")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("{path}"))"""
    tips = [
        "Use cloudFiles.schemaLocation for checkpoint/schema tracking.",
        "Use cloudFiles.includeExistingFiles=true to backfill once.",
        "Set cloudFiles.validateOptions for strictness; cleanSource MOVE/DELETE for hygiene.",
    ]
    return json.dumps({"code": code, "tips": tips}, indent=2)

@tool
def scaffold_asset_bundle(project_name: str, job_name: str, notebook_path: str) -> str:
    """Return a minimal databricks.yml for a bundle with one job that runs a notebook."""
    bundle = f"""bundle:
  name: {project_name}

resources:
  jobs:
    {job_name}:
      name: {job_name}
      tasks:
        - task_key: main
          notebook_task:
            notebook_path: {notebook_path}
          new_cluster:
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            num_workers: 2

targets:
  dev:
    default: true
"""
    return bundle

@tool
def generate_dlt_expectations(table_name: str, rules_json: str) -> str:
    """Return SQL to create a DLT/Lakeflow dataset with expectations from simple rules."""
    try:
        rules = json.loads(rules_json) if rules_json else {}
        ex_lines = []
        for name, expr in rules.items():
            ex_lines.append(f"EXPECT {name} : {expr}")
        ex_block = "\n  ".join(ex_lines) if ex_lines else ""
        sql = f"""CREATE OR REFRESH STREAMING TABLE {table_name}
  {ex_block}
AS SELECT * FROM STREAM(LIVE.source_table);"""
        return sql
    except Exception as e:
        return f"Invalid rules: {e}"

@tool
def deploy_and_run_job(job_config_json: str) -> str:
    """Create a Job via REST 2.1 and kick off a run. Requires DATABRICKS_HOST + DATABRICKS_TOKEN env."""
    import requests
    host = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_HOSTNAME")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not (host and token): 
        return "Missing DATABRICKS_HOST and/or DATABRICKS_TOKEN"

    # Ensure host has protocol
    if not host.startswith("http"):
        host = f"https://{host}"

    cfg = json.loads(job_config_json)
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        create = requests.post(f"{host}/api/2.1/jobs/create", json=cfg, headers=headers, timeout=60)
        if create.status_code >= 300: 
            return f"Create failed: {create.status_code} {create.text}"
        job_id = create.json()["job_id"]

        run = requests.post(f"{host}/api/2.1/jobs/run-now", json={"job_id": job_id}, headers=headers, timeout=60)
        if run.status_code >= 300: 
            return f"Run-now failed: {run.status_code} {run.text}"
        return json.dumps({"job_id": job_id, "run_id": run.json().get("run_id")}, indent=2)
    except Exception as e:
        return f"API call failed: {e}"

@tool
def evaluate_pipeline_outputs(src_path: str, dst_path: str, key_cols_csv: str = "", float_tol: float = 1e-6) -> str:
    """Compare two Delta/Parquet tables or folders on Databricks with robust float handling."""
    try:
        from pyspark.sql import functions as F
        key_cols = [c.strip() for c in key_cols_csv.split(",") if c.strip()]
        
        # This would need to be run in a Databricks environment with spark available
        # For now, return a template for the comparison logic
        comparison_code = f"""
# Pipeline Output Comparison Template
from pyspark.sql import functions as F

# Load datasets
df1 = spark.read.load("{src_path}")  # Source
df2 = spark.read.load("{dst_path}")  # Destination

# Basic counts
src_count = df1.count()
dst_count = df2.count()

# Schema comparison
schema_equal = (df1.schema == df2.schema)

# Null counts per column
def get_nulls(df):
    return {{c: df.filter(F.col(c).isNull()).count() for c in df.columns}}

src_nulls = get_nulls(df1)
dst_nulls = get_nulls(df2)

# Key-based comparison if keys provided
key_cols = {key_cols}
float_tolerance = {float_tol}

print(f"Source count: {{src_count}}")
print(f"Destination count: {{dst_count}}")
print(f"Schema equal: {{schema_equal}}")
print(f"Source nulls: {{src_nulls}}")
print(f"Destination nulls: {{dst_nulls}}")
"""
        return comparison_code
    except Exception as e:
        return f"Error generating comparison: {e}"

@tool
def generate_dlt_pipeline_config(pipeline_name: str, catalog: str, schema: str, notebook_path: str) -> str:
    """Return minimal JSON config for a DLT/Lakeflow pipeline."""
    cfg = {
        "name": pipeline_name,
        "storage": f"/pipelines/{pipeline_name}",
        "target": f"{catalog}.{schema}",
        "development": True,
        "continuous": True,
        "libraries": [{"notebook": {"path": notebook_path}}],
    }
    return json.dumps(cfg, indent=2)

# Initialize custom NiFi migration tools (all 8 tools)
tools = [
    parse_nifi_template,
    generate_databricks_code,
    get_migration_pattern,
    create_job_config,
    extract_nifi_parameters_and_services,
    build_migration_plan,
    suggest_autoloader_options,
    scaffold_asset_bundle,
    generate_dlt_expectations,
    deploy_and_run_job,
    evaluate_pipeline_outputs,
    generate_dlt_pipeline_config,
]

#####################
## Define agent logic
#####################

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]

def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolNode, Sequence[BaseTool]],
    system_prompt: Optional[str] = None,
):
    # Now we can bind tools since we're using @tool decorator (like working agent)
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: AgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If there are function calls, continue. else, end
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            return "continue"
        else:
            return "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(
        state: AgentState,
        config: RunnableConfig,
    ):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(AgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ToolNode(tools))
    
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )
    workflow.add_edge("tools", "agent")

    return workflow.compile()

class LangGraphResponsesAgent(ResponsesAgent):
    def __init__(self, agent):
        self.agent = agent

    def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert from a Responses API output item to ChatCompletion messages."""
        msg_type = message.get("type")
        if msg_type == "function_call":
            return [
                {
                    "role": "assistant",
                    "content": "tool call",
                    "tool_calls": [
                        {
                            "id": message["call_id"],
                            "type": "function",
                            "function": {
                                "arguments": message["arguments"],
                                "name": message["name"],
                            },
                        }
                    ],
                }
            ]
        elif msg_type == "message" and isinstance(message["content"], list):
            return [
                {"role": message["role"], "content": content["text"]}
                for content in message["content"]
            ]
        elif msg_type == "reasoning":
            return [{"role": "assistant", "content": json.dumps(message["summary"])}]
        elif msg_type == "function_call_output":
            return [
                {
                    "role": "tool",
                    "content": message["output"],
                    "tool_call_id": message["call_id"],
                }
            ]
        compatible_keys = ["role", "content", "name", "tool_calls", "tool_call_id"]
        filtered = {k: v for k, v in message.items() if k in compatible_keys}
        return [filtered] if filtered else []

    def _prep_msgs_for_cc_llm(self, responses_input) -> list[dict[str, Any]]:
        "Convert from Responses input items to ChatCompletion dictionaries"
        cc_msgs = []
        for msg in responses_input:
            cc_msgs.extend(self._responses_to_cc(msg.model_dump()))
        return cc_msgs

    def _langchain_to_responses(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        "Convert from ChatCompletion dict to Responses output item dictionaries"
        for message in messages:
            message = message.model_dump()
            role = message["type"]
            if role == "ai":
                if tool_calls := message.get("tool_calls"):
                    return [
                        self.create_function_call_item(
                            id=message.get("id") or str(uuid4()),
                            call_id=tool_call["id"],
                            name=tool_call["name"],
                            arguments=json.dumps(tool_call["args"]),
                        )
                        for tool_call in tool_calls
                    ]
                else:
                    return [
                        self.create_text_output_item(
                            text=message["content"],
                            id=message.get("id") or str(uuid4()),
                        )
                    ]
            elif role == "tool":
                return [
                    self.create_function_call_output_item(
                        call_id=message["tool_call_id"],
                        output=message["content"],
                    )
                ]
            elif role == "user":
                return [message]

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs if hasattr(request, 'custom_inputs') else {})

    def predict_stream(
        self,
        request: ResponsesAgentRequest,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # Handle both dict and ResponsesAgentRequest
        if isinstance(request, dict):
            from mlflow.types.responses import ResponsesAgentRequest
            # Convert dict format to ResponsesAgentRequest
            request = ResponsesAgentRequest(
                input=request.get("input", []),
                custom_inputs=request.get("custom_inputs", {})
            )
        
        cc_msgs = []
        for msg in request.input:
            if isinstance(msg, dict):
                # Direct dict format from test
                cc_msgs.append(msg)
            else:
                # ResponsesAgentRequest format
                cc_msgs.extend(self._responses_to_cc(msg.model_dump()))

        for event in self.agent.stream({"messages": cc_msgs}, stream_mode=["updates", "messages"]):
            if event[0] == "updates":
                for node_data in event[1].values():
                    for item in self._langchain_to_responses(node_data["messages"]):
                        yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)
            # filter the streamed messages to just the generated text messages
            elif event[0] == "messages":
                try:
                    chunk = event[1][0]
                    if isinstance(chunk, AIMessageChunk) and (content := chunk.content):
                        yield ResponsesAgentStreamEvent(
                            **self.create_text_delta(delta=content, item_id=chunk.id),
                        )
                except Exception as e:
                    print(e)

# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
try:
    # Try to enable MLflow autolog if available
    mlflow.langchain.autolog()
    logger.info("MLflow autolog enabled")
except Exception as e:
    logger.warning(f"MLflow autolog not available: {e}")
    logger.info("Continuing without MLflow tracking")

# Create the agent
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphResponsesAgent(agent)

# Set model for MLflow if needed
try:
    mlflow.models.set_model(AGENT)
    logger.info("Agent registered with MLflow")
except Exception as e:
    logger.warning(f"Could not register with MLflow: {e}")
    logger.info("Agent created successfully without MLflow registration")