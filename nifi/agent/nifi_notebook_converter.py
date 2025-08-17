#!/usr/bin/env python3
"""
NiFi to Databricks Notebook Converter
=====================================

Notebook-friendly converter that creates a complete Databricks project structure
from NiFi XML templates. Generates modular code, configurations, and deployment files.

Usage:
    # In Databricks notebook:
    from nifi_notebook_converter import convert_in_notebook
    
    convert_in_notebook(
        xml_path="/path/to/nifi_template.xml",
        out_dir="/dbfs/FileStore/nifi2dbx_out",
        project="my_nifi_migration",
        job="my_migration_job", 
        notebook_path="/Workspace/Users/you@company.com/migration/main",
        emit_job_json=True,
        deploy_job=False,
        also_import_notebook=True
    )
"""

import json
import re
import os
import base64
import requests
from pathlib import Path

from nifi_databricks_agent import (
    parse_nifi_template,
    build_migration_plan,
    extract_nifi_parameters_and_services,
    generate_databricks_code,
    suggest_autoloader_options,
    scaffold_asset_bundle,
    create_job_config,
    generate_dlt_pipeline_config,
    deploy_and_run_job,
)

def _safe_name(s: str) -> str:
    """Convert any string to a safe filename/identifier."""
    s = s or "unnamed"
    s = re.sub(r"[^a-zA-Z0-9_\-]+", "_", s.strip())
    return s[:80].strip("_") or "unnamed"

def _write_text(path: Path, text: str):
    """Write text to file, creating directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def convert_in_notebook(
    xml_path: str,
    out_dir: str = "/dbfs/FileStore/nifi2dbx_out",
    project: str = "nifi2dbx",
    job: str = "nifi2dbx_job",
    notebook_path: str = "/Workspace/Users/you@example.com/nifi2dbx/main",  # workspace notebook object path (no .py)
    emit_job_json: bool = True,
    deploy_job: bool = False,
    also_import_notebook: bool = False,  # import generated main.py into a Workspace notebook
):
    """
    Convert NiFi XML to complete Databricks project structure.
    
    Args:
        xml_path: Path to NiFi XML template file
        out_dir: Output directory for generated project
        project: Project name for Asset Bundle
        job: Job name for Databricks Jobs
        notebook_path: Workspace path for main notebook (no .py extension)
        emit_job_json: Whether to generate Jobs API JSON
        deploy_job: Whether to actually deploy and run the job
        also_import_notebook: Whether to import main.py to Workspace
    
    Returns:
        str: Path to output directory
    """
    
    print(f"🔄 Converting NiFi XML: {xml_path}")
    print(f"📁 Output directory: {out_dir}")
    
    out = Path(out_dir)
    (out / "src/steps").mkdir(parents=True, exist_ok=True)

    # Read NiFi XML (supports DBFS paths like /dbfs/... as well)
    xml_text = Path(xml_path).read_text(encoding="utf-8")

    # 1) Parse the flow
    print("📋 Parsing NiFi template...")
    parsed_js = json.loads(parse_nifi_template.func(xml_text))
    processors = parsed_js.get("processors", [])
    # connections = parsed_js.get("connections", [])

    # 2) Build DAG plan (topological order of processors)
    print("🔗 Building migration plan...")
    plan_js = json.loads(build_migration_plan.func(xml_text))
    ordered = plan_js.get("tasks", [])

    # 3) Parameter Contexts & Controller Services
    print("⚙️ Extracting parameters and services...")
    params_js = json.loads(extract_nifi_parameters_and_services.func(xml_text))

    # 4) Per-processor code generation
    print("🔧 Generating processor code...")
    step_files = []
    for idx, task in enumerate(ordered, start=10):
        full = next(
            (p for p in processors if p.get("type") == task.get("type") or p.get("name") == task.get("name")),
            task,
        )
        props = full.get("properties", {})
        proc_type = task.get("type", "Unknown")
        name_sn = _safe_name(task.get("name") or proc_type.split(".")[-1])

        code = generate_databricks_code.func(
            processor_type=proc_type,
            properties=json.dumps(props),
        )

        # Fallback hint for file ingestion
        if ("GetFile" in proc_type or "ListFile" in proc_type) and "cloudFiles" not in code:
            al = json.loads(suggest_autoloader_options.func(json.dumps(props)))
            code = f"# Suggested Auto Loader for {proc_type}\n{al['code']}\n# Tips:\n# - " + "\n# - ".join(al["tips"])

        step_path = out / f"src/steps/{idx:02d}_{name_sn}.py"
        _write_text(step_path, code)
        step_files.append(step_path)

    # 5) Bundle + README
    print("📦 Creating Asset Bundle configuration...")
    bundle_yaml = scaffold_asset_bundle.func(project, job, notebook_path)
    _write_text(out / "databricks.yml", bundle_yaml)

    readme = [
        f"# {project}",
        "",
        "Generated from NiFi flow using NiFi to Databricks Migration Agent.",
        "",
        "## Contents",
        "- `src/steps/` - Individual processor translations",
        "- `databricks.yml` - Databricks Asset Bundle (jobs-as-code)",
        "- `jobs/` - Optional Jobs 2.1 JSON",
        "- `conf/` - Mappings from NiFi Parameter Contexts & Controller Services",
        "- `notebooks/` - Main orchestrator notebook",
        "",
        "## Next steps",
        "1. Review/merge the per-step code into your main notebook or DLT pipeline.",
        "2. Use `databricks bundle validate && databricks bundle deploy` (if using Bundles).",
        "3. Or create a job from `jobs/job.json` and run it.",
        "",
        "## File Structure",
        "```",
        f"{project}/",
        "├── databricks.yml          # Asset Bundle config",
        "├── README.md               # This file",
        "├── src/steps/             # Individual processor code",
        "├── conf/                  # Parameter contexts & DLT configs", 
        "├── jobs/                  # Databricks Jobs JSON",
        "└── notebooks/             # Main orchestrator notebook",
        "```",
        "",
        "## Migration Summary",
        f"- **Processors found:** {len(processors)}",
        f"- **Execution order:** {len(ordered)} tasks",
        f"- **Parameter contexts:** {len(params_js.get('parameter_contexts', []))}",
        f"- **Controller services:** {len(params_js.get('controller_services', []))}",
    ]
    _write_text(out / "README.md", "\n".join(readme))

    # 6) Save Parameter Contexts & Controller Services
    print("💾 Saving configuration mappings...")
    _write_text(out / "conf/parameter_contexts.json", json.dumps(params_js, indent=2))

    # 7) Generate a simple orchestrator script (to import into a Workspace notebook)
    main_py = [
        "# Databricks notebook source",
        "",
        f"# {project} - Main Orchestrator",
        "# This is a starter orchestrator. Import your step modules and wire your stream here.",
        "",
        "# COMMAND ----------",
        "",
        "# Import necessary libraries",
        "from pyspark.sql import SparkSession",
        "from pyspark.sql.functions import *",
        "from pyspark.sql.types import *",
        "",
        "# COMMAND ----------",
        "",
        "# Initialize Spark session (if not already available)",
        "# spark = SparkSession.builder.appName('NiFi Migration').getOrCreate()",
        "",
        "print('🚀 NiFi to Databricks Migration Pipeline')",
        "print('📁 Per-processor translations are in src/steps/')",
        "",
        "# TODO: Import and orchestrate your step modules here",
        "# Example:",
        "# from steps.01_GetFile import process_files",
        "# from steps.02_Transform import transform_data", 
        "# from steps.03_PutHDFS import write_data",
        "",
        "# COMMAND ----------",
        "",
        "# Your pipeline logic here",
        "print('✅ Pipeline setup complete')",
    ]
    _write_text(out / "notebooks/main.py", "\n".join(main_py))

    # 8) Optional: Jobs JSON + optional run
    if emit_job_json or deploy_job:
        print("📄 Generating Jobs API configuration...")
        job_cfg = create_job_config.func(job, notebook_path)
        _write_text(out / "jobs/job.json", job_cfg)
        if deploy_job:
            print("🚀 Deploying and running job...")
            res = deploy_and_run_job.func(job_cfg)
            print("Deploy result:", res)

    # 9) Optional: DLT config
    print("🔄 Generating DLT pipeline configuration...")
    dlt_cfg = generate_dlt_pipeline_config.func(
        pipeline_name=f"{project}_pipeline",
        catalog="main",
        schema="default",
        notebook_path=notebook_path,
    )
    _write_text(out / "conf/dlt_pipeline.json", dlt_cfg)

    # 10) Optional: import generated main.py into a Workspace notebook path
    if also_import_notebook:
        print("📓 Importing notebook to Workspace...")
        host = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_HOSTNAME")
        token = os.environ.get("DATABRICKS_TOKEN")
        if not (host and token):
            print("⚠️ Skipping notebook import (missing DATABRICKS_HOST/TOKEN).")
        else:
            if not host.startswith("http"):
                host = "https://" + host
            src = (out / "notebooks/main.py").read_text(encoding="utf-8")
            payload = {
                "path": notebook_path,          # e.g., /Workspace/Users/<you>/nifi2dbx/main
                "format": "SOURCE",
                "language": "PYTHON",
                "overwrite": True,
                "content": base64.b64encode(src.encode()).decode(),
            }
            try:
                r = requests.post(f"{host}/api/2.0/workspace/import",
                                  headers={"Authorization": f"Bearer {token}"},
                                  json=payload, timeout=60)
                if r.status_code < 300:
                    print(f"✅ Notebook imported to: {notebook_path}")
                else:
                    print(f"❌ Notebook import failed: {r.status_code} {r.text[:300]}")
            except Exception as e:
                print(f"❌ Notebook import error: {e}")

    # Summary
    print("\n" + "="*70)
    print("✅ CONVERSION COMPLETE")
    print("="*70)
    print(f"📁 Output folder: {out}")
    print(f"📋 Processors converted: {len(processors)}")
    print(f"🔗 Execution steps: {len(ordered)}")
    print("🧩 Generated files:")
    for p in step_files[:10]:
        print("  -", str(p).replace(str(out), ""))
    if len(step_files) > 10:
        print(f"  ... (+{len(step_files)-10} more)")
    
    print(f"\n📖 Next steps:")
    print(f"1. Review generated code in: {out}/src/steps/")
    print(f"2. Check configuration: {out}/conf/")
    print(f"3. Deploy using: {out}/databricks.yml")
    
    return str(out)

# Example usage configuration
def run_example():
    """Example usage with common parameters."""
    
    # Set your parameters (adjust paths as needed)
    XML_PATH = "/Workspace/Users/eliao@bpcs.com/create_agent_using_langGraph/nifi_pipeline_eric_1.xml"
    OUT_DIR = "/Workspace/Users/eliao@bpcs.com/create_agent_using_langGraph/nifi2dbx_out"
    PROJECT = "nifi2dbx"
    JOB = "nifi2dbx_job"
    NOTEBOOK_PATH = "/Workspace/Users/eliao@bpcs.com/nifi2dbx/main"
    
    return convert_in_notebook(
        xml_path=XML_PATH,
        out_dir=OUT_DIR,
        project=PROJECT,
        job=JOB,
        notebook_path=NOTEBOOK_PATH,
        emit_job_json=True,
        deploy_job=False,          # set True only if DATABRICKS_HOST/TOKEN env are set
        also_import_notebook=True  # auto-import the generated main.py into a Workspace notebook
    )

if __name__ == "__main__":
    # Run example when executed directly
    result = run_example()
    print(f"Conversion completed. Output: {result}")