#!/usr/bin/env python3
"""
Command-line tool to convert NiFi XML to Databricks migration report

Usage:
    python3 convert_nifi_xml.py <xml_file_path>
    
Example:
    python3 convert_nifi_xml.py nifi_pipeline_eric_1.xml
"""

import os
import sys
import argparse
from extract_clean_output import clean_agent_output, DEFAULT_OUTPUT_FILE, DATABRICKS_TOKEN, DATABRICKS_HOST

# Set credentials (imported from extract_clean_output configuration)
os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN
os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

def main():
    parser = argparse.ArgumentParser(description='Convert NiFi XML to Databricks migration report')
    parser.add_argument('xml_file', help='Path to NiFi XML template file')
    parser.add_argument('-o', '--output', default=DEFAULT_OUTPUT_FILE, 
                       help=f'Output file name (default: {DEFAULT_OUTPUT_FILE})')
    parser.add_argument('--no-save', action='store_true', 
                       help='Print to console only, do not save file')
    
    args = parser.parse_args()
    
    # Check if XML file exists
    if not os.path.exists(args.xml_file):
        print(f"❌ Error: XML file not found: {args.xml_file}")
        sys.exit(1)
    
    print("🔄 Loading NiFi XML template...")
    try:
        with open(args.xml_file, "r", encoding="utf-8") as f:
            xml_content = f.read()
    except Exception as e:
        print(f"❌ Error reading XML file: {e}")
        sys.exit(1)
    
    print("🤖 Running NiFi Databricks Agent...")
    try:
        from nifi_databricks_agent import AGENT
    except ImportError as e:
        print(f"❌ Error importing agent: {e}")
        print("Make sure nifi_databricks_agent.py is in the same directory")
        sys.exit(1)
    
    # Create user prompt
    user_prompt = f"""
    I have a NiFi XML pipeline. Please:
    1. Parse the processors and connections
    2. For each processor, generate equivalent Databricks PySpark code
    3. Create a complete Databricks job configuration
    4. Provide migration recommendations

    NiFi XML:
    ```xml
    {xml_content}
    ```
    """

    try:
        # Run agent
        result = AGENT.predict({
            "input": [{"role": "user", "content": user_prompt}]
        })
        
        # Convert to clean output
        report = clean_agent_output(
            result, 
            save_to_file=not args.no_save, 
            output_file=args.output
        )
        
        # Always print to console
        print("\n" + "="*70)
        print("📋 MIGRATION REPORT")
        print("="*70)
        print(report)
        
        if not args.no_save:
            print(f"\n✅ Full report saved to: {args.output}")
        
    except Exception as e:
        print(f"❌ Error running agent: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()