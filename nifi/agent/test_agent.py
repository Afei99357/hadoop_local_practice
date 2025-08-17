#!/usr/bin/env python3
"""
Simple test script for NiFi to Databricks migration agent
Uses same format as working agent: AGENT.predict({"input": [...]})
"""

import os

def setup_credentials():
    """Setup Databricks credentials - you need to set these!"""
    print("="*50)
    print("CHECKING DATABRICKS CREDENTIALS")
    print("="*50)
    
    required_vars = {
        'DATABRICKS_TOKEN': 'Your Databricks personal access token',
        'DATABRICKS_HOSTNAME': 'Your Databricks workspace URL',
        'MODEL_ENDPOINT': 'databricks-meta-llama-3-3-70b-instruct'
    }
    
    missing_vars = []
    for var, description in required_vars.items():
        if not os.environ.get(var):
            missing_vars.append(f"  {var}: {description}")
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(var)
        print("\nTo fix this, set the environment variables:")
        print("export DATABRICKS_TOKEN='your-token-here'")
        print("export DATABRICKS_HOSTNAME='https://your-workspace.cloud.databricks.com'") 
        print("export MODEL_ENDPOINT='databricks-meta-llama-3-3-70b-instruct'")
        print("\nOr in Databricks notebook:")
        print("import os")
        print("os.environ['DATABRICKS_TOKEN'] = 'your-token'")
        print("os.environ['DATABRICKS_HOSTNAME'] = 'your-hostname'")
        return False
    
    print("✓ All required environment variables are set")
    print(f"  DATABRICKS_HOSTNAME: {os.environ['DATABRICKS_HOSTNAME']}")
    print(f"  MODEL_ENDPOINT: {os.environ['MODEL_ENDPOINT']}")
    print(f"  DATABRICKS_TOKEN: {'*' * 10}...{os.environ['DATABRICKS_TOKEN'][-4:]}")
    return True

# Check credentials first
if not setup_credentials():
    exit(1)

# Import the agent after credentials check
try:
    from nifi_databricks_agent import AGENT
    print("✓ Agent imported successfully")
except Exception as e:
    print(f"❌ Failed to import agent: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

def test_with_xml_file(xml_file_path):
    """Test the agent with a NiFi XML template file - using working agent format"""
    
    # Read the XML file
    print(f"Reading XML file: {xml_file_path}")
    with open(xml_file_path, 'r') as f:
        xml_content = f.read()
    
    # Test 1: Parse the XML template using simple dict format (like working agent)
    print("\n" + "="*50)
    print("STEP 1: Parsing NiFi Template")
    print("="*50)
    
    try:
        # Use same format as working agent: {"input": [{"role": "user", "content": "..."}]}
        result = AGENT.predict({"input": [{
            "role": "user", 
            "content": f"Please parse this NiFi XML template and show me what processors it contains:\n\n{xml_content}"
        }]})
        
        print("✓ Agent responded successfully!")
        print(result)
                
    except Exception as e:
        print(f"❌ Step 1 failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 2: Ask for migration patterns
    print("\n" + "="*50)
    print("STEP 2: Getting Migration Patterns")
    print("="*50)
    
    try:
        result2 = AGENT.predict({"input": [{
            "role": "user",
            "content": "Based on the parsed template, what are the best migration patterns for converting this NiFi workflow to Databricks?"
        }]})
        
        print("✓ Migration patterns retrieved successfully!")
        print(result2)
                
    except Exception as e:
        print(f"❌ Step 2 failed: {e}")
        import traceback
        traceback.print_exc()

def simple_llm_test():
    """Test just the LLM connection - using working agent format"""
    print("\n" + "="*50)
    print("TESTING LLM CONNECTION")
    print("="*50)
    
    try:
        print("Sending test request to LLM...")
        # Use same format as working agent
        result = AGENT.predict({"input": [{"role": "user", "content": "Hello! Are you working? Just respond briefly."}]})
        print("✓ LLM responded successfully!")
        print(result)
        return True
        
    except Exception as e:
        print(f"❌ LLM test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Test 1: Simple LLM test (like your working agent)
    print("="*50)
    print("NiFi to Databricks Agent Test")
    print("="*50)
    
    # Simple test like your working agent
    result = AGENT.predict({"input": [{"role": "user", "content": "What are the main NiFi processors that need migration to Databricks?"}]})
    print(result)
    
    # XML file test (optional - update path as needed)
    xml_file = "/Workspace/Users/eliao@bpcs.com/create_agent_using_langGraph/nifi_pipeline_eric_1.xml"
    if os.path.exists(xml_file):
        print("\n" + "="*50)
        print("XML FILE TEST")
        print("="*50)
        test_with_xml_file(xml_file)
    else:
        print(f"\n⚠ XML file not found: {xml_file}")
        print("Update the path or add your XML file to test parsing functionality")