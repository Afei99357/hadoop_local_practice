#!/usr/bin/env python3
"""
Simple test script to verify Databricks LLM connection
Tests the basic connection with hostname, token, and endpoint
"""

import os
import sys

def test_databricks_connection():
    """Test basic Databricks LLM connection"""
    
    print("="*60)
    print("DATABRICKS LLM CONNECTION TEST")
    print("="*60)
    
    # Step 1: Check environment variables
    print("\n1. Checking environment variables...")
    
    token = os.environ.get('DATABRICKS_TOKEN')
    hostname = os.environ.get('DATABRICKS_HOSTNAME', 'https://adb-5721858900606423.3.azuredatabricks.net')
    endpoint = os.environ.get('MODEL_ENDPOINT', 'databricks-meta-llama-3-3-70b-instruct')
    
    if not token:
        print("❌ DATABRICKS_TOKEN not set!")
        print("\nTo set it:")
        print("export DATABRICKS_TOKEN='your-token-here'")
        print("\nOr in Python:")
        print("os.environ['DATABRICKS_TOKEN'] = 'your-token'")
        return False
    
    print(f"✓ DATABRICKS_TOKEN: {'*' * 10}...{token[-4:] if len(token) > 4 else '***'}")
    print(f"✓ DATABRICKS_HOSTNAME: {hostname}")
    print(f"✓ MODEL_ENDPOINT: {endpoint}")
    
    # Step 2: Set up environment for ChatDatabricks
    print("\n2. Setting up environment for ChatDatabricks...")
    
    # Remove https:// prefix if present
    if hostname.startswith('https://'):
        hostname = hostname.replace('https://', '')
    if hostname.startswith('http://'):
        hostname = hostname.replace('http://', '')
    
    # ChatDatabricks expects DATABRICKS_HOST (not HOSTNAME) and DATABRICKS_TOKEN
    os.environ['DATABRICKS_HOST'] = hostname
    os.environ['DATABRICKS_TOKEN'] = token
    
    print(f"✓ Set DATABRICKS_HOST: {hostname}")
    print(f"✓ Set DATABRICKS_TOKEN: {'*' * 10}...{token[-4:] if len(token) > 4 else '***'}")
    
    # Step 3: Try to import and initialize ChatDatabricks
    print("\n3. Testing ChatDatabricks initialization...")
    
    try:
        from databricks_langchain import ChatDatabricks
        print("✓ Successfully imported ChatDatabricks")
    except ImportError as e:
        print(f"❌ Failed to import ChatDatabricks: {e}")
        print("\nInstall with: pip install databricks-langchain")
        return False
    
    try:
        # Initialize ChatDatabricks with endpoint
        llm = ChatDatabricks(endpoint=endpoint)
        print(f"✓ Successfully initialized ChatDatabricks with endpoint: {endpoint}")
    except Exception as e:
        print(f"❌ Failed to initialize ChatDatabricks: {e}")
        print("\nPossible issues:")
        print("1. Invalid token - generate a new one from Databricks workspace")
        print("2. Wrong hostname - check your workspace URL")
        print("3. Endpoint not available - verify the model endpoint name")
        return False
    
    # Step 4: Test actual LLM call
    print("\n4. Testing LLM call...")
    
    try:
        response = llm.invoke("Hello! Please respond with 'Connection successful!' if you can hear me.")
        print("✓ LLM responded successfully!")
        print(f"Response: {response.content if hasattr(response, 'content') else str(response)}")
        return True
    except Exception as e:
        print(f"❌ LLM call failed: {e}")
        
        # Check for specific error types
        error_str = str(e)
        if "401" in error_str:
            print("\n401 Authentication Error:")
            print("- Your token might be expired or invalid")
            print("- Generate a new token from Databricks: User Settings > Access Tokens")
        elif "404" in error_str:
            print("\n404 Not Found Error:")
            print("- The endpoint name might be incorrect")
            print("- Check available endpoints in your Databricks workspace")
        elif "403" in error_str:
            print("\n403 Forbidden Error:")
            print("- You might not have access to this endpoint")
            print("- Check your permissions in Databricks")
        
        return False

def main():
    """Main test function"""
    
    # Test with different configurations if first attempt fails
    success = test_databricks_connection()
    
    if success:
        print("\n" + "="*60)
        print("🎉 SUCCESS! Your Databricks LLM connection is working!")
        print("="*60)
        print("\nYou can now use the nifi_databricks_agent.py")
    else:
        print("\n" + "="*60)
        print("❌ Connection test failed. Please check the errors above.")
        print("="*60)
        print("\nTroubleshooting steps:")
        print("1. Verify your Databricks token is valid")
        print("2. Check the hostname (should be like: adb-xxx.azuredatabricks.net)")
        print("3. Confirm the model endpoint name is correct")
        print("4. Ensure you have the required packages installed:")
        print("   pip install databricks-langchain mlflow")

if __name__ == "__main__":
    main()