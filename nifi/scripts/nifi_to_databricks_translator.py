#!/usr/bin/env python3
"""
NiFi to Databricks Workflow Translator using Local Ollama/Qwen3
This script analyzes NiFi XML templates and generates equivalent Databricks workflows
"""

import json
import xml.etree.ElementTree as ET
import requests
import argparse
import sys
from pathlib import Path

class NiFiToDatabricksTranslator:
    def __init__(self, ollama_host="http://localhost:11434", model="qwen2.5:latest"):
        self.ollama_host = ollama_host
        self.model = model
        
    def parse_nifi_template(self, xml_file):
        """Parse NiFi XML template and extract key components"""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
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
                conn_info = {
                    "source": "Unknown",
                    "destination": "Unknown",
                    "relationships": []
                }
                connections.append(conn_info)
            
            return {
                "processors": processors,
                "connections": connections
            }
            
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            return None
        except FileNotFoundError:
            print(f"File not found: {xml_file}")
            return None
    
    def query_ollama(self, prompt):
        """Send query to local Ollama instance"""
        try:
            response = requests.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "top_p": 0.9
                    }
                },
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()["response"]
            else:
                print(f"Ollama API error: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to Ollama: {e}")
            print("Make sure Ollama is running: ollama serve")
            return None
    
    def create_translation_prompt(self, nifi_data):
        """Create prompt for NiFi to Databricks translation"""
        
        processors_summary = "\n".join([
            f"- {proc['name']} ({proc['type']}): {proc['properties']}"
            for proc in nifi_data["processors"]
        ])
        
        prompt = f"""
You are an expert in both Apache NiFi and Databricks. I need help translating a NiFi workflow to Databricks.

NiFi Workflow Analysis:
{processors_summary}

Please provide:
1. Databricks Job/Workflow equivalent
2. Python/PySpark code for each NiFi processor
3. Configuration steps for Databricks
4. Best practices for the migration

Focus on:
- Auto Loader for file ingestion (equivalent to GetFile)
- Delta Lake writes (equivalent to PutHDFS)
- Error handling and monitoring
- Performance optimization

Provide practical, executable code examples.
"""
        return prompt
    
    def generate_databricks_code(self, nifi_data):
        """Generate Databricks workflow code using LLM"""
        prompt = self.create_translation_prompt(nifi_data)
        
        print("🤖 Querying Qwen3 model for Databricks translation...")
        print(f"Using model: {self.model}")
        
        response = self.query_ollama(prompt)
        
        if response:
            return response
        else:
            return "❌ Failed to get response from Ollama. Check if the service is running."
    
    def translate_template(self, xml_file, output_file=None):
        """Main translation function"""
        print(f"🔄 Parsing NiFi template: {xml_file}")
        
        # Parse NiFi template
        nifi_data = self.parse_nifi_template(xml_file)
        if not nifi_data:
            return False
        
        print(f"📊 Found {len(nifi_data['processors'])} processors")
        
        # Generate Databricks equivalent
        databricks_code = self.generate_databricks_code(nifi_data)
        
        # Save results
        if output_file:
            with open(output_file, 'w') as f:
                f.write("# Databricks Workflow - Translated from NiFi Template\n\n")
                f.write(databricks_code)
            print(f"💾 Translation saved to: {output_file}")
        else:
            print("\n" + "="*50)
            print("DATABRICKS TRANSLATION")
            print("="*50)
            print(databricks_code)
        
        return True

def main():
    parser = argparse.ArgumentParser(description='Translate NiFi XML template to Databricks workflow')
    parser.add_argument('xml_file', help='Path to NiFi XML template file')
    parser.add_argument('--output', '-o', help='Output file for Databricks code')
    parser.add_argument('--model', '-m', default='qwen2.5:latest', help='Ollama model to use')
    parser.add_argument('--host', default='http://localhost:11434', help='Ollama host URL')
    
    args = parser.parse_args()
    
    if not Path(args.xml_file).exists():
        print(f"❌ File not found: {args.xml_file}")
        sys.exit(1)
    
    translator = NiFiToDatabricksTranslator(
        ollama_host=args.host,
        model=args.model
    )
    
    success = translator.translate_template(args.xml_file, args.output)
    
    if success:
        print("✅ Translation completed successfully!")
    else:
        print("❌ Translation failed")
        sys.exit(1)

if __name__ == "__main__":
    main()