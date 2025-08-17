# NiFi to Databricks Translator using Local LLM

This script uses your local Qwen3 model (via Ollama) to translate NiFi XML templates to Databricks workflows.

## Prerequisites

1. **Ollama installed and running:**
   ```bash
   # Start Ollama service
   ollama serve
   
   # Verify Qwen model is available
   ollama list
   ```

2. **Python dependencies:**
   ```bash
   pip install requests
   ```

## Usage

### Basic Translation
```bash
# Translate your downloaded NiFi template
python3 nifi_to_databricks_translator.py /path/to/your/downloaded_template.xml
```

### Save to File
```bash
# Save translation to a Databricks notebook file
python3 nifi_to_databricks_translator.py \
    /path/to/your/template.xml \
    --output ../databricks_migration/notebooks/translated_pipeline.py
```

### Specify Different Model
```bash
# Use a different Qwen model
python3 nifi_to_databricks_translator.py \
    /path/to/your/template.xml \
    --model qwen2.5:7b \
    --output translated_workflow.py
```

## Example Usage with Your Template

```bash
cd /home/eric/Projects/hadoop_practice/nifi/scripts

# Translate your CSV-to-HDFS template
python3 nifi_to_databricks_translator.py \
    ../flows/csv_to_hdfs_template.xml \
    --output ../databricks_migration/notebooks/csv_to_delta_databricks.py
```

## What the Script Does

1. **Parses NiFi XML:** Extracts processors, properties, and connections
2. **Analyzes Workflow:** Identifies data flow patterns and configurations
3. **Queries LLM:** Sends structured prompt to your local Qwen3 model
4. **Generates Code:** Creates equivalent Databricks/PySpark code
5. **Provides Migration Guide:** Includes setup steps and best practices

## Sample Output

The translator will generate:
- **Auto Loader code** (equivalent to GetFile processor)
- **Delta Lake write operations** (equivalent to PutHDFS)
- **Error handling patterns**
- **Job scheduling configuration**
- **Performance optimization tips**

## Troubleshooting

### Ollama Not Running
```bash
# Error: Error connecting to Ollama
ollama serve

# In another terminal, test:
curl http://localhost:11434/api/tags
```

### Model Not Available
```bash
# Pull the model if not available
ollama pull qwen2.5:latest

# Or use a different model you have
ollama list
```

### Connection Issues
```bash
# Check if Ollama is listening on the right port
netstat -tlnp | grep 11434

# Or specify different host
python3 nifi_to_databricks_translator.py \
    template.xml \
    --host http://127.0.0.1:11434
```

## Advanced Usage

### Custom Prompts
You can modify the `create_translation_prompt()` method in the script to:
- Focus on specific Databricks features
- Include your organization's coding standards
- Add specific migration requirements

### Batch Processing
```bash
# Translate multiple templates
for template in ../flows/*.xml; do
    python3 nifi_to_databricks_translator.py "$template" \
        --output "translated_$(basename "$template" .xml).py"
done
```

This tool leverages your local LLM to provide intelligent, context-aware translations from NiFi workflows to Databricks, helping you understand migration patterns and best practices.