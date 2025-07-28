from mcp.server.fastmcp import FastMCP
import subprocess
import json
import re
from typing import Dict, Any, List, Union

# Configure the server
mcp = FastMCP(
    "TemplatedSearchServer",           
    host="0.0.0.0",         
    port=8000,            
    streamable_http_path="/mcp",
)

# OpenSearch connection settings
OPENSEARCH_URL = "http://localhost:9200"
OPENSEARCH_PASSWORD = "MyPassword123!"

# Define search templates
TEMPLATES = [
    
    {
        "name": "hybrid_search_template_amazon_products_text_embeddings_index",
        "description": "Hybrid lexical + semantic search with normalization on amazon_products_text_embeddings index",
        "use_cases": ["product discovery", "re-ranking", "faceted search"],
        "parameters": {
            "search_query": {"type": "string", "required": True, "description": "Customer query text"},
            "k": {"type": "integer", "required": False, "default": 10, "description": "Vector candidate pool size"},
            "boost_lexical": {"type": "float", "required": False, "description": "BM25 weight"},
            "boost_semantic": {"type": "float", "required": False, "description": "Vector-score weight"},
            "size": {"type": "integer", "required": False, "default": 10, "description": "Final hit count"}
        },
        "index_name": "amazon_products_text_embeddings",
        "template": """
        {
          "search_pipeline": "norm-pipeline",
          "query": {
            "hybrid": {
              "queries": [
                {
                  "match": {
                    "text": {
                      "query": "{{search_query}}",
                      "operator": "and"{{#boost_lexical}},
                      "boost": {{boost_lexical}}{{/boost_lexical}}
                    }
                  }
                },
                {
                  "neural": {
                    "text_embedding_bedrock": {
                      "query_text": "{{search_query}}",
                      "model_id": "nDLO9ZcBTvQhE8paOUO_",
                      "k": {{k|default:10}}{{#boost_semantic}},
                      "boost": {{boost_semantic}}{{/boost_semantic}}
                    }
                  }
                }
              ]
            }
          },
          "_source": { "excludes": ["text_embedding_bedrock"] },
          "size": {{size|default:10}}
        }
        """
    }
]

@mcp.tool()
def opensearch_list_indices() -> list:
    """List all indices in the OpenSearch cluster"""
    curl_command = f"curl -X GET '{OPENSEARCH_URL}/_cat/indices?v' -u admin:{OPENSEARCH_PASSWORD} --insecure"
    response = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    if response.returncode == 0:
        # Parse the response and format it nicely
        lines = response.stdout.strip().split('\n')
        if len(lines) > 1:  # Skip header line
            indices = []
            for line in lines[1:]:  # Skip the header
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        index_name = parts[2]  # index name
                        # Skip indices that start with a dot, security, or top
                        if not (index_name.startswith('.') or index_name.startswith('security') or index_name.startswith('top')):
                            indices.append({
                                "index": index_name,
                                "health": parts[0],  # health status
                                "status": parts[1],  # open/close status
                                "docs_count": parts[6] if len(parts) > 6 else "N/A",
                                "store_size": parts[8] if len(parts) > 8 else "N/A"
                            })
            return indices
        else:
            return ["No indices found"]
    else:
        return [f"Error: {response.stderr}"]
    

@mcp.tool()
def opensearch_get_index_mapping(index_name: str) -> dict:
    """Retrieves index mapping and setting information for an index in OpenSearch."""
    curl_command = f"curl -X GET '{OPENSEARCH_URL}/{index_name}' -u admin:{OPENSEARCH_PASSWORD} --insecure"
    response = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    
    if response.returncode == 0:
        try:
            return json.loads(response.stdout)
        except json.JSONDecodeError:
            return {"error": "Failed to parse JSON response", "raw_response": response.stdout}
    else:
        return {"error": f"Request failed: {response.stderr}", "returncode": response.returncode}


@mcp.tool()
def opensearch_search_index(index_name: str, query_dsl: Any) -> dict:
    """Searches an index using a query written in query domain-specific language (DSL) in OpenSearch."""
    # Convert query to JSON string
    query_json = json.dumps(query_dsl)
    
    curl_command = f"curl -X POST '{OPENSEARCH_URL}/{index_name}/_search' -H 'Content-Type: application/json' -d '{query_json}' -u admin:{OPENSEARCH_PASSWORD} --insecure"
    response = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    
    if response.returncode == 0:
        try:
            return json.loads(response.stdout)
        except json.JSONDecodeError:
            return {"error": "Failed to parse JSON response", "raw_response": response.stdout}
    else:
        return {"error": f"Request failed: {response.stderr}", "returncode": response.returncode}


@mcp.tool()
def templated_search(operation: str, template_name: str = None, placeholders_json: Union[str, Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute preconfigured OpenSearch templates with placeholder substitution.
    You can also do getTemplate operation to understand query structure and parameters and reuse knowledge from it for a generic search.
Operations:
• `listTemplates` - List available templates with metadata
• `getTemplate` - Get template details and query structure  
• `executeTemplate` - Run template with provided placeholders

Required fields:
• `template_name` (except for listTemplates)
• `placeholders_json` (JSON string or dict for executeTemplate)

Template syntax: {{placeholder}} with optional defaults {{name|default:value}}.
Returns search results, template metadata, or error details.
* Missing required placeholders aborts run.
* On execution error, agent may switch template or use generic search tool by modifying template as required.

"""
    # Only parse placeholders for operations that need them
    placeholders = None
    if operation == "executeTemplate":
        if not placeholders_json:
            return {"error": "placeholders_json is required for executeTemplate operation"}
        
        # Handle both string and dict inputs
        if isinstance(placeholders_json, str):
            if placeholders_json.strip() == "":
                return {"error": "placeholders_json cannot be empty for executeTemplate operation"}
            try:
                placeholders = json.loads(placeholders_json)
            except json.JSONDecodeError as e:
                return {"error": f"Invalid JSON in placeholders_json: {str(e)}"}
        elif isinstance(placeholders_json, dict):
            placeholders = placeholders_json
        else:
            return {"error": f"placeholders_json must be a string or dict, got {type(placeholders_json)}"}

    if operation == "listTemplates":
        # Return list of templates with metadata
        template_info = []
        for template in TEMPLATES:
            template_info.append({
                "name": template["name"],
                "description": template["description"],
                "use_cases": template["use_cases"],
                "parameters": [
                    {
                        "name": name,
                        "type": param["type"],
                        "required": param.get("required", False),
                        "description": param["description"],
                        "default": param.get("default", None) if "default" in param else None
                    }
                    for name, param in template["parameters"].items()
                ]
            })
        
        return {"templates": template_info}
    
    elif operation == "getTemplate":
        if not template_name:
            return {"error": "template_name is required for getTemplate operation"}
        
        # Find the template
        template_data = None
        for template in TEMPLATES:
            if template["name"] == template_name:
                template_data = template
                break
        
        if not template_data:
            return {"error": f"Template '{template_name}' not found"}
        
        return {
            "name": template_data["name"],
            "description": template_data["description"],
            "use_cases": template_data["use_cases"],
            "parameters": template_data["parameters"],
            "template": template_data["template"]
        }
    
    elif operation == "executeTemplate":
        if not template_name:
            return {"error": "template_name is required for executeTemplate operation"}
        
        if not placeholders:
            return {"error": "placeholders are required for executeTemplate operation"}
        
        # Find the template
        template_data = None
        for template in TEMPLATES:
            if template["name"] == template_name:
                template_data = template
                break
        
        if not template_data:
            return {"error": f"Template '{template_name}' not found"}
        
        # Validate required parameters
        missing_params = []
        for param_name, param_info in template_data["parameters"].items():
            if param_info.get("required", False) and param_name not in placeholders:
                print(f"Missing required parameter: {param_name}")
                missing_params.append(param_name)
        
        if missing_params:
            return {"error": f"Missing required parameters: {', '.join(missing_params)}"}
        
        # Process the template
        template_str = template_data["template"]
        
        # Process placeholders
        processed_template = process_template(template_str, placeholders)
        
        # Parse the processed template as JSON
        try:
            query_dsl = json.loads(processed_template)
        except json.JSONDecodeError as e:
            return {
                "error": f"Failed to parse template as JSON: {str(e)}",
                "processed_template": processed_template
            }
        
        # Get the index name from placeholders or default
        index_name = template_data["index_name"]
        if not index_name:
            # Check if there's a default index in the template parameters
            for param_name, param_info in template_data["parameters"].items():
                if param_name == "index_name" and "default" in param_info:
                    index_name = param_info["default"]
                    break
        
        if not index_name:
            return {"error": "index_name is required but not provided in placeholders or template defaults"}
        
        # Execute the search
        return execute_search(index_name, query_dsl, template_name)
    
    else:
        return {"error": f"Invalid operation: {operation}. Valid operations are: listTemplates, getTemplate, executeTemplate"}

def process_template(template_str: str, placeholders: Dict[str, Any]) -> str:
    """Process a template string by replacing placeholders and handling conditional blocks"""
    print(f"Processing template: {template_str}")
    print(f"Placeholders: {placeholders}")
    # Simple placeholder replacement
    for key, value in placeholders.items():
        # Handle simple placeholders
        placeholder = f"{{{{{key}}}}}"
        print(f"Placeholder: {placeholder}")
        if isinstance(value, (str, int, float, bool)):
            template_str = template_str.replace(placeholder, str(value))
        elif value is None:
            # Skip None values
            continue
        else:
            # For objects/dicts, convert to JSON string
            template_str = template_str.replace(placeholder, json.dumps(value))
        
        # Handle conditional blocks with this placeholder
        start_tag = f"{{{{#{key}}}}}"
        end_tag = f"{{{{/{key}}}}}"
        if start_tag in template_str and end_tag in template_str:
            # If value exists, remove the conditional tags but keep content
            start_idx = template_str.find(start_tag)
            end_idx = template_str.find(end_tag) + len(end_tag)
            if start_idx >= 0 and end_idx > start_idx:
                content = template_str[start_idx + len(start_tag):template_str.find(end_tag)]
                template_str = template_str[:start_idx] + content + template_str[end_idx:]
    
    # Handle default values
    default_pattern = re.compile(r'{{([^}|]+)\|default:([^}]+)}}')
    
    def replace_default(match):
        key, default = match.groups()
        key = key.strip()
        default = default.strip()
        if key in placeholders and placeholders[key] is not None:
            return str(placeholders[key])
        return default
    
    template_str = default_pattern.sub(replace_default, template_str)
    
    # Remove any remaining conditional blocks
    block_pattern = re.compile(r'{{#[^}]+}}.*?{{/[^}]+}}', re.DOTALL)
    template_str = block_pattern.sub('', template_str)
    
    # Remove any remaining placeholders
    placeholder_pattern = re.compile(r'{{[^}]+}}')
    template_str = placeholder_pattern.sub('', template_str)
    
    return template_str


def execute_search(index_name: str, query_dsl: Dict[str, Any], template_name: str) -> Dict[str, Any]:
    """Execute a search against OpenSearch"""
    print(f"Executing search for index {index_name} with query {query_dsl}")
    curl_command = f"curl -X POST '{OPENSEARCH_URL}/{index_name}/_search' -H 'Content-Type: application/json' -d '{json.dumps(query_dsl)}' -u admin:{OPENSEARCH_PASSWORD} --insecure"
    response = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    
    if response.returncode == 0:
        try:
            result = json.loads(response.stdout)
            return {
                "template_name": template_name,
                "index_name": index_name,
                "query": query_dsl,
                "result": result
            }
        except json.JSONDecodeError:
            return {"error": "Failed to parse search results", "raw_response": response.stdout}
    else:
        return {"error": f"Search request failed: {response.stderr}", "query": query_dsl}


if __name__ == "__main__":
    # Start the server using Streamable HTTP transport
    mcp.run(transport="streamable-http")
