from mcp.server.fastmcp import FastMCP
import subprocess
from typing import Union, Any
from pydantic import BaseModel, Field

# Configure the server for stdio transport
mcp = FastMCP("OpenSearchServer")

OPENSEARCH_URL = "http://k8s-mlplaygr-opensear-f79b1ba7c0-1930554821.us-east-1.elb.amazonaws.com"
OPENSEARCH_PASSWORD = "myStrongPassword123!"
# OPENSEARCH_URL = "http://localhost:9200"
# OPENSEARCH_PASSWORD = "MyPassword123!"


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
            import json
            return json.loads(response.stdout)
        except json.JSONDecodeError:
            return {"error": "Failed to parse JSON response", "raw_response": response.stdout}
    else:
        return {"error": f"Request failed: {response.stderr}", "returncode": response.returncode}


class SearchIndexArgs(BaseModel):
    index_name: str = Field(description='The name of the index to search in')
    query_dsl: Any = Field(description='The search query in OpenSearch query DSL format')

@mcp.tool()
def opensearch_search_index(index_name: str, query_dsl: Any) -> dict:
    """Searches an index using a query written in query domain-specific language (DSL) in OpenSearch."""
    import json
    
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


if __name__ == "__main__":
    # Start the server using stdio transport
    mcp.run(transport="stdio") 