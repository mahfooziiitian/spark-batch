import datetime

import requests
from rich import print
from rich.pretty import pprint

# --- Optional Libraries ---
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity

    AI_LIBRARIES_INSTALLED = True
except ImportError:
    AI_LIBRARIES_INSTALLED = False

try:
    from databricks import sql
    from databricks.sql.exc import Error as DatabricksError

    DB_CONNECTOR_INSTALLED = True
except ImportError:
    DB_CONNECTOR_INSTALLED = False


# --- Utility Functions ---
def get_nested_value(data: dict, path: str, default=None):
    keys = path.split(".")
    current_value = data
    for key in keys:
        if isinstance(current_value, dict):
            current_value = current_value.get(key)
        else:
            return default
        if current_value is None:
            return default

    if isinstance(current_value, list):
        return current_value[0]
    return current_value


def ms_timestamp_to_datetime(ms: int) -> datetime.datetime | None:
    if not ms:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000.0)


def flatten_dict_paths_ignore_lists(data: dict, prefix: str = "") -> list[str]:
    paths = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                paths.extend(flatten_dict_paths_ignore_lists(value, new_path))
            elif isinstance(value, list):
                continue
            else:
                paths.append(new_path)
    return paths


# --- Databricks API ---
def fetch_api_response_generic(api_config: dict) -> dict | None:
    """Fetch API response using generic config."""
    url = api_config["url"]
    print(f"url: {url}")
    headers = api_config.get("headers", {})
    print(f"headers: {headers}")
    params = api_config.get("params", {})
    print(f"params: {params}")
    data = api_config.get("data", {})
    try:
        response = requests.get(
            url, headers=headers, params=params, data=data, timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"❌ Error fetching API response: {e}")
        return None


def get_table_schema_from_catalog(
    server_hostname, http_path, access_token, catalog, schema, table
):
    if not DB_CONNECTOR_INSTALLED:
        print(
            "Required database connector not found. Please run: pip install databricks-sql-connector"
        )
        return None
    try:
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        ) as connection:
            with connection.cursor() as cursor:
                query = f"""
                SELECT column_name, comment AS column_description
                FROM system.information_schema.columns
                WHERE table_catalog = '{catalog}'
                  AND table_schema = '{schema}'
                  AND table_name = '{table}'
                ORDER BY ordinal_position;
                """
                cursor.execute(query)
                result = cursor.fetchall()
                if not result:
                    print(
                        f"  ❌ Error: Table '{table}' not found in schema '{schema}' or no columns returned."
                    )
                    return None
                columns = [
                    {
                        "name": row.column_name,
                        "description": row.column_description or "",
                    }
                    for row in result
                ]
                return {"table_name": table, "columns": columns}
    except DatabricksError as e:
        print(f"  ❌ Database Error: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")
        return None


# --- Mapping Logic ---
def map_api_to_table_record(api_response: dict, mapping_config: dict) -> dict:
    mapped_record = {}
    for column_name, mapping_info in mapping_config.items():
        if isinstance(mapping_info, str):
            raw_value = get_nested_value(api_response, mapping_info)
            mapped_record[column_name] = raw_value
        elif isinstance(mapping_info, tuple) and len(mapping_info) == 2:
            json_path, transform_func = mapping_info
            raw_value = get_nested_value(api_response, json_path)
            mapped_record[column_name] = (
                transform_func(raw_value) if raw_value is not None else None
            )
    return mapped_record


def generate_mapping_config_ai(
    api_paths: list[str], table_schema: dict, similarity_threshold: float = 0.5
) -> dict:
    if not AI_LIBRARIES_INSTALLED:
        print(
            "Required AI libraries not found. Please run: pip install sentence-transformers scikit-learn"
        )
        return {}
    model = SentenceTransformer("all-MiniLM-L6-v2")
    column_details = table_schema["columns"]
    column_texts = [f"{col['name']} {col['description']}" for col in column_details]
    api_path_texts = [
        f"{path.split('.')[-1].replace('_', ' ')} {path.replace('.', ' ')}"
        for path in api_paths
    ]
    column_embeddings = model.encode(column_texts, convert_to_tensor=True)
    api_path_embeddings = model.encode(api_path_texts, convert_to_tensor=True)
    similarity_matrix = cosine_similarity(api_path_embeddings, column_embeddings)
    generated_config = {}
    for i, api_path in enumerate(api_paths):
        best_match_index = similarity_matrix[i].argmax()
        max_score = similarity_matrix[i][best_match_index]
        col_name = column_details[best_match_index]["name"]
        if max_score > similarity_threshold:
            generated_config[api_path] = col_name
        else:
            generated_config[api_path] = "Not Found"
    return generated_config


# --- Orchestration ---
def process_table(api_response, table, dbx_config):
    catalog, schema, table_name = table.split(".")
    table_schema = get_table_schema_from_catalog(
        server_hostname=dbx_config["server_hostname"],
        http_path=dbx_config["http_path"],
        access_token=dbx_config["access_token"],
        catalog=catalog,
        schema=schema,
        table=table_name,
    )
    if not table_schema:
        return
    api_paths = flatten_dict_paths_ignore_lists(api_response)
    print(f"api paths: {api_paths}")
    auto_generated_mapping = generate_mapping_config_ai(api_paths, table_schema)
    if auto_generated_mapping:
        print("\n--- AI-Generated Mapping Config ---")
        pprint(auto_generated_mapping)
        print("This record is now ready for database insertion.")
    else:
        print("Could not generate mapping. Please ensure AI libraries are installed.")


def process_all_tables(api_response, dbx_config, tables_config):
    for table_info in tables_config:
        table_name = table_info["table"]
        data_path = table_info.get("data_path")
        data = get_nested_value(api_response, data_path) if data_path else api_response
        print(f"\n--- Processing Table: {table_name} ---")
        print(f"data_path: {data_path}")
        if data:
            process_table(data, table_name, dbx_config)
