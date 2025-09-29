import datetime
import pprint
import re

# --- Core Data Transformation (from original script) ---


def get_nested_value(data_dict: dict, path: str, default=None):
    """
    Safely retrieves a value from a nested dictionary using a dot-separated path.
    """
    keys = path.split(".")
    current_value = data_dict
    for key in keys:
        if isinstance(current_value, dict):
            current_value = current_value.get(key)
        else:
            return default
        if current_value is None:
            return default
    return current_value


def ms_timestamp_to_datetime(ms: int) -> datetime.datetime | None:
    """
    Converts a Unix timestamp in milliseconds to a Python datetime object.
    """
    if not ms:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000.0)


def map_api_to_table_record(api_response: dict, mapping_config: dict) -> dict:
    """
    Maps an API response dictionary to a new dictionary structured for a table row.
    """
    mapped_record = {}
    for column_name, mapping_info in mapping_config.items():
        raw_value = None

        if isinstance(mapping_info, str):
            json_path = mapping_info
            raw_value = get_nested_value(api_response, json_path)
            mapped_record[column_name] = raw_value

        elif isinstance(mapping_info, tuple) and len(mapping_info) == 2:
            json_path, transform_func = mapping_info
            raw_value = get_nested_value(api_response, json_path)

            if raw_value is not None:
                mapped_record[column_name] = transform_func(raw_value)
            else:
                mapped_record[column_name] = None

    return mapped_record


# --- NEW: Automatic Mapping Generation Logic ---


def get_all_api_paths(data: dict, prefix: str = "") -> list[str]:
    """
    Recursively flattens a dictionary to get all possible dot-notation paths.
    """
    paths = []
    for key, value in data.items():
        new_path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            paths.extend(get_all_api_paths(value, new_path))
        # Exclude lists for simplicity in this example
        elif not isinstance(value, list):
            paths.append(new_path)
    return paths


def calculate_similarity(text1: str, text2: str) -> float:
    """
    Calculates a similarity score between two strings based on common words.
    """
    # Pre-process text: lowercase, split into words
    words1 = set(re.findall(r"\w+", text1.lower()))
    words2 = set(re.findall(r"\w+", text2.lower()))

    if not words1 or not words2:
        return 0.0

    # Jaccard similarity: (intersection) / (union)
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))

    return intersection / union if union > 0 else 0.0


def generate_mapping_config(
    api_paths: list[str], table_schema: dict, similarity_threshold: float = 0.2
) -> dict:
    """
    Generates a mapping configuration by matching column descriptions to API paths.
    """
    generated_config = {}
    print(f"\n--- Generating Mapping for Table: '{table_schema['table_name']}' ---")

    for column in table_schema["columns"]:
        col_name = column["name"]
        col_desc = column["description"]

        best_match_path = None
        max_score = 0.0

        # The text to match against combines column name and its description for better results
        text_to_match = f"{col_name} {col_desc}"

        for path in api_paths:
            # We compare against the last part of the path and the full path
            path_for_comparison = f"{path.split('.')[-1]} {path.replace('.', ' ')}"
            score = calculate_similarity(text_to_match, path_for_comparison)

            if score > max_score:
                max_score = score
                best_match_path = path

        if max_score > similarity_threshold:
            generated_config[col_name] = best_match_path
            print(
                f"  ✅ Mapped column '{col_name}' to API path '{best_match_path}' (Score: {max_score:.2f})"
            )
        else:
            print(
                f"  ⚠️ No confident mapping found for column '{col_name}' (Best score: {max_score:.2f})"
            )

    return generated_config


# --- Main Execution ---
if __name__ == "__main__":
    # 1. SAMPLE API RESPONSE
    sample_api_response = {
        "job_id": 789123,
        "run_id": 10111213,
        "creator_user_name": "user@example.com",
        "run_name": "quarterly_report_run_2025_q3",
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS",
            "state_message": "Job run succeeded.",
        },
        "trigger": "PERIODIC",
        "start_time": 1756890000000,
        "end_time": 1756890120000,
        "execution_duration": 120000,
        "cluster_instance": {"cluster_id": "0902-205300-abcde123"},
    }

    # 2. TARGET TABLE SCHEMAS WITH DESCRIPTIONS
    # A list of tables your system might have. Note the column names may not match the API keys.
    TABLE_SCHEMAS = [
        {
            "table_name": "job_run_logs",
            "columns": [
                {
                    "name": "run_identifier",
                    "description": "The unique ID for this specific job run.",
                },
                {
                    "name": "parent_job_id",
                    "description": "The master job this run belongs to.",
                },
                {"name": "started_by", "description": "User who created the run."},
                {
                    "name": "run_label",
                    "description": "The descriptive name for this execution.",
                },
                {
                    "name": "final_status",
                    "description": "The result state of the run, e.g., SUCCESS or FAILED.",
                },
                {
                    "name": "cluster_identifier",
                    "description": "The ID of the compute cluster used.",
                },
                {
                    "name": "total_duration_ms",
                    "description": "Total time for execution in milliseconds.",
                },
            ],
        }
    ]

    # 3. AUTOMATICALLY GENERATE THE MAPPING CONFIG
    # First, get all possible paths from the API response
    api_paths = get_all_api_paths(sample_api_response)

    # We will process the first table schema from our list
    target_table_schema = TABLE_SCHEMAS[0]

    # Generate the mapping based on similarity
    auto_generated_mapping = generate_mapping_config(api_paths, target_table_schema)

    # 4. PERFORM THE MAPPING USING THE GENERATED CONFIG
    # Manual overrides or additions can be done here if needed. For example, for timestamps.
    auto_generated_mapping["start_time_dt"] = ("start_time", ms_timestamp_to_datetime)
    auto_generated_mapping["end_time_dt"] = ("end_time", ms_timestamp_to_datetime)

    final_table_record = map_api_to_table_record(
        sample_api_response, auto_generated_mapping
    )

    # 5. DISPLAY THE RESULTS
    print("\n" + "=" * 50 + "\n")
    print("--- Original API Response ---")
    pprint.pprint(sample_api_response)
    print("\n--- Auto-Generated Mapping Config ---")
    pprint.pprint(auto_generated_mapping)
    print("\n--- Final Mapped Table Record ---")
    pprint.pprint(final_table_record)
    print("\n" + "=" * 50 + "\n")
    print("This record is now ready for database insertion.")
