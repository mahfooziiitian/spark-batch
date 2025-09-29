import datetime
import pprint


def get_nested_value(data_dict: dict, path: str, default=None):
    """
    Safely retrieves a value from a nested dictionary using a dot-separated path.

    Args:
        data_dict: The dictionary to search within.
        path: The dot-separated path to the desired value (e.g., 'state.result_state').
        default: The value to return if the path is not found.

    Returns:
        The value found at the specified path, or the default value.
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
    Handles None or 0 inputs gracefully.
    """
    if not ms:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000.0)


def map_api_to_table_record(api_response: dict, mapping_config: dict) -> dict:
    """
    Maps an API response dictionary to a new dictionary structured for a table row.

    Args:
        api_response: The JSON response from the API, as a Python dictionary.
        mapping_config: A configuration dictionary that defines the mapping rules.

    Returns:
        A new dictionary with keys matching the table columns and processed values.
    """
    mapped_record = {}
    for column_name, mapping_info in mapping_config.items():
        raw_value = None

        if isinstance(mapping_info, str):
            # Simple direct mapping from a JSON path
            json_path = mapping_info
            raw_value = get_nested_value(api_response, json_path)
            mapped_record[column_name] = raw_value

        elif isinstance(mapping_info, tuple) and len(mapping_info) == 2:
            # Mapping with a transformation function
            json_path, transform_func = mapping_info
            raw_value = get_nested_value(api_response, json_path)

            # Apply the transformation function only if a value was found
            if raw_value is not None:
                mapped_record[column_name] = transform_func(raw_value)
            else:
                mapped_record[column_name] = None

    return mapped_record


# --- Main Execution ---
if __name__ == "__main__":
    # 1. SAMPLE API RESPONSE (from a Databricks Jobs run)
    # This is the JSON data you would get from your API call.
    sample_api_response = {
        "job_id": 789123,
        "run_id": 10111213,
        "creator_user_name": "user@example.com",
        "run_name": "quarterly_report_run_2025_q3",
        "run_page_url": "https://<your-workspace>.databricks.com/?o=<id>#job/789123/run/10111213",
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS",
            "state_message": "Job run succeeded.",
        },
        "trigger": "PERIODIC",
        "start_time": 1756890000000,  # Represents: Tuesday, September 2, 2025 9:00:00 PM GMT
        "end_time": 1756890120000,  # Represents: Tuesday, September 2, 2025 9:02:00 PM GMT
        "execution_duration": 120000,
        "cluster_spec": {
            "new_cluster": {
                "spark_version": "14.1.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            }
        },
        "cluster_instance": {"cluster_id": "0902-205300-abcde123"},
        "tasks": [
            {
                "run_id": 10111213,
                "task_key": "ingest_data",
                "notebook_task": {"notebook_path": "/Repos/data_eng/ingestion_logic"},
                "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            }
        ],
    }

    # 2. TARGET TABLE COLUMN DEFINITIONS
    # These are the columns in your destination table (e.g., system.jobs.job_run_history)
    table_columns = [
        "run_id",
        "job_id",
        "creator_user_name",
        "run_name",
        "status",
        "result_state",
        "status_message",
        "trigger_type",
        "start_time_dt",
        "end_time_dt",
        "duration_ms",
        "cluster_id",
    ]

    # 3. MAPPING CONFIGURATION
    # This dictionary is the core of the solution.
    # Key: The name of the table column.
    # Value:
    #   - A string representing the dot-separated path in the JSON.
    #   - A tuple containing (json_path, transformation_function).
    MAPPING_CONFIG = {
        "run_id": "run_id",
        "job_id": "job_id",
        "creator_user_name": "creator_user_name",
        "run_name": "run_name",
        "status": "state.life_cycle_state",
        "result_state": "state.result_state",
        "status_message": "state.state_message",
        "trigger_type": "trigger",
        "start_time_dt": ("start_time", ms_timestamp_to_datetime),
        "end_time_dt": ("end_time", ms_timestamp_to_datetime),
        "duration_ms": "execution_duration",
        "cluster_id": "cluster_instance.cluster_id",
    }

    # 4. PERFORM THE MAPPING
    final_table_record = map_api_to_table_record(sample_api_response, MAPPING_CONFIG)

    # 5. DISPLAY THE RESULTS
    print("--- Original API Response ---")
    pprint.pprint(sample_api_response)
    print("\n" + "=" * 40 + "\n")
    print("--- Mapped Table Record ---")
    pprint.pprint(final_table_record)
    print("\n" + "=" * 40 + "\n")
    print("This record is now ready for database insertion.")
