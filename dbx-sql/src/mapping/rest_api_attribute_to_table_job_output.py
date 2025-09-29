import os

from rich import print

from mapping.attribute_map_util import fetch_api_response_generic, process_all_tables


def main():
    dbx_config = {
        "server_hostname": os.environ["M_DBX_HOST"],
        "http_path": os.environ["M_DBX_HTTP_PATH"],
        "access_token": os.environ["M_DBX_TOKEN"],
    }
    api_config = {
        "url": f"https://{dbx_config['server_hostname']}/api/2.2/jobs/runs/get-output",
        "headers": {"Authorization": f"Bearer {dbx_config['access_token']}"},
        "params": {"run_id": 894187550364888},
    }
    tables_config = [
        {"table": "system.lakeflow.job_run_timeline", "data_path": "metadata"},
        {
            "table": "system.lakeflow.job_task_run_timeline",
            "data_path": "metadata.tasks",
        },
    ]
    api_response = fetch_api_response_generic(api_config)
    if not api_response:
        print("No API response received. Exiting.")
        return
    else:
        print("\n--- API Response received---")
        print(api_response)
    process_all_tables(api_response, dbx_config, tables_config)


if __name__ == "__main__":
    main()
