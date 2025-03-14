import json
import os

# Define the data as a list of dictionaries
data = [
    {
        "id": 1,
        "info": {
            "name": "Alice",
            "attributes": ["a", "b", "c"],
            "details": {
                "age": 30,
                "city": "New York"
            }
        }
    },
    {
        "id": 2,
        "info": {
            "name": "Bob",
            "attributes": [],
            "details": {
                "age": 25,
                "city": "Los Angeles"
            }
        }
    },
    {
        "id": 3,
        "info": {
            "name": "Mahfooz",
            "attributes": None,
            "details": {
                "age": 38,
                "city": "Chennai"
            }
        }
    }
]

if __name__ == '__main__':
    data_file = os.environ['DATA_HOME'].replace("\\", "/")+"/FileData/Json/complex_type_null_array.json"

    with open(data_file, "w") as json_file:
        json.dump(data, json_file, indent=4)

    print("JSON file generated successfully.")
