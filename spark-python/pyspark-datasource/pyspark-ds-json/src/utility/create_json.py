import os

if __name__ == '__main__':
    import json

    data = {
        "name": "Jane Doe",
        "age": 25,
        "city": "Bengaluru",
        "languages": [
            "English",
            "ಕನ್ನಡ",
            "हिन्दी"
        ],
        "bio": "Data analyst with a passion for uncovering insights from data."
    }

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/encoding/utf_32/utf_32.json"

    with open(data_file, "w", encoding="utf-32") as file:
        json.dump(data, file, ensure_ascii=False)
