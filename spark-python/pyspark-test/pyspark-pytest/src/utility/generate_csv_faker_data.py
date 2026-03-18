import os

from faker import Faker
import pandas as pd

if __name__ == '__main__':

    # Initialize the Faker generator
    fake = Faker()

    # Generate a list of fake data
    num_records = 100
    data = []

    csv_data_file = f"{os.environ['DATA_HOME']}\\FileData\\Csv\\peoples.csv"

    for _ in range(num_records):
        record = {
            "name": fake.name(),
            "age": fake.random_int(min=18, max=70),
            "salary": fake.random_int(min=30000, max=120000),
            "country": fake.country()
        }
        data.append(record)

    # Convert the list of records to a DataFrame
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    df.to_csv(csv_data_file, index=False)

    print(f"CSV file {csv_data_file} has been created.")
