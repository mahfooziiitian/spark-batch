"""CSV test data generator using Faker for creating sample datasets."""

import os
from pathlib import Path

import pandas as pd
from faker import Faker


def generate_people_data(count: int = 100, seed: int | None = None) -> pd.DataFrame:
    """Generate a DataFrame of fake people data.

    Args:
        count: Number of records to generate.
        seed: Optional seed for reproducible output.

    Returns:
        pandas DataFrame with name, age, salary, and country columns.
    """
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)

    data = [
        {
            "name": fake.name(),
            "age": fake.random_int(min=18, max=70),
            "salary": fake.random_int(min=30000, max=120000),
            "country": fake.country(),
        }
        for _ in range(count)
    ]
    return pd.DataFrame(data)


def save_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    """Save a pandas DataFrame to CSV.

    Args:
        df: DataFrame to save.
        output_path: Path to the output CSV file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

