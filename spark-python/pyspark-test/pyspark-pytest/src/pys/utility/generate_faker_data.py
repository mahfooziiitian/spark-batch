"""JSON test data generator using Faker for creating sample profile datasets."""

import json
import sys
from pathlib import Path

from faker import Faker


def generate_profiles(count: int = 10, seed: int | None = None) -> list[dict[str, str]]:
    """Generate a list of fake user profiles.

    Args:
        count: Number of profiles to generate.
        seed: Optional seed for reproducible output.

    Returns:
        List of dictionaries with name, address, email, and job fields.
    """
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)
    return [
        {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "job": fake.job(),
        }
        for _ in range(count)
    ]


def save_profiles_to_json(profiles: list[dict[str, str]], output_path: Path) -> None:
    """Save profiles to a JSON file.

    Args:
        profiles: List of profile dictionaries.
        output_path: Destination file path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(profiles, indent=4))


