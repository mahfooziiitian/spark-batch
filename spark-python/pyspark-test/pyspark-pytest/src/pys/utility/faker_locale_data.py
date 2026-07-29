"""Locale-aware Faker data generator for internationalized test data."""

import json
import sys
from pathlib import Path

from faker import Faker


def generate_locale_profile(locale: str = "fr_FR") -> dict[str, str]:
    """Generate a fake profile using a specific locale.

    Args:
        locale: Faker locale string (e.g., 'fr_FR', 'de_DE', 'ja_JP').

    Returns:
        Dictionary containing localized name, address, email, and job.
    """
    fake = Faker(locale)
    return {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "job": fake.job(),
        "locale": locale,
    }


def generate_locale_profiles(locale: str = "fr_FR", count: int = 10) -> list[dict[str, str]]:
    """Generate multiple locale-aware fake profiles.

    Args:
        locale: Faker locale string.
        count: Number of profiles to generate.

    Returns:
        List of localized profile dictionaries.
    """
    fake = Faker(locale)
    return [
        {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "job": fake.job(),
            "locale": locale,
        }
        for _ in range(count)
    ]


def save_profiles_to_json(profiles: list[dict[str, str]], output_path: Path) -> None:
    """Save profiles to a JSON file.

    Args:
        profiles: List of profile dictionaries to save.
        output_path: Path to the output JSON file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(profiles, indent=4, ensure_ascii=False))

