"""Faker data generator demonstrating customized fake data generation."""

from faker import Faker


def generate_profile(seed: int | None = None) -> dict[str, str]:
    """Generate a single fake user profile.

    Args:
        seed: Optional seed for reproducible output.

    Returns:
        Dictionary containing name, address, email, date_of_birth, company, and text.
    """
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)
    return {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "date_of_birth": str(fake.date_of_birth(minimum_age=18, maximum_age=90)),
        "company": fake.company(),
        "text": fake.text(max_nb_chars=200),
    }


def generate_profiles(count: int = 10, seed: int | None = None) -> list[dict[str, str]]:
    """Generate multiple fake user profiles.

    Args:
        count: Number of profiles to generate.
        seed: Optional seed for reproducible output.

    Returns:
        List of profile dictionaries.
    """
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)
    return [
        {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "date_of_birth": str(fake.date_of_birth(minimum_age=18, maximum_age=90)),
            "company": fake.company(),
            "text": fake.text(max_nb_chars=200),
        }
        for _ in range(count)
    ]

