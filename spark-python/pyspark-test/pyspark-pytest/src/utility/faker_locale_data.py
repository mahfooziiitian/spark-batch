import json

from faker import Faker

if __name__ == '__main__':
    # Initialize the Faker generator with a specific locale
    fake = Faker('fr_FR')  # French locale

    # Generate a fake French profile
    fake_french_profile = {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "job": fake.job()
    }

    print(fake_french_profile)

    # Save the fake profiles to a JSON file
    with open('fake_french_profile.json', 'w') as f:
        json.dump(fake_french_profile, f, indent=4)
