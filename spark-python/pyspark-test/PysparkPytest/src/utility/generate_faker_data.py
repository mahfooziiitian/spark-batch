import json

from faker import Faker

if __name__ == '__main__':

    # Initialize the Faker generator
    fake = Faker()

    # Generate a list of fake profiles
    num_profiles = 10  # Number of profiles to generate
    fake_profiles = []

    for _ in range(num_profiles):
        profile = {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "job": fake.job()
        }
        fake_profiles.append(profile)

    # Print the fake profiles
    for profile in fake_profiles:
        print(profile)

    # Save the fake profiles to a JSON file
    with open('profile.json', 'w') as f:
        json.dump(fake_profiles, f, indent=4)

