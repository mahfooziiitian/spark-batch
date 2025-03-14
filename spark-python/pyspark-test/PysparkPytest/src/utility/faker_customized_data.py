from faker import Faker

if __name__ == '__main__':
    # Initialize the Faker generator
    fake = Faker()
    
    # Generate a single fake profile with various details
    profile = fake.profile()
    print(profile)

    # Generate specific types of fake data
    fake_name = fake.name()
    fake_address = fake.address()
    fake_email = fake.email()
    fake_date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90)
    fake_company = fake.company()
    fake_text = fake.text(max_nb_chars=200)

    print(f"Name: {fake_name}")
    print(f"Address: {fake_address}")
    print(f"Email: {fake_email}")
    print(f"Date of Birth: {fake_date_of_birth}")
    print(f"Company: {fake_company}")
    print(f"Text: {fake_text}")
