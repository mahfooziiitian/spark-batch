# Faker Data Generation

The `utility/` module contains standalone scripts that generate realistic
test data using the [Faker](https://faker.readthedocs.io/) library.

## Scripts

### faker_customized_data.py

Generates a single fake profile with various fields.

```python title="src/utility/faker_customized_data.py"
--8<-- "src/utility/faker_customized_data.py"
```

### faker_locale_data.py

Generates fake data using a specific locale (French in this example).

```python title="src/utility/faker_locale_data.py"
--8<-- "src/utility/faker_locale_data.py"
```

### generate_csv_faker_data.py

Generates a CSV file with 100 fake records containing name, age, salary,
and country.

```python title="src/utility/generate_csv_faker_data.py"
--8<-- "src/utility/generate_csv_faker_data.py"
```

### generate_faker_data.py

Generates fake records and outputs them as JSON.

```python title="src/utility/generate_faker_data.py"
--8<-- "src/utility/generate_faker_data.py"
```

## Usage

Run any script directly:

```bash
uv run python src/utility/faker_customized_data.py
uv run python src/utility/faker_locale_data.py
uv run python src/utility/generate_csv_faker_data.py
```

## Faker Features Used

| Feature | Example |
| --- | --- |
| `fake.name()` | Random full name |
| `fake.email()` | Random email address |
| `fake.address()` | Random street address |
| `fake.date_of_birth()` | Random date with age constraints |
| `fake.company()` | Random company name |
| `fake.random_int()` | Random integer in range |
| `fake.country()` | Random country name |
| `Faker('fr_FR')` | Locale-specific generator |

!!! tip "Reproducible data"
    Use `Faker.seed(42)` to generate deterministic test data across runs.
