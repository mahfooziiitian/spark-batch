# Data Processing

A transaction classification pipeline that normalises transaction data, joins
with account information, and classifies transactions as debit or credit.

## Source

```python title="src/data_processing.py"
--8<-- "src/data_processing.py"
```

## Pipeline

```mermaid
graph LR
    A[Transactions DF] --> B[Normalise]
    B --> C[Join with Accounts]
    C --> D[Classify debit/credit]
    D --> E[Output DF]
```

## Functions

### classify_debit_credit_transactions

Orchestrator that runs the full pipeline.

```python
from data_processing import classify_debit_credit_transactions

output = classify_debit_credit_transactions(transactions_df, accounts_df)
```

### normalise_transaction_information

Removes special characters from the `transaction_information` column using regex,
producing a `transaction_information_cleaned` column.

```python
from data_processing import normalise_transaction_information

cleaned = normalise_transaction_information(transactions_df)
```

| Input | Output |
| --- | --- |
| `"123-456-789"` | `"123456789"` |
| `"TEXT*?WITH.*CHARS"` | `"TEXTWITHCHARS"` |

### join_transactions_df_to_accounts_df

Joins transactions to accounts by matching the first 9 characters of
`transaction_information_cleaned` against `account_number`.

```python
from data_processing import join_transactions_df_to_accounts_df

joined = join_transactions_df_to_accounts_df(transactions_df, accounts_df)
```

### apply_debit_credit_business_classification

Classifies transactions based on `business_line_id`:

| Business Line IDs | Classification |
| --- | --- |
| 101, 102, 103 | credit |
| 202, 203 | debit |
| other | other |

## Run Tests

```bash
uv run pytest tests/test_data_processing.py -v
```
