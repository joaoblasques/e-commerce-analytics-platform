
# Rule-Based Fraud Detection Engine

This document describes the rule-based fraud detection engine within the e-commerce analytics platform.

## Overview

The rule-based fraud detection engine allows defining custom rules to identify suspicious transactions based on predefined conditions. Each rule can have a severity, contributing to a total fraud score for a transaction.

## Features

- **Configurable Rules**: Define rules using SQL-like conditions.
- **Severity Levels**: Assign severity (low, medium, high) to each rule.
- **Fraud Scoring**: Calculate a cumulative fraud score for each transaction.
- **Alerts**: Identify which rules were triggered for a transaction.

## How to Use

To detect fraud using predefined rules, use the `detect` command:

```bash
poetry run python src/analytics/fraud_detection_rules_cli.py detect \
    --input-path "data/delta/transactions" \
    --output-path "data/delta/fraud_results" \
    --rules-config '[
        {"name": "high_amount_transaction", "condition": "amount > 1000", "severity": "high"},
        {"name": "multiple_items_high_quantity", "condition": "quantity > 10 AND item_count > 5", "severity": "medium"}
    ]'
```

- `--input-path`: Path to the input data (e.g., a Delta table of transactions).
- `--output-path`: Path to write the fraud detection results.
- `--rules-config`: A JSON string representing a list of rule dictionaries. Each dictionary must have `name` and `condition` keys, and optionally a `severity` key (defaults to "medium"). Alternatively, this can be a path to a JSON file containing the rules.
