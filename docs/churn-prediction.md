# Churn Prediction Model

This document describes the churn prediction model used in the e-commerce analytics platform.

## Overview

The churn prediction model is designed to identify customers who are likely to stop using the platform. It uses a logistic regression model trained on customer transaction data.

## Features

The model uses the following features:

- **Recency**: Days since the customer's last purchase.
- **Frequency**: Total number of transactions.
- **Monetary**: Total amount spent by the customer.

## How to Use

To train the churn model, run the following command:

```bash
poetry run python src/analytics/churn_cli.py train
```
