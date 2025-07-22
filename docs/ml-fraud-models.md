
# Machine Learning Fraud Models

This document describes the integration of machine learning models for fraud detection within the e-commerce analytics platform.

## Overview

The platform leverages PySpark MLlib to train and deploy machine learning models, such as Random Forest Classifiers, to identify fraudulent transactions based on historical data patterns.

## Features

- **Model Training**: Train fraud detection models using historical transaction data.
- **Feature Engineering**: Automatic feature engineering for model training and prediction.
- **Model Serving**: Apply trained models to new, incoming data for real-time fraud prediction.
- **Model Evaluation**: Evaluate model performance using metrics like AUC-ROC.

## How to Use

### Train ML Fraud Model

To train a new ML fraud detection model, use the `train` command:

```bash
poetry run python src/analytics/ml_fraud_models_cli.py train \
    --transactions-path "data/delta/transactions" \
    --model-output-path "models/fraud_detection/random_forest_v1"
```

- `--transactions-path`: Path to the Delta table containing historical transaction data for training.
- `--model-output-path`: Path where the trained model will be saved.

### Predict Fraud with ML Model

To use a trained ML model to predict fraud on new data, use the `predict` command:

```bash
poetry run python src/analytics/ml_fraud_models_cli.py predict \
    --model-path "models/fraud_detection/random_forest_v1" \
    --input-path "data/delta/new_transactions" \
    --output-path "data/delta/fraud_predictions"
```

- `--model-path`: Path to the previously trained ML model.
- `--input-path`: Path to the input data (e.g., a Delta table of new transactions) for which predictions are to be made.
- `--output-path`: Path to save the prediction results.
