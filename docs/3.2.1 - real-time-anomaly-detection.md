
# Real-time Anomaly Detection

This document describes the real-time anomaly detection system within the e-commerce analytics platform.

## Overview

The real-time anomaly detection system identifies unusual patterns in streaming data, which can indicate fraudulent activities, system malfunctions, or other critical events.

## Algorithms

The system uses statistical methods to detect anomalies, such as identifying data points that are multiple standard deviations away from the mean within a defined window.

## How to Use

To start the real-time anomaly detection pipeline, use the `detect` command:

```bash
poetry run python src/analytics/anomaly_detection_cli.py detect --feature-cols "price,quantity" --stream-source-path "data/delta/transactions" --output-path "data/delta/anomalies"
```

- `--feature-cols`: Comma-separated list of numeric columns to use for anomaly detection.
- `--stream-source-path`: Path to the streaming data source (e.g., a Delta table).
- `--output-path`: Optional path to write the detected anomalies to (e.g., another Delta table).
