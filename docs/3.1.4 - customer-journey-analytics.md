
# Customer Journey Analytics

This document describes the customer journey analytics capabilities within the e-commerce analytics platform.

## Overview

Customer journey analytics provides insights into how users interact with the platform, from initial touchpoints to conversion events. It helps identify bottlenecks and optimize user experience.

## Features

- **Touchpoint Tracking**: Order events within a customer session.
- **Funnel Analysis**: Analyze conversion rates through predefined steps.
- **Conversion Rate Calculation**: Calculate conversion between any two events.

## How to Use

### Analyze Funnel

To analyze a customer funnel, use the `analyze-funnel` command:

```bash
poetry run python src/analytics/customer_journey_cli.py analyze-funnel --funnel-steps "page_view,add_to_cart,purchase"
```

### Calculate Conversion Rate

To calculate the conversion rate between two events, use the `calculate-conversion` command:

```bash
poetry run python src/analytics/customer_journey_cli.py calculate-conversion --start-event "add_to_cart" --end-event "purchase"
```
