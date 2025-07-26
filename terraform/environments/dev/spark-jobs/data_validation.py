#!/usr/bin/env python3
"""
Data Validation Spark Job
Validates incoming e-commerce data for quality and completeness

This job performs:
1. Schema validation
2. Data quality checks
3. Missing value analysis
4. Duplicate detection
5. Business rule validation
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session(app_name: str) -> SparkSession:
    """Create optimized Spark session for data validation"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def define_schemas():
    """Define expected schemas for e-commerce data"""

    transactions_schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("category", StringType(), True),
            StructField("amount", DecimalType(10, 2), False),
            StructField("timestamp", TimestampType(), False),
            StructField("payment_method", StringType(), True),
            StructField("location_lat", DoubleType(), True),
            StructField("location_lon", DoubleType(), True),
            StructField("session_id", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("ip_address", StringType(), True),
        ]
    )

    customers_schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("email", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("gender", StringType(), True),
            StructField("country", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), True),
        ]
    )

    products_schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("subcategory", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DecimalType(10, 2), False),
            StructField("cost", DecimalType(10, 2), True),
            StructField("in_stock", BooleanType(), False),
            StructField("stock_quantity", IntegerType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), True),
        ]
    )

    return {
        "transactions": transactions_schema,
        "customers": customers_schema,
        "products": products_schema,
    }


def validate_schema(df, expected_schema, dataset_name):
    """Validate DataFrame schema against expected schema"""
    validation_results = {
        "dataset": dataset_name,
        "schema_valid": True,
        "missing_columns": [],
        "extra_columns": [],
        "type_mismatches": [],
    }

    expected_fields = {field.name: field.dataType for field in expected_schema.fields}
    actual_fields = {field.name: field.dataType for field in df.schema.fields}

    # Check for missing columns
    missing_columns = set(expected_fields.keys()) - set(actual_fields.keys())
    validation_results["missing_columns"] = list(missing_columns)

    # Check for extra columns
    extra_columns = set(actual_fields.keys()) - set(expected_fields.keys())
    validation_results["extra_columns"] = list(extra_columns)

    # Check for type mismatches
    for col_name in set(expected_fields.keys()) & set(actual_fields.keys()):
        if expected_fields[col_name] != actual_fields[col_name]:
            validation_results["type_mismatches"].append(
                {
                    "column": col_name,
                    "expected": str(expected_fields[col_name]),
                    "actual": str(actual_fields[col_name]),
                }
            )

    # Set overall validation status
    validation_results["schema_valid"] = (
        len(missing_columns) == 0 and len(validation_results["type_mismatches"]) == 0
    )

    return validation_results


def perform_data_quality_checks(df, dataset_name):
    """Perform comprehensive data quality checks"""

    total_records = df.count()

    quality_results = {
        "dataset": dataset_name,
        "total_records": total_records,
        "null_value_analysis": {},
        "duplicate_analysis": {},
        "data_distribution": {},
        "business_rule_violations": [],
    }

    # Null value analysis
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0

        quality_results["null_value_analysis"][col_name] = {
            "null_count": null_count,
            "null_percentage": round(null_percentage, 2),
        }

    # Duplicate analysis
    if "transaction_id" in df.columns:
        unique_ids = df.select("transaction_id").distinct().count()
        duplicates = total_records - unique_ids
        quality_results["duplicate_analysis"]["transaction_duplicates"] = duplicates

    if "user_id" in df.columns:
        unique_users = df.select("user_id").distinct().count()
        quality_results["duplicate_analysis"]["unique_users"] = unique_users

    # Business rule validation for transactions
    if dataset_name == "transactions":
        # Check for negative amounts
        negative_amounts = df.filter(col("amount") < 0).count()
        if negative_amounts > 0:
            quality_results["business_rule_violations"].append(
                {
                    "rule": "negative_transaction_amounts",
                    "violations": negative_amounts,
                    "description": "Transaction amounts should not be negative",
                }
            )

        # Check for future timestamps
        future_transactions = df.filter(col("timestamp") > current_timestamp()).count()
        if future_transactions > 0:
            quality_results["business_rule_violations"].append(
                {
                    "rule": "future_timestamps",
                    "violations": future_transactions,
                    "description": "Transaction timestamps should not be in the future",
                }
            )

        # Check for unrealistic amounts (> $10,000)
        high_amounts = df.filter(col("amount") > 10000).count()
        quality_results["data_distribution"]["high_value_transactions"] = high_amounts

    # Business rule validation for customers
    if dataset_name == "customers":
        # Check for invalid email formats
        invalid_emails = df.filter(
            ~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        ).count()
        if invalid_emails > 0:
            quality_results["business_rule_violations"].append(
                {
                    "rule": "invalid_email_format",
                    "violations": invalid_emails,
                    "description": "Email addresses should follow valid format",
                }
            )

    # Business rule validation for products
    if dataset_name == "products":
        # Check for negative prices
        negative_prices = df.filter(col("price") < 0).count()
        if negative_prices > 0:
            quality_results["business_rule_violations"].append(
                {
                    "rule": "negative_prices",
                    "violations": negative_prices,
                    "description": "Product prices should not be negative",
                }
            )

    return quality_results


def generate_data_profile(df, dataset_name):
    """Generate comprehensive data profiling information"""

    profile = {
        "dataset": dataset_name,
        "record_count": df.count(),
        "column_count": len(df.columns),
        "column_profiles": {},
    }

    for col_name in df.columns:
        col_type = dict(df.dtypes)[col_name]
        col_profile = {
            "data_type": col_type,
            "null_count": df.filter(col(col_name).isNull()).count(),
            "distinct_count": df.select(col_name).distinct().count(),
        }

        # Add type-specific profiling
        if col_type in ["int", "bigint", "double", "decimal"]:
            stats = df.select(
                min(col(col_name)).alias("min_val"),
                max(col(col_name)).alias("max_val"),
                avg(col(col_name)).alias("avg_val"),
                stddev(col(col_name)).alias("stddev_val"),
            ).collect()[0]

            col_profile.update(
                {
                    "min_value": stats["min_val"],
                    "max_value": stats["max_val"],
                    "average": stats["avg_val"],
                    "std_deviation": stats["stddev_val"],
                }
            )

        elif col_type == "string":
            string_stats = df.select(
                min(length(col(col_name))).alias("min_length"),
                max(length(col(col_name))).alias("max_length"),
                avg(length(col(col_name))).alias("avg_length"),
            ).collect()[0]

            col_profile.update(
                {
                    "min_length": string_stats["min_length"],
                    "max_length": string_stats["max_length"],
                    "average_length": string_stats["avg_length"],
                }
            )

        profile["column_profiles"][col_name] = col_profile

    return profile


def main():
    parser = argparse.ArgumentParser(description="Data Validation Spark Job")
    parser.add_argument("--input-path", required=True, help="Input data path")
    parser.add_argument(
        "--output-path", required=True, help="Output path for validated data"
    )
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    parser.add_argument(
        "--environment", default="dev", help="Environment (dev/staging/prod)"
    )

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session(f"DataValidation-{args.environment}-{args.date}")

    try:
        print(f"Starting data validation for {args.date}")

        # Define expected schemas
        schemas = define_schemas()

        # Validation results
        validation_summary = {
            "processing_date": args.date,
            "environment": args.environment,
            "start_time": datetime.now().isoformat(),
            "datasets_processed": [],
            "validation_results": {},
            "quality_results": {},
            "data_profiles": {},
        }

        # Process each dataset
        datasets = ["transactions", "customers", "products"]

        for dataset in datasets:
            dataset_path = f"{args.input_path}/{dataset}/"
            output_dataset_path = f"{args.output_path}/{dataset}/"

            print(f"Processing {dataset} dataset...")

            try:
                # Read dataset
                df = spark.read.option("multiline", "true").json(dataset_path)

                if df.count() == 0:
                    print(f"Warning: No data found for {dataset}")
                    continue

                # Schema validation
                schema_validation = validate_schema(df, schemas[dataset], dataset)
                validation_summary["validation_results"][dataset] = schema_validation

                # Data quality checks
                quality_results = perform_data_quality_checks(df, dataset)
                validation_summary["quality_results"][dataset] = quality_results

                # Data profiling
                data_profile = generate_data_profile(df, dataset)
                validation_summary["data_profiles"][dataset] = data_profile

                # Filter out invalid records for output
                validated_df = df

                # Apply basic data cleaning
                if dataset == "transactions":
                    validated_df = validated_df.filter(
                        (col("amount") >= 0) & (col("timestamp") <= current_timestamp())
                    )
                elif dataset == "customers":
                    validated_df = validated_df.filter(
                        col("email").rlike(
                            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                        )
                    )
                elif dataset == "products":
                    validated_df = validated_df.filter(col("price") >= 0)

                # Add validation metadata
                validated_df = validated_df.withColumn(
                    "validation_timestamp", current_timestamp()
                ).withColumn("validation_date", lit(args.date))

                # Write validated data
                validated_df.coalesce(10).write.mode("overwrite").option(
                    "compression", "snappy"
                ).parquet(output_dataset_path)

                validation_summary["datasets_processed"].append(dataset)
                print(
                    f"✓ {dataset} dataset validated and written to {output_dataset_path}"
                )

            except Exception as e:
                print(f"Error processing {dataset}: {str(e)}")
                validation_summary["validation_results"][dataset] = {
                    "error": str(e),
                    "status": "failed",
                }

        # Write validation summary
        validation_summary["end_time"] = datetime.now().isoformat()
        validation_summary["total_datasets"] = len(datasets)
        validation_summary["successful_datasets"] = len(
            validation_summary["datasets_processed"]
        )

        # Convert to DataFrame and save
        summary_df = spark.createDataFrame([validation_summary])
        summary_df.coalesce(1).write.mode("overwrite").json(
            f"{args.output_path}/validation_summary/"
        )

        print(
            f"✓ Data validation completed. Summary written to {args.output_path}/validation_summary/"
        )
        print(
            f"Processed {validation_summary['successful_datasets']}/{validation_summary['total_datasets']} datasets successfully"
        )

    except Exception as e:
        print(f"Fatal error in data validation: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
