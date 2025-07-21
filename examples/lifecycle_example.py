"""
Data Lifecycle Management Example

This example demonstrates the complete data lifecycle management
workflow including retention policies, archiving, lineage tracking,
and cost optimization.
"""

import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

# Import lifecycle management components
from src.data_lake.delta import DeltaLakeManager
from src.data_lake.lifecycle_manager import DataLifecycleManager, RetentionRule, ArchivePolicy
from src.data_lake.lifecycle_config import LifecycleConfigManager


def create_sample_data(spark):
    """Create sample e-commerce transaction data for demonstration."""
    
    # Define schema
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
    # Generate sample data with different ages
    current_date = datetime.now()
    sample_data = []
    
    # Hot data (last 10 days)
    for days_ago in range(10):
        timestamp = current_date - timedelta(days=days_ago)
        for i in range(100):
            sample_data.append((
                f"txn_{days_ago}_{i:03d}",
                f"user_{i % 50:03d}",
                f"prod_{i % 20:03d}",
                round(10.0 + (i % 100), 2),
                timestamp,
                timestamp
            ))
    
    # Warm data (30-60 days ago)
    for days_ago in range(30, 60, 5):
        timestamp = current_date - timedelta(days=days_ago)
        for i in range(50):
            sample_data.append((
                f"txn_warm_{days_ago}_{i:03d}",
                f"user_{i % 50:03d}",
                f"prod_{i % 20:03d}",
                round(10.0 + (i % 100), 2),
                timestamp,
                timestamp
            ))
    
    # Cold data (120-180 days ago)
    for days_ago in range(120, 180, 10):
        timestamp = current_date - timedelta(days=days_ago)
        for i in range(25):
            sample_data.append((
                f"txn_cold_{days_ago}_{i:03d}",
                f"user_{i % 50:03d}",
                f"prod_{i % 20:03d}",
                round(10.0 + (i % 100), 2),
                timestamp,
                timestamp
            ))
    
    return spark.createDataFrame(sample_data, schema)


def main():
    """Main example workflow."""
    
    print("🚀 Data Lifecycle Management Example")
    print("=" * 50)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("LifecycleExample") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # Step 1: Initialize managers
        print("\n1. 📋 Initializing Delta Lake and Lifecycle Managers...")
        
        delta_manager = DeltaLakeManager(
            base_path="file:///tmp/delta-lake-example",
            spark_session=spark
        )
        
        lifecycle_manager = DataLifecycleManager(delta_manager)
        
        # Enable lifecycle tracking
        delta_manager.enable_lifecycle_tracking(lifecycle_manager)
        
        print("✅ Managers initialized successfully")
        
        # Step 2: Load default configurations
        print("\n2. ⚙️  Loading Default Configurations...")
        
        config_manager = LifecycleConfigManager()
        
        # Load retention rules for transactions
        transactions_rule = config_manager.get_retention_rule("transactions")
        lifecycle_manager.add_retention_rule(transactions_rule)
        
        # Load archive policy
        archive_policy = config_manager.get_archive_policy("transactions")
        lifecycle_manager.add_archive_policy(archive_policy)
        
        print(f"✅ Loaded retention rule: {transactions_rule.hot_days}/{transactions_rule.warm_days}/{transactions_rule.cold_days} days")
        print(f"✅ Loaded archive policy: {archive_policy.format} format, {archive_policy.compression} compression")
        
        # Step 3: Create sample data and table
        print("\n3. 📊 Creating Sample Transaction Data...")
        
        sample_df = create_sample_data(spark)
        record_count = sample_df.count()
        
        print(f"✅ Generated {record_count:,} sample transaction records")
        
        # Create Delta table with lifecycle management
        success = delta_manager.create_delta_table_with_lifecycle(
            df=sample_df,
            table_name="transactions",
            partition_columns=["transaction_date"],
            enable_retention=True
        )
        
        if success:
            print("✅ Created transactions table with lifecycle management enabled")
        else:
            print("❌ Failed to create transactions table")
            return
        
        # Step 4: Demonstrate lineage tracking
        print("\n4. 🔗 Demonstrating Lineage Tracking...")
        
        # Simulate data transformation and write
        processed_df = sample_df.select("user_id", "amount", "transaction_date") \
            .groupBy("user_id", "transaction_date") \
            .agg({"amount": "sum"}) \
            .withColumnRenamed("sum(amount)", "daily_total")
        
        # Write with lineage tracking
        delta_manager.write_to_delta_with_lifecycle(
            df=processed_df,
            table_name="user_daily_totals",
            mode="overwrite",
            source_tables=["transactions"]
        )
        
        print("✅ Created user_daily_totals table with lineage tracking")
        
        # Get lineage information
        lineage = lifecycle_manager.get_lineage_graph("transactions")
        print(f"📈 Lineage tracked: {len(lineage['operations'])} operations")
        
        # Step 5: Apply retention policies
        print("\n5. 🗂️  Applying Retention Policies...")
        
        # First, do a dry run
        dry_run_results = lifecycle_manager.apply_retention_policies(dry_run=True)
        
        if "transactions" in dry_run_results:
            result = dry_run_results["transactions"]
            print("📊 Dry Run Results:")
            print(f"   Hot records (recent): {result.get('hot_records', 0):,}")
            print(f"   Warm records: {result.get('warm_records', 0):,}")
            print(f"   Cold records: {result.get('cold_records', 0):,}")
            print(f"   Would archive: {result.get('archived_records', 0):,}")
            print(f"   Would delete: {result.get('deleted_records', 0):,}")
        
        # Step 6: Storage cost optimization
        print("\n6. 💰 Analyzing Storage Costs...")
        
        storage_metrics = lifecycle_manager.optimize_storage_costs()
        
        if storage_metrics:
            total_size_gb = sum(m.size_bytes for m in storage_metrics.values()) / (1024**3)
            total_cost = sum(m.storage_cost_usd for m in storage_metrics.values())
            
            print(f"📊 Storage Analysis:")
            print(f"   Total tables analyzed: {len(storage_metrics)}")
            print(f"   Total storage size: {total_size_gb:.2f} GB")
            print(f"   Estimated monthly cost: ${total_cost:.2f}")
            
            for table_name, metrics in storage_metrics.items():
                size_gb = metrics.size_bytes / (1024**3)
                print(f"   📋 {table_name}: {size_gb:.2f} GB, ${metrics.storage_cost_usd:.2f}, {metrics.tier.value} tier")
        
        # Step 7: Generate comprehensive report
        print("\n7. 📋 Generating Lifecycle Report...")
        
        report = lifecycle_manager.generate_lifecycle_report()
        
        print("📊 Lifecycle Report Summary:")
        print(f"   Report generated: {report['timestamp']}")
        print(f"   Tables managed: {report['storage_summary']['total_tables']}")
        print(f"   Total storage: {report['storage_summary']['total_size_gb']:.2f} GB")
        print(f"   Monthly cost: ${report['storage_summary']['total_monthly_cost_usd']:.2f}")
        
        # Show recommendations
        recommendations = report['recommendations']
        if recommendations:
            print(f"\n💡 Optimization Recommendations ({len(recommendations)}):")
            for i, rec in enumerate(recommendations, 1):
                print(f"   {i}. {rec}")
        else:
            print("✅ No optimization recommendations - system is well optimized!")
        
        # Step 8: Save detailed report
        print("\n8. 💾 Saving Detailed Report...")
        
        report_file = f"lifecycle_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"✅ Detailed report saved to: {report_file}")
        
        # Step 9: Demonstrate table lifecycle summary
        print("\n9. 📖 Table Lifecycle Summary...")
        
        config_summary = config_manager.get_table_lifecycle_summary("transactions")
        
        print(f"📋 Transactions Table Lifecycle:")
        print(f"   Hot period: {config_summary['retention']['hot_days']} days")
        print(f"   Warm period: {config_summary['retention']['warm_days']} days")  
        print(f"   Cold period: {config_summary['retention']['cold_days']} days")
        if config_summary['retention']['archive_after_days']:
            print(f"   Archive after: {config_summary['retention']['archive_after_days']} days")
        if config_summary['retention']['delete_after_days']:
            print(f"   Delete after: {config_summary['retention']['delete_after_days']} days")
        
        print(f"   Archive format: {config_summary['archiving']['format']}")
        print(f"   Compression: {config_summary['archiving']['compression']}")
        
        # Step 10: Configuration validation
        print("\n10. ✅ Validating Configuration...")
        
        issues = config_manager.validate_configuration()
        if issues:
            print(f"⚠️  Found {len(issues)} configuration issues:")
            for issue in issues[:3]:  # Show first 3 issues
                print(f"   • {issue}")
            if len(issues) > 3:
                print(f"   ... and {len(issues) - 3} more issues")
        else:
            print("✅ All configurations are valid!")
        
        print("\n" + "=" * 50)
        print("🎉 Data Lifecycle Management Example Completed Successfully!")
        print("=" * 50)
        
        print("\n📚 Next Steps:")
        print("1. Customize retention policies for your specific needs")
        print("2. Set up automated scheduling for retention policy application") 
        print("3. Integrate with monitoring and alerting systems")
        print("4. Configure cost optimization automation")
        print("5. Set up compliance reporting for your organization")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        spark.stop()


if __name__ == "__main__":
    main()