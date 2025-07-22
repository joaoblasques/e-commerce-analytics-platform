from pyspark.sql import SparkSession

from src.analytics.churn_model import ChurnModel


def main() -> None:
    """Main function to run the churn prediction example."""
    spark = SparkSession.builder.appName("ChurnPredictionExample").getOrCreate()
    churn_model = ChurnModel(spark)

    # Run the full pipeline
    model = churn_model.run()

    # You can now use the trained model for predictions
    # For example, to predict on the entire dataset:
    # features_df = churn_model._feature_engineering(churn_model._load_data())
    # predictions = model.transform(features_df)
    # predictions.show()


if __name__ == "__main__":
    main()
