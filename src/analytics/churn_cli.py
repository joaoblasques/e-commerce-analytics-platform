import click
from pyspark.sql import SparkSession

from src.analytics.churn_model import ChurnModel


@click.group()
@click.pass_context
def churn(ctx: click.Context) -> None:
    """Churn prediction commands."""
    pass


@churn.command()
@click.option(
    "--delta-path",
    default="data/delta/transactions",
    help="Path to the transactions Delta table.",
)
def train(delta_path: str) -> None:
    """Trains the churn prediction model."""
    spark = SparkSession.builder.appName("ChurnModelTraining").getOrCreate()
    churn_model = ChurnModel(spark, delta_path)
    churn_model.run()
    print("Churn model training complete.")


if __name__ == "__main__":
    churn()
