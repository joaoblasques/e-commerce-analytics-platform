import click
from pyspark.sql import SparkSession

from src.analytics.customer_journey import CustomerJourney


@click.group()
def journey():
    """Customer journey analytics commands."""
    pass


@journey.command()
@click.option(
    "--user-behavior-path",
    default="data/delta/user_behavior",
    help="Path to the user behavior Delta table.",
)
@click.option(
    "--funnel-steps",
    required=True,
    help="Comma-separated list of event types for the funnel.",
)
def analyze_funnel(user_behavior_path: str, funnel_steps: str):
    """
    Analyzes conversion rates through a defined funnel.
    Example: --funnel-steps "page_view,add_to_cart,purchase"
    """
    spark = SparkSession.builder.appName("FunnelAnalysis").getOrCreate()
    customer_journey = CustomerJourney(spark, user_behavior_path)
    steps_list = [step.strip() for step in funnel_steps.split(",")]
    customer_journey.run_funnel_analysis(steps_list)
    print("Funnel analysis complete.")


@journey.command()
@click.option(
    "--user-behavior-path",
    default="data/delta/user_behavior",
    help="Path to the user behavior Delta table.",
)
@click.option(
    "--start-event",
    required=True,
    help="Starting event type for conversion calculation.",
)
@click.option(
    "--end-event", required=True, help="Ending event type for conversion calculation."
)
def calculate_conversion(user_behavior_path: str, start_event: str, end_event: str):
    """
    Calculates the conversion rate between two events.
    Example: --start-event "add_to_cart" --end-event "purchase"
    """
    spark = SparkSession.builder.appName("ConversionRateCalculation").getOrCreate()
    customer_journey = CustomerJourney(spark, user_behavior_path)
    customer_journey.run_conversion_analysis(start_event, end_event)
    print("Conversion rate calculation complete.")


if __name__ == "__main__":
    journey()
