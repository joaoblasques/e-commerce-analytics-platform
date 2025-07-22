
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

class CustomerJourney:
    """
    A class to analyze customer journeys, including touchpoints, funnels, and conversion.
    """

    def __init__(self, spark: SparkSession, user_behavior_path: str = "data/delta/user_behavior"):
        self.spark = spark
        self.user_behavior_path = user_behavior_path

    def _load_user_behavior_data(self) -> DataFrame:
        """Loads user behavior data from the Delta Lake table."""
        return self.spark.read.format("delta").load(self.user_behavior_path)

    def track_touchpoints(self, df: DataFrame) -> DataFrame:
        """
        Tracks customer touchpoints by ordering events within a session.

        Args:
            df: DataFrame with user behavior events.

        Returns:
            DataFrame with ordered touchpoints.
        """
        window_spec = Window.partitionBy("session_id").orderBy("timestamp")
        return df.withColumn("touchpoint_order", F.row_number().over(window_spec))

    def analyze_funnel(self, df: DataFrame, funnel_steps: list) -> DataFrame:
        """
        Analyzes conversion rates through a defined funnel.

        Args:
            df: DataFrame with user behavior events.
            funnel_steps: List of event types defining the funnel.

        Returns:
            DataFrame with funnel analysis results.
        """
        # Ensure events are ordered within sessions
        df = self.track_touchpoints(df)

        # Filter for sessions that started the funnel
        first_step_df = df.filter(F.col("event_type") == funnel_steps[0])
        sessions_in_funnel = first_step_df.select("session_id").distinct()

        funnel_df = df.join(sessions_in_funnel, "session_id")

        results = []
        for i, step in enumerate(funnel_steps):
            step_count = funnel_df.filter(F.col("event_type") == step).select("session_id").distinct().count()
            results.append((step, step_count))

        return self.spark.createDataFrame(results, ["funnel_step", "session_count"])

    def calculate_conversion_rate(self, df: DataFrame, start_event: str, end_event: str) -> float:
        """
        Calculates the conversion rate between two events.

        Args:
            df: DataFrame with user behavior events.
            start_event: The starting event type.
            end_event: The ending event type.

        Returns:
            The conversion rate as a float.
        """
        # Sessions that performed the start event
        start_sessions = df.filter(F.col("event_type") == start_event).select("session_id").distinct()
        start_count = start_sessions.count()

        # Sessions that performed both start and end events
        end_sessions = df.filter(F.col("event_type") == end_event).select("session_id").distinct()
        converted_sessions = start_sessions.join(end_sessions, "session_id").count()

        if start_count == 0:
            return 0.0
        return converted_sessions / start_count

    def run_funnel_analysis(self, funnel_steps: list):
        """
        Runs the full funnel analysis pipeline.
        """
        user_behavior_df = self._load_user_behavior_data()
        funnel_results = self.analyze_funnel(user_behavior_df, funnel_steps)
        funnel_results.show()

    def run_conversion_analysis(self, start_event: str, end_event: str):
        """
        Runs the full conversion rate analysis pipeline.
        """
        user_behavior_df = self._load_user_behavior_data()
        conversion_rate = self.calculate_conversion_rate(user_behavior_df, start_event, end_event)
        print(f"Conversion rate from {start_event} to {end_event}: {conversion_rate:.2f}")
