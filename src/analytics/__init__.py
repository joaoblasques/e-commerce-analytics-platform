"""Analytics module for customer behavior, fraud detection, and business intelligence."""
from .jobs.base_job import SparkJob
from .jobs.sample_batch_job import SampleBatchJob
from .jobs.sample_streaming_job import SampleStreamingJob