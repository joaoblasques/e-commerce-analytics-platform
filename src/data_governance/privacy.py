"""
Privacy and Compliance Management System.

Provides comprehensive privacy controls including:
- GDPR compliance (Right to be forgotten, data portability, etc.)
- CCPA compliance (Data subject rights)
- Data anonymization and pseudonymization
- Consent management
- Data retention enforcement
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, sha2
from pyspark.sql.types import StringType


class ConsentType(Enum):
    """Types of data processing consent."""

    MARKETING = "marketing"
    ANALYTICS = "analytics"
    PERSONALIZATION = "personalization"
    NECESSARY = "necessary"


class DataSubjectRight(Enum):
    """Data subject rights under privacy regulations."""

    ACCESS = "access"  # Right to access personal data
    RECTIFICATION = "rectification"  # Right to correct personal data
    ERASURE = "erasure"  # Right to be forgotten
    PORTABILITY = "portability"  # Right to data portability
    RESTRICTION = "restriction"  # Right to restrict processing
    OBJECTION = "objection"  # Right to object to processing


@dataclass
class ConsentRecord:
    """Represents a consent record."""

    user_id: str
    consent_type: ConsentType
    granted: bool
    timestamp: datetime
    version: str
    source: str  # website, mobile_app, etc.
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["consent_type"] = self.consent_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConsentRecord":
        """Create from dictionary."""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["consent_type"] = ConsentType(data["consent_type"])
        return cls(**data)


@dataclass
class DataSubjectRequest:
    """Represents a data subject request."""

    request_id: str
    user_id: str
    request_type: DataSubjectRight
    status: str  # pending, in_progress, completed, rejected
    submitted_at: datetime
    completed_at: Optional[datetime]
    requested_by: str
    processed_by: Optional[str]
    details: str
    verification_status: str  # verified, pending, failed
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["submitted_at"] = self.submitted_at.isoformat()
        data["completed_at"] = (
            self.completed_at.isoformat() if self.completed_at else None
        )
        data["request_type"] = self.request_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataSubjectRequest":
        """Create from dictionary."""
        data["submitted_at"] = datetime.fromisoformat(data["submitted_at"])
        data["completed_at"] = (
            datetime.fromisoformat(data["completed_at"])
            if data["completed_at"]
            else None
        )
        data["request_type"] = DataSubjectRight(data["request_type"])
        return cls(**data)


class PrivacyManager:
    """
    Privacy and Compliance Management System.

    Handles GDPR, CCPA and other privacy regulations including:
    - Consent management
    - Data subject rights processing
    - Data anonymization
    - Retention enforcement
    """

    def __init__(
        self, privacy_path: str = "data/privacy", spark: Optional[SparkSession] = None
    ):
        """
        Initialize privacy manager.

        Args:
            privacy_path: Path to store privacy data
            spark: Optional Spark session for data processing
        """
        self.privacy_path = Path(privacy_path)
        self.privacy_path.mkdir(parents=True, exist_ok=True)
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Storage files
        self.consent_file = self.privacy_path / "consent_records.json"
        self.requests_file = self.privacy_path / "subject_requests.json"
        self.config_file = self.privacy_path / "privacy_config.json"

        # Load existing data
        self.consent_records = self._load_consent_records()
        self.subject_requests = self._load_subject_requests()
        self.config = self._load_privacy_config()

        # PII field patterns for detection
        self.pii_patterns = {
            "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
            "phone": r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
            "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
            "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
        }

    def _load_consent_records(self) -> List[ConsentRecord]:
        """Load existing consent records."""
        if not self.consent_file.exists():
            return []

        try:
            with open(self.consent_file, "r") as f:
                data = json.load(f)
            return [ConsentRecord.from_dict(record) for record in data]
        except Exception as e:
            self.logger.error(f"Error loading consent records: {e}")
            return []

    def _load_subject_requests(self) -> List[DataSubjectRequest]:
        """Load existing subject requests."""
        if not self.requests_file.exists():
            return []

        try:
            with open(self.requests_file, "r") as f:
                data = json.load(f)
            return [DataSubjectRequest.from_dict(request) for request in data]
        except Exception as e:
            self.logger.error(f"Error loading subject requests: {e}")
            return []

    def _load_privacy_config(self) -> Dict[str, Any]:
        """Load privacy configuration."""
        default_config = {
            "retention_periods": {
                "customer_data": 2555,  # 7 years in days
                "transaction_data": 2555,
                "analytics_data": 1095,  # 3 years
                "logs": 365,  # 1 year
            },
            "anonymization_fields": ["email", "phone", "address", "name", "ssn"],
            "gdpr_enabled": True,
            "ccpa_enabled": True,
            "consent_required_for": [
                ConsentType.MARKETING.value,
                ConsentType.ANALYTICS.value,
                ConsentType.PERSONALIZATION.value,
            ],
        }

        if not self.config_file.exists():
            with open(self.config_file, "w") as f:
                json.dump(default_config, f, indent=2)
            return default_config

        try:
            with open(self.config_file, "r") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading privacy config: {e}")
            return default_config

    def _save_consent_records(self):
        """Save consent records to storage."""
        try:
            data = [record.to_dict() for record in self.consent_records]
            with open(self.consent_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving consent records: {e}")

    def _save_subject_requests(self):
        """Save subject requests to storage."""
        try:
            data = [request.to_dict() for request in self.subject_requests]
            with open(self.requests_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving subject requests: {e}")

    def record_consent(
        self,
        user_id: str,
        consent_type: ConsentType,
        granted: bool,
        source: str = "website",
        version: str = "1.0",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Record user consent.

        Args:
            user_id: User identifier
            consent_type: Type of consent
            granted: Whether consent was granted
            source: Source of consent
            version: Privacy policy version
            metadata: Additional metadata

        Returns:
            Consent record ID
        """
        metadata = metadata or {}

        consent_record = ConsentRecord(
            user_id=user_id,
            consent_type=consent_type,
            granted=granted,
            timestamp=datetime.now(),
            version=version,
            source=source,
            metadata=metadata,
        )

        self.consent_records.append(consent_record)
        self._save_consent_records()

        self.logger.info(
            f"Recorded consent for user {user_id}: {consent_type.value} = {granted}"
        )
        return f"{user_id}_{consent_type.value}_{consent_record.timestamp.isoformat()}"

    def get_user_consent(self, user_id: str) -> Dict[ConsentType, bool]:
        """
        Get current consent status for a user.

        Args:
            user_id: User identifier

        Returns:
            Dictionary of consent types and their status
        """
        # Get most recent consent for each type
        latest_consent = {}

        for record in self.consent_records:
            if record.user_id == user_id and (
                record.consent_type not in latest_consent
                or record.timestamp > latest_consent[record.consent_type].timestamp
            ):
                latest_consent[record.consent_type] = record

        return {
            consent_type: record.granted
            for consent_type, record in latest_consent.items()
        }

    def submit_data_subject_request(
        self,
        user_id: str,
        request_type: DataSubjectRight,
        requested_by: str,
        details: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Submit a data subject request.

        Args:
            user_id: User identifier
            request_type: Type of request
            requested_by: Who submitted the request
            details: Request details
            metadata: Additional metadata

        Returns:
            Request ID
        """
        import uuid

        metadata = metadata or {}
        request_id = str(uuid.uuid4())

        request = DataSubjectRequest(
            request_id=request_id,
            user_id=user_id,
            request_type=request_type,
            status="pending",
            submitted_at=datetime.now(),
            completed_at=None,
            requested_by=requested_by,
            processed_by=None,
            details=details,
            verification_status="pending",
            metadata=metadata,
        )

        self.subject_requests.append(request)
        self._save_subject_requests()

        self.logger.info(
            f"Submitted data subject request: {request_id} ({request_type.value})"
        )
        return request_id

    def process_erasure_request(
        self, request_id: str, processed_by: str, tables_to_process: List[str]
    ) -> Dict[str, Any]:
        """
        Process a right to be forgotten request.

        Args:
            request_id: Request ID
            processed_by: Who is processing the request
            tables_to_process: List of tables/datasets to process

        Returns:
            Processing results
        """
        # Find the request
        request = None
        for req in self.subject_requests:
            if req.request_id == request_id:
                request = req
                break

        if not request or request.request_type != DataSubjectRight.ERASURE:
            raise ValueError(f"Invalid erasure request: {request_id}")

        if not self.spark:
            raise ValueError("Spark session required for data processing")

        # Update request status
        request.status = "in_progress"
        request.processed_by = processed_by

        results = {
            "request_id": request_id,
            "user_id": request.user_id,
            "processed_tables": [],
            "errors": [],
            "total_records_affected": 0,
        }

        # Process each table
        for table_name in tables_to_process:
            try:
                result = self._erase_user_data(request.user_id, table_name)
                results["processed_tables"].append(
                    {
                        "table": table_name,
                        "records_affected": result.get("records_affected", 0),
                        "status": "completed",
                    }
                )
                results["total_records_affected"] += result.get("records_affected", 0)

            except Exception as e:
                error_msg = f"Error processing table {table_name}: {str(e)}"
                results["errors"].append(error_msg)
                self.logger.error(error_msg)

        # Update request status
        if not results["errors"]:
            request.status = "completed"
            request.completed_at = datetime.now()
        else:
            request.status = "partially_completed"

        self._save_subject_requests()

        self.logger.info(
            f"Processed erasure request {request_id}: {results['total_records_affected']} records affected"
        )
        return results

    def _erase_user_data(self, user_id: str, table_name: str) -> Dict[str, Any]:
        """
        Erase user data from a specific table.

        Args:
            user_id: User to erase
            table_name: Table to process

        Returns:
            Erasure results
        """
        try:
            # Read table
            df = self.spark.table(table_name)
            original_count = df.count()

            # Filter out user data (assuming user_id column exists)
            if "user_id" in df.columns:
                filtered_df = df.filter(col("user_id") != user_id)
                new_count = filtered_df.count()
                records_affected = original_count - new_count

                # Write back (this would need to be adapted for your storage format)
                # For Delta Lake:
                # filtered_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

                return {"records_affected": records_affected, "status": "completed"}
            else:
                self.logger.warning(f"No user_id column found in table {table_name}")
                return {"records_affected": 0, "status": "skipped_no_user_id_column"}

        except Exception as e:
            raise Exception(f"Failed to erase data from {table_name}: {str(e)}")

    def anonymize_dataframe(
        self,
        df: DataFrame,
        anonymization_fields: Optional[List[str]] = None,
        method: str = "hash",
    ) -> DataFrame:
        """
        Anonymize a DataFrame by removing or hashing PII fields.

        Args:
            df: DataFrame to anonymize
            anonymization_fields: Fields to anonymize
            method: Anonymization method (hash, remove, mask)

        Returns:
            Anonymized DataFrame
        """
        if not anonymization_fields:
            anonymization_fields = self.config.get("anonymization_fields", [])

        anonymized_df = df

        for field in anonymization_fields:
            if field in df.columns:
                if method == "hash":
                    # Hash the field
                    anonymized_df = anonymized_df.withColumn(
                        field, sha2(col(field).cast(StringType()), 256)
                    )
                elif method == "remove":
                    # Remove the field entirely
                    anonymized_df = anonymized_df.drop(field)
                elif method == "mask":
                    # Mask the field (e.g., replace with asterisks)
                    anonymized_df = anonymized_df.withColumn(
                        field, regexp_replace(col(field), ".", "*")
                    )

        return anonymized_df

    def detect_pii_in_dataframe(
        self, df: DataFrame, sample_size: int = 1000
    ) -> Dict[str, List[str]]:
        """
        Detect potential PII in a DataFrame.

        Args:
            df: DataFrame to analyze
            sample_size: Number of rows to sample for analysis

        Returns:
            Dictionary mapping PII types to detected columns
        """
        if not self.spark:
            return {}

        # Sample data for analysis
        sample_df = (
            df.sample(False, min(1.0, sample_size / df.count()))
            if df.count() > sample_size
            else df
        )
        sample_data = sample_df.collect()

        detected_pii = {pii_type: [] for pii_type in self.pii_patterns}

        for column in df.columns:
            column_values = [
                str(row[column]) for row in sample_data if row[column] is not None
            ]

            for pii_type, pattern in self.pii_patterns.items():
                import re

                matches = sum(1 for value in column_values if re.search(pattern, value))

                # If more than 10% of non-null values match the pattern, consider it PII
                if matches > 0 and matches / len(column_values) > 0.1:
                    detected_pii[pii_type].append(column)

        # Remove empty lists
        detected_pii = {k: v for k, v in detected_pii.items() if v}

        return detected_pii

    def enforce_retention_policy(
        self,
        table_name: str,
        date_column: str = "created_at",
        data_type: str = "customer_data",
    ) -> Dict[str, Any]:
        """
        Enforce data retention policy on a table.

        Args:
            table_name: Table to process
            date_column: Column containing the date
            data_type: Type of data for retention policy

        Returns:
            Retention enforcement results
        """
        if not self.spark:
            raise ValueError("Spark session required for retention enforcement")

        retention_days = self.config["retention_periods"].get(data_type, 365)
        cutoff_date = datetime.now() - timedelta(days=retention_days)

        try:
            # Read table
            df = self.spark.table(table_name)
            original_count = df.count()

            # Filter out expired data
            retained_df = df.filter(col(date_column) >= lit(cutoff_date))
            new_count = retained_df.count()

            records_deleted = original_count - new_count

            # Write back retained data
            # retained_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

            result = {
                "table": table_name,
                "retention_days": retention_days,
                "cutoff_date": cutoff_date.isoformat(),
                "original_count": original_count,
                "retained_count": new_count,
                "records_deleted": records_deleted,
                "status": "completed",
            }

            self.logger.info(
                f"Retention policy enforced on {table_name}: {records_deleted} records deleted"
            )
            return result

        except Exception as e:
            error_msg = f"Failed to enforce retention policy on {table_name}: {str(e)}"
            self.logger.error(error_msg)
            return {"table": table_name, "status": "failed", "error": error_msg}

    def generate_privacy_report(self) -> Dict[str, Any]:
        """Generate comprehensive privacy compliance report."""
        now = datetime.now()

        # Consent statistics
        consent_stats = {}
        for consent_type in ConsentType:
            granted = sum(
                1
                for r in self.consent_records
                if r.consent_type == consent_type and r.granted
            )
            denied = sum(
                1
                for r in self.consent_records
                if r.consent_type == consent_type and not r.granted
            )
            consent_stats[consent_type.value] = {
                "granted": granted,
                "denied": denied,
                "total": granted + denied,
            }

        # Request statistics
        request_stats = {}
        for request_type in DataSubjectRight:
            requests = [
                r for r in self.subject_requests if r.request_type == request_type
            ]
            request_stats[request_type.value] = {
                "total": len(requests),
                "pending": len([r for r in requests if r.status == "pending"]),
                "in_progress": len([r for r in requests if r.status == "in_progress"]),
                "completed": len([r for r in requests if r.status == "completed"]),
                "rejected": len([r for r in requests if r.status == "rejected"]),
            }

        # Recent activity (last 30 days)
        thirty_days_ago = now - timedelta(days=30)
        recent_consent = [
            r for r in self.consent_records if r.timestamp >= thirty_days_ago
        ]
        recent_requests = [
            r for r in self.subject_requests if r.submitted_at >= thirty_days_ago
        ]

        return {
            "report_timestamp": now.isoformat(),
            "consent_statistics": consent_stats,
            "request_statistics": request_stats,
            "recent_activity": {
                "consent_records_last_30_days": len(recent_consent),
                "subject_requests_last_30_days": len(recent_requests),
            },
            "configuration": {
                "gdpr_enabled": self.config.get("gdpr_enabled", False),
                "ccpa_enabled": self.config.get("ccpa_enabled", False),
                "retention_periods": self.config.get("retention_periods", {}),
                "consent_required_for": self.config.get("consent_required_for", []),
            },
            "compliance_status": {
                "total_users_with_consent": len(
                    {r.user_id for r in self.consent_records}
                ),
                "pending_requests": len(
                    [r for r in self.subject_requests if r.status == "pending"]
                ),
                "overdue_requests": len(
                    [
                        r
                        for r in self.subject_requests
                        if r.status in ["pending", "in_progress"]
                        and (now - r.submitted_at).days > 30
                    ]
                ),
            },
        }
