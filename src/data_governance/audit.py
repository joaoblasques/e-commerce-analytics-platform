"""
Access Auditing and Security Controls System.

Provides comprehensive access auditing including:
- User access tracking
- Permission management
- Security event logging
- Compliance reporting
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


class AccessEventType(Enum):
    """Types of access events."""

    LOGIN = "login"
    LOGOUT = "logout"
    DATA_ACCESS = "data_access"
    DATA_EXPORT = "data_export"
    PERMISSION_CHANGE = "permission_change"
    FAILED_ACCESS = "failed_access"
    PRIVILEGE_ESCALATION = "privilege_escalation"


class UserRole(Enum):
    """User roles for access control."""

    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"
    API_USER = "api_user"
    AUDITOR = "auditor"


class DataClassification(Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class AccessEvent:
    """Represents an access event."""

    event_id: str
    timestamp: datetime
    event_type: AccessEventType
    user_id: str
    user_role: str
    resource: str
    resource_type: str  # table, file, api_endpoint, etc.
    action: str  # read, write, delete, etc.
    source_ip: str
    user_agent: str
    session_id: str
    success: bool
    details: Dict[str, Any]
    risk_score: float  # 0.0 to 1.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["event_type"] = self.event_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AccessEvent":
        """Create from dictionary."""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["event_type"] = AccessEventType(data["event_type"])
        return cls(**data)


@dataclass
class UserPermission:
    """Represents user permissions for a resource."""

    user_id: str
    resource: str
    resource_type: str
    permissions: Set[str]  # read, write, delete, admin
    granted_by: str
    granted_at: datetime
    expires_at: Optional[datetime]
    conditions: Dict[str, Any]  # Time-based, IP-based, etc.

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["permissions"] = list(self.permissions)
        data["granted_at"] = self.granted_at.isoformat()
        data["expires_at"] = self.expires_at.isoformat() if self.expires_at else None
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserPermission":
        """Create from dictionary."""
        data["permissions"] = set(data["permissions"])
        data["granted_at"] = datetime.fromisoformat(data["granted_at"])
        data["expires_at"] = (
            datetime.fromisoformat(data["expires_at"]) if data["expires_at"] else None
        )
        return cls(**data)


@dataclass
class SecurityAlert:
    """Represents a security alert."""

    alert_id: str
    alert_type: str
    severity: str  # low, medium, high, critical
    title: str
    description: str
    user_id: str
    resource: str
    timestamp: datetime
    events: List[str]  # Related event IDs
    resolved: bool
    resolved_at: Optional[datetime]
    resolved_by: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["resolved_at"] = self.resolved_at.isoformat() if self.resolved_at else None
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SecurityAlert":
        """Create from dictionary."""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["resolved_at"] = (
            datetime.fromisoformat(data["resolved_at"]) if data["resolved_at"] else None
        )
        return cls(**data)


class AccessAuditor:
    """
    Access Auditing and Security Controls System.

    Provides comprehensive access monitoring including:
    - User access tracking
    - Permission management
    - Security event detection
    - Compliance reporting
    """

    def __init__(self, audit_path: str = "data/audit"):
        """
        Initialize access auditor.

        Args:
            audit_path: Path to store audit data
        """
        self.audit_path = Path(audit_path)
        self.audit_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

        # Storage files
        self.events_file = self.audit_path / "access_events.json"
        self.permissions_file = self.audit_path / "user_permissions.json"
        self.alerts_file = self.audit_path / "security_alerts.json"
        self.config_file = self.audit_path / "audit_config.json"

        # Load existing data
        self.events = self._load_events()
        self.permissions = self._load_permissions()
        self.alerts = self._load_alerts()
        self.config = self._load_audit_config()

        # Risk scoring patterns
        self.risk_patterns = {
            "unusual_hours": {"weight": 0.3, "threshold": "22:00-06:00"},
            "multiple_failures": {"weight": 0.4, "threshold": 3},
            "privilege_escalation": {"weight": 0.8, "threshold": 1},
            "data_export": {"weight": 0.5, "threshold": 1},
            "unknown_ip": {"weight": 0.3, "threshold": 1},
            "role_mismatch": {"weight": 0.6, "threshold": 1},
        }

    def _load_events(self) -> List[AccessEvent]:
        """Load existing access events."""
        if not self.events_file.exists():
            return []

        try:
            with open(self.events_file, "r") as f:
                data = json.load(f)
            return [AccessEvent.from_dict(event) for event in data]
        except Exception as e:
            self.logger.error(f"Error loading access events: {e}")
            return []

    def _load_permissions(self) -> Dict[str, List[UserPermission]]:
        """Load existing user permissions."""
        if not self.permissions_file.exists():
            return {}

        try:
            with open(self.permissions_file, "r") as f:
                data = json.load(f)
            permissions = {}
            for user_id, perms_data in data.items():
                permissions[user_id] = [UserPermission.from_dict(p) for p in perms_data]
            return permissions
        except Exception as e:
            self.logger.error(f"Error loading permissions: {e}")
            return {}

    def _load_alerts(self) -> List[SecurityAlert]:
        """Load existing security alerts."""
        if not self.alerts_file.exists():
            return []

        try:
            with open(self.alerts_file, "r") as f:
                data = json.load(f)
            return [SecurityAlert.from_dict(alert) for alert in data]
        except Exception as e:
            self.logger.error(f"Error loading security alerts: {e}")
            return []

    def _load_audit_config(self) -> Dict[str, Any]:
        """Load audit configuration."""
        default_config = {
            "retention_days": 2555,  # 7 years
            "alert_thresholds": {
                "failed_logins": 5,
                "unusual_hours": True,
                "data_export_size_mb": 100,
                "privilege_escalation": True,
            },
            "monitored_resources": [
                "customer_data",
                "transaction_data",
                "analytics_data",
            ],
            "sensitive_operations": [
                "data_export",
                "user_creation",
                "permission_change",
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
            self.logger.error(f"Error loading audit config: {e}")
            return default_config

    def _save_events(self):
        """Save access events to storage."""
        try:
            # Keep only recent events to prevent file from growing too large
            retention_days = self.config.get("retention_days", 2555)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            recent_events = [e for e in self.events if e.timestamp >= cutoff_date]

            data = [event.to_dict() for event in recent_events]
            with open(self.events_file, "w") as f:
                json.dump(data, f, indent=2)

            self.events = recent_events
        except Exception as e:
            self.logger.error(f"Error saving access events: {e}")

    def _save_permissions(self):
        """Save user permissions to storage."""
        try:
            data = {}
            for user_id, perms in self.permissions.items():
                data[user_id] = [perm.to_dict() for perm in perms]

            with open(self.permissions_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving permissions: {e}")

    def _save_alerts(self):
        """Save security alerts to storage."""
        try:
            data = [alert.to_dict() for alert in self.alerts]
            with open(self.alerts_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving security alerts: {e}")

    def log_access_event(
        self,
        event_type: AccessEventType,
        user_id: str,
        user_role: str,
        resource: str,
        resource_type: str,
        action: str,
        source_ip: str,
        user_agent: str,
        session_id: str,
        success: bool,
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Log an access event.

        Args:
            event_type: Type of access event
            user_id: User identifier
            user_role: User role
            resource: Accessed resource
            resource_type: Type of resource
            action: Action performed
            source_ip: Source IP address
            user_agent: User agent string
            session_id: Session identifier
            success: Whether the access was successful
            details: Additional event details

        Returns:
            Event ID
        """
        import uuid

        details = details or {}
        event_id = str(uuid.uuid4())

        # Calculate risk score
        risk_score = self._calculate_risk_score(
            event_type,
            user_id,
            user_role,
            resource,
            action,
            source_ip,
            success,
            details,
        )

        event = AccessEvent(
            event_id=event_id,
            timestamp=datetime.now(),
            event_type=event_type,
            user_id=user_id,
            user_role=user_role,
            resource=resource,
            resource_type=resource_type,
            action=action,
            source_ip=source_ip,
            user_agent=user_agent,
            session_id=session_id,
            success=success,
            details=details,
            risk_score=risk_score,
        )

        self.events.append(event)

        # Check for security alerts
        self._check_security_patterns(event)

        # Save events periodically
        if len(self.events) % 100 == 0:
            self._save_events()

        self.logger.info(
            f"Logged access event: {event_id} ({event_type.value}, risk: {risk_score:.2f})"
        )
        return event_id

    def _calculate_risk_score(
        self,
        event_type: AccessEventType,
        user_id: str,
        user_role: str,
        resource: str,
        action: str,
        source_ip: str,
        success: bool,
        details: Dict[str, Any],
    ) -> float:
        """Calculate risk score for an access event."""
        risk_score = 0.0

        # Base risk by event type
        event_type_risks = {
            AccessEventType.FAILED_ACCESS: 0.7,
            AccessEventType.PRIVILEGE_ESCALATION: 0.9,
            AccessEventType.DATA_EXPORT: 0.5,
            AccessEventType.DATA_ACCESS: 0.2,
            AccessEventType.LOGIN: 0.1,
            AccessEventType.LOGOUT: 0.0,
        }
        risk_score += event_type_risks.get(event_type, 0.2)

        # Check for unusual hours
        current_hour = datetime.now().hour
        if current_hour >= 22 or current_hour <= 6:
            risk_score += self.risk_patterns["unusual_hours"]["weight"]

        # Check for multiple failures
        recent_failures = self._get_recent_failures(user_id, hours=1)
        if len(recent_failures) >= self.risk_patterns["multiple_failures"]["threshold"]:
            risk_score += self.risk_patterns["multiple_failures"]["weight"]

        # Check for unknown IP
        if not self._is_known_ip(source_ip):
            risk_score += self.risk_patterns["unknown_ip"]["weight"]

        # Check for role mismatch with resource
        if self._is_role_resource_mismatch(user_role, resource):
            risk_score += self.risk_patterns["role_mismatch"]["weight"]

        # Failure increases risk
        if not success:
            risk_score += 0.3

        return min(risk_score, 1.0)  # Cap at 1.0

    def _get_recent_failures(self, user_id: str, hours: int = 1) -> List[AccessEvent]:
        """Get recent failed access events for a user."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            event
            for event in self.events
            if (
                event.user_id == user_id
                and not event.success
                and event.timestamp >= cutoff_time
            )
        ]

    def _is_known_ip(self, ip: str) -> bool:
        """Check if IP is from a known/trusted range."""
        # This would typically check against a whitelist of corporate IPs
        # For now, assume all internal IPs (192.168.x.x, 10.x.x.x) are known
        return (
            ip.startswith("192.168.") or ip.startswith("10.") or ip.startswith("127.")
        )

    def _is_role_resource_mismatch(self, role: str, resource: str) -> bool:
        """Check if there's a mismatch between user role and accessed resource."""
        # Define role-resource compatibility
        role_access = {
            "viewer": ["analytics_data", "reports"],
            "analyst": ["analytics_data", "customer_data", "reports"],
            "admin": ["*"],  # Admin can access everything
            "api_user": ["api_endpoints"],
        }

        allowed_resources = role_access.get(role, [])
        return "*" not in allowed_resources and resource not in allowed_resources

    def _check_security_patterns(self, event: AccessEvent):
        """Check for security alert patterns."""
        alerts_to_generate = []

        # Multiple failed logins
        if event.event_type == AccessEventType.FAILED_ACCESS:
            recent_failures = self._get_recent_failures(event.user_id, hours=1)
            threshold = self.config["alert_thresholds"]["failed_logins"]

            if len(recent_failures) >= threshold:
                alerts_to_generate.append(
                    {
                        "type": "multiple_failed_logins",
                        "severity": "high",
                        "title": "Multiple Failed Login Attempts",
                        "description": f"User {event.user_id} has {len(recent_failures)} failed login attempts in the last hour",
                    }
                )

        # Privilege escalation
        if (
            event.event_type == AccessEventType.PRIVILEGE_ESCALATION
            and self.config["alert_thresholds"]["privilege_escalation"]
        ):
            alerts_to_generate.append(
                {
                    "type": "privilege_escalation",
                    "severity": "critical",
                    "title": "Privilege Escalation Detected",
                    "description": f"User {event.user_id} attempted privilege escalation on {event.resource}",
                }
            )

        # Large data export
        if (
            event.event_type == AccessEventType.DATA_EXPORT
            and event.details.get("size_mb", 0)
            > self.config["alert_thresholds"]["data_export_size_mb"]
        ):
            alerts_to_generate.append(
                {
                    "type": "large_data_export",
                    "severity": "medium",
                    "title": "Large Data Export",
                    "description": f'User {event.user_id} exported {event.details.get("size_mb")}MB from {event.resource}',
                }
            )

        # High risk score
        if event.risk_score > 0.8:
            alerts_to_generate.append(
                {
                    "type": "high_risk_activity",
                    "severity": "high",
                    "title": "High Risk Activity Detected",
                    "description": f"High risk activity (score: {event.risk_score:.2f}) detected for user {event.user_id}",
                }
            )

        # Generate alerts
        for alert_data in alerts_to_generate:
            self._generate_security_alert(event, alert_data)

    def _generate_security_alert(self, event: AccessEvent, alert_data: Dict[str, Any]):
        """Generate a security alert."""
        import uuid

        alert = SecurityAlert(
            alert_id=str(uuid.uuid4()),
            alert_type=alert_data["type"],
            severity=alert_data["severity"],
            title=alert_data["title"],
            description=alert_data["description"],
            user_id=event.user_id,
            resource=event.resource,
            timestamp=datetime.now(),
            events=[event.event_id],
            resolved=False,
            resolved_at=None,
            resolved_by=None,
        )

        self.alerts.append(alert)
        self._save_alerts()

        self.logger.warning(
            f"Generated security alert: {alert.alert_id} ({alert.severity})"
        )

    def grant_permission(
        self,
        user_id: str,
        resource: str,
        resource_type: str,
        permissions: Set[str],
        granted_by: str,
        expires_at: Optional[datetime] = None,
        conditions: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Grant permissions to a user for a resource.

        Args:
            user_id: User to grant permissions to
            resource: Resource identifier
            resource_type: Type of resource
            permissions: Set of permissions to grant
            granted_by: Who granted the permissions
            expires_at: When the permissions expire
            conditions: Additional conditions

        Returns:
            Permission ID
        """
        conditions = conditions or {}

        permission = UserPermission(
            user_id=user_id,
            resource=resource,
            resource_type=resource_type,
            permissions=permissions,
            granted_by=granted_by,
            granted_at=datetime.now(),
            expires_at=expires_at,
            conditions=conditions,
        )

        if user_id not in self.permissions:
            self.permissions[user_id] = []

        self.permissions[user_id].append(permission)
        self._save_permissions()

        # Log permission change
        self.log_access_event(
            event_type=AccessEventType.PERMISSION_CHANGE,
            user_id=granted_by,
            user_role="admin",
            resource=resource,
            resource_type=resource_type,
            action="grant_permission",
            source_ip="system",
            user_agent="system",
            session_id="system",
            success=True,
            details={
                "target_user": user_id,
                "permissions_granted": list(permissions),
                "expires_at": expires_at.isoformat() if expires_at else None,
            },
        )

        self.logger.info(
            f"Granted permissions to {user_id} for {resource}: {permissions}"
        )
        return f"{user_id}_{resource}_{permission.granted_at.isoformat()}"

    def check_permission(
        self,
        user_id: str,
        resource: str,
        action: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Check if a user has permission to perform an action on a resource.

        Args:
            user_id: User identifier
            resource: Resource identifier
            action: Action to perform
            context: Additional context (IP, time, etc.)

        Returns:
            True if permission is granted
        """
        context = context or {}

        user_permissions = self.permissions.get(user_id, [])
        now = datetime.now()

        for permission in user_permissions:
            # Check if permission applies to this resource
            if permission.resource != resource:
                continue

            # Check if permission is expired
            if permission.expires_at and now > permission.expires_at:
                continue

            # Check if user has the required action permission
            if action not in permission.permissions:
                continue

            # Check conditions (time-based, IP-based, etc.)
            if not self._check_permission_conditions(permission.conditions, context):
                continue

            return True

        return False

    def _check_permission_conditions(
        self, conditions: Dict[str, Any], context: Dict[str, Any]
    ) -> bool:
        """Check if permission conditions are met."""
        for condition_type, condition_value in conditions.items():
            if condition_type == "time_range":
                # Check if current time is within allowed range
                current_hour = datetime.now().hour
                start_hour, end_hour = condition_value.split("-")
                if not (int(start_hour) <= current_hour <= int(end_hour)):
                    return False

            elif condition_type == "ip_whitelist":
                # Check if source IP is in whitelist
                source_ip = context.get("source_ip")
                if source_ip not in condition_value:
                    return False

            elif condition_type == "max_sessions":
                # Check if user has too many active sessions
                active_sessions = context.get("active_sessions", 0)
                if active_sessions > condition_value:
                    return False

        return True

    def get_user_access_report(self, user_id: str, days: int = 30) -> Dict[str, Any]:
        """Generate access report for a specific user."""
        cutoff_date = datetime.now() - timedelta(days=days)
        user_events = [
            event
            for event in self.events
            if event.user_id == user_id and event.timestamp >= cutoff_date
        ]

        # Access statistics
        total_accesses = len(user_events)
        successful_accesses = len([e for e in user_events if e.success])
        failed_accesses = total_accesses - successful_accesses

        # Resources accessed
        resources_accessed = {event.resource for event in user_events}

        # Access patterns
        access_by_hour = {}
        for event in user_events:
            hour = event.timestamp.hour
            access_by_hour[hour] = access_by_hour.get(hour, 0) + 1

        # Risk analysis
        high_risk_events = [e for e in user_events if e.risk_score > 0.7]
        avg_risk_score = (
            sum(e.risk_score for e in user_events) / len(user_events)
            if user_events
            else 0.0
        )

        # Recent alerts
        user_alerts = [
            alert
            for alert in self.alerts
            if alert.user_id == user_id and alert.timestamp >= cutoff_date
        ]

        return {
            "user_id": user_id,
            "report_period": f"{days} days",
            "report_timestamp": datetime.now().isoformat(),
            "access_statistics": {
                "total_accesses": total_accesses,
                "successful_accesses": successful_accesses,
                "failed_accesses": failed_accesses,
                "success_rate": successful_accesses / total_accesses
                if total_accesses > 0
                else 0.0,
            },
            "resources_accessed": list(resources_accessed),
            "access_patterns": {
                "by_hour": access_by_hour,
                "most_active_hour": max(access_by_hour.keys(), key=access_by_hour.get)
                if access_by_hour
                else None,
            },
            "risk_analysis": {
                "high_risk_events": len(high_risk_events),
                "average_risk_score": avg_risk_score,
                "maximum_risk_score": max(
                    (e.risk_score for e in user_events), default=0.0
                ),
            },
            "security_alerts": [
                {
                    "alert_id": alert.alert_id,
                    "type": alert.alert_type,
                    "severity": alert.severity,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved,
                }
                for alert in user_alerts
            ],
            "current_permissions": [
                {
                    "resource": perm.resource,
                    "permissions": list(perm.permissions),
                    "expires_at": perm.expires_at.isoformat()
                    if perm.expires_at
                    else None,
                }
                for perm in self.permissions.get(user_id, [])
            ],
        }

    def get_compliance_report(self, days: int = 30) -> Dict[str, Any]:
        """Generate compliance report for auditing purposes."""
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_events = [
            event for event in self.events if event.timestamp >= cutoff_date
        ]

        # User activity
        unique_users = {event.user_id for event in recent_events}

        # Access patterns
        access_by_type = {}
        for event_type in AccessEventType:
            type_events = [e for e in recent_events if e.event_type == event_type]
            access_by_type[event_type.value] = len(type_events)

        # Resource access
        resource_access = {}
        for event in recent_events:
            resource_access[event.resource] = resource_access.get(event.resource, 0) + 1

        # Security incidents
        recent_alerts = [
            alert for alert in self.alerts if alert.timestamp >= cutoff_date
        ]

        alerts_by_severity = {}
        for alert in recent_alerts:
            alerts_by_severity[alert.severity] = (
                alerts_by_severity.get(alert.severity, 0) + 1
            )

        # Failed access attempts
        failed_events = [e for e in recent_events if not e.success]
        failed_by_user = {}
        for event in failed_events:
            failed_by_user[event.user_id] = failed_by_user.get(event.user_id, 0) + 1

        return {
            "report_period": f"{days} days",
            "report_timestamp": datetime.now().isoformat(),
            "overview": {
                "total_events": len(recent_events),
                "unique_users": len(unique_users),
                "successful_accesses": len([e for e in recent_events if e.success]),
                "failed_accesses": len(failed_events),
                "security_alerts": len(recent_alerts),
            },
            "access_patterns": {
                "by_event_type": access_by_type,
                "most_accessed_resources": sorted(
                    resource_access.items(), key=lambda x: x[1], reverse=True
                )[:10],
            },
            "security_summary": {
                "alerts_by_severity": alerts_by_severity,
                "users_with_failed_access": list(failed_by_user.keys()),
                "top_failed_users": sorted(
                    failed_by_user.items(), key=lambda x: x[1], reverse=True
                )[:5],
            },
            "compliance_metrics": {
                "audit_trail_completeness": 100.0,  # Assuming complete logging
                "retention_compliance": True,
                "access_control_violations": len(
                    [e for e in recent_events if e.risk_score > 0.8]
                ),
            },
        }
