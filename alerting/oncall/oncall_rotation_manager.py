#!/usr/bin/env python3
"""
On-Call Rotation Management System for ECAP.

This module provides:
- On-call rotation scheduling and management
- Automatic escalation handling
- Multi-channel notification system
- Override and substitution management
- On-call analytics and reporting
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ShiftType(Enum):
    """Types of on-call shifts."""

    PRIMARY = "primary"
    SECONDARY = "secondary"
    ESCALATION = "escalation"
    WEEKEND = "weekend"
    HOLIDAY = "holiday"


class NotificationType(Enum):
    """Types of notifications."""

    PHONE_CALL = "phone_call"
    SMS = "sms"
    EMAIL = "email"
    SLACK_DM = "slack_dm"
    PUSH_NOTIFICATION = "push_notification"
    PAGERDUTY = "pagerduty"


class EscalationStatus(Enum):
    """Status of escalation attempts."""

    PENDING = "pending"
    ACKNOWLEDGED = "acknowledged"
    NO_RESPONSE = "no_response"
    ESCALATED = "escalated"
    RESOLVED = "resolved"


@dataclass
class OnCallEngineer:
    """On-call engineer information."""

    email: str
    name: str
    phone: str
    slack_user_id: str
    timezone: str
    backup_phone: Optional[str] = None
    escalation_preference: List[NotificationType] = None
    availability_hours: Dict[str, str] = None  # {"monday": "09:00-17:00"}
    skills: List[str] = None
    seniority_level: str = "senior"

    def __post_init__(self):
        """Initialize default values after dataclass creation."""
        if self.escalation_preference is None:
            self.escalation_preference = [
                NotificationType.PHONE_CALL,
                NotificationType.SMS,
                NotificationType.SLACK_DM,
            ]
        if self.availability_hours is None:
            self.availability_hours = dict.fromkeys(
                [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ],
                "00:00-23:59",
            )
        if self.skills is None:
            self.skills = ["general"]


@dataclass
class OnCallShift:
    """On-call shift definition."""

    shift_id: str
    engineer: OnCallEngineer
    backup_engineer: Optional[OnCallEngineer]
    shift_type: ShiftType
    start_time: datetime
    end_time: datetime
    timezone: str
    is_active: bool = True
    override_engineer: Optional[OnCallEngineer] = None
    notes: str = ""


@dataclass
class EscalationAttempt:
    """Individual escalation attempt tracking."""

    attempt_id: str
    engineer: OnCallEngineer
    notification_type: NotificationType
    sent_at: datetime
    status: EscalationStatus
    response_time: Optional[timedelta] = None
    acknowledged_at: Optional[datetime] = None


@dataclass
class OnCallEvent:
    """On-call event (alert, incident, etc.)."""

    event_id: str
    alert_name: str
    severity: str
    service: str
    description: str
    created_at: datetime
    escalation_attempts: List[EscalationAttempt]
    current_escalation_level: int = 1
    max_escalation_level: int = 4
    acknowledged: bool = False
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    resolution_time: Optional[timedelta] = None


class NotificationService:
    """Service for sending notifications through various channels."""

    def __init__(self, config: Dict):
        self.config = config
        self.notification_config = config.get("notification_config", {})

    async def send_notification(
        self,
        engineer: OnCallEngineer,
        notification_type: NotificationType,
        event: OnCallEvent,
        escalation_level: int,
    ) -> bool:
        """Send notification to engineer."""
        try:
            if notification_type == NotificationType.PHONE_CALL:
                return await self._make_phone_call(engineer, event, escalation_level)
            elif notification_type == NotificationType.SMS:
                return await self._send_sms(engineer, event, escalation_level)
            elif notification_type == NotificationType.EMAIL:
                return await self._send_email(engineer, event, escalation_level)
            elif notification_type == NotificationType.SLACK_DM:
                return await self._send_slack_dm(engineer, event, escalation_level)
            elif notification_type == NotificationType.PUSH_NOTIFICATION:
                return await self._send_push_notification(
                    engineer, event, escalation_level
                )
            elif notification_type == NotificationType.PAGERDUTY:
                return await self._send_pagerduty_notification(
                    engineer, event, escalation_level
                )
            else:
                logger.error(f"Unknown notification type: {notification_type}")
                return False

        except Exception as e:
            logger.error(f"Failed to send {notification_type} to {engineer.email}: {e}")
            return False

    async def _make_phone_call(
        self, engineer: OnCallEngineer, event: OnCallEvent, escalation_level: int
    ) -> bool:
        """Make phone call to engineer."""
        # Integration with phone service (Twilio, AWS Connect, etc.)
        phone_number = engineer.phone
        message = self._format_voice_message(event, escalation_level)

        logger.info(f"Making phone call to {engineer.name} at {phone_number}")

        # Simulate phone call API
        phone_config = self.notification_config.get("phone", {})
        if phone_config.get("enabled", False):
            # Real implementation would use Twilio or similar
            await asyncio.sleep(1)  # Simulate API call
            return True

        logger.warning("Phone notifications not configured")
        return False

    async def _send_sms(
        self, engineer: OnCallEngineer, event: OnCallEvent, escalation_level: int
    ) -> bool:
        """Send SMS to engineer."""
        phone_number = engineer.phone
        message = self._format_sms_message(event, escalation_level)

        logger.info(f"Sending SMS to {engineer.name} at {phone_number}")

        # Simulate SMS API
        sms_config = self.notification_config.get("sms", {})
        if sms_config.get("enabled", False):
            # Real implementation would use Twilio or similar
            await asyncio.sleep(0.5)  # Simulate API call
            return True

        logger.warning("SMS notifications not configured")
        return False

    async def _send_email(
        self, engineer: OnCallEngineer, event: OnCallEvent, escalation_level: int
    ) -> bool:
        """Send email to engineer."""
        email_address = engineer.email
        subject, body = self._format_email_message(event, escalation_level)

        logger.info(f"Sending email to {engineer.name} at {email_address}")

        # Simulate email API
        email_config = self.notification_config.get("email", {})
        if email_config.get("enabled", False):
            # Real implementation would use SES, SendGrid, or SMTP
            await asyncio.sleep(0.3)  # Simulate API call
            return True

        logger.warning("Email notifications not configured")
        return False

    async def _send_slack_dm(
        self, engineer: OnCallEngineer, event: OnCallEvent, escalation_level: int
    ) -> bool:
        """Send Slack direct message to engineer."""
        slack_user_id = engineer.slack_user_id
        message = self._format_slack_message(event, escalation_level)

        logger.info(f"Sending Slack DM to {engineer.name} ({slack_user_id})")

        slack_config = self.notification_config.get("slack", {})
        if slack_config.get("enabled", False):
            webhook_url = slack_config.get("webhook_url")
            if webhook_url:
                payload = {
                    "channel": f"@{slack_user_id}",
                    "text": message,
                    "username": "ECAP OnCall Bot",
                }

                async with aiohttp.ClientSession() as session:
                    async with session.post(webhook_url, json=payload) as response:
                        return response.status == 200

        logger.warning("Slack notifications not configured")
        return False

    async def _send_push_notification(
        self, engineer: OnCallEngineer, event: OnCallEvent, escalation_level: int
    ) -> bool:
        """Send push notification to engineer's mobile app."""
        message = self._format_push_message(event, escalation_level)

        logger.info(f"Sending push notification to {engineer.name}")

        # Simulate push notification API
        push_config = self.notification_config.get("push", {})
        if push_config.get("enabled", False):
            # Real implementation would use FCM, APNS, or similar
            await asyncio.sleep(0.2)  # Simulate API call
            return True

        logger.warning("Push notifications not configured")
        return False

    async def _send_pagerduty_notification(
        self, engineer: OnCallEngineer, event: OnCallEvent, escalation_level: int
    ) -> bool:
        """Send PagerDuty notification."""
        logger.info(f"Sending PagerDuty notification for {engineer.name}")

        pagerduty_config = self.notification_config.get("pagerduty", {})
        if pagerduty_config.get("enabled", False):
            integration_key = pagerduty_config.get("integration_key")
            if integration_key:
                payload = {
                    "routing_key": integration_key,
                    "event_action": "trigger",
                    "payload": {
                        "summary": f"ECAP Alert: {event.alert_name}",
                        "source": event.service,
                        "severity": event.severity.lower(),
                        "custom_details": {
                            "description": event.description,
                            "escalation_level": escalation_level,
                            "engineer": engineer.name,
                        },
                    },
                }

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "https://events.pagerduty.com/v2/enqueue", json=payload
                    ) as response:
                        return response.status == 202

        logger.warning("PagerDuty notifications not configured")
        return False

    def _format_voice_message(self, event: OnCallEvent, escalation_level: int) -> str:
        """Format voice message for phone calls."""
        return (
            f"This is an urgent alert from ECAP. "
            f"Alert: {event.alert_name}. "
            f"Service: {event.service}. "
            f"Severity: {event.severity}. "
            f"Escalation level: {escalation_level}. "
            f"Please acknowledge immediately."
        )

    def _format_sms_message(self, event: OnCallEvent, escalation_level: int) -> str:
        """Format SMS message."""
        return (
            f"🚨 ECAP Alert L{escalation_level}\n"
            f"{event.alert_name}\n"
            f"Service: {event.service}\n"
            f"Severity: {event.severity}\n"
            f"Please acknowledge ASAP"
        )

    def _format_email_message(
        self, event: OnCallEvent, escalation_level: int
    ) -> Tuple[str, str]:
        """Format email message (subject, body)."""
        subject = f"🚨 ECAP Alert L{escalation_level}: {event.alert_name}"
        body = f"""
URGENT: On-Call Alert - Escalation Level {escalation_level}

Alert: {event.alert_name}
Service: {event.service}
Severity: {event.severity}
Time: {event.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}

Description:
{event.description}

Please acknowledge this alert immediately by:
1. Logging into the ECAP dashboard
2. Responding to this email
3. Joining the incident bridge: [BRIDGE_URL]

If you cannot respond within 5 minutes, this alert will be escalated to the next level.

ECAP On-Call System
        """
        return subject, body

    def _format_slack_message(self, event: OnCallEvent, escalation_level: int) -> str:
        """Format Slack message."""
        severity_emoji = {"critical": "🔥", "warning": "⚠️", "info": "ℹ️"}.get(
            event.severity.lower(), "🚨"
        )

        return (
            f"{severity_emoji} *ECAP Alert - Level {escalation_level}*\n"
            f"*Alert:* {event.alert_name}\n"
            f"*Service:* {event.service}\n"
            f"*Severity:* {event.severity}\n"
            f"*Time:* {event.created_at.strftime('%H:%M:%S UTC')}\n"
            f"*Description:* {event.description}\n\n"
            f"Please acknowledge immediately!"
        )

    def _format_push_message(self, event: OnCallEvent, escalation_level: int) -> str:
        """Format push notification message."""
        return f"ECAP Alert L{escalation_level}: {event.alert_name} - {event.service}"


class OnCallScheduler:
    """Manage on-call scheduling and rotations."""

    def __init__(self, config: Dict):
        self.config = config
        self.engineers = self._load_engineers()
        self.shifts = []
        self.holidays = self._load_holidays()

    def _load_engineers(self) -> Dict[str, OnCallEngineer]:
        """Load engineer information."""
        engineers_config = self.config.get("engineers", {})
        engineers = {}

        for email, engineer_data in engineers_config.items():
            engineers[email] = OnCallEngineer(email=email, **engineer_data)

        return engineers

    def _load_holidays(self) -> List[datetime]:
        """Load holiday dates."""
        # In real implementation, this would load from calendar API or config
        return [
            datetime(2025, 1, 1),  # New Year's Day
            datetime(2025, 7, 4),  # Independence Day
            datetime(2025, 12, 25),  # Christmas Day
            # Add more holidays as needed
        ]

    def generate_rotation_schedule(
        self, start_date: datetime, duration_weeks: int, rotation_type: str = "weekly"
    ) -> List[OnCallShift]:
        """Generate on-call rotation schedule."""
        shifts = []
        current_date = start_date
        engineer_emails = list(self.engineers.keys())

        if rotation_type == "weekly":
            shift_duration = timedelta(weeks=1)
        elif rotation_type == "daily":
            shift_duration = timedelta(days=1)
        else:
            raise ValueError(f"Unsupported rotation type: {rotation_type}")

        for week in range(duration_weeks):
            for shift_type in [ShiftType.PRIMARY, ShiftType.SECONDARY]:
                # Round-robin assignment
                engineer_index = (
                    week * 2 + (0 if shift_type == ShiftType.PRIMARY else 1)
                ) % len(engineer_emails)
                backup_index = (engineer_index + 1) % len(engineer_emails)

                engineer = self.engineers[engineer_emails[engineer_index]]
                backup_engineer = self.engineers[engineer_emails[backup_index]]

                shift_start = current_date + timedelta(weeks=week)
                shift_end = shift_start + shift_duration

                # Check for holidays and adjust if needed
                if self._is_holiday_period(shift_start, shift_end):
                    shift_type = ShiftType.HOLIDAY

                shift = OnCallShift(
                    shift_id=f"{shift_type.value}-{shift_start.strftime('%Y%m%d')}",
                    engineer=engineer,
                    backup_engineer=backup_engineer,
                    shift_type=shift_type,
                    start_time=shift_start,
                    end_time=shift_end,
                    timezone=engineer.timezone,
                )

                shifts.append(shift)

        self.shifts = shifts
        return shifts

    def _is_holiday_period(self, start_date: datetime, end_date: datetime) -> bool:
        """Check if period includes holidays."""
        return any(start_date <= holiday <= end_date for holiday in self.holidays)

    def get_current_oncall(
        self, shift_type: ShiftType = ShiftType.PRIMARY
    ) -> Optional[OnCallEngineer]:
        """Get currently on-call engineer."""
        current_time = datetime.now(timezone.utc)

        for shift in self.shifts:
            if (
                shift.shift_type == shift_type
                and shift.start_time <= current_time <= shift.end_time
                and shift.is_active
            ):
                return shift.override_engineer or shift.engineer

        return None

    def get_escalation_chain(self) -> List[OnCallEngineer]:
        """Get current escalation chain."""
        primary = self.get_current_oncall(ShiftType.PRIMARY)
        secondary = self.get_current_oncall(ShiftType.SECONDARY)

        escalation_chain = []
        if primary:
            escalation_chain.append(primary)
        if secondary and secondary != primary:
            escalation_chain.append(secondary)

        # Add escalation engineers from config
        escalation_config = self.config.get("escalation_engineers", [])
        for email in escalation_config:
            if email in self.engineers:
                escalation_chain.append(self.engineers[email])

        return escalation_chain

    def create_override(
        self, shift_id: str, override_engineer_email: str, reason: str
    ) -> bool:
        """Create on-call override."""
        for shift in self.shifts:
            if shift.shift_id == shift_id and override_engineer_email in self.engineers:
                shift.override_engineer = self.engineers[override_engineer_email]
                shift.notes = f"Override: {reason}"
                logger.info(
                    f"Created override for shift {shift_id}: {override_engineer_email}"
                )
                return True

        return False

    def get_schedule_conflicts(self) -> List[Dict]:
        """Identify potential scheduling conflicts."""
        conflicts = []

        for shift in self.shifts:
            # Check for engineer availability
            day_of_week = shift.start_time.strftime("%A").lower()
            availability = shift.engineer.availability_hours.get(day_of_week)

            if availability and availability != "00:00-23:59":
                conflicts.append(
                    {
                        "type": "availability_conflict",
                        "shift_id": shift.shift_id,
                        "engineer": shift.engineer.email,
                        "issue": f"Engineer not available {availability} on {day_of_week}",
                    }
                )

        return conflicts


class EscalationManager:
    """Manage alert escalations through on-call chain."""

    def __init__(
        self,
        scheduler: OnCallScheduler,
        notification_service: NotificationService,
        config: Dict,
    ):
        self.scheduler = scheduler
        self.notification_service = notification_service
        self.config = config
        self.escalation_config = config.get("escalation_config", {})
        self.active_escalations = {}

    async def handle_alert(self, event: OnCallEvent) -> str:
        """Handle incoming alert and start escalation."""
        logger.info(f"Handling alert: {event.alert_name}")

        # Start escalation process
        escalation_id = f"ESC-{event.event_id}"
        self.active_escalations[escalation_id] = event

        # Begin escalation chain
        await self._escalate_to_level(event, 1)

        return escalation_id

    async def _escalate_to_level(self, event: OnCallEvent, level: int):
        """Escalate to specific level in on-call chain."""
        escalation_chain = self.scheduler.get_escalation_chain()

        if level > len(escalation_chain):
            logger.error(f"Escalation level {level} exceeds available engineers")
            return

        if level > event.max_escalation_level:
            logger.error(f"Maximum escalation level reached for {event.event_id}")
            return

        engineer = escalation_chain[level - 1]
        event.current_escalation_level = level

        logger.info(f"Escalating {event.event_id} to level {level}: {engineer.email}")

        # Send notifications based on engineer preferences
        for notification_type in engineer.escalation_preference:
            attempt = EscalationAttempt(
                attempt_id=f"{event.event_id}-L{level}-{notification_type.value}",
                engineer=engineer,
                notification_type=notification_type,
                sent_at=datetime.now(timezone.utc),
                status=EscalationStatus.PENDING,
            )

            success = await self.notification_service.send_notification(
                engineer, notification_type, event, level
            )

            if success:
                event.escalation_attempts.append(attempt)

            # Add delay between notification types
            await asyncio.sleep(1)

        # Schedule escalation to next level if no response
        response_timeout = self.escalation_config.get("response_timeout_minutes", 5)
        asyncio.create_task(
            self._schedule_next_escalation(event, level, response_timeout)
        )

    async def _schedule_next_escalation(
        self, event: OnCallEvent, current_level: int, timeout_minutes: int
    ):
        """Schedule escalation to next level after timeout."""
        await asyncio.sleep(timeout_minutes * 60)

        # Check if incident was acknowledged or resolved
        if not event.acknowledged and not event.resolved:
            logger.info(
                f"No response to level {current_level}, escalating {event.event_id}"
            )
            await self._escalate_to_level(event, current_level + 1)

    async def acknowledge_alert(self, event_id: str, engineer_email: str) -> bool:
        """Acknowledge alert by engineer."""
        for _escalation_id, event in self.active_escalations.items():
            if event.event_id == event_id:
                event.acknowledged = True
                acknowledgment_time = datetime.now(timezone.utc)

                # Update escalation attempts
                for attempt in event.escalation_attempts:
                    if (
                        attempt.engineer.email == engineer_email
                        and attempt.status == EscalationStatus.PENDING
                    ):
                        attempt.status = EscalationStatus.ACKNOWLEDGED
                        attempt.acknowledged_at = acknowledgment_time
                        attempt.response_time = acknowledgment_time - attempt.sent_at

                logger.info(f"Alert {event_id} acknowledged by {engineer_email}")
                return True

        return False

    async def resolve_alert(self, event_id: str, resolution: str) -> bool:
        """Mark alert as resolved."""
        for _escalation_id, event in self.active_escalations.items():
            if event.event_id == event_id:
                event.resolved = True
                event.resolved_at = datetime.now(timezone.utc)
                event.resolution_time = event.resolved_at - event.created_at

                logger.info(f"Alert {event_id} resolved: {resolution}")
                return True

        return False

    def get_escalation_metrics(self) -> Dict[str, Any]:
        """Get escalation performance metrics."""
        total_events = len(self.active_escalations)
        acknowledged_events = sum(
            1 for event in self.active_escalations.values() if event.acknowledged
        )
        resolved_events = sum(
            1 for event in self.active_escalations.values() if event.resolved
        )

        # Calculate average response times
        response_times = []
        for event in self.active_escalations.values():
            for attempt in event.escalation_attempts:
                if attempt.response_time:
                    response_times.append(attempt.response_time.total_seconds())

        avg_response_time = (
            sum(response_times) / len(response_times) if response_times else 0
        )

        return {
            "total_events": total_events,
            "acknowledged_events": acknowledged_events,
            "resolved_events": resolved_events,
            "acknowledgment_rate": acknowledged_events / total_events
            if total_events > 0
            else 0,
            "resolution_rate": resolved_events / total_events
            if total_events > 0
            else 0,
            "average_response_time_seconds": avg_response_time,
        }


class OnCallRotationManager:
    """Main orchestrator for on-call rotation management."""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.scheduler = OnCallScheduler(self.config)
        self.notification_service = NotificationService(self.config)
        self.escalation_manager = EscalationManager(
            self.scheduler, self.notification_service, self.config
        )

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from file."""
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    async def initialize_rotation(self, start_date: datetime, duration_weeks: int = 12):
        """Initialize on-call rotation schedule."""
        schedule = self.scheduler.generate_rotation_schedule(start_date, duration_weeks)
        logger.info(f"Generated {len(schedule)} shifts for {duration_weeks} weeks")

        # Check for conflicts
        conflicts = self.scheduler.get_schedule_conflicts()
        if conflicts:
            logger.warning(f"Found {len(conflicts)} scheduling conflicts")
            for conflict in conflicts:
                logger.warning(f"Conflict: {conflict}")

        return schedule

    async def handle_webhook_alert(self, alert_data: Dict) -> str:
        """Handle incoming alert webhook."""
        # Convert alert data to OnCallEvent
        event = OnCallEvent(
            event_id=alert_data.get(
                "event_id", f"EVT-{int(datetime.now().timestamp())}"
            ),
            alert_name=alert_data.get("alert_name", "Unknown Alert"),
            severity=alert_data.get("severity", "warning"),
            service=alert_data.get("service", "unknown"),
            description=alert_data.get("description", ""),
            created_at=datetime.now(timezone.utc),
            escalation_attempts=[],
        )

        # Start escalation process
        escalation_id = await self.escalation_manager.handle_alert(event)
        return escalation_id

    def get_current_oncall_status(self) -> Dict[str, Any]:
        """Get current on-call status."""
        primary = self.scheduler.get_current_oncall(ShiftType.PRIMARY)
        secondary = self.scheduler.get_current_oncall(ShiftType.SECONDARY)

        return {
            "primary_oncall": {
                "name": primary.name if primary else None,
                "email": primary.email if primary else None,
                "phone": primary.phone if primary else None,
            },
            "secondary_oncall": {
                "name": secondary.name if secondary else None,
                "email": secondary.email if secondary else None,
                "phone": secondary.phone if secondary else None,
            },
            "escalation_chain": [
                {"name": eng.name, "email": eng.email}
                for eng in self.scheduler.get_escalation_chain()
            ],
        }

    def get_rotation_schedule(self, weeks: int = 4) -> List[Dict]:
        """Get upcoming rotation schedule."""
        current_time = datetime.now(timezone.utc)
        upcoming_shifts = [
            shift for shift in self.scheduler.shifts if shift.end_time > current_time
        ][
            : weeks * 7
        ]  # Approximate weeks to days

        return [
            {
                "shift_id": shift.shift_id,
                "engineer": shift.engineer.name,
                "backup": shift.backup_engineer.name if shift.backup_engineer else None,
                "shift_type": shift.shift_type.value,
                "start_time": shift.start_time.isoformat(),
                "end_time": shift.end_time.isoformat(),
                "override": shift.override_engineer.name
                if shift.override_engineer
                else None,
            }
            for shift in upcoming_shifts
        ]

    async def create_shift_override(
        self, shift_id: str, override_engineer_email: str, reason: str
    ) -> bool:
        """Create on-call shift override."""
        success = self.scheduler.create_override(
            shift_id, override_engineer_email, reason
        )

        if success:
            # Notify relevant parties about override
            logger.info(f"Override created for shift {shift_id}")
            # In real implementation, send notifications about override

        return success

    def generate_oncall_report(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        """Generate on-call performance report."""
        escalation_metrics = self.escalation_manager.get_escalation_metrics()

        # Add more detailed analytics
        report = {
            "period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
            "escalation_metrics": escalation_metrics,
            "shift_coverage": self._calculate_shift_coverage(start_date, end_date),
            "engineer_workload": self._calculate_engineer_workload(
                start_date, end_date
            ),
        }

        return report

    def _calculate_shift_coverage(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        """Calculate shift coverage statistics."""
        period_shifts = [
            shift
            for shift in self.scheduler.shifts
            if start_date <= shift.start_time <= end_date
        ]

        total_shifts = len(period_shifts)
        covered_shifts = sum(1 for shift in period_shifts if shift.is_active)
        override_shifts = sum(1 for shift in period_shifts if shift.override_engineer)

        return {
            "total_shifts": total_shifts,
            "covered_shifts": covered_shifts,
            "coverage_percentage": (covered_shifts / total_shifts * 100)
            if total_shifts > 0
            else 0,
            "override_shifts": override_shifts,
            "override_percentage": (override_shifts / total_shifts * 100)
            if total_shifts > 0
            else 0,
        }

    def _calculate_engineer_workload(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        """Calculate workload distribution among engineers."""
        engineer_shifts = {}

        for shift in self.scheduler.shifts:
            if start_date <= shift.start_time <= end_date:
                engineer_email = shift.engineer.email
                if engineer_email not in engineer_shifts:
                    engineer_shifts[engineer_email] = {
                        "name": shift.engineer.name,
                        "primary_shifts": 0,
                        "secondary_shifts": 0,
                        "total_hours": 0,
                    }

                if shift.shift_type == ShiftType.PRIMARY:
                    engineer_shifts[engineer_email]["primary_shifts"] += 1
                elif shift.shift_type == ShiftType.SECONDARY:
                    engineer_shifts[engineer_email]["secondary_shifts"] += 1

                shift_duration = (
                    shift.end_time - shift.start_time
                ).total_seconds() / 3600
                engineer_shifts[engineer_email]["total_hours"] += shift_duration

        return engineer_shifts


# Example usage and configuration
async def main():
    """Example usage of the on-call rotation system."""
    # Example configuration
    config = {
        "engineers": {
            "alice@ecap.local": {
                "name": "Alice Smith",
                "phone": "+1-555-0001",
                "slack_user_id": "U123456",
                "timezone": "America/Los_Angeles",
                "skills": ["api", "database", "security"],
            },
            "bob@ecap.local": {
                "name": "Bob Johnson",
                "phone": "+1-555-0002",
                "slack_user_id": "U234567",
                "timezone": "America/Los_Angeles",
                "skills": ["frontend", "infrastructure"],
            },
            "charlie@ecap.local": {
                "name": "Charlie Brown",
                "phone": "+1-555-0003",
                "slack_user_id": "U345678",
                "timezone": "America/New_York",
                "skills": ["data", "analytics", "ml"],
            },
        },
        "notification_config": {
            "phone": {"enabled": True},
            "sms": {"enabled": True},
            "email": {"enabled": True},
            "slack": {"enabled": True, "webhook_url": "https://hooks.slack.com/..."},
            "pagerduty": {"enabled": True, "integration_key": "..."},
        },
        "escalation_config": {"response_timeout_minutes": 5},
    }

    # Create rotation manager
    rotation_manager = OnCallRotationManager("/dev/null")  # Would use real config file
    rotation_manager.config = config
    rotation_manager.scheduler = OnCallScheduler(config)
    rotation_manager.notification_service = NotificationService(config)
    rotation_manager.escalation_manager = EscalationManager(
        rotation_manager.scheduler, rotation_manager.notification_service, config
    )

    # Initialize rotation
    start_date = datetime.now(timezone.utc)
    schedule = await rotation_manager.initialize_rotation(start_date, 12)

    print(f"Created rotation schedule with {len(schedule)} shifts")

    # Get current status
    status = rotation_manager.get_current_oncall_status()
    print(f"Current on-call status: {status}")

    # Simulate an alert
    alert_data = {
        "alert_name": "APIServiceDown",
        "severity": "critical",
        "service": "api",
        "description": "API service is not responding",
    }

    escalation_id = await rotation_manager.handle_webhook_alert(alert_data)
    print(f"Started escalation: {escalation_id}")


if __name__ == "__main__":
    asyncio.run(main())
