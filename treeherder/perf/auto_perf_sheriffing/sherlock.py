import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from json import JSONDecodeError, loads
from logging import INFO, WARNING

import requests
from django.conf import settings
from django.db.models import QuerySet
from taskcluster.helper import TaskclusterConfig

from treeherder.perf.auto_perf_sheriffing.backfill_reports import (
    BackfillReportMaintainer,
)
from treeherder.perf.auto_perf_sheriffing.backfill_tool import BackfillTool
from treeherder.perf.auto_perf_sheriffing.secretary import Secretary
from treeherder.perf.exceptions import CannotBackfillError, MaxRuntimeExceededError
from treeherder.perf.models import (
    BackfillNotificationRecord,
    BackfillRecord,
    BackfillReport,
    PerformanceFramework,
    PerformanceTelemetryAlert,
    PerformanceTelemetryAlertSummary,
    PerformanceTelemetrySignature,
    Push,
    Repository,
)
from treeherder.services import taskcluster

logger = logging.getLogger(__name__)

CLIENT_ID = settings.PERF_SHERIFF_BOT_CLIENT_ID
ACCESS_TOKEN = settings.PERF_SHERIFF_BOT_ACCESS_TOKEN

BUILDID_MAPPING = "https://hg.mozilla.org/mozilla-central/json-firefoxreleases"
REVISION_INFO = "https://hg.mozilla.org/mozilla-central/json-log/%s"
GLEAN_DICTIONARY = "https://dictionary.telemetry.mozilla.org/apps/{page}/metrics/{probe}"
TREEHERDER_PUSH = "https://treeherder.mozilla.org/jobs?repo={repo}&revision={revision}"
TREEHERDER_DATES = (
    "https://treeherder.mozilla.org/jobs?repo={repo}&"
    "fromchange={from_change}&tochange={to_change}"
)
PUSH_LOG = (
    "https://hg-edge.mozilla.org/mozilla-central/pushloghtml?"
    "startdate={start_date}&enddate={end_date}"
)
BZ_TELEMETRY_ALERTS = (
    "https://bugzilla.mozilla.org/buglist.cgi?"
    "keywords=telemetry-alert&keywords_type=allwords"
    "&chfield=[Bug%20creation]&chfieldto={today}&chfieldfrom={prev_day}&"
)

DESKTOP_PLATFORMS = ("Windows", "Linux", "Darwin",)
CHANNEL_TO_REPO_MAPPING = {
    "Nightly": "mozilla-central",
    "Release": "mozilla-release",
    "Beta": "mozilla-beta",
}

INITIAL_PROBES = (
    "memory_ghost_windows",
    "cycle_collector_time",
    "mouseup_followed_by_click_present_latency",
    "network_tcp_connection",
    "network_tls_handshake",
    "networking_http_channel_page_open_to_first_sent",
    "performance_pageload_fcp",
    "perf_largest_contentful_paint",
)


class Sherlock:
    """
    Robot variant of a performance sheriff (the main class)

    Automates backfilling of skipped perf jobs.
    """

    DEFAULT_MAX_RUNTIME = timedelta(minutes=50)

    def __init__(
        self,
        report_maintainer: BackfillReportMaintainer,
        backfill_tool: BackfillTool,
        secretary: Secretary,
        max_runtime: timedelta = None,
        supported_platforms: list[str] = None,
    ):
        self.report_maintainer = report_maintainer
        self.backfill_tool = backfill_tool
        self.secretary = secretary
        self._max_runtime = self.DEFAULT_MAX_RUNTIME if max_runtime is None else max_runtime

        self.supported_platforms = supported_platforms or settings.SUPPORTED_PLATFORMS
        self._wake_up_time = datetime.now()
        self._buildid_mappings = {}

    def sheriff(self, since: datetime, frameworks: list[str], repositories: list[str]):
        logger.info("Sherlock: Validating settings...")
        self.secretary.validate_settings()

        logger.info("Sherlock: Marking reports for backfill...")
        self.secretary.mark_reports_for_backfill()
        self.assert_can_run()

        # secretary checks the status of all backfilled jobs
        self.secretary.check_outcome()
        self.assert_can_run()

        # reporter tool should always run *(only handles preliminary records/reports)*
        logger.info("Sherlock: Reporter tool is creating/maintaining  reports...")
        self._report(since, frameworks, repositories)
        self.assert_can_run()

        # backfill tool follows
        logger.info("Sherlock: Starting to backfill...")
        self._backfill(frameworks, repositories)
        self.assert_can_run()

    def runtime_exceeded(self) -> bool:
        elapsed_runtime = datetime.now() - self._wake_up_time
        return self._max_runtime <= elapsed_runtime

    def assert_can_run(self):
        if self.runtime_exceeded():
            raise MaxRuntimeExceededError("Sherlock: Max runtime exceeded.")

    def _report(
        self, since: datetime, frameworks: list[str], repositories: list[str]
    ) -> list[BackfillReport]:
        return self.report_maintainer.provide_updated_reports(since, frameworks, repositories)

    def _backfill(self, frameworks: list[str], repositories: list[str]):
        for platform in self.supported_platforms:
            self.__backfill_on(platform, frameworks, repositories)

    def __backfill_on(self, platform: str, frameworks: list[str], repositories: list[str]):
        left = self.secretary.backfills_left(on_platform=platform)
        total_consumed = 0

        records_to_backfill = self.__fetch_records_requiring_backfills_on(
            platform, frameworks, repositories
        )
        logger.info(
            f"Sherlock: {records_to_backfill.count()} records found to backfill on {platform.title()}."
        )

        for record in records_to_backfill:
            if left <= 0 or self.runtime_exceeded():
                break
            left, consumed = self._backfill_record(record, left)
            logger.info(f"Sherlock: Backfilled record with id {record.alert.id}.")
            # Model used for reporting backfill outcome
            BackfillNotificationRecord.objects.create(record=record)
            total_consumed += consumed

        self.secretary.consume_backfills(platform, total_consumed)
        logger.info(f"Sherlock: Consumed {total_consumed} backfills for {platform.title()}.")
        logger.debug(f"Sherlock: Having {left} backfills left on {platform.title()}.")

    @staticmethod
    def __fetch_records_requiring_backfills_on(
        platform: str, frameworks: list[str], repositories: list[str]
    ) -> QuerySet:
        records_to_backfill = BackfillRecord.objects.select_related(
            "alert",
            "alert__series_signature",
            "alert__series_signature__platform",
            "alert__summary__framework",
            "alert__summary__repository",
        ).filter(
            status=BackfillRecord.READY_FOR_PROCESSING,
            alert__series_signature__platform__platform__icontains=platform,
            alert__summary__framework__name__in=frameworks,
            alert__summary__repository__name__in=repositories,
        )
        return records_to_backfill

    def _backfill_record(self, record: BackfillRecord, left: int) -> tuple[int, int]:
        consumed = 0

        try:
            context = record.get_context()
        except JSONDecodeError:
            logger.warning(f"Failed to backfill record {record.alert.id}: invalid JSON context.")
            record.status = BackfillRecord.FAILED
            record.save()
        else:
            data_points_to_backfill = self.__get_data_points_to_backfill(context)
            for data_point in data_points_to_backfill:
                if left <= 0 or self.runtime_exceeded():
                    break
                try:
                    using_job_id = data_point["job_id"]
                    self.backfill_tool.backfill_job(using_job_id)
                    left, consumed = left - 1, consumed + 1
                except (KeyError, CannotBackfillError, Exception) as ex:
                    logger.debug(f"Failed to backfill record {record.alert.id}: {ex}")
                else:
                    record.try_remembering_job_properties(using_job_id)

            success, outcome = self._note_backfill_outcome(
                record, len(data_points_to_backfill), consumed
            )
            log_level = INFO if success else WARNING
            logger.log(log_level, f"{outcome} (for backfill record {record.alert.id})")

        return left, consumed

    @staticmethod
    def _note_backfill_outcome(
        record: BackfillRecord, to_backfill: int, actually_backfilled: int
    ) -> tuple[bool, str]:
        success = False

        record.total_actions_triggered = actually_backfilled

        if actually_backfilled == to_backfill:
            record.status = BackfillRecord.BACKFILLED
            success = True
            outcome = "Backfilled all data points"
        else:
            record.status = BackfillRecord.FAILED
            if actually_backfilled == 0:
                outcome = "Backfill attempts on all data points failed right upon request."
            elif actually_backfilled < to_backfill:
                outcome = "Backfill attempts on some data points failed right upon request."
            else:
                raise ValueError(
                    f"Cannot have backfilled more than available attempts ({actually_backfilled} out of {to_backfill})."
                )

        record.set_log_details({"action": "BACKFILL", "outcome": outcome})
        record.save()
        return success, outcome

    @staticmethod
    def _is_queue_overloaded(provisioner_id: str, worker_type: str, acceptable_limit=100) -> bool:
        """
        Helper method for Sherlock to check load on processing queue.
        Usage example: _queue_is_too_loaded('gecko-3', 'b-linux')
        :return: True/False
        """
        tc = TaskclusterConfig("https://firefox-ci-tc.services.mozilla.com")
        tc.auth(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
        queue = tc.get_service("queue")

        pending_tasks_count = queue.pendingTasks(provisioner_id, worker_type).get("pendingTasks")

        return pending_tasks_count > acceptable_limit

    @staticmethod
    def __get_data_points_to_backfill(context: list[dict]) -> list[dict]:
        context_len = len(context)
        start = None

        if context_len == 1:
            start = 0
        elif context_len > 1:
            start = 1

        return context[start:]

    def telemetry_alert(self):
        if not settings.TELEMETRY_ENABLE_ALERTS:
            logger.info("Telemetry alerting is disabled. Enable it with TELEMETRY_ENABLE_ALERTS=1")
            return

        import mozdetect
        from mozdetect.telemetry_query import get_metric_table

        if (
            not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            and settings.SITE_HOSTNAME == "treeherder.mozilla.org"
        ):
            raise Exception(
                "GOOGLE_APPLICATION_CREDENTIALS must be defined in production. "
                "Use GCLOUD_DIR for local testing."
            )

        ts_detectors = mozdetect.get_timeseries_detectors()

        metric_definitions = self._get_metric_definitions()

        repository = Repository.objects.get(name="mozilla-central")
        framework = PerformanceFramework.objects.get(name="telemetry")
        for metric_info in metric_definitions:
            if metric_info["name"] not in INITIAL_PROBES:
                continue
            logger.info(f"Running detection for {metric_info['name']}")
            cdf_ts_detector = ts_detectors[
                metric_info["data"]
                .get("monitor", {})
                .get("change-detection-technique", "cdf_squared")
            ]

            for platform in ("Windows", "Darwin", "Linux"):
                if metric_info["platform"] == "mobile" and platform != "Mobile":
                    continue
                elif metric_info["platform"] == "desktop" and platform == "Mobile":
                    continue
                logger.info(f"On Platform {platform}")
                try:
                    data = get_metric_table(
                        metric_info["name"],
                        platform,
                        android=(platform == "Mobile"),
                        use_fog=True,
                    )
                    if data.empty:
                        logger.info("No data found")
                        continue

                    timeseries = mozdetect.TelemetryTimeSeries(data)

                    ts_detector = cdf_ts_detector(timeseries)
                    detections = ts_detector.detect_changes()

                    alerts = []
                    for detection in detections:
                        # Only get buildids if there might be a detection
                        if not self._buildid_mappings:
                            self._make_buildid_to_date_mapping()
                        alert = self._create_detection_alert(
                            detection, metric_info, platform, repository, framework
                        )
                        if alert:
                            alerts.append(alert)
                        break

                    if alerts:
                        alert_manager = TelemetryAlertManager(
                            metric_definitions, TelemetryBugManager(), TelemetryEmailManager()
                        )
                        alert_manager.manage_alerts(
                            alerts
                        )

                except Exception:
                    logger.info(f"Failed: {traceback.format_exc()}")

    def _create_detection_alert(
        self,
        detection: object,
        probe_info: dict,
        platform: str,
        repository: Repository,
        framework: PerformanceFramework,
    ):
        # Get, or create the signature
        # TODO: Allow multiple channels, legacy probes, and different apps
        probe_signature, _ = PerformanceTelemetrySignature.objects.update_or_create(
            channel="Nightly",
            platform=platform,
            probe=probe_info["name"],
            probe_type="Glean",
            application="Firefox",
        )

        detection_date = str(detection.location)
        if detection_date not in self._buildid_mappings[platform]:
            # TODO: See if we should expand the range in this situation
            detection_date = self._find_closest_build_date(detection_date, platform)

        detection_build = self._buildid_mappings[platform][detection_date]
        prev_build = self._buildid_mappings[platform][detection_build["prev_build"]]
        next_build = self._buildid_mappings[platform][detection_build["next_build"]]

        # Get the pushes for these builds
        detection_push = Push.objects.get(
            revision=detection_build["node"], repository__name=repository.name
        )
        prev_push = Push.objects.get(revision=prev_build["node"], repository__name=repository.name)
        next_push = Push.objects.get(revision=next_build["node"], repository__name=repository.name)

        # Check that an alert summary doesn't already exist around this point (+/- 1 day)
        latest_timestamp = next_push.time + timedelta(days=1)
        oldest_timestamp = next_push.time - timedelta(days=1)
        try:
            detection_summary = PerformanceTelemetryAlertSummary.objects.filter(
                repository=repository,
                framework=framework,
                push__time__gte=oldest_timestamp,
                push__time__lte=latest_timestamp,
            ).latest("push__time")
        except PerformanceTelemetryAlertSummary.DoesNotExist:
            detection_summary = None

        if not detection_summary:
            # Create an alert summary to capture all alerts
            # that occurred on the same date range
            detection_summary, _ = PerformanceTelemetryAlertSummary.objects.get_or_create(
                repository=repository,
                framework=framework,
                prev_push=prev_push,
                push=next_push,
                original_push=detection_push,
                defaults={
                    "manually_created": False,
                    "created": datetime.now(timezone.utc),
                },
            )

        try:
            detection_alert = PerformanceTelemetryAlert.objects.get(
                summary_id=detection_summary.id,
                series_signature_id=probe_signature.id
            )
        except PerformanceTelemetryAlertSummary.DoesNotExist:
            detection_alert = None


        # if not detection_alert:
        detection_alert, _ = PerformanceTelemetryAlert.objects.update_or_create(
            summary_id=detection_summary.id,
            series_signature=probe_signature,
            defaults={
                "is_regression": True,
                "amount_pct": round(
                    (100.0 * abs(detection.new_value - detection.previous_value))
                    / float(detection.previous_value),
                    2,
                ),
                "amount_abs": abs(detection.new_value - detection.previous_value),
                "sustained": True,
                "direction": detection.direction,
                "confidence": detection.confidence,
                "prev_value": detection.previous_value,
                "new_value": detection.new_value,
                "prev_median": detection.optional_detection_info["Interpolated Median"][0],
                "new_median": detection.optional_detection_info["Interpolated Median"][1],
                "prev_p90": detection.optional_detection_info["Interpolated p05"][0],
                "new_p90": detection.optional_detection_info["Interpolated p05"][1],
                "prev_p95": detection.optional_detection_info["Interpolated p95"][0],
                "new_p95": detection.optional_detection_info["Interpolated p95"][1],
            },
        )
        return TelemetryAlert(probe_signature, detection_alert, detection_summary)

        # return None

    def _get_metric_definitions(self) -> list[dict]:
        metric_definition_urls = [
            ("https://dictionary.telemetry.mozilla.org/data/firefox_desktop/index.json", "desktop"),
            ("https://dictionary.telemetry.mozilla.org/data/fenix/index.json", "mobile"),
        ]

        merged_metrics = []

        for url, platform in metric_definition_urls:
            try:
                logger.info(f"Getting probes from {url}")
                response = requests.get(url)
                response.raise_for_status()

                data = response.json()
                metrics = data.get("metrics", [])
                for metric in metrics:
                    merged_metrics.append(
                        {
                            "name": metric["name"].replace(".", "_"),
                            "data": metric,
                            "platform": platform,
                        }
                    )

                logger.info(f"Found {len(metrics)} probes")
            except requests.RequestException as e:
                logger.info(f"Failed to fetch from {url}: {e}")
            except ValueError:
                logger.info(f"Invalid JSON from {url}")

        return merged_metrics

    def _make_buildid_to_date_mapping(self):
        # Always returned in order of newest to oldest, only capture
        # the newest build for each day, and ignore others. This can
        # differ between platforms too (e.g. failed builds)
        buildid_mappings = self._get_buildid_mappings()

        prev_date = {}
        for build in buildid_mappings["builds"]:
            platform = self._replace_platform_build_name(build["platform"])
            if not platform:
                continue
            curr_date = str(datetime.strptime(build["buildid"][:8], "%Y%m%d").date())

            platform_builds = self._buildid_mappings.setdefault(platform, {})
            if curr_date not in platform_builds:
                platform_builds[curr_date] = build

                if prev_date.get(platform):
                    platform_builds[prev_date[platform]]["prev_build"] = curr_date
                    platform_builds[curr_date]["next_build"] = prev_date[platform]
                else:
                    platform_builds[curr_date]["next_build"] = curr_date

            prev_date[platform] = curr_date

    def _get_buildid_mappings(self) -> dict:
        try:
            response = requests.get(BUILDID_MAPPING)
            response.raise_for_status()
            return loads(response.content)
        except requests.RequestException as e:
            raise Exception(f"Failed to download buildid mappings, cannot produce detections: {e}")

    def _replace_platform_build_name(self, platform: str) -> str:
        if platform == "win64":
            return "Windows"
        if platform == "linux64":
            return "Linux"
        if platform == "mac":
            return "Darwin"
        return ""

    def _find_closest_build_date(self, detection_date: str, platform: str) -> str:
        # Get the closest date to the detection date
        prev_date = None

        for date in sorted(list(self._buildid_mappings[platform].keys())):
            if date > detection_date:
                break
            prev_date = date

        return prev_date


from treeherder.perf.email import Email, EmailWriter


###############
#### utils ####
###############


def get_glean_dictionary_link(telemetry_signature):
    if telemetry_signature.platform in DESKTOP_PLATFORMS:
        dictionary_page = "firefox_desktop"
    else:
        dictionary_page = "fenix"
    return GLEAN_DICTIONARY.format(
        page=dictionary_page,
        probe=telemetry_signature.probe
    )


def get_treeherder_detection_link(detection_range, telemetry_signature):
    repo = CHANNEL_TO_REPO_MAPPING.get(
        telemetry_signature.channel, "mozilla-central"
    )

    return TREEHERDER_PUSH.format(
        repo=repo,
        revision=detection_range["detection"].revision
    )


def get_treeherder_detection_range_link(detection_range, telemetry_signature):
    repo = CHANNEL_TO_REPO_MAPPING.get(
        telemetry_signature.channel, "mozilla-central"
    )

    return TREEHERDER_DATES.format(
        repo=repo,
        from_change=detection_range["from"].revision,
        to_change=detection_range["to"].revision
    )


def get_notification_emails(metric_info, default="gmierzwinski@mozilla.com"):
    return metric_info["data"].get("monitor", {}).get(
        "bugzilla_notification_emails",
        metric_info.get("notification_emails", [default])
    )



#########################################
#### telemetry probe representation #####
#########################################


class TelemetryProbe:
    def __init__(self, metric_info):
        self.metric_info = metric_info
        self.verify_probe_definition(metric_info)

    def verify_probe_definition(self):
        pass

    def should_file_bug(self):
        pass

    def should_email(self):
        pass

    def get_notification_emails(metric_info, default="gmierzwinski@mozilla.com"):
        return metric_info["data"].get("monitor", {}).get(
            "bugzilla_notification_emails",
            metric_info.get("notification_emails", [default])
        )

#######################
#### alert manager ####
#######################


class TelemetryAlert:
    def __init__(self, telemetry_signature, telemetry_alert, telemetry_alert_summary):
        self.telemetry_signature = telemetry_signature
        self.telemetry_alert = telemetry_alert
        self.telemetry_alert_summary = telemetry_alert_summary
        self.related_telemetry_alerts = None
        self.detection_range = None

    def get_related_alerts(self):
        if self.related_telemetry_alerts:
            return self.related_telemetry_alerts

        self.related_telemetry_alerts = PerformanceTelemetryAlert.objects.filter(
            summary_id=self.telemetry_alert_summary.id
        ).exclude(
            id=self.telemetry_alert.id
        )

        return self.related_telemetry_alerts

    def get_detection_range(self):
        if self.detection_range:
            return self.detection_range

        self.detection_range = {
            "from": self.telemetry_alert_summary.prev_push,
            "to": self.telemetry_alert_summary.push,
            "detection": self.telemetry_alert_summary.original_push,
        }

        return self.detection_range

class AlertManager:
    """Handles the alert management.

    This includes the following:
        (1) Filing bugs
        (2) Setting status for the alerts when bugs are updated
        (3) Adding comments to bugs for addition alerts when they are produced
            after the bug was.
    """
    def __init__(self, bug_manager, email_manager):
        self.bug_manager = bug_manager
        self.email_manager = email_manager

    def manage_alerts(self, alerts, *args, **kwargs):
        """Handles everything related to alert notifications.

        None of these depend on each other, so a failure in one doesn't always
        mean that a failure in another one will happen. 
        """
        # if not settings.TELEMETRY_ENABLE_ALERTS_MANAGER:
        #     logger.info(
        #         "Telemetry alerts manager is not enabled. Set "
        #         "TELEMETRY_ENABLE_ALERTS_MANAGER=1 to enable it."
        #     )
        #     return

        try:
            # Update alerts with bug info. Done before filing new bugs
            # to prevent updating from recently filed bugs
            self.update_alerts(*args, **kwargs)
        except Exception:
            logger.info(f"Failed to update alerts: {traceback.format_exc()}")

        try:
            # Comment on existing bugs. Done before filing new bugs to
            # prevent commenting on recently filed bugs
            commented_bugs = self.comment_alert_bugs(*args, **kwargs)
        except Exception:
            logger.info(f"Failed to comment on existing bugs: {traceback.format_exc()}")
        
        try:
            # File new bugs
            new_bugs = self.file_alert_bugs(alerts, *args, **kwargs)
            return
        except Exception:
            logger.info(f"Failed to file alert bugs: {traceback.format_exc()}")

        try:
            # Modify any of the bugs that were commented on, or created
            self.modify_alert_bugs(alerts, commented_bugs, new_bugs, *args, **kwargs)
            return
        except Exception:
            logger.info(f"Failed to file alert bugs: {traceback.format_exc()}")

        try:
            # Produce email notifications. Done after filing bugs to include
            # bug information if that becomes a feature at some point
            self.email_alerts(alerts, *args, **kwargs)
        except Exception:
            logger.info(f"Failed to send email alerts: {traceback.format_exc()}")

    def get_bugzilla_bugs(self, *args, **kwargs):
        """Query to get all the bugzilla bugs for alerts."""
        raise NotImplementedError()

    def update_alerts(self, alerts, *args, **kwargs):
        """Updates all alerts with status changes from the associated bugs."""
        raise NotImplementedError()

    def comment_alert_bugs(self, alerts, *args, **kwargs):
        """Comments on bugs to mention additional alerting measurements."""
        raise NotImplementedError()

    def file_alert_bugs(self, alerts, *args, **kwargs):
        """Files a bug for each telemetry alert summary."""
        bugs = []

        for alert in alerts:
            bug = self._file_alert_bug(alert, *args, **kwargs)
            if bug is None:
                continue
            bugs.append(bug)

        return bugs

    def _file_alert_bug(self, *args, **kwargs):
        """Create a bug for an alert."""
        raise NotImplementedError()

    def modify_alert_bugs(self, alerts, commented_bugs, new_bugs, *args, **kwargs):
        """Modify alert bugs."""
        raise NotImplementedError()

    def email_alerts(self, alerts, *args, **kwargs):
        """Sends out emails for each new alert."""
        for alert in alerts:
            self._email_alert(alert, *args, **kwargs)

    def _email_alert(self, *args, **kwargs):
        """Produces an email for a new telemetry alert summary."""
        raise NotImplementedError()


@BugModifier.add
class SeeAlsoModifier:
    @staticmethod
    def modify(alerts, commented_bugs, new_bugs, **kwargs):
        pass


class BugModifier:
    modifiers = []

    @staticmethod
    def add(modifier_class):
        BugModifier.modifiers.append(modifier_class)

    @staticmethod
    def get_modifiers():
        return BugModifier.modifiers

    @staticmethod
    def get_bug_modifications(alerts, commented_bugs, new_bugs, **kwargs):
        all_changes = {}
        for modifier in BugModifier.get_modifiers():
            changes = modifier.modify(alerts, commented_bugs, new_bugs, **kwargs)
            if not changes:
                continue
            for bug, changes in changes.items():
                all_changes.setdefault(bug, []).append(changes)
        return BugModifier._merge_changes(all_changes)

    @staticmethod
    def _merge_changes(all_changes):

        def __merge_change(field, value, existing_value):
            if field in ("priority", "severity"):
                if value != existing_value:
                    raise Exception(
                        f"Modification in `{field}` from multiple Modifiers is not"
                        f"allowed. Values found: {value}, and {existing_value}"
                    )
                return existing_value
            elif field in ("comment",):
                raise Exception("Cannot post multiple comments to a bug.")
            elif field in ("see_also", "keywords", "whiteboard",):
                return f"{existing_value},{value}"
            raise Exception(
                f"Unable to consolidate field `{field}` with multiple values."
            )

        bug_changes = {}

        for bug, changes in all_changes:
            for field, value in changes.items():
                if field not in bug_changes:
                    bug_changes[field] = value
                    continue
                bug_changes[field] = __merge_change(field, value, bug_changes[field])

        return bug_changes


class TelemetryAlertManager(AlertManager):
    """Alert Management for Telemetry Alerts.

    TODOs:
        * Update alerts with bug status changes.
        * Add code to comment, or post changes to existing bugs
    """

    def __init__(self, metrics_info, bug_manager, email_manager):
        super().__init__(bug_manager, email_manager)

        # Convert it to a dictionary for quicker access
        self.metrics_info = {}
        for metric in metrics_info:
            if metric["name"] in self.metrics_info:
                # This happens when a metric is defined for desktop, and mobile
                continue
            self.metrics_info[metric["name"]] = metric

    def _get_metric_info(self, probe):
        metric_info = self.metrics_info.get(probe, None)
        if not metric_info:
            raise Exception(
                f"Unknown probe alerted. No information known about it: "
                f"{alert.telemetry_alert.telemetry_signature.probe}"
            )
        return metric_info

    def comment_alert_bugs(self, alerts):
        """Comments on bugs to mention additional alerting probes.

        Telemetry alerting doesn't currently have any commenting behaviour.
        Associations between related alerts will be done through the modify_alerts
        method, and the "See Also" Bugzilla field.
        """
        pass

    def update_alerts(self, alerts):
        """Updates all alerts with status changes from the associated bugs.

        Queries bugzilla for all telemetry-alert bugs, then goes through the
        PerformanceTelemetryAlertSummary objects to update those that changed.

        An alternative is to go through every telemetry alert in the DB, however
        that results in many more network requests. However, this approach involves
        more DB queries. 
        """
        pass

    def modify_alert_bugs(self, alerts, commented_bugs, new_bugs):
        """Modifies the alert bugs.

        Modifies existing telemetry alert bugs with new bugs that alerted on
        the same day. It adds these bugs to the "See Also" field for the other bugs.
        """


    def __should_file_bug(self, metric_info, alert):
        """Ensure that the alert should have a bug filed.

        Current criteria for bug filling are:
            (1) Monitor field must be defined (already checked by this point).
            (2) Monitor field is set to an object, AND the alert field is 
                set to True.
            (3) Alert does not already have a bug filed for it.
        """
        monitor = metric_info["data"].get("monitor". {})
        if monitor and not alert.telemetry_alert.bug_number:
            # Monitor field is defined, and no bug was created
            if isinstance(monitor, bool):
                # Monitor field is set to True, should only produce emails
                return False
            if isinstance(monitor, dict) and monitor.get("alert", False):
                # Alert field is set to True
                return True
        return False


    def _file_alert_bug(self, alert):
        """Files a bug for each telemetry alert summary.

        Only produced for telemetry alert summaries without a bug number. Those
        with a bug number are handled by comment_alert_bugs.
        """
        metric_info = self._get_metric_info(alert.telemetry_signature.probe)
        if not __should_file_bug(metric_info, alert):
            return

        # File a bug
        bug_info = self.bug_manager.file_bug(metric_info, alert)

        # Associate it with the alert
        alert.telemetry_alert.bug_number = int(bug_info["id"])
        alert.telemetry_alert.save()

        return bug_info["id"]


    def __should_notify(self, metric_info, alert):
        """Ensure that the alert should produce notifications.

        Current criteria for notifications are:
            (1) Monitor field must be defined (already checked by this point).
            (2) Monitor field is set to True OR monitor field is set to 
                an object with alert set to False.
            (3) Bug has not been filed for the alert.
        """
        monitor = metric_info["data"].get("monitor". {})
        if monitor and not alert.telemetry_alert.bug_number:
            # Monitor field is defined, and no bug was created
            if isinstance(monitor, bool):
                # Monitor field is set to True
                return True
            if isinstance(monitor, dict) and not monitor.get("alert", False):
                # Alert field is set to False
                return True
        return False


    def _email_alert(self, alert):
        """Sends out emails for each new alert.

        Each probe that alerted will have an email sent out to all alert notification
        emails. This means that if a telemetry alert summary (a grouping of alerts)
        contains multiple probes that alert, each of those probes will have an email
        sent out.
        """
        metric_info = self._get_metric_info(alert.telemetry_signature.probe)
        if not __should_notify(metric_info, alert):
            return

        # Send notification emails for the alert
        self.email_manager.email_alert(metric_info, alert)

        # Set the alert to notified
        alert.telemetry_alert.notified = True
        alert.telemetry_alert.save()


##########################
"""Bug management below"""
##########################


class BugManager:
    """Files bugs, and comments on them for alerts."""

    def __init__(self):
        self.bz_url = settings.BUGFILER_API_URL + "/rest/bug"
        self.bz_headers = {"Accept": "application/json"}

    def _get_default_bug_creation_data(self):
        return {
            "summary": "",
            "type": "defect",
            "product": "",
            "component": "",
            "keywords": "",
            "whiteboard": None,
            "regressed_by": None,
            "see_also": None,
            "version": None,
            "severity": "",
            "priority": "",
            "description": "",
        }

    def _get_default_bug_comment_data(self):
        return {
            "comment": {"body": ""}
        }

    def _add_needinfo(self, bugzilla_email, bug_data):
        bug_data.setdefault("flags", []).append({
            "name": "needinfo",
            "status": "?",
            "requestee": bugzilla_email,
        })

    def _create(self, bug_data):
        """Create a new bug.

        See `_get_default_bug_creation_data` for an example of what the
        `bug_data` should be.
        """
        headers = self.bz_headers
        headers["x-bugzilla-api-key"] = settings.BUGFILER_API_KEY

        resp = requests.post(
            url=self.bz_url,
            json=bug_data,
            headers=headers,
            verify=True,
            timeout=30,
        )
        resp.raise_for_status()

        return resp.json()

    def _modify(self, bug, changes):
        """Add a comment, or modify a bug.

        See `_get_default_bug_comment_data` for what the `bug_data`
        should be.
        """
        modification_url = self.bz_url + f"/{bug}"
        headers = self.bz_headers
        headers["x-bugzilla-api-key"] = settings.COMMENTER_API_KEY
        headers["User-Agent"] = f"treeherder/{settings.SITE_HOSTNAME}",

        resp = requests.put(
            url=self.bz_url,
            json=changes,
            headers=headers,
            verify=True,
            timeout=30,
        )
        resp.raise_for_status()

        return resp.json()

    def file_bug(self, *args, **kwargs):
        raise NotImplementedError()

    def modify_bug(self, *args, **kwargs):
        raise NotImplementedError()

    def comment_bug(self, *args, **kwargs):
        raise NotImplementedError()


class TelemetryBugManager(BugManager):
    """Files bugs, and comments on them for telemetry alerts."""

    def __init__(self):
        super().__init__()

    def file_bug(self, metric_info, alert):
        logger.info(f"Filing bug for alert on probe {metric_info['name']}")
        bug_data = self._get_default_bug_creation_data()
        bug_content = TelemetryBugContent().build_bug_content(alert)

        bug_data["summary"] = bug_content["title"]
        bug_data["description"] = bug_content["description"]

        # For testing use Testing :: Performance, later switch to
        # using tags from metrics_info
        bug_data["product"] = "Testing"
        bug_data["component"] = "Performance"

        bug_data["severity"] = "S4"
        bug_data["priority"] = "P5"
        bug_data["keywords"] = "telemetry-alert,regression"

        # Only set a needinfo on the first email in the notification list
        needinfo_emails = get_notification_emails(
            metric_info, default="gmierz2@outlook.com"
        )
        if not needinfo_emails:
            raise Exception(
                f"Unable to file bug for {alert.telemetry_signature.probe} since "
                f"there are no emails specified for needinfos."
            )
        self._add_needinfo(needinfo_emails[0], bug_data)

        bug_info = self._create(bug_data)
        logger.info(f"Filed bug {bug_info['id']}")

        return bug_info

    def modify_bug(self, bug, changes):
        pass

    def comment_bug(self, alert):
        pass


class TelemetryBugContent:
    """Formats the content of a bug.

    Used for producing the first comment of a bug (description, or comment #0),
    and for producing comments after an initial bug is created.
    """

    BUG_DESCRIPTION = (
        "MozDetect has detected changes in the following telemetry probes on "
        "builds from [{date}]({detection_push_link}). As the probe owner of the "
        "following probes, we need your help to address this regression and "
        "find a culprit."
        "\n\n{change_table}\n"
        "[See these Treeherder pushes for possible culprits for this detected change]"
        "({detection_range_link}).\n\n"
        "[A push log can be found here for a quicker overview of the changes that"
        "occurred around this change]({push_log_link}).\n\n"
        "For more information on how to handle these probe changes, and "
        "what the various columns mean [see here](link to docs).\n\n"
        "Note that it’s possible the culprit is from a commit in the day before, "
        "or the day after these push logs. It’s also possible that these culprits "
        "are not the cause, and the change could be coming from a popular "
        "website. Please reach out to the Performance team if you suspect this to be "
        "the case in [#perf on Matrix](https://matrix.to/#/#perf:mozilla.org), or "
        "[#perf-help on Slack](https://mozilla.enterprise.slack.com/archives/C03U19JCSFQ)."
        "\n\n"
        "[See this bugzilla query for other telemetry alerts that were "
        "produced on this date]({bz_telemetry_alerts})."
    )

    BUG_COMMENT = (
        "MozDetect has detected changes in additional probes on the same date "
        "which may be related to the changes the bug was originally filed for."
        "\n\n{change_table}"
    )

    TABLE_HEADERS = (
        "| **Probe** | **Platform** | **Magnitude** "
        "| **Previous Values** | **New Values** |\n"
        "| :---: | :---: | :---: | :---: | :---: |\n"
    )

    REGRESSION_TITLE = "### Regressions\n"
    IMPROVMENT_TITLE = "### Improvement\n"
    GENERIC_TITLE = "### Changes Detected\n"

    BUG_TITLE = "Telemetry Alert for {probe} on {date}"

    def build_bug_content(self, alert):
        bug_content = {"title": "", "description": ""}

        detection_range = alert.get_detection_range()
        detection_date = detection_range["detection"].time.strftime("%Y-%m-%d")
        repo = CHANNEL_TO_REPO_MAPPING.get(
            alert.telemetry_signature.channel,
            "mozilla-central"
        )

        # End date is exclusive so we need to add 1 day to it
        start_date = detection_range["from"].time.strftime("%Y-%m-%d")
        end_date = (detection_range["to"].time + timedelta(days=1)).strftime("%Y-%m-%d")

        bug_content["title"] = self.BUG_TITLE.format(
            probe=alert.telemetry_signature.probe,
            date=detection_date
        )

        bug_content["description"] = self.BUG_DESCRIPTION.format(
            date=detection_date,
            detection_push_link=TREEHERDER_PUSH.format(
                repo=repo,
                revision=detection_range["detection"].revision
            ),
            change_table=self._build_change_table(alert),
            detection_range_link=TREEHERDER_DATES.format(
                repo=repo,
                from_change=detection_range["from"].revision,
                to_change=detection_range["to"].revision
            ),
            push_log_link=PUSH_LOG.format(
                start_date=start_date,
                end_date=end_date
            ),
            bz_telemetry_alerts=BZ_TELEMETRY_ALERTS.format(
                today=datetime.now().strftime("%Y-%m-%d"),
                prev_day=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            )
        )

        return bug_content

    def build_comment_content(self, alert):
        comment_content = {"comment": ""}
        pass

    def _build_change_table(self, alert):
        change_table = self.GENERIC_TITLE

        if alert.telemetry_alert.is_regression:
            change_table = self.REGRESSION_TITLE

        change_table += self.TABLE_HEADERS+self._build_probe_alert_row(alert)

        return change_table

    def _build_probe_alert_row(self, alert):
        values = (
            "| **Median:** {prev_median} | **Median:** {new_median} |\n"
            "| | | | **P90:** {prev_p90} | **P90:** {new_p90} |\n"
            "| | | | **P95:** {prev_p95} | **P95:** {new_p95} |"
        ).format(
            prev_median=round(alert.telemetry_alert.prev_median, 2),
            prev_p90=round(alert.telemetry_alert.prev_p90, 2),
            prev_p95=round(alert.telemetry_alert.prev_p95, 2),
            new_median=round(alert.telemetry_alert.new_median, 2),
            new_p90=round(alert.telemetry_alert.new_p90, 2),
            new_p95=round(alert.telemetry_alert.new_p95, 2)
        )

        return (
            "| [{probe}]({glean_dictionary_link}) | {platform} | {magnitude} {values} \n"
        ).format(
            probe=alert.telemetry_signature.probe,
            platform=alert.telemetry_signature.platform,
            magnitude=alert.telemetry_alert.confidence,
            values=values,
            glean_dictionary_link=get_glean_dictionary_link(
                alert.telemetry_signature
            )
        )



############################
"""Email management below"""
############################



class EmailManager:
    """Formats and emails alert notifications."""

    def __init__(self):
        self.notify_client = taskcluster.notify_client_factory()

    def get_email_func(self):
        return self.notify_client.email

    def email_alert(self, *args, **kwargs):
        pass


class TelemetryEmailManager(EmailManager):
    """Formats and emails alert notifications."""

    def email_alert(self, metric_info, alert):
        telemetry_email = TelemetryEmail(self.get_email_func())

        for email in get_notification_emails(metric_info):
            telemetry_email.email(email, metric_info, alert)


class TelemetryEmail:
    """Adapter for the email producers."""

    def __init__(self, email_func):
        self.email_writer = TelemetryEmailWriter()
        self.email_client = None
        self._set_email_method(email_func)

    def email(self, *args, **kwargs):
        # Email through Taskcluster client
        self.email_func(self._prepare_email(*args, **kwargs))

    def _set_email_method(self, func):
        self.email_func = func

    def _prepare_email(self, *args, **kwargs):
        return self.email_writer.prepare_email(*args, **kwargs)


class TelemetryEmailWriter(EmailWriter):

    def prepare_email(self, email, metric_info, alert, **kwargs):
        self._write_address(email)
        self._write_subject(metric_info)
        self._write_content(metric_info, alert)

        return self.email

    def _write_address(self, email):
        self._email.address = email

    def _write_subject(self, metric_info):
        self._email.subject = f"Telemetry Alert for Probe {metric_info['name']}"

    def _write_content(self, metric_info, alert):
        content = TelemetryEmailContent()
        content.write_email(metric_info, alert)
        self._email.content = str(content)


class TelemetryEmailContent:
    DESCRIPTION = (
        "MozDetect has detected a telemetry change in a probe "
        "you are subscribed to:"
        "\n---\n"
    )

    TABLE_HEADERS = (
        "| Channel | Probe | Platform | Date Range | Detection Push |\n"
        "| :---: | :---: | :---: | :---: | :---: |\n"
    )

    ADDITIONAL_PROBES = (
        "See below for additional probes that alerted at the same time (or near "
        "the same push):"
        "\n---\n"
    )

    def __init__(self):
        self._raw_content = None

    def write_email(self, metric_info, alert):
        self._initialize_report_intro()
        self._include_probe(
            alert.get_detection_range(),
            alert.telemetry_signature,
            alert.telemetry_alert_summary
        )

        if alert.get_related_alerts():
            self._include_additional_probes(alert)

    def _initialize_report_intro(self):
        if self._raw_content is None:
            self._raw_content = self.DESCRIPTION + self.TABLE_HEADERS

    def _include_probe(
        self, detection_range, telemetry_signature, telemetry_alert_summary
    ):
        new_table_row = self._build_table_row(
            detection_range, telemetry_signature, telemetry_alert_summary
        )
        self._raw_content += f"{new_table_row}\n"

    def _include_additional_probes(self, alert):
        self._raw_content += f"\n{self.ADDITIONAL_PROBES}{self.TABLE_HEADERS}"
        for related_alert in alert.get_related_alerts():
            self._include_probe(
                alert.get_detection_range(),
                related_alert.series_signature,
                alert.telemetry_alert_summary
            )

    def _build_table_row(
        self, detection_range, telemetry_signature, telemetry_alert_summary
    ) -> str:
        return (
            "| {channel} | [{probe}]({glean_dictionary_link}) | {platform} "
            "| [{date_from} - {date_to}]({treeherder_date_link})"
            "| [Detection Push]({treeherder_push_link}) |"
        ).format(
            channel=telemetry_signature.channel,
            probe=telemetry_signature.probe,
            platform=telemetry_signature.platform,
            date_from=detection_range["from"].time.strftime("%Y-%m-%d"),
            date_to=detection_range["to"].time.strftime("%Y-%m-%d"),
            treeherder_date_link=get_treeherder_detection_range_link(
                detection_range, telemetry_signature
            ),
            treeherder_push_link=get_treeherder_detection_link(
                detection_range, telemetry_signature
            ),
            glean_dictionary_link=get_glean_dictionary_link(
                telemetry_signature
            )
        )

    def __str__(self):
        if self._raw_content is None:
            raise ValueError("No content set")
        return self._raw_content
