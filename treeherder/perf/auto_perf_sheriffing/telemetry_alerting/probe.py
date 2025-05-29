from treeherder.perf.auto_perf_sheriffing.telemetry_alerting.utils import DEFAULT_CHANGE_DETECTION


class TelemetryProbeValidationError(Exception):
    """Raised when a probes information is incorrect, or missing."""

    def __init__(self, name, message):
        super().__init__(f"Probe {name}: {message}")


class TelemetryProbe:
    def __init__(self, metric_info):
        self.metric_info = metric_info
        self.name = self.metric_info["name"]
        self._should_file_bug = None
        self._should_email = None

        self.monitor_info = self.metric_info["data"].get("monitor")
        if self.monitor_info["detect_changes"]:
            self.verify_probe_definition()
            self.should_file_bug()
            self.should_email()

    @property
    def monitor_info(self):
        return self._monitor_info

    @monitor_info.setter
    def monitor_info(self, monitor_info):
        self._monitor_info = {}
        if isinstance(monitor_info, bool):
            self._monitor_info["detect_changes"] = monitor_info
        elif isinstance(monitor_info, dict) and monitor_info:
            self._monitor_info["detect_changes"] = True
            self._monitor_info.update(monitor_info)
        elif monitor_info is None or (isinstance(monitor_info, dict) and not monitor_info):
            self._monitor_info["detect_changes"] = False
        else:
            raise TelemetryProbeValidationError(
                self.name,
                f"`monitor` field must by either a boolean or dictionary. "
                f"Found: {type(monitor_info)}",
            )

    def get_change_detection_technique(self):
        return self.monitor_info.get("change-detection-technique", DEFAULT_CHANGE_DETECTION)

    def should_file_bug(self):
        # Only file bugs when alert is set to True
        return self.monitor_info.get("alert", False)

    def should_email(self):
        # Only produce emails when alert is undefined or set to False
        return not self.monitor_info.get("alert", False)

    def should_detect_changes(self):
        return self.monitor_info.get("detect_changes", False)

    def get_notification_emails(self, default="gmierzwinski@mozilla.com"):
        return self.monitor_info.get(
            "bugzilla_notification_emails", self.metric_info.get("notification_emails", [default])
        )

    def _verify_monitor_probe(self):
        return  # Notification_emails not being set in telemtry index.json definitions
        # TODO: Get notification email from here:
        # https://dictionary.telemetry.mozilla.org/data/firefox_desktop/
        # metrics/data_perf_largest_contentful_paint.json
        if not self.metric_info.get("notification_emails", []):
            raise TelemetryProbeValidationError(
                self.name, f"`notification_emails` must be set to produce emails for monitoring."
            )

    def verify_probe_definition(self):
        if not self.monitor_info.get("alert", False):
            # At this stage, it's just a monitor probe but it can optionally
            # set the bugzilla_notification_emails to send emails to
            if not self.monitor_info.get("bugzilla_notification_emails"):
                self._verify_monitor_probe()
        elif not self.monitor_info.get("bugzilla_notification_emails"):
            # This probe will produce bugs, so it needs to have the
            # bugzilla_notification_emails set
            raise TelemetryProbeValidationError(
                self.name,
                f"`bugzilla_notification_emails` must be set to valid Bugzilla account "
                f"emails when a probe is set to produce alerts.",
            )
