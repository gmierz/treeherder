import pytest

from treeherder.perf.auto_perf_sheriffing.telemetry_alerting.utils import (
    CHANNEL_TO_REPO_MAPPING,
    DESKTOP_PLATFORMS,
    get_glean_dictionary_link,
    get_treeherder_detection_link,
    get_treeherder_detection_range_link,
)


class TestGetGleanDictionaryLink:
    def test_desktop_platform_windows(self, test_telemetry_signature):
        """Test Glean dictionary link generation for Windows platform."""
        test_telemetry_signature.platform = "Windows"
        test_telemetry_signature.probe = "test_probe"

        link = get_glean_dictionary_link(test_telemetry_signature)

        assert (
            link
            == "https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/test_probe"
        )

    def test_desktop_platform_linux(self, test_telemetry_signature):
        """Test Glean dictionary link generation for Linux platform."""
        test_telemetry_signature.platform = "Linux"
        test_telemetry_signature.probe = "memory_probe"

        link = get_glean_dictionary_link(test_telemetry_signature)

        assert (
            link
            == "https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/memory_probe"
        )

    def test_desktop_platform_darwin(self, test_telemetry_signature):
        """Test Glean dictionary link generation for Darwin (macOS) platform."""
        test_telemetry_signature.platform = "Darwin"
        test_telemetry_signature.probe = "cpu_probe"

        link = get_glean_dictionary_link(test_telemetry_signature)

        assert (
            link
            == "https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/cpu_probe"
        )

    def test_mobile_platform_fenix(self, test_telemetry_signature):
        """Test Glean dictionary link generation for mobile (non-desktop) platform."""
        test_telemetry_signature.platform = "Android"
        test_telemetry_signature.probe = "mobile_probe"

        link = get_glean_dictionary_link(test_telemetry_signature)

        assert link == "https://dictionary.telemetry.mozilla.org/apps/fenix/metrics/mobile_probe"

    def test_with_probe_containing_special_characters(self, test_telemetry_signature):
        """Test Glean dictionary link with probe name containing underscores and numbers."""
        test_telemetry_signature.platform = "Windows"
        test_telemetry_signature.probe = "networking_http_channel_page_open_to_first_sent_v2"

        link = get_glean_dictionary_link(test_telemetry_signature)

        assert (
            link
            == "https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/networking_http_channel_page_open_to_first_sent_v2"
        )


class TestGetTreeherderDetectionLink:
    def test_nightly_channel(self, test_telemetry_signature):
        """Test Treeherder detection link for Nightly channel."""
        test_telemetry_signature.channel = "Nightly"
        detection_range = {"detection": type("obj", (object,), {"revision": "abcdef123456"})()}

        link = get_treeherder_detection_link(detection_range, test_telemetry_signature)

        assert (
            link == "https://treeherder.mozilla.org/jobs?repo=mozilla-central&revision=abcdef123456"
        )

    def test_release_channel(self, test_telemetry_signature):
        """Test Treeherder detection link for Release channel."""
        test_telemetry_signature.channel = "Release"
        detection_range = {"detection": type("obj", (object,), {"revision": "release123456"})()}

        link = get_treeherder_detection_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-release&revision=release123456"
        )

    def test_beta_channel(self, test_telemetry_signature):
        """Test Treeherder detection link for Beta channel."""
        test_telemetry_signature.channel = "Beta"
        detection_range = {"detection": type("obj", (object,), {"revision": "beta987654"})()}

        link = get_treeherder_detection_link(detection_range, test_telemetry_signature)

        assert link == "https://treeherder.mozilla.org/jobs?repo=mozilla-beta&revision=beta987654"

    def test_unknown_channel_defaults_to_central(self, test_telemetry_signature):
        """Test Treeherder detection link defaults to mozilla-central for unknown channel."""
        test_telemetry_signature.channel = "UnknownChannel"
        detection_range = {"detection": type("obj", (object,), {"revision": "unknown123456"})()}

        link = get_treeherder_detection_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-central&revision=unknown123456"
        )

    def test_with_long_revision(self, test_telemetry_signature):
        """Test Treeherder detection link with full-length revision hash."""
        test_telemetry_signature.channel = "Nightly"
        detection_range = {
            "detection": type(
                "obj", (object,), {"revision": "abcdef1234567890abcdef1234567890abcdef12"}
            )()
        }

        link = get_treeherder_detection_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-central&revision=abcdef1234567890abcdef1234567890abcdef12"
        )


class TestGetTreeherderDetectionRangeLink:
    def test_nightly_channel_range(self, test_telemetry_signature):
        """Test Treeherder detection range link for Nightly channel."""
        test_telemetry_signature.channel = "Nightly"
        detection_range = {
            "from": type("obj", (object,), {"revision": "from123456"})(),
            "to": type("obj", (object,), {"revision": "to789012"})(),
        }

        link = get_treeherder_detection_range_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-central&fromchange=from123456&tochange=to789012"
        )

    def test_release_channel_range(self, test_telemetry_signature):
        """Test Treeherder detection range link for Release channel."""
        test_telemetry_signature.channel = "Release"
        detection_range = {
            "from": type("obj", (object,), {"revision": "releaseFrom123"})(),
            "to": type("obj", (object,), {"revision": "releaseTo456"})(),
        }

        link = get_treeherder_detection_range_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-release&fromchange=releaseFrom123&tochange=releaseTo456"
        )

    def test_beta_channel_range(self, test_telemetry_signature):
        """Test Treeherder detection range link for Beta channel."""
        test_telemetry_signature.channel = "Beta"
        detection_range = {
            "from": type("obj", (object,), {"revision": "betaFrom789"})(),
            "to": type("obj", (object,), {"revision": "betaTo012"})(),
        }

        link = get_treeherder_detection_range_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-beta&fromchange=betaFrom789&tochange=betaTo012"
        )

    def test_unknown_channel_defaults_to_central_range(self, test_telemetry_signature):
        """Test Treeherder detection range link defaults to mozilla-central for unknown channel."""
        test_telemetry_signature.channel = "DevChannel"
        detection_range = {
            "from": type("obj", (object,), {"revision": "devFrom345"})(),
            "to": type("obj", (object,), {"revision": "devTo678"})(),
        }

        link = get_treeherder_detection_range_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-central&fromchange=devFrom345&tochange=devTo678"
        )

    def test_with_full_length_revisions(self, test_telemetry_signature):
        """Test Treeherder detection range link with full-length revision hashes."""
        test_telemetry_signature.channel = "Nightly"
        detection_range = {
            "from": type(
                "obj", (object,), {"revision": "abcdef1234567890abcdef1234567890abcdef12"}
            )(),
            "to": type(
                "obj", (object,), {"revision": "fedcba0987654321fedcba0987654321fedcba98"}
            )(),
        }

        link = get_treeherder_detection_range_link(detection_range, test_telemetry_signature)

        assert (
            link
            == "https://treeherder.mozilla.org/jobs?repo=mozilla-central&fromchange=abcdef1234567890abcdef1234567890abcdef12&tochange=fedcba0987654321fedcba0987654321fedcba98"
        )


class TestConstants:
    """Test that constants are properly defined (no specific logic tests needed)."""

    def test_desktop_platforms_constant(self):
        """Verify DESKTOP_PLATFORMS contains expected platforms."""
        assert "Windows" in DESKTOP_PLATFORMS
        assert "Linux" in DESKTOP_PLATFORMS
        assert "Darwin" in DESKTOP_PLATFORMS
        assert len(DESKTOP_PLATFORMS) == 3

    def test_channel_to_repo_mapping_constant(self):
        """Verify CHANNEL_TO_REPO_MAPPING contains expected mappings."""
        assert CHANNEL_TO_REPO_MAPPING["Nightly"] == "mozilla-central"
        assert CHANNEL_TO_REPO_MAPPING["Release"] == "mozilla-release"
        assert CHANNEL_TO_REPO_MAPPING["Beta"] == "mozilla-beta"
        assert len(CHANNEL_TO_REPO_MAPPING) == 3
