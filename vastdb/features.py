"""Version-dependent features."""

import logging

from .errors import NotSupportedVersion

log = logging.getLogger()


class Features:
    """VAST database features - check if server is already support a feature."""

    def __init__(self, vast_version):
        """Save the server version."""
        self.vast_version = vast_version
        log.info("VAST version: %s", self.vast_version)

        self.check_imports_table = self._check(
            "Imported objects' table feature requires 5.2+ VAST release",
            vast_version >= (5, 2))

        self.check_return_row_ids = self._check(
            "Returning row IDs requires 5.1+ VAST release",
            vast_version >= (5, 1))

        self.check_enforce_semisorted_projection = self._check(
            "Semi-sorted projection enforcement requires 5.1+ VAST release",
            vast_version >= (5, 1))

        self.check_external_row_ids_allocation = self._check(
            "External row IDs allocation requires 5.1+ VAST release",
            vast_version >= (5, 1))

        self.check_elysium = self._check(
            "Elysium requires 5.3.5+ VAST release",
            vast_version >= (5, 3))  # TODO: make this validation stricter for v5.4 (beta/poc version is 5.3.0.x)

        self.check_zip_import = self._check(
            "Zip import requires 5.3.1+ VAST release",
            vast_version >= (5, 3, 1))

    def _check(self, msg, supported):
        log.debug("%s (current version is %s): supported=%s", msg, self.vast_version, supported)
        if not supported:
            def fail():
                raise NotSupportedVersion(msg, self.vast_version)
            return fail

        def noop():
            pass
        return noop
