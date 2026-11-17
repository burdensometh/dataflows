import os

from .load import load
from .dumpers import versioning


class load_versioned(load):
    """
    Load a package previously published by
    :class:`dataflows.processors.dumpers.to_versioned_path.VersionedPathDumper`.

    The version to read is resolved when the flow starts processing (not when
    this processor is constructed). The ``current`` symlink is resolved once to
    a concrete version directory, so even if a new version is published while
    the resources are being iterated, the reader keeps reading the exact data
    of the version it pinned at load start.

    :param package_path: root directory of the versioned package (the
        directory containing ``current`` and ``versions/``), or any path
        inside it.
    :param version: ``None``/``'current'`` for the latest version, or an
        explicit version id (``int`` or numeric ``str``) to read an old
        version.
    :param options: all other :class:`load` options (``resources``,
        ``cast_strategy`` ...) are forwarded unchanged.
    """

    def __init__(self, package_path, version=None, **options):
        # Resolve lazily in process_datapackage(): constructing a processor
        # must not require the package to exist yet, and pinning happens when
        # the reader actually starts loading.
        super(load_versioned, self).__init__(package_path, **options)
        self.package_path = package_path
        self.version = version
        self.resolved_version_dir = None
        self.resolved_descriptor_path = None
        self.pinned_version_id = None

    @staticmethod
    def _find_root(package_path):
        """Walk upwards from a path to the versioned package root."""
        root = os.path.abspath(package_path)
        candidate = root
        while True:
            if versioning.is_versioned_root(candidate):
                return candidate
            parent = os.path.dirname(candidate)
            if parent == candidate:
                break
            candidate = parent
        raise FileNotFoundError(
            '{!r} is not inside a versioned package '
            '(no {!r} link or {!r} directory found)'
            .format(package_path, versioning.CURRENT_LINKNAME,
                    versioning.VERSIONS_DIRNAME))

    def _pin_version(self):
        """Resolve the requested version once, pinning this reader to it."""
        if self.resolved_descriptor_path is not None:
            return
        root = self._find_root(self.package_path)
        version_dir = versioning.resolve_version(root, self.version)
        # realpath collapses the symlink: the pinned path is a concrete
        # versions/<id> directory and cannot change underneath us.
        version_dir = os.path.realpath(version_dir)
        self.resolved_version_dir = version_dir
        self.resolved_descriptor_path = os.path.join(
            version_dir, versioning.DESCRIPTOR_FILENAME)
        self.pinned_version_id = os.path.basename(version_dir)
        # Hand the plain load processor a concrete descriptor path.
        self.load_source = self.resolved_descriptor_path
        self.options['format'] = 'datapackage'

    def process_datapackage(self, dp):
        self._pin_version()
        return super(load_versioned, self).process_datapackage(dp)
