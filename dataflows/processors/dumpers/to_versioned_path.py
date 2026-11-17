import os
import shutil
import datetime

from .file_dumper import FileDumper
from .dumper_base import DumperBase
from . import versioning


class VersionedPathDumper(FileDumper):
    """
    Dump a multi-resource package using local versioned publishing.

    All resource files and the final ``datapackage.json`` (with the real row
    counts, byte sizes and hashes - including for empty resources) are first
    written into a private staging directory. Only when the whole stream has
    been transformed and validated successfully is the staging directory
    published atomically as a new immutable version under
    ``<out_path>/versions/<version>`` and the ``current`` symlink repointed at
    it. If any step fails the staging directory is discarded and the previous
    version remains the one readers see.

    Readers should use :class:`dataflows.processors.load_versioned.load_versioned`
    (or :func:`dataflows.load_versioned`) so that the version they read is fixed
    when loading starts.

    Any number of readers may iterate concurrently with a publish (they pin a
    version and read immutable files). Concurrent publishers writing into the
    same root are not supported; the publish swap itself is locked and atomic,
    so a clashing run fails cleanly instead of corrupting the current version.
    """

    def __init__(self, out_path='.', **options):
        super(VersionedPathDumper, self).__init__(options)
        self.out_path = os.path.abspath(out_path)
        os.makedirs(self.out_path, exist_ok=True)
        # Never reuse staging data from a crashed earlier run.
        versioning.cleanup_staging(self.out_path)
        self.version_id = versioning.next_version_id(self.out_path)
        self.staging_path = versioning.staging_dir(self.out_path,
                                                   self.version_id)
        os.makedirs(self.staging_path, exist_ok=True)
        self._published = False
        self._failed = False

    def write_file_to_output(self, filename, path):
        # Every file lands in the private staging directory. Nothing visible
        # to readers is touched until finalize().
        path = os.path.join(self.staging_path, path)
        if self.add_filehash_to_path and os.path.exists(path):
            return
        path_part = os.path.dirname(path)
        os.makedirs(path_part, exist_ok=True)
        shutil.copy(filename, path)
        return path

    def _guard_resource(self, iterator):
        """Iterate one resource, converting any failure into a failed publish."""
        try:
            for item in iterator:
                yield item
        except Exception:
            # The error may surface while rows are transformed, validated or
            # written. Discard the partial output so the previously published
            # version stays usable, then propagate to stop the flow.
            self._failed = True
            self._discard_staging()
            raise

    def process_resources(self, resources):
        try:
            for resource_iterator in \
                    super(VersionedPathDumper, self).process_resources(resources):
                yield self._guard_resource(resource_iterator)
        except Exception:
            self._failed = True
            self._discard_staging()
            raise

    def row_counter(self, resource, iterator):
        # Mirror DumperBase.row_counter, but make sure the true row count is
        # persisted on the descriptor entry that actually gets serialised into
        # datapackage.json (the base implementation only touches the Resource
        # object's private descriptor copy). This covers empty resources too:
        # they are still written and recorded with a count of zero.
        counter = 0
        for row in iterator:
            counter += 1
            yield row
        descriptor = None
        for candidate in self.datapackage.descriptor.get('resources', []):
            if candidate.get('name') == resource.res.descriptor.get('name'):
                descriptor = candidate
                break
        if descriptor is not None:
            DumperBase.set_attr(descriptor, self.resource_rowcount, counter)
        DumperBase.inc_attr(self.datapackage.descriptor,
                            self.datapackage_rowcount, counter)
        resource.res.commit()
        self.datapackage.commit()

    def finalize(self):
        if self._failed or self._published:
            super(VersionedPathDumper, self).finalize()
            return
        descriptor = self.datapackage.descriptor
        metadata = {
            'published_at': datetime.datetime.utcnow()
                .replace(microsecond=0).isoformat() + 'Z',
            'name': descriptor.get('name'),
            'count_of_rows': self.stats.get('count_of_rows'),
            'bytes': self.stats.get('bytes'),
            'hash': self.stats.get('hash'),
            'resources': [
                {
                    'name': resource.get('name'),
                    'path': resource.get('path'),
                    'count_of_rows': resource.get(
                        self.resource_rowcount, 0),
                    'bytes': resource.get(self.resource_bytes, 0),
                    'hash': resource.get(self.resource_hash),
                }
                for resource in descriptor.get('resources', [])
            ],
        }
        # The full stream (all resources, descriptor with final row counts and
        # hashes) is in staging now. Swap it in atomically.
        try:
            versioning.publish_version(self.out_path, self.version_id,
                                       metadata)
        except Exception:
            # publish_version rolls the build back to staging; discard it so
            # the last known-good version remains the only visible one.
            self._failed = True
            self._discard_staging()
            raise
        self._published = True
        super(VersionedPathDumper, self).finalize()

    def _discard_staging(self):
        if not self._published and os.path.isdir(self.staging_path):
            shutil.rmtree(self.staging_path, ignore_errors=True)


# Public processor name, mirroring ``dump_to_path``.
dump_to_versioned_path = VersionedPathDumper
