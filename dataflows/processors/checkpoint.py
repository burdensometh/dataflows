import os
import hashlib
import itertools
import logging

from datapackage import Package

from dataflows import Flow, DataStreamProcessor
from ..base.datastream import DataStream
from ..base.resource_wrapper import ResourceWrapper
from ..helpers.extended_json import ejson
from ..base.exceptions import CheckpointError
from .stream import stream, ACTIVE_SUFFIX
from .unstream import unstream


METADATA_FILENAME = 'checkpoint-metadata.json'
METADATA_FORMAT_VERSION = 1
CHUNK_SIZE = 1024 * 1024

log = logging.getLogger(__name__)


class _CheckpointLock:
    """Exclusive lock held for the whole lifetime of the generated flow.

    Uses an OS advisory lock so synchronization works both between threads
    and between separate processes. On platforms without ``fcntl`` the lock
    degrades gracefully (the on-disk commit stays atomic regardless).
    """

    def __init__(self, lock_path):
        self._lock_file = open(lock_path, 'a+')
        self._locked = False
        try:
            import fcntl
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
            self._locked = True
        except ImportError:
            log.warning('fcntl is unavailable on this platform; '
                        'checkpoint locking is reduced to atomic file replacement')

    def release(self):
        if self._lock_file is None:
            return
        if self._locked:
            try:
                import fcntl
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            except ImportError:
                pass
        self._lock_file.close()
        self._lock_file = None
        self._locked = False

    def close(self):
        # Alias matching the vocabulary of the flow's finalizer step.
        self.release()


def _hash_file(path):
    """Content hash of a file. Only file contents participate - never mtime."""
    hasher = hashlib.sha256()
    with open(path, 'rb') as file:
        for chunk in iter(lambda: file.read(CHUNK_SIZE), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def _canonical(value):
    """Stable JSON encoding so equal parameters always compare equal."""
    try:
        return ejson.dumps(value, sort_keys=True, ensure_ascii=True)
    except (TypeError, ValueError) as exc:
        raise CheckpointError(
            'checkpoint parameters and transformation_version must be '
            'JSON serializable: {!r}'.format(value)) from exc


def _build_input_manifest(inputs):
    """Hash every declared input file by content."""
    manifest = []
    for entry in inputs:
        if isinstance(entry, dict):
            if 'path' not in entry:
                raise CheckpointError(
                    'input mapping entries must contain a "path" key: {!r}'.format(entry))
            path = entry['path']
            label = entry.get('name', path)
        else:
            path = entry
            label = path
        path = os.path.abspath(os.fspath(path))
        if not os.path.isfile(path):
            raise CheckpointError('declared checkpoint input file does not exist: {}'.format(path))
        manifest.append({
            'name': str(label),
            'path': path,
            'sha256': _hash_file(path),
        })
    manifest.sort(key=lambda item: (item['path'], item['name']))
    return manifest


def _read_metadata(path):
    try:
        with open(path, 'rb') as file:
            return ejson.loads(file.read().decode('utf-8'))
    except (OSError, ValueError):
        return None


def _verify_cached_stream(filename, metadata):
    """Validate the cached stream against its recorded metadata.

    Hashes the full file (types, resource schemas and every row are all
    serialized inside it), then parses the descriptor and row blocks to make
    sure the file is structurally complete and matches the recorded
    descriptor, resource count and per-resource row counts.
    """
    hasher = hashlib.sha256()
    descriptor_line = None
    resource_blocks = []
    current = None
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file):
                hasher.update(line.encode('utf-8'))
                if line_number == 0:
                    descriptor_line = line.strip()
                    continue
                if line == '\n':
                    current = None
                    continue
                if current is None:
                    current = []
                    resource_blocks.append(current)
                current.append(line.strip())
    except OSError as exc:
        return 'cached stream is unreadable: {}'.format(exc)

    if hasher.hexdigest() != metadata.get('stream_sha256'):
        return 'cached stream content does not match its recorded hash'

    try:
        descriptor = ejson.loads(descriptor_line)
        Package(descriptor)
    except (TypeError, ValueError):
        return 'cached stream descriptor is not a valid datapackage'

    resources = descriptor.get('resources')
    if not isinstance(resources, list):
        return 'cached stream descriptor contains no resources list'
    if len(resources) != len(resource_blocks):
        return 'cached stream resource count does not match its descriptor'

    recorded_counts = metadata.get('row_counts')
    if not isinstance(recorded_counts, list) or len(recorded_counts) != len(resource_blocks):
        return 'cached stream row counts are missing or incomplete'

    for index, block in enumerate(resource_blocks):
        if len(block) != recorded_counts[index]:
            return 'cached stream row count for resource #{} does not match the record'.format(index)
        for line in block:
            try:
                ejson.loads(line)
            except ValueError:
                return 'cached stream contains an unparseable row'
        schema = resources[index].get('schema')
        if not isinstance(schema, dict) or 'fields' not in schema:
            return 'cached resource schema is missing or invalid'

    if _canonical(descriptor) != metadata.get('descriptor'):
        return 'cached stream descriptor does not match the recorded descriptor'

    return None


class _ValidatingUnstream(DataStreamProcessor):
    """Load a cached stream while re-checking schema and row counts.

    The stream hash is verified before the flow is built; this processor adds
    a second, consumption-time guard so the restored datapackage (types and
    resource schemas included) and every yielded row agree with the record.
    """

    def __init__(self, filename, metadata):
        super().__init__()
        self.filename = filename
        self.metadata = metadata

    def _process(self):
        with open(self.filename, 'r', encoding='utf-8') as file:
            descriptor = ejson.loads(file.readline().strip())
        if _canonical(descriptor) != self.metadata.get('descriptor'):
            raise CheckpointError(
                'checkpoint descriptor does not match the recorded descriptor')
        self.datapackage = Package(descriptor)
        self.datapackage.commit()
        return DataStream(self.datapackage,
                          self._resource_iterators(descriptor['resources']),
                          [])

    def _resource_iterators(self, resources):
        file = open(self.filename, 'r', encoding='utf-8')
        file.readline()  # descriptor line already parsed in _process

        def make_iterator(index, expected):
            seen = 0

            def rows():
                nonlocal seen
                try:
                    while True:
                        line = file.readline()
                        if line == '\n' or line == '':
                            break
                        seen += 1
                        yield ejson.loads(line.strip())
                    if seen != expected:
                        raise CheckpointError(
                            'checkpoint stream for resource #{} delivered {} rows, '
                            'expected {}'.format(index, seen, expected))
                finally:
                    if index == len(resources) - 1:
                        file.close()

            return rows

        for index, resource in enumerate(resources):
            expected = self.metadata['row_counts'][index]
            yield ResourceWrapper(self.datapackage.resources[index],
                                  make_iterator(index, expected)())


class _MetadataCommitProcessor(DataStreamProcessor):
    """Commit checkpoint metadata atomically, but only on a complete prefix.

    The stream processor renames its active file into place only after every
    row of every resource was consumed. If the prefix fails or only consumes
    part of its rows the final stream file is absent: the stale active file
    is discarded and the previously committed cache (if any) is left
    untouched. Cleanup is guarded both around chain initialization (failures
    during schema inference happen before resource iterators exist) and
    around resource iteration itself.
    """

    def __init__(self, checkpoint_name, filename, metadata_filename, active_filename,
                 hasher, stream_stats, metadata):
        super().__init__()
        self.checkpoint_name = checkpoint_name
        self.filename = filename
        self.metadata_filename = metadata_filename
        self.active_filename = active_filename
        self.hasher = hasher
        self.stream_stats = stream_stats
        self.metadata = metadata

    def _abort(self):
        # The final stream file exists only after the stream step's atomic
        # rename, which in turn happens only when every row was consumed.
        # Hence any active file present on failure is partial - even if its
        # generator never ran (e.g. failure during upstream schema inference).
        if os.path.exists(self.active_filename):
            os.unlink(self.active_filename)
        print('checkpoint not saved (prefix did not complete): '
              '{}'.format(self.checkpoint_name))

    def _process(self):
        try:
            return super()._process()
        except BaseException:
            self._abort()
            raise

    def get_iterator(self, datastream):
        base_func = super().get_iterator(datastream)

        def func():
            try:
                yield from base_func()
            except BaseException:
                self._abort()
                raise
            self.metadata['stream_sha256'] = self.hasher.hexdigest()
            self.metadata['row_counts'] = self.stream_stats['row_counts']
            self.metadata['descriptor'] = _canonical(datastream.dp.descriptor)
            active_metadata = self.metadata_filename + ACTIVE_SUFFIX
            with open(active_metadata, 'w', encoding='utf-8') as file:
                file.write(ejson.dumps(self.metadata, sort_keys=True, ensure_ascii=True))
                file.flush()
                os.fsync(file.fileno())
            # Metadata first, stream already in place: a reader always sees
            # either the old complete pair or the new complete pair.
            os.rename(active_metadata, self.metadata_filename)
            print('checkpoint saved: {}'.format(self.checkpoint_name))

        return func


class _LockReleaseProcessor(DataStreamProcessor):
    """Release the checkpoint lock no matter how the flow terminates.

    The cleanup is wrapped around ``_process`` (not only the resource
    iterator) so it also runs when the failure happens while the processor
    chain itself is being built - for example during schema inference,
    before any resource iterator exists.
    """

    def __init__(self, lock):
        super().__init__()
        self.lock = lock

    def _process(self):
        try:
            return super()._process()
        except BaseException:
            self.lock.release()
            raise

    def get_iterator(self, datastream):
        base_func = super().get_iterator(datastream)
        lock = self.lock

        def func():
            try:
                yield from base_func()
            finally:
                lock.release()

        return func


class checkpoint(Flow):
    """Cache the results of the preceding flow steps.

    Legacy usage (no ``input_files`` / ``transformation_version``) is
    unchanged: a checkpoint is reused whenever the stream file exists.

    Verified usage is opt-in. When ``input_files`` and/or
    ``transformation_version`` are declared, the prefix is skipped only when
    all of the following match a *completed* record:

    * the SHA-256 content hash of every declared input file (mtime ignored),
    * the transformation version,
    * the cleaning parameters,
    * the content hash, descriptor (types and resource schemas) and row
      counts of the cached stream itself.

    Any mismatch - including a corrupt or partially written cache - causes
    the prefix to run again. A failed or partially consumed prefix never
    replaces an existing cache, and concurrent writers are serialized with
    an OS lock while commits stay atomic.
    """

    def __init__(self, checkpoint_name, checkpoint_path='.checkpoints', steps=None,
                 resources=None, input_files=None, transformation_version=None,
                 parameters=None):
        if not steps:
            steps = []
        super().__init__(*steps)
        self.checkpoint_name = checkpoint_name
        self.checkpoint_path = os.path.join(checkpoint_path, checkpoint_name)
        self.resources = resources
        self.input_files = list(input_files) if input_files is not None else None
        self.transformation_version = transformation_version
        self.parameters = {} if parameters is None else dict(parameters)
        self._lock = None

    @property
    def filename(self):
        return os.path.join(self.checkpoint_path, 'stream.ndjson')

    @property
    def metadata_filename(self):
        return os.path.join(self.checkpoint_path, METADATA_FILENAME)

    @property
    def active_filename(self):
        return self.filename + ACTIVE_SUFFIX

    @property
    def lock_filename(self):
        return os.path.join(self.checkpoint_path, 'checkpoint.lock')

    @property
    def is_verified(self):
        return self.input_files is not None or self.transformation_version is not None

    def exists(self):
        return os.path.exists(self.filename)

    def _preprocess_chain(self):
        if not self.is_verified:
            return self._legacy_chain()
        return self._verified_chain()

    def _legacy_chain(self):
        if os.path.exists(self.filename):
            print('using checkpoint data from {}'.format(self.checkpoint_path))
            return unstream(self.filename),
        print('saving checkpoint to: {}'.format(self.checkpoint_path))
        return itertools.chain(self.chain, (stream(self.filename),
                                            _notify_checkpoint_saved(self.checkpoint_name)))

    def _verified_chain(self):
        os.makedirs(self.checkpoint_path, exist_ok=True)
        # Fail early on non-serializable declarations before running anything.
        _canonical(self.parameters)
        if self.transformation_version is not None:
            _canonical(self.transformation_version)
        # Lock first: after this point no other writer can touch the folder.
        lock = _CheckpointLock(self.lock_filename)
        self._lock = lock
        try:
            # Remove leftovers of a crashed run from any previous process.
            for stale in (self.active_filename, self.metadata_filename + ACTIVE_SUFFIX):
                if os.path.exists(stale):
                    os.unlink(stale)

            invalid_reason = self._try_load_reason()
            if invalid_reason is None:
                print('using checkpoint data from {}'.format(self.checkpoint_path))
                return [_ValidatingUnstream(self.filename, self._load_metadata()),
                        _LockReleaseProcessor(lock)]

            print('rebuilding checkpoint {}: {}'.format(self.checkpoint_name, invalid_reason))
            print('saving checkpoint to: {}'.format(self.checkpoint_path))
            hasher = hashlib.sha256()
            stream_stats = {}
            metadata = self._build_metadata()
            return itertools.chain(
                self.chain,
                (stream(self.filename, hasher=hasher, stats=stream_stats),
                 _MetadataCommitProcessor(self.checkpoint_name, self.filename,
                                          self.metadata_filename, self.active_filename,
                                          hasher, stream_stats, metadata),
                 _LockReleaseProcessor(lock))
            )
        except BaseException:
            lock.release()
            raise

    def _build_metadata(self):
        return {
            'metadata_format_version': METADATA_FORMAT_VERSION,
            'checkpoint_name': self.checkpoint_name,
            'transformation_version': self.transformation_version,
            'parameters': self.parameters,
            'parameters_canonical': _canonical(self.parameters),
            'inputs': _build_input_manifest(self.input_files or []),
        }

    def _load_metadata(self):
        return _read_metadata(self.metadata_filename)

    def _try_load_reason(self):
        """Return None when the cache is valid and reusable, else a reason."""
        if not os.path.exists(self.filename):
            return 'no checkpoint stream found'
        metadata = self._load_metadata()
        if not isinstance(metadata, dict):
            return 'checkpoint metadata is missing or unreadable'
        if metadata.get('metadata_format_version') != METADATA_FORMAT_VERSION:
            return 'checkpoint metadata format is outdated'
        if metadata.get('checkpoint_name') != self.checkpoint_name:
            return 'checkpoint name mismatch'
        if metadata.get('transformation_version') != self.transformation_version:
            return 'transformation version changed'
        if metadata.get('parameters_canonical') != _canonical(self.parameters):
            return 'cleaning parameters changed'

        recorded_inputs = metadata.get('inputs')
        if not isinstance(recorded_inputs, list):
            return 'checkpoint input record is missing'
        try:
            current_manifest = _build_input_manifest(self.input_files or [])
        except CheckpointError as exc:
            return str(exc)
        if len(recorded_inputs) != len(current_manifest):
            return 'declared input files changed'
        for recorded, current in zip(recorded_inputs, current_manifest):
            if recorded.get('path') != current['path'] or recorded.get('name') != current['name']:
                return 'declared input files changed'
            if recorded.get('sha256') != current['sha256']:
                return 'input file content changed: {}'.format(current['name'])

        return _verify_cached_stream(self.filename, metadata)

    def handle_flow_checkpoint(self, parent_chain):
        self.chain = itertools.chain(self.chain, parent_chain)
        return [self]


def _notify_checkpoint_saved(checkpoint_name):

    def step(package):
        yield package.pkg
        for rows in package:
            yield (row for row in rows)
        print(f'checkpoint saved: {checkpoint_name}')

    return step
