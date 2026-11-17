"""
Local versioned publishing support.

A versioned package lives under a single root directory and is written in two
phases:

1. A run dumps every resource file and the final ``datapackage.json`` into a
   private staging directory (``.staging-<id>``). Nothing visible to readers
   changes during this phase, so a crash or a validation failure half way
   through a multi-resource dump can never leave old and new files mixed
   together.
2. Only after every resource has been transformed, validated and written, the
   staging directory is atomically renamed to ``versions/<id>`` and the
   ``current`` symlink is switched over in one step. The previous version is
   left untouched and remains available for explicit reads.

Readers pin a version when they start loading: the ``current`` symlink is
resolved once, at load time, so republishing a new version while a reader is
still iterating does not affect the data the reader sees.
"""
import os
import json
import shutil
import tempfile


#: Directory (relative to the package root) holding every published version.
VERSIONS_DIRNAME = 'versions'
#: Symlink (relative to the package root) pointing at the latest version.
CURRENT_LINKNAME = 'current'
#: Name of the data package descriptor inside a version directory.
DESCRIPTOR_FILENAME = 'datapackage.json'
#: Name of the version registry, one entry per published version.
INDEX_FILENAME = 'index.json'
#: Prefix of the private staging directories used while a version is built.
STAGING_PREFIX = '.staging-'
#: File used to serialise concurrent publishes.
LOCK_FILENAME = '.publish.lock'


def versions_dir(root_path):
    return os.path.join(root_path, VERSIONS_DIRNAME)


def current_link_path(root_path):
    return os.path.join(root_path, CURRENT_LINKNAME)


def lock_path(root_path):
    return os.path.join(root_path, LOCK_FILENAME)


def index_path(root_path):
    return os.path.join(root_path, INDEX_FILENAME)


def version_dir(root_path, version_id):
    return os.path.join(versions_dir(root_path), str(version_id))


def staging_dir(root_path, version_id):
    return os.path.join(root_path, '{}{}'.format(STAGING_PREFIX, version_id))


def is_versioned_root(root_path):
    """True when ``root_path`` looks like a versioned package root."""
    return os.path.islink(current_link_path(root_path)) or \
        os.path.isdir(versions_dir(root_path))


def load_index(root_path):
    """Return the persisted version registry (a list of metadata dicts)."""
    path = index_path(root_path)
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        index = json.load(f)
    if not isinstance(index, list):
        raise ValueError('Invalid version index at {!r}'.format(path))
    return index


def write_index(root_path, index):
    """Atomically replace the version registry."""
    path = index_path(root_path)
    fd, tmp_path = tempfile.mkstemp(prefix='.index-', suffix='.tmp',
                                    dir=root_path)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2, sort_keys=True, ensure_ascii=False)
            f.write('\n')
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def next_version_id(root_path):
    """Pick the next sequential, not-yet-used version id."""
    existing = set()
    vdir = versions_dir(root_path)
    if os.path.isdir(vdir):
        for name in os.listdir(vdir):
            if name.isdigit():
                existing.add(int(name))
    # Staging directories from an interrupted process must not be reused.
    for name in os.listdir(root_path):
        if name.startswith(STAGING_PREFIX) and name[len(STAGING_PREFIX):].isdigit():
            existing.add(int(name[len(STAGING_PREFIX):]))
    version_id = 1
    while version_id in existing:
        version_id += 1
    return version_id


def resolve_version(root_path, version=None):
    """
    Resolve ``version`` (``None``/``'current'``/an int/a numeric string) to the
    absolute path of the directory holding that version.

    ``None`` and ``'current'`` follow the ``current`` symlink. The symlink is
    resolved with :func:`os.path.realpath`, pinning the result to whatever
    directory it points at right now - later symlink switches cannot affect it.
    """
    if version is None or version == 'current':
        link = current_link_path(root_path)
        if not os.path.exists(link):
            raise FileNotFoundError(
                'No published version found at {!r}'.format(root_path))
        resolved = os.path.realpath(link)
        if not os.path.isdir(resolved):
            raise FileNotFoundError(
                'Current version at {!r} is missing'.format(root_path))
        return resolved
    version_id = str(version)
    path = version_dir(root_path, version_id)
    if not os.path.isdir(path):
        raise FileNotFoundError(
            'Version {!r} not found at {!r}'.format(version_id, root_path))
    return path


def descriptor_path_for_version(root_path, version=None):
    return os.path.join(resolve_version(root_path, version),
                        DESCRIPTOR_FILENAME)


def cleanup_staging(root_path):
    """Remove staging directories left behind by failed/interrupted runs."""
    if not os.path.isdir(root_path):
        return
    for name in os.listdir(root_path):
        if name.startswith(STAGING_PREFIX):
            shutil.rmtree(os.path.join(root_path, name), ignore_errors=True)


def publish_version(root_path, version_id, metadata):
    """
    Publish a fully built staging directory as ``versions/<version_id>`` and
    switch the ``current`` symlink over atomically.

    The whole swap is guarded by an advisory lock so that concurrent publishers
    cannot interleave their symlink/index updates. On failure the previously
    current version (if any) is left exactly as it was.
    """
    staged = staging_dir(root_path, version_id)
    target = version_dir(root_path, version_id)
    if not os.path.isdir(staged):
        raise FileNotFoundError(
            'Staging directory {!r} is missing; nothing was published'.format(staged))
    if os.path.exists(target):
        raise FileExistsError(
            'Version {!r} already exists'.format(version_id))

    os.makedirs(versions_dir(root_path), exist_ok=True)
    lock = open(lock_path(root_path), 'a+')
    moved = False
    linked = False
    previous_link_target = None
    try:
        try:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        except ImportError:  # pragma: no cover - non-POSIX platforms
            pass

        # Atomic move of the complete version into place.
        os.rename(staged, target)
        moved = True

        # Remember where the current link pointed so it can be restored.
        link = current_link_path(root_path)
        if os.path.islink(link):
            previous_link_target = os.readlink(link)

        # Atomically repoint the current symlink.
        tmp_link = os.path.join(root_path, '.{}.tmp'.format(CURRENT_LINKNAME))
        if os.path.islink(tmp_link) or os.path.exists(tmp_link):
            os.unlink(tmp_link)
        os.symlink(os.path.relpath(target, root_path), tmp_link)
        os.replace(tmp_link, link)
        linked = True

        index = load_index(root_path)
        index = [entry for entry in index
                 if str(entry.get('version')) != str(version_id)]
        entry = {'version': version_id}
        entry.update(metadata)
        index.append(entry)
        index.sort(key=lambda item: item['version'])
        write_index(root_path, index)
    except Exception:
        # Undo as much as possible so the last known-good version stays
        # current and the partial build is returned to staging for cleanup.
        if linked and previous_link_target is not None:
            try:
                tmp_link = os.path.join(
                    root_path, '.{}.tmp'.format(CURRENT_LINKNAME))
                if os.path.islink(tmp_link) or os.path.exists(tmp_link):
                    os.unlink(tmp_link)
                os.symlink(previous_link_target, tmp_link)
                os.replace(tmp_link, link)
            except OSError:
                pass
        if moved and os.path.isdir(target) and not os.path.isdir(staged):
            try:
                os.rename(target, staged)
            except OSError:
                pass
        raise
    finally:
        lock.close()
