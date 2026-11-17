import sys
import os

from ..helpers.extended_json import ejson

ACTIVE_SUFFIX = '.active'


def stream(file=sys.stdout, hasher=None, stats=None):
    """Serialize a datastream to newline-delimited JSON.

    When ``hasher`` (a hashlib hash object) is provided, every byte written
    to the stream is fed to it and the resulting digest is available as
    ``hasher.hexdigest()`` once the stream is finalized. When ``stats`` (a
    dict) is provided, per-resource row counts are recorded in it under the
    ``row_counts`` key. Both arguments are optional, keeping the historical
    call convention intact.
    """

    filename = None

    if isinstance(file, str):
        filename = file + ACTIVE_SUFFIX
        basedir = os.path.dirname(filename)
        os.makedirs(basedir, exist_ok=True)
        # A leftover active file means a previous run died before the
        # atomic rename. It never was a valid cache, so discard it.
        if os.path.exists(filename):
            os.unlink(filename)
        file = open(filename, 'w')

    def emit(obj):
        data = ejson.dumps(obj, sort_keys=True, ensure_ascii=True) + '\n'
        if hasher is not None:
            hasher.update(data.encode('utf-8'))
        file.write(data)
        file.flush()

    def res_writer(res):
        index = len(stats['row_counts']) if stats is not None else None
        if stats is not None:
            stats['row_counts'].append(0)
        for r in res:
            if stats is not None:
                stats['row_counts'][index] += 1
            emit(r)
            yield r

    def func(package):
        if stats is not None:
            stats['row_counts'] = []
        try:
            emit(package.pkg.descriptor)
            yield package.pkg
            for res in package:
                yield res_writer(res)
                if hasher is not None:
                    hasher.update(b'\n')
                file.write('\n')
                file.flush()
        except BaseException:
            # The prefix failed or only consumed part of its rows. Drop the
            # partial active file so it can never masquerade as a cache.
            file.close()
            if filename and os.path.exists(filename):
                os.unlink(filename)
            raise
        file.close()
        if filename:
            os.rename(filename, filename[:-len(ACTIVE_SUFFIX)])

    return func
