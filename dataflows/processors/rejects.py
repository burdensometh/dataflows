"""
Rejected-row handling for DataFlows.

A cleaning chain normally either raises on the first bad row or silently drops
it.  This module adds a small SQLite-backed *reject ledger* around an ordinary,
row-wise DataFlows chain:

* every source row gets a stable id,
* rows which fail the chain are persisted with their **original** values, the
  failing stage and the reason,
* only rows which pass the chain are handed downstream (e.g. to
  ``dump_to_sql``),
* an operator can correct the original value of a rejected row and replay only
  that row through the *same* cleaning chain,
* a replay is atomic: either all rows produced by the source row **and** the
  new status are committed, or nothing is - a failing row never leaves half of
  its expansion behind,
* replaying is idempotent: rows written by an earlier replay are replaced,
  never duplicated,
* the current rejection reason and the rows finally written to the target can
  be queried.

Typical usage::

    from dataflows import Flow, load, set_type, dump_to_sql, reject_rows

    clean = [
        set_type('age', type='integer'),
        normalize_name,                       # any ordinary DataFlows step
    ]

    rejects = reject_rows(
        clean,
        db='out/ledger.db',
        target=dict(path='out/warehouse.db', table='people'),
        add_id_field='source_id',             # optional traceability column
    )

    Flow(
        load('people.csv'),
        rejects,
        dump_to_sql({'people': {'resource-name': 'people'}},
                    engine='sqlite:///out/warehouse.db'),
    ).process()

    # ... after fixing a bad value:
    rejects.store.correct('people:3', age=42)
    rejects.replay(['people:3'])              # only this row is re-processed

The chain must be row-wise: a step may filter a row (zero outputs) or expand
it (several outputs), but must not reorder or buffer the whole resource
(``sort_rows``, ``join``, ``deduplicate``...).  That is what makes per-row,
all-or-nothing replay possible.
"""

import copy
import datetime
import decimal
import json
import os
import re
import sqlite3
import threading

from datapackage import Package
from tableschema.exceptions import CastError

from ..base import DataStream, DataStreamProcessor, ResourceWrapper
from ..base.flow import Flow
from ..helpers.extended_json import ejson
from ..helpers.resource_matcher import ResourceMatcher


LEDGER_VERSION = 1

STATUS_REJECTED = 'rejected'
STATUS_ACCEPTED = 'accepted'
STATUS_FILTERED = 'filtered'

SOURCE_ID_COLUMN = 'dataflows_source_id'
SOURCE_SEQ_COLUMN = 'dataflows_source_seq'

LEDGER_TABLE = 'dataflows_rejected_rows'
RESULTS_TABLE = 'dataflows_rejected_results'
INFO_TABLE = 'dataflows_ledger_info'

LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS {ledger} (
    id                   TEXT PRIMARY KEY,
    resource             TEXT NOT NULL,
    row_index            INTEGER,
    status               TEXT NOT NULL,
    stage_index          INTEGER,
    stage_name           TEXT,
    reason               TEXT,
    last_reason          TEXT,
    error_type           TEXT,
    original_row         TEXT NOT NULL,
    corrected_row        TEXT,
    resource_descriptor  TEXT,
    result_count         INTEGER NOT NULL DEFAULT 0,
    attempts             INTEGER NOT NULL DEFAULT 1,
    replay_count         INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rejected_status ON {ledger} (status);
CREATE INDEX IF NOT EXISTS ix_rejected_resource ON {ledger} (resource);
CREATE TABLE IF NOT EXISTS {results} (
    id   TEXT NOT NULL,
    seq  INTEGER NOT NULL,
    row  TEXT NOT NULL,
    PRIMARY KEY (id, seq)
);
CREATE TABLE IF NOT EXISTS {info} (
    key   TEXT PRIMARY KEY,
    value TEXT
);
""".format(ledger=LEDGER_TABLE, results=RESULTS_TABLE, info=INFO_TABLE)


SQL_TYPES = {
    'integer': 'INTEGER',
    'number': 'REAL',
    'boolean': 'INTEGER',
    'date': 'TEXT',
    'datetime': 'TEXT',
    'time': 'TEXT',
    'year': 'INTEGER',
    'yearmonth': 'TEXT',
    'duration': 'TEXT',
    'object': 'TEXT',
    'array': 'TEXT',
    'geojson': 'TEXT',
    'geopoint': 'TEXT',
    'any': 'TEXT',
    'string': 'TEXT',
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _utcnow():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


def _step_name(step):
    return getattr(step, '__name__', None) or step.__class__.__name__


def _clone(obj):
    try:
        return copy.deepcopy(obj)
    except Exception:  # noqa: BLE001 - some callables / generators are not deep-copyable
        return obj


def _format_exception(exception):
    """A stable, human-readable reason for a failing row."""
    seen = set()
    while getattr(exception, 'cause', None) is not None and id(exception) not in seen:
        seen.add(id(exception))
        exception = exception.cause
    validation_error = getattr(exception, 'cast_error', None)
    if validation_error is not None:
        return _format_exception(validation_error)
    errors = getattr(exception, 'errors', None)
    if isinstance(exception, CastError) or errors:
        if errors:
            return '; '.join(str(error) for error in errors)
    text = str(exception).strip()
    if not text:
        return exception.__class__.__name__
    return ' '.join(line.strip() for line in text.splitlines() if line.strip())


def source_row_id(resource_name, row_index, row=None, id_field=None, id_prefix=''):
    """Return the stable id of a source row.

    By default the id is positional (``resource:row-number``), which is stable
    between runs over the same source file.  When the source has a natural key,
    pass ``id_field`` and the id is derived from it - it then also survives
    reordering of the source.
    """
    row = row or {}
    if id_field is not None and row.get(id_field) is not None:
        return '{}{}:{}={}'.format(id_prefix, resource_name, id_field, row.get(id_field))
    return '{}{}:{}'.format(id_prefix, resource_name, row_index)


def _quote(identifier):
    return '"{0}"'.format(str(identifier).replace('"', '""'))


def _jsonable(value):
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    return value


def _sqlite_value(value):
    if value is None or isinstance(value, (int, float, str)):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(_jsonable(value), sort_keys=True, ensure_ascii=True)
    return str(value)


def _sqlite_path(value):
    if value is None:
        raise ValueError('A SQLite database path is required')
    if isinstance(value, SQLiteTarget):
        return value.path
    value = str(value)
    if value.startswith('sqlite:///'):
        return value[len('sqlite:///'):]
    if value.startswith('sqlite://'):
        return value[len('sqlite://'):]
    if '://' in value:
        raise ValueError('Only SQLite targets are supported, got {!r}'.format(value))
    return value


# ---------------------------------------------------------------------------
# Target description
# ---------------------------------------------------------------------------

class SQLiteTarget:
    """The SQLite table that replayed rows (and, usually, the bulk run) write to."""

    def __init__(self, path=None, table=None, engine=None, schema=None):
        self.path = _sqlite_path(engine or path)
        if not table:
            raise ValueError('SQLiteTarget requires a table name')
        self.table = table
        self.schema = copy.deepcopy(schema) if schema else None

    def __repr__(self):
        return 'SQLiteTarget(path={!r}, table={!r})'.format(self.path, self.table)

    @classmethod
    def coerce(cls, target):
        if target is None or isinstance(target, cls):
            return target
        if isinstance(target, dict):
            return cls(path=target.get('path'),
                       engine=target.get('engine', target.get('url')),
                       table=target.get('table', target.get('table-name')),
                       schema=target.get('schema'))
        raise TypeError('Unsupported target {!r}'.format(target))


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

class RejectionStore:
    """Persistent, SQLite-backed ledger of source rows and their outcome."""

    def __init__(self, path='rejects.db'):
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(LEDGER_SCHEMA)
            self._conn.execute(
                'INSERT OR IGNORE INTO {} (key, value) VALUES (?, ?)'.format(INFO_TABLE),
                ('version', str(LEDGER_VERSION)))

    # -- plumbing ----------------------------------------------------------

    def _fetchone(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def _fetchall(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _execute(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params)

    def _transaction(self, callback):
        with self._lock:
            self._conn.execute('BEGIN IMMEDIATE')
            try:
                result = callback(self._conn)
            except Exception:
                self._conn.rollback()
                raise
            self._conn.commit()
            return result

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        record = dict(row)
        original = record.pop('original_row', None)
        record['original'] = ejson.loads(original) if original else None
        corrected = record.pop('corrected_row', None)
        record['corrected'] = ejson.loads(corrected) if corrected else None
        descriptor = record.pop('resource_descriptor', None)
        record['resource_descriptor'] = ejson.loads(descriptor) if descriptor else None
        record['current_reason'] = record.get('reason') if record.get('status') == STATUS_REJECTED else None
        return record

    def close(self):
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # -- write -------------------------------------------------------------

    def record_rejection(self, id, resource, row_index, original, reason,
                         stage_index=None, stage_name=None, error_type=None,
                         resource_descriptor=None):
        """Insert or refresh a rejected source row."""
        now = _utcnow()
        with self._lock:
            existing = self._fetchone(
                'SELECT created_at FROM {} WHERE id = ?'.format(LEDGER_TABLE), (id,))
            created_at = existing['created_at'] if existing else now
            self._execute(
                """
                INSERT INTO {ledger}
                    (id, resource, row_index, status, stage_index, stage_name, reason,
                     last_reason, error_type, original_row, corrected_row,
                     resource_descriptor, result_count, attempts, replay_count,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 0, 1, 0, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    resource = excluded.resource,
                    row_index = excluded.row_index,
                    status = excluded.status,
                    stage_index = excluded.stage_index,
                    stage_name = excluded.stage_name,
                    reason = excluded.reason,
                    last_reason = excluded.last_reason,
                    error_type = excluded.error_type,
                    original_row = excluded.original_row,
                    resource_descriptor = COALESCE(excluded.resource_descriptor,
                                                   {ledger}.resource_descriptor),
                    result_count = 0,
                    attempts = {ledger}.attempts + 1,
                    updated_at = excluded.updated_at
                """.format(ledger=LEDGER_TABLE),
                (id, resource, row_index, STATUS_REJECTED, stage_index, stage_name,
                 reason, reason, error_type, ejson.dumps(original, sort_keys=True),
                 ejson.dumps(resource_descriptor) if resource_descriptor else None,
                 created_at, now))
        return self.get(id)

    def _update_outcome(self, conn, id, status, reason, result_count=0,
                        stage_index=None, stage_name=None, error_type=None,
                        replay=False):
        now = _utcnow()
        if status == STATUS_REJECTED:
            conn.execute(
                """
                UPDATE {ledger} SET
                    status = ?, reason = ?, last_reason = ?, error_type = ?,
                    stage_index = COALESCE(?, stage_index),
                    stage_name = COALESCE(?, stage_name),
                    result_count = ?, updated_at = ?, attempts = attempts + 1
                WHERE id = ?
                """.format(ledger=LEDGER_TABLE),
                (status, reason, reason, error_type, stage_index, stage_name,
                 result_count, now, id))
        else:
            conn.execute(
                """
                UPDATE {ledger} SET
                    status = ?, reason = NULL, result_count = ?, updated_at = ?,
                    replay_count = replay_count + ?, attempts = attempts + 1
                WHERE id = ?
                """.format(ledger=LEDGER_TABLE),
                (status, result_count, now, 1 if replay else 0, id))

    def _set_status(self, id, status, reason=None, result_count=0,
                    stage_index=None, stage_name=None, error_type=None,
                    replay=False, results=None):
        def callback(conn):
            self._update_outcome(conn, id, status, reason, result_count=result_count,
                                 stage_index=stage_index, stage_name=stage_name,
                                 error_type=error_type, replay=replay)
            if results is not None:
                conn.execute('DELETE FROM {} WHERE id = ?'.format(RESULTS_TABLE), (id,))
                conn.executemany(
                    'INSERT INTO {} (id, seq, row) VALUES (?, ?, ?)'.format(RESULTS_TABLE),
                    [(id, seq, ejson.dumps(row, sort_keys=True))
                     for seq, row in enumerate(results)])
        self._transaction(callback)
        return self.get(id)

    def forget(self, id):
        with self._lock:
            self._execute('DELETE FROM {} WHERE id = ?'.format(RESULTS_TABLE), (id,))
            self._execute('DELETE FROM {} WHERE id = ?'.format(LEDGER_TABLE), (id,))

    def clear(self):
        with self._lock:
            self._execute('DELETE FROM {}'.format(RESULTS_TABLE))
            self._execute('DELETE FROM {}'.format(LEDGER_TABLE))

    # -- corrections -------------------------------------------------------

    def correct(self, id, row=None, **changes):
        """Correct the value a rejected row will be replayed with.

        ``row`` replaces the corrected representation completely, while
        ``changes`` are merged into the current effective representation.
        """
        record = self.get(id)
        if record is None:
            raise KeyError('Unknown rejected row id {!r}'.format(id))
        if row is not None:
            if not isinstance(row, dict):
                raise ValueError('Corrected row must be a dict')
            new_row = copy.deepcopy(row)
        else:
            effective = record['corrected'] if record['corrected'] is not None else record['original']
            new_row = copy.deepcopy(effective)
        new_row.update(changes)
        with self._lock:
            self._execute(
                'UPDATE {} SET corrected_row = ?, updated_at = ? WHERE id = ?'.format(LEDGER_TABLE),
                (ejson.dumps(new_row, sort_keys=True), _utcnow(), id))
        return new_row

    def correct_many(self, corrections):
        return {id: self.correct(id, row) for id, row in corrections.items()}

    def clear_correction(self, id):
        with self._lock:
            self._execute(
                'UPDATE {} SET corrected_row = NULL, updated_at = ? WHERE id = ?'.format(LEDGER_TABLE),
                (_utcnow(), id))
        return self.get(id)

    def effective_row(self, id):
        record = self.get(id)
        if record is None:
            raise KeyError('Unknown rejected row id {!r}'.format(id))
        value = record['corrected'] if record['corrected'] is not None else record['original']
        return copy.deepcopy(value)

    # -- reads -------------------------------------------------------------

    def get(self, id):
        return self._decode(self._fetchone(
            'SELECT * FROM {} WHERE id = ?'.format(LEDGER_TABLE), (id,)))

    def status(self, id):
        record = self.get(id)
        if record is None:
            raise KeyError('Unknown rejected row id {!r}'.format(id))
        return record['status']

    def original(self, id):
        record = self.get(id)
        if record is None:
            raise KeyError('Unknown rejected row id {!r}'.format(id))
        return copy.deepcopy(record['original'])

    def reason(self, id):
        """The current rejection reason, or ``None`` for an accepted row."""
        record = self.get(id)
        if record is None:
            raise KeyError('Unknown rejected row id {!r}'.format(id))
        return record['current_reason']

    def last_reason(self, id):
        record = self.get(id)
        if record is None:
            raise KeyError('Unknown rejected row id {!r}'.format(id))
        return record['last_reason']

    def results(self, id):
        """Rows produced by the last successful replay of a source row."""
        with self._lock:
            rows = self._conn.execute(
                'SELECT row FROM {} WHERE id = ? ORDER BY seq'.format(RESULTS_TABLE),
                (id,)).fetchall()
        return [ejson.loads(row['row']) for row in rows]

    def query(self, status=None, resource=None, limit=None):
        sql = 'SELECT * FROM {}'.format(LEDGER_TABLE)
        clauses = []
        params = []
        if status is not None:
            if isinstance(status, (list, tuple, set)):
                status = list(status)
                clauses.append('status IN ({})'.format(','.join('?' for _ in status)))
                params.extend(status)
            else:
                clauses.append('status = ?')
                params.append(status)
        if resource is not None:
            clauses.append('resource = ?')
            params.append(resource)
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' ORDER BY resource, row_index, id'
        if limit:
            sql += ' LIMIT ?'
            params.append(int(limit))
        return [self._decode(row) for row in self._fetchall(sql, params)]

    def rejected(self, resource=None):
        return self.query(status=STATUS_REJECTED, resource=resource)

    def accepted(self, resource=None):
        return self.query(status=STATUS_ACCEPTED, resource=resource)

    def filtered(self, resource=None):
        return self.query(status=STATUS_FILTERED, resource=resource)

    rejected_rows = rejected

    def ids(self, status=STATUS_REJECTED, resource=None):
        return [record['id'] for record in self.query(status=status, resource=resource)]

    def info(self):
        with self._lock:
            rows = self._conn.execute('SELECT key, value FROM {}'.format(INFO_TABLE)).fetchall()
        return {row['key']: row['value'] for row in rows}


# Backwards/forwards friendly alias
RejectedRows = RejectionStore


# ---------------------------------------------------------------------------
# Isolation helpers
# ---------------------------------------------------------------------------

class _TrackingIterator:
    """Iterator which knows which source row is currently in flight.

    DataFlows row processors stream: they pull input row ``i + 1`` only after
    they finished producing the outputs of row ``i``.  Watching the pulls
    gives us the commit boundary of the current source row.
    """

    def __init__(self, indexed_rows, on_advance):
        self._rows = iter(indexed_rows)
        self._on_advance = on_advance
        self.pulled = 0
        self.index = None
        self.row = None
        self.finished = False

    def __iter__(self):
        return self

    def __next__(self):
        try:
            index, row = next(self._rows)
        except StopIteration:
            self.finished = True
            raise
        except Exception as exception:  # noqa: BLE001 - propagate source errors untouched
            setattr(exception, '_dataflows_source_error', True)
            raise
        self.pulled += 1
        # Pulling a new source row means the previous row is complete.
        self._on_advance(index, _clone(row))
        self.index = index
        self.row = _clone(row)
        return row


class _ChainRun:
    """Result of running a chain on a single source row."""

    def __init__(self, descriptor, rows):
        self.descriptor = descriptor
        self.rows = rows


def _resource_package(resource_descriptor):
    descriptor = copy.deepcopy(resource_descriptor)
    # A schema is only exposed by datapackage for tabular resources; without
    # this the sandboxed chain would fail to validate its own input.
    if descriptor.get('schema'):
        descriptor.setdefault('profile', 'tabular-data-resource')
    return Package(descriptor={
        'profile': 'tabular-data-package',
        'resources': [descriptor],
    })


def _run_chain(steps, input_descriptor, resource_name, rows):
    """Run ``steps`` over ``rows`` in an isolated package; return all outputs."""
    package = _resource_package(input_descriptor)
    package.commit()
    resource = package.get_resource(resource_name)
    if resource is None:
        raise ValueError('Resource {!r} not found'.format(resource_name))
    stream = DataStream(package, [ResourceWrapper(resource, iter(rows))])
    if steps:
        stream = Flow(*[_clone(step) for step in steps]).datastream(stream)
    output_resource = stream.dp.get_resource(resource_name)
    output_rows = []
    for resource_wrapper in stream.res_iter:
        if resource_wrapper.res.name == resource_name:
            output_rows.extend(list(resource_wrapper))
    descriptor = copy.deepcopy(output_resource.descriptor) if output_resource else None
    return _ChainRun(descriptor or copy.deepcopy(input_descriptor), output_rows)


def infer_output_descriptor(steps, input_descriptor, resource_name):
    """Ask the chain which schema it produces, without running any row through it."""
    try:
        run = _run_chain(steps, input_descriptor, resource_name, [])
        return run.descriptor
    except Exception:  # noqa: BLE001 - fall back to the source schema
        return copy.deepcopy(input_descriptor)


# ---------------------------------------------------------------------------
# The processor
# ---------------------------------------------------------------------------

class reject_rows(DataStreamProcessor):
    """Run an ordinary cleaning chain and persist the rows it rejects.

    Parameters
    ----------
    steps : list | Flow | callable
        The ordinary, row-wise cleaning chain.
    db / path : str
        Path of the SQLite ledger (``db`` and ``path`` are equivalent).
    store : RejectionStore
        Reuse an existing ledger instead of opening one.
    resource : ResourceMatcher value
        Which resources the chain applies to (default: all).
    id_field : str
        Optional source field used to derive stable ids.  Defaults to the
        row position (``resource:index``).
    id_prefix : str
        Optional namespace prepended to every generated id.
    add_id_field : str
        Optional name of a column added to output rows carrying the stable
        source-row id (makes warehouse rows traceable - and makes replay
        deduplication work for rows written by the bulk run).
    target : dict | SQLiteTarget
        Default SQLite target used by :meth:`replay`.
    record_accepted : bool
        Also record passing rows in the ledger (default: only rejections).
    """

    def __init__(self, steps=None, db=None, path=None, store=None, resource=None,
                 id_field=None, id_prefix='', add_id_field=None, target=None,
                 record_accepted=False):
        super(reject_rows, self).__init__()
        self.steps = self._normalize_steps(steps)
        if store is None:
            store = RejectionStore(db or path or 'rejects.db')
        self.store = store
        self.resource = resource
        self.id_field = id_field
        self.id_prefix = id_prefix or ''
        self.add_id_field = add_id_field
        self.target = SQLiteTarget.coerce(target)
        self.record_accepted = record_accepted
        self._configs = {}

    @staticmethod
    def _normalize_steps(steps):
        if steps is None:
            return []
        if isinstance(steps, Flow):
            return list(steps.chain)
        if isinstance(steps, (list, tuple)):
            return list(steps)
        return [steps]

    # -- flow integration --------------------------------------------------

    def process_datapackage(self, dp):
        # Snapshot the *input* descriptors before this processor rewrites the
        # package - replay needs the schema the cleaning chain originally saw.
        matcher = ResourceMatcher(self.resource, dp)
        self._configs = {}
        for descriptor in dp.descriptor.get('resources', []):
            name = descriptor.get('name')
            if not matcher.match(name):
                continue
            input_descriptor = copy.deepcopy(descriptor)
            output_descriptor = infer_output_descriptor(
                self.steps, input_descriptor, name)
            self._configs[name] = {
                'input': input_descriptor,
                'output': output_descriptor,
            }
            descriptor['schema'] = copy.deepcopy(output_descriptor.get('schema', {}))
            if self.add_id_field:
                schema = descriptor.setdefault('schema', {}).setdefault('fields', [])
                if all(field.get('name') != self.add_id_field for field in schema):
                    schema.append({'name': self.add_id_field, 'type': 'string',
                                   'format': 'default'})
        return dp

    def process_resources(self, resources):
        for resource in resources:
            name = resource.res.name
            config = self._configs.get(name)
            if config is None:
                yield resource
            else:
                yield ResourceWrapper(resource.res,
                                      self._isolate(resource, name, config))

    def process_resource(self, resource):
        raise NotImplementedError  # process_resources handles everything

    # -- per-resource isolation -------------------------------------------

    def _isolate(self, resource, resource_name, config):
        input_descriptor = config['input']
        package_descriptor = {'profile': 'tabular-data-package',
                              'resources': [copy.deepcopy(input_descriptor)]}

        ready = []        # confirmed (row, source-id) outputs
        pending = []      # outputs of the row currently in flight
        completed = {'index': None, 'row': None, 'id': None}
        # Rows rejected in an earlier run: when they pass this time (e.g. the
        # source was fixed) the ledger must stop reporting them as rejected.
        known_rejected = set(self.store.ids(status=STATUS_REJECTED,
                                            resource=resource_name))

        def make_id(index, row):
            return source_row_id(resource_name, index, row,
                                 id_field=self.id_field, id_prefix=self.id_prefix)

        def confirm():
            if completed['id'] is not None:
                if self.record_accepted or completed['id'] in known_rejected:
                    self._record_accepted(resource_name, completed, len(pending))
                    known_rejected.discard(completed['id'])
                ready.extend((row, completed['id']) for row in pending)
            pending.clear()

        def advance(index, row):
            # Called when the chain pulls a new source row: the previous row
            # finished successfully, so its outputs may leave the processor.
            confirm()
            completed['index'] = index
            completed['row'] = row
            completed['id'] = make_id(index, row)

        def chain_iter():
            package = Package(descriptor=copy.deepcopy(package_descriptor))
            package.commit()
            source = package.get_resource(resource_name)
            stream = DataStream(package, [ResourceWrapper(source, tracking)])
            if self.steps:
                stream = Flow(*[_clone(step) for step in self.steps]).datastream(stream)
            for resource_wrapper in stream.res_iter:
                if resource_wrapper.res.name == resource_name:
                    yield from resource_wrapper

        def release():
            while ready:
                row, row_id = ready.pop(0)
                if self.add_id_field:
                    row = dict(row)
                    row[self.add_id_field] = row_id
                yield row

        tracking = _TrackingIterator(enumerate(resource, start=1), advance)

        while True:
            yield from release()
            if tracking.finished:
                break
            pulled_before = tracking.pulled
            failure = None
            try:
                for output_row in chain_iter():
                    yield from release()
                    pending.append(output_row)
            except Exception as exception:  # noqa: BLE001 - isolate per source row
                if getattr(exception, '_dataflows_source_error', False):
                    raise
                failure = exception
            if failure is not None:
                if tracking.pulled == pulled_before and pulled_before == 0:
                    # No row was ever pulled: a chain configuration error.
                    raise failure
                if tracking.index is None:
                    raise failure
                current_index = tracking.index
                original = tracking.row
                # Only a failure which reproduces on this row alone is a row
                # rejection; anything else is a chain / configuration error
                # which must not be swallowed per-row.
                reproduced = self._fails_alone(original, resource_name,
                                               input_descriptor)
                if not reproduced:
                    raise failure
                if tracking.finished:
                    # The source was exhausted before the failure: a step
                    # raised after consuming the last row.  That is not a
                    # property of one bad row, so surface it instead of
                    # silently swallowing it.
                    raise failure
                # Discard the failing row's partial outputs - never emit half.
                pending.clear()
                row_id = make_id(current_index, original)
                stage_index, stage_name, reason, error_type = self._diagnose(
                    original, resource_name, input_descriptor, failure)
                self.store.record_rejection(
                    row_id, resource_name, current_index, original, reason,
                    stage_index=stage_index, stage_name=stage_name,
                    error_type=error_type, resource_descriptor=input_descriptor)
                # The failing row is accounted for; the pending/current state
                # must not spill over onto the next row.
                completed['index'] = None
                completed['row'] = None
                completed['id'] = None
                tracking.index = None
                tracking.row = None
                # The failed row has been consumed; restart with the rest.
                continue
            # The chain consumed no new source row and produced everything it
            # was going to produce - there is nothing left to do.
            if tracking.pulled == pulled_before and not pending:
                break
            # Chain returned normally: the last row's outputs are confirmed.
            confirm()
            break

        yield from release()

    def _record_accepted(self, resource_name, completed, result_count):
        index = completed['index']
        row = completed['row']
        row_id = source_row_id(resource_name, index, row,
                               id_field=self.id_field, id_prefix=self.id_prefix)
        status = STATUS_ACCEPTED if result_count else STATUS_FILTERED
        with self.store._lock:
            existing = self.store.get(row_id)
            if existing is None:
                # Keep accepted rows lightweight: no original payload unless
                # the caller asked for full accounting.
                self.store._execute(
                    """
                    INSERT OR IGNORE INTO {ledger}
                        (id, resource, row_index, status, original_row,
                         resource_descriptor, result_count, attempts,
                         replay_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, NULL, ?, 1, 0, ?, ?)
                    """.format(ledger=LEDGER_TABLE),
                    (row_id, resource_name, index, status,
                     ejson.dumps(row, sort_keys=True), result_count,
                     _utcnow(), _utcnow()))
            else:
                self.store._set_status(row_id, status, result_count=result_count)

    # -- diagnosis ---------------------------------------------------------

    def _fails_alone(self, row, resource_name, input_descriptor):
        """Whether ``row`` on its own reproduces the failure.

        This prevents a genuine chain/configuration error (a bug in a step, a
        missing field, a broken sink) from being swallowed as "this row is
        bad".  A row is only rejected when running it alone fails too.
        """
        try:
            _run_chain(self.steps, input_descriptor, resource_name,
                       [copy.deepcopy(row)])
            return False
        except Exception:  # noqa: BLE001 - reproduces => row's fault
            return True

    def _diagnose(self, original, resource_name, input_descriptor, exception,
                  steps=None):
        """Find the stage which rejected the row and produce a reason string."""
        steps = self.steps if steps is None else steps
        current_descriptor = copy.deepcopy(input_descriptor)
        current_rows = [copy.deepcopy(original)]
        for stage_index, step in enumerate(steps, start=1):
            try:
                run = _run_chain([step], current_descriptor, resource_name, current_rows)
            except Exception as stage_exception:  # noqa: BLE001 - diagnosis only
                return (stage_index, _step_name(step),
                        _format_exception(stage_exception),
                        stage_exception.__class__.__name__)
            current_descriptor = run.descriptor
            current_rows = run.rows
            if not current_rows:
                break
        return (None, None, _format_exception(exception), exception.__class__.__name__)

    # -- replay ------------------------------------------------------------

    def replay(self, ids=None, target=None, steps=None, force=False):
        """Replay selected rejected rows through the same cleaning chain.

        ``ids`` is an id, a list of ids, or ``None`` for every currently
        rejected row.  Every row is committed independently and atomically.
        Accepted rows are skipped unless ``force`` is set; replaying is
        idempotent either way.
        """
        target = SQLiteTarget.coerce(target) or self.target
        if target is None:
            raise ValueError('replay() needs a target (SQLiteTarget or dict)')
        replay_steps = self.steps if steps is None else self._normalize_steps(steps)
        if ids is None:
            ids = self.store.ids(status=STATUS_REJECTED)
        elif isinstance(ids, str):
            ids = [ids]
        else:
            ids = list(ids)

        report = []
        for row_id in ids:
            record = self.store.get(row_id)
            if record is None:
                report.append({'id': row_id, 'status': 'missing'})
                continue
            if record['status'] == STATUS_ACCEPTED and not force:
                report.append({'id': row_id, 'status': 'skipped',
                               'reason': 'already accepted'})
                continue
            report.append(self._replay_one(record, target, replay_steps))
        return report

    def _replay_one(self, record, target, steps):
        row_id = record['id']
        resource_name = record['resource']
        row = self.store.effective_row(row_id)
        input_descriptor = record.get('resource_descriptor') or {
            'name': resource_name,
            'schema': {'fields': [{'name': name, 'type': 'string', 'format': 'default'}
                                  for name in (row or {})]},
        }

        try:
            run = _run_chain(steps, input_descriptor, resource_name, [row])
        except Exception as exception:  # noqa: BLE001 - rejected again
            stage_index, stage_name, reason, error_type = self._diagnose(
                row, resource_name, input_descriptor, exception, steps=steps)
            # A row that was previously replayed but is rejected again must
            # not keep its old target rows - the ledger and the warehouse
            # always agree.
            self._purge_replay(row_id, target)
            self.store._set_status(
                row_id, STATUS_REJECTED, reason=reason, result_count=0,
                stage_index=stage_index, stage_name=stage_name,
                error_type=error_type, results=[])
            return {'id': row_id, 'status': 'rejected', 'reason': reason,
                    'stage_index': stage_index, 'stage_name': stage_name}

        output_schema = copy.deepcopy((run.descriptor or {}).get('schema')
                                      or target.schema or {'fields': []})
        output_fields = output_schema.setdefault('fields', [])
        if self.add_id_field and all(field.get('name') != self.add_id_field
                                     for field in output_fields):
            output_fields.append({'name': self.add_id_field, 'type': 'string',
                                  'format': 'default'})
        output_rows = list(run.rows)
        if self.add_id_field:
            output_rows = [dict(row, **{self.add_id_field: row_id})
                           for row in output_rows]
        status = STATUS_ACCEPTED if output_rows else STATUS_FILTERED
        try:
            self._commit_replay(row_id, target, output_schema, output_rows, status)
        except Exception as exception:  # noqa: BLE001 - target refused the row
            reason = 'Failed writing to target: {}'.format(_format_exception(exception))
            self._purge_replay(row_id, target)
            self.store._set_status(
                row_id, STATUS_REJECTED, reason=reason, result_count=0,
                error_type=exception.__class__.__name__, results=[])
            return {'id': row_id, 'status': 'rejected', 'reason': reason,
                    'stage_index': None, 'stage_name': None}

        return {'id': row_id, 'status': status, 'result_count': len(output_rows)}

    def _purge_replay(self, row_id, target):
        """Remove a source row's rows from the target (best effort)."""
        try:
            path = os.path.abspath(target.path)
            if not os.path.exists(path):
                return
            connection = sqlite3.connect(path)
            try:
                exists = connection.execute(
                    'SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?',
                    ('table', target.table)).fetchone()
                if not exists:
                    return
                columns = [row[1] for row in connection.execute(
                    'PRAGMA table_info({})'.format(_quote(target.table))).fetchall()]
                if SOURCE_ID_COLUMN not in columns:
                    return
                connection.execute(
                    'DELETE FROM {} WHERE {} = ?'.format(
                        _quote(target.table), _quote(SOURCE_ID_COLUMN)), (row_id,))
                connection.commit()
            finally:
                connection.close()
        except Exception:  # noqa: BLE001 - purge must never mask the real error
            pass

    # -- atomic target + ledger commit ------------------------------------

    def _commit_replay(self, row_id, target, schema, rows, status):
        store = self.store
        target_path = os.path.abspath(target.path)
        store_path = os.path.abspath(store.path)
        directory = os.path.dirname(target_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with store._lock:
            conn = store._conn
            if target_path == store_path:
                schema_name = 'main'
            else:
                attach_name = 'dataflows_replay_target'
                attached = {row[1]: row[2] for row in
                            conn.execute('PRAGMA database_list').fetchall()}
                if attach_name in attached:
                    if os.path.abspath(attached[attach_name] or '') != target_path:
                        conn.execute('DETACH DATABASE {}'.format(_quote(attach_name)))
                        attached.pop(attach_name)
                if attach_name not in attached:
                    conn.execute('ATTACH DATABASE ? AS {}'.format(attach_name), (target.path,))
                schema_name = attach_name
            table_ref = '{}.{}'.format(_quote(schema_name), _quote(target.table))

            conn.execute('BEGIN IMMEDIATE')
            try:
                self._ensure_target_table(conn, schema_name, target.table, table_ref, schema)
                field_names = [field['name'] for field in schema.get('fields', [])
                               if field.get('name')]
                self._delete_existing_rows(conn, schema_name, target.table, table_ref,
                                           row_id, field_names)
                columns = field_names + [SOURCE_ID_COLUMN, SOURCE_SEQ_COLUMN]
                insert = 'INSERT INTO {} ({}) VALUES ({})'.format(
                    table_ref,
                    ', '.join(_quote(column) for column in columns),
                    ', '.join('?' for _ in columns))
                for seq, row in enumerate(rows):
                    values = [_sqlite_value(row.get(name)) for name in field_names]
                    values.extend([row_id, seq])
                    conn.execute(insert, values)
                # Result rows and status live in the same transaction.
                conn.execute('DELETE FROM {} WHERE id = ?'.format(RESULTS_TABLE), (row_id,))
                conn.executemany(
                    'INSERT INTO {} (id, seq, row) VALUES (?, ?, ?)'.format(RESULTS_TABLE),
                    [(row_id, seq, ejson.dumps(row, sort_keys=True))
                     for seq, row in enumerate(rows)])
                store._update_outcome(
                    conn, row_id, status, None, result_count=len(rows), replay=True)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _table_exists(conn, schema_name, table):
        return conn.execute(
            'SELECT 1 FROM {}.sqlite_master WHERE type = ? AND name = ?'.format(_quote(schema_name)),
            ('table', table)).fetchone() is not None

    def _delete_existing_rows(self, conn, schema_name, table, table_ref, row_id, field_names):
        conn.execute(
            'DELETE FROM {} WHERE {} = ?'.format(table_ref, _quote(SOURCE_ID_COLUMN)),
            (row_id,))
        # When the bulk run also wrote this row (add_id_field), remove those
        # copies too - replaying must never duplicate a source row.
        if self.add_id_field and self.add_id_field in field_names:
            conn.execute(
                'DELETE FROM {} WHERE {} = ?'.format(table_ref, _quote(self.add_id_field)),
                (row_id,))

    @staticmethod
    def _table_columns(conn, schema_name, table):
        rows = conn.execute('PRAGMA {}.table_info({})'.format(
            _quote(schema_name), _quote(table))).fetchall()
        return {row[1] for row in rows}

    def _ensure_target_table(self, conn, schema_name, table, table_ref, schema):
        fields = [field for field in schema.get('fields', []) if field.get('name')]
        if not self._table_exists(conn, schema_name, table):
            columns = ['{} {}'.format(_quote(field['name']),
                                      SQL_TYPES.get(field.get('type'), 'TEXT'))
                       for field in fields]
            columns.append('{} TEXT NOT NULL'.format(_quote(SOURCE_ID_COLUMN)))
            columns.append('{} INTEGER NOT NULL'.format(_quote(SOURCE_SEQ_COLUMN)))
            conn.execute('CREATE TABLE {} ({})'.format(table_ref, ', '.join(columns)))
        else:
            existing = self._table_columns(conn, schema_name, table)
            for field in fields:
                if field['name'] not in existing:
                    conn.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(
                        table_ref, _quote(field['name']),
                        SQL_TYPES.get(field.get('type'), 'TEXT')))
            for column, sql_type in ((SOURCE_ID_COLUMN, 'TEXT'),
                                     (SOURCE_SEQ_COLUMN, 'INTEGER')):
                if column not in existing:
                    conn.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(
                        table_ref, _quote(column), sql_type))
        index_name = 'ix_{}_{}'.format(re.sub(r'\W+', '_', table), SOURCE_ID_COLUMN)
        # SQLite only accepts "CREATE INDEX schema.name ON table (cols)" - the
        # table itself must not be schema-qualified here.
        conn.execute('CREATE INDEX IF NOT EXISTS {}.{} ON {} ({})'.format(
            _quote(schema_name), _quote(index_name), _quote(table),
            _quote(SOURCE_ID_COLUMN)))

    # -- querying warehouse results ---------------------------------------

    def target_results(self, row_id, target=None):
        """Final rows written to the SQLite target for a source row."""
        target = SQLiteTarget.coerce(target) or self.target
        if target is None:
            return self.store.results(row_id)
        if not os.path.exists(target.path):
            return []
        connection = sqlite3.connect(target.path)
        connection.row_factory = sqlite3.Row
        try:
            exists = connection.execute(
                'SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?',
                ('table', target.table)).fetchone()
            if not exists:
                return []
            rows = connection.execute(
                'SELECT * FROM {} WHERE {} = ? ORDER BY {}'.format(
                    _quote(target.table), _quote(SOURCE_ID_COLUMN),
                    _quote(SOURCE_SEQ_COLUMN)),
                (row_id,)).fetchall()
        finally:
            connection.close()
        return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Functional helpers
# ---------------------------------------------------------------------------

def replay_rejected(store, steps, ids=None, target=None, db=None):
    """Replay rejected rows using a standalone :class:`reject_rows` instance."""
    processor = reject_rows(steps=steps, store=store, db=db, target=target)
    return processor.replay(ids=ids, target=target)


__all__ = [
    'reject_rows',
    'replay_rejected',
    'RejectionStore',
    'RejectedRows',
    'SQLiteTarget',
    'source_row_id',
    'infer_output_descriptor',
    'STATUS_REJECTED',
    'STATUS_ACCEPTED',
    'STATUS_FILTERED',
    'SOURCE_ID_COLUMN',
    'SOURCE_SEQ_COLUMN',
]
