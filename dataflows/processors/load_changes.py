import os
import json
import hashlib
import logging
import datetime

from sqlalchemy import create_engine, MetaData, Table, Column, select, and_
from sqlalchemy import Text, String, Integer, Float, Boolean, Date, DateTime, Time

from .. import DataStreamProcessor
from ..base.exceptions import ChangeConflictError
from ..helpers.resource_matcher import ResourceMatcher

_UNSET = object()


class load_changes(DataStreamProcessor):
    """Loads row changes (upserts & deletes) into a SQLite "current" table.

    Change records are expected to arrive from a collector which may deliver
    them repeatedly and out of order. Each record is a regular flow row
    containing:

    - the business key field(s) - identifying the entity (`keys`)
    - a monotonic version (`version_field`)
    - an operation (`op_field`) - 'upsert' (full row values are stored)
      or 'delete' (the row is removed)
    - a source batch id (`batch_field`) - records are committed to the
      database batch by batch
    - the complete row values (all remaining fields)

    Only changes with a version newer than the stored one are applied.
    A record carrying the same version and the same content as the stored
    one is a no-op (safe replay), while the same version with different
    content raises a ChangeConflictError. Deletes leave a tombstone behind,
    so older updates arriving afterwards cannot resurrect a deleted record.

    For every source batch, the target rows, the per-key versions and the
    batch progress marker are committed in a single transaction - a failing
    batch leaves no trace and is not marked as done. Already committed
    batches are skipped, so re-running a flow after a restart continues
    where the previous run left off.

    Three tables are maintained in the target database:

    - `<table>` - the "current" table, holding the latest row values
    - `<table>__versions` - per business key: latest version, content hash
      and a deleted (tombstone) flag
    - `<table>__batches` - ids of source batches which were fully committed
    """

    UPSERT = 'upsert'
    DELETE = 'delete'

    VERSION_COLUMN = '__version__'
    HASH_COLUMN = '__hash__'
    DELETED_COLUMN = '__deleted__'
    SOURCE_COLUMN = 'source'
    BATCH_COLUMN = 'batch_id'

    JTS_TYPES = {
        'string': Text,
        'integer': Integer,
        'number': Float,
        'boolean': Boolean,
        'date': Date,
        'datetime': DateTime,
        'time': Time,
        'object': Text,
        'array': Text,
        'any': Text,
    }

    def __init__(self, table, keys=None, *,
                 version_field='version', op_field='op', batch_field='batch_id',
                 upsert_op=UPSERT, delete_op=DELETE,
                 source='default', resources=None,
                 engine='env://DATAFLOWS_DB_ENGINE'):
        super(load_changes, self).__init__()
        self.table = table
        if isinstance(keys, str):
            keys = [keys]
        self.keys = keys
        self.version_field = version_field
        self.op_field = op_field
        self.batch_field = batch_field
        self.upsert_op = upsert_op
        self.delete_op = delete_op
        self.source_name = source
        self.resources = resources

        if isinstance(engine, str):
            if engine.startswith('env://'):
                env_var = engine[6:]
                engine = os.environ.get(env_var)
                if engine is None:
                    raise ValueError("Couldn't connect to DB - "
                                     "Please set your '%s' environment variable" % env_var)
            self.engine = create_engine(engine)
            # check connection
            with self.engine.connect():
                pass
        else:
            self.engine = engine

        self.resource_matcher = None
        self.initialized = False
        self.counters = dict(batches_committed=0, batches_skipped=0,
                             records_applied=0, records_replayed=0, records_stale=0)
        self.stats['load_changes'] = self.counters

    def process_datapackage(self, dp):
        self.resource_matcher = ResourceMatcher(self.resources, dp.descriptor)
        return dp

    def process_resource(self, resource):
        if not self.resource_matcher.match(resource.res.name):
            yield from resource
            return
        if not self.initialized:
            self._initialize(resource)
        buffer = []
        batch_id = None
        skipped = set()
        for row in resource:
            row_batch = row.get(self.batch_field)
            if row_batch is None:
                raise ValueError('Change record is missing the batch field %r: %r'
                                 % (self.batch_field, row))
            row_batch = str(row_batch)
            if row_batch in self.completed_batches:
                # batch was fully committed in a previous run - skip it
                if row_batch not in skipped:
                    skipped.add(row_batch)
                    self.counters['batches_skipped'] += 1
                    logging.info('load_changes: skipping already committed batch %s', row_batch)
                yield row
                continue
            if batch_id is not None and row_batch != batch_id:
                self._commit_batch(batch_id, buffer)
                buffer = []
            batch_id = row_batch
            buffer.append(row)
            yield row
        if buffer:
            self._commit_batch(batch_id, buffer)

    # Initialization

    def _initialize(self, resource):
        descriptor = resource.res.descriptor
        schema = descriptor.get('schema') or {}
        fields = dict((f['name'], f.get('type', 'any')) for f in schema.get('fields', []))
        if not fields:
            raise ValueError('load_changes requires the resource to have a schema')
        keys = self.keys
        if keys is None:
            keys = schema.get('primaryKey') or descriptor.get('primaryKey')
            if isinstance(keys, str):
                keys = [keys]
        if not keys:
            raise ValueError('load_changes requires business keys: '
                             'pass `keys` or set a primaryKey in the resource schema')
        metadata_fields = [self.version_field, self.op_field, self.batch_field]
        for field in keys + metadata_fields:
            if field not in fields:
                raise ValueError('Change record schema is missing required field %r' % field)
        for field in keys:
            if field in metadata_fields:
                raise ValueError('Business key field %r collides with a change metadata field' % field)
        self.keys = keys
        self.value_fields = [f for f in fields if f not in metadata_fields]
        self.normalizers = dict((f, self._normalizer(fields[f])) for f in self.value_fields)
        self._create_tables(fields)
        with self.engine.connect() as conn:
            query = select(self.batches_table.c[self.BATCH_COLUMN]) \
                .where(self.batches_table.c[self.SOURCE_COLUMN] == self.source_name)
            self.completed_batches = set(str(r[0]) for r in conn.execute(query))
        self.initialized = True
        logging.info('load_changes: loading into table %r (keys=%r), %d batches already committed',
                     self.table, self.keys, len(self.completed_batches))

    def _create_tables(self, fields):
        metadata = MetaData()

        def column(name, primary_key=False):
            return Column(name, self.JTS_TYPES.get(fields[name], Text)(),
                          primary_key=primary_key)

        self.current_table = Table(
            self.table, metadata,
            *[column(name, primary_key=name in self.keys) for name in self.value_fields])
        self.versions_table = Table(
            '%s__versions' % self.table, metadata,
            *[column(name, primary_key=True) for name in self.keys],
            Column(self.VERSION_COLUMN,
                   self.JTS_TYPES.get(fields[self.version_field], Text)()),
            Column(self.HASH_COLUMN, String(64)),
            Column(self.DELETED_COLUMN, Boolean))
        self.batches_table = Table(
            '%s__batches' % self.table, metadata,
            Column(self.SOURCE_COLUMN, String(256), primary_key=True),
            Column(self.BATCH_COLUMN, String(256), primary_key=True))
        metadata.create_all(self.engine)

    @staticmethod
    def _normalizer(jts_type):
        if jts_type in ('object', 'array'):
            return lambda value: json.dumps(value) if value is not None else None
        if jts_type in ('date', 'datetime', 'time'):
            return lambda value: value

        def normalize(value):
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                return value.isoformat()
            return value
        return normalize

    # Batch commit - target rows, per-key versions and the batch progress
    # marker are always committed together in a single transaction.

    def _commit_batch(self, batch_id, buffer):
        logging.info('load_changes: committing batch %s (%d records) to table %r',
                     batch_id, len(buffer), self.table)
        with self.engine.begin() as conn:
            pending = {}
            for row in buffer:
                self._apply_record(conn, pending, row)
            for key, state in pending.items():
                self._store_state(conn, key, state)
            conn.execute(self.batches_table.insert().values({
                self.SOURCE_COLUMN: self.source_name,
                self.BATCH_COLUMN: batch_id,
            }))
        self.completed_batches.add(batch_id)
        self.counters['batches_committed'] += 1

    def _apply_record(self, conn, pending, row):
        try:
            version = row[self.version_field]
            op = row[self.op_field]
        except KeyError as e:
            raise ValueError('Change record is missing required field %s: %r' % (e, row))
        if version is None:
            raise ValueError('Change record is missing a version: %r' % (row,))
        if op not in (self.upsert_op, self.delete_op):
            raise ValueError('Change record has unknown op %r (expected %r or %r)'
                             % (op, self.upsert_op, self.delete_op))
        key = tuple(self.normalizers[f](row.get(f)) for f in self.keys)
        if any(value is None for value in key):
            raise ValueError('Change record is missing business key values: %r' % (row,))
        state = pending.get(key, _UNSET)
        if state is _UNSET:
            state = self._load_state(conn, key)
        new_state = self._reduce(state, key, version, op, row)
        if new_state is not state:
            pending[key] = new_state
            self._write_current(conn, key, new_state, row)

    def _reduce(self, state, key, version, op, row):
        digest = self._content_hash(op, row)
        deleted = op == self.delete_op
        if state is None:
            self.counters['records_applied'] += 1
            return (version, digest, deleted)
        current_version, current_digest, _ = state
        if version > current_version:
            self.counters['records_applied'] += 1
            return (version, digest, deleted)
        if version == current_version:
            if digest == current_digest:
                # same version, same content - a replay, nothing to do
                self.counters['records_replayed'] += 1
                return state
            raise ChangeConflictError(
                'Conflicting content for key %r at version %r' % (key, version))
        # older version arriving late - ignore it (never resurrects deletes)
        self.counters['records_stale'] += 1
        return state

    def _content_hash(self, op, row):
        values = dict((f, row.get(f)) for f in self.value_fields)
        payload = json.dumps([op, values], sort_keys=True, default=str, ensure_ascii=True)
        return hashlib.sha1(payload.encode('utf-8')).hexdigest()

    # Database access helpers

    def _key_clause(self, table, key):
        return and_(*[table.c[name] == value for name, value in zip(self.keys, key)])

    def _load_state(self, conn, key):
        query = select(self.versions_table.c[self.VERSION_COLUMN],
                       self.versions_table.c[self.HASH_COLUMN],
                       self.versions_table.c[self.DELETED_COLUMN]) \
            .where(self._key_clause(self.versions_table, key))
        row = conn.execute(query).first()
        if row is None:
            return None
        return (row[0], row[1], bool(row[2]))

    def _store_state(self, conn, key, state):
        version, digest, deleted = state
        values = dict(zip(self.keys, key))
        values.update({
            self.VERSION_COLUMN: version,
            self.HASH_COLUMN: digest,
            self.DELETED_COLUMN: deleted,
        })
        conn.execute(self.versions_table.insert().prefix_with('OR REPLACE').values(values))

    def _write_current(self, conn, key, state, row):
        _, _, deleted = state
        if deleted:
            conn.execute(self.current_table.delete()
                         .where(self._key_clause(self.current_table, key)))
        else:
            values = dict((f, self.normalizers[f](row.get(f))) for f in self.value_fields)
            conn.execute(self.current_table.insert().prefix_with('OR REPLACE').values(values))
