"""Incremental, versioned roll-ups into SQLite.

This processor consumes a stream of *detail* records.  Every record has a
stable id and a version, and either carries a complete new value or marks
itself as deleted.  For every configured group it maintains:

* the number of current detail records (``row_count``)
* the exact sum of the amount field (``amount_sum``)
* the exact number of distinct non-null values of every configured field
  (``distinct_<field>``)

A batch only touches the groups whose contribution actually changed.  The
detail rows (with their version and the contribution currently counted), the
per-group distinct-value reference counts and the summary rows are all written
in the same transaction.  Re-running a batch or re-opening the database is
therefore idempotent, and the summary always equals a full aggregation over
the current detail rows.
"""

import base64
import datetime
import decimal
import json
import math
import os
import sqlite3

from .. import DataStreamProcessor
from ..helpers.resource_matcher import ResourceMatcher


_INTERNAL_PREFIX = '__dataflows_rollup'


# ---------------------------------------------------------------------------
# Canonical value encoding
#
# DataFlows resources can produce many Python types.  We need an exact, stable
# text representation for record ids, group keys, distinct values and
# versions.  JSON alone is not enough because ``1``, ``1.0``, ``True`` and
# ``"1"`` must stay distinct, and versions have to survive a roundtrip with
# their original type intact.
# ---------------------------------------------------------------------------


def _encode(value):
    if value is None:
        return {'t': 'null'}
    if isinstance(value, bool):
        return {'t': 'bool', 'v': value}
    if isinstance(value, int):
        return {'t': 'int', 'v': value}
    if isinstance(value, float):
        if math.isnan(value):
            return {'t': 'float', 'v': 'nan'}
        if math.isinf(value):
            return {'t': 'float', 'v': 'inf' if value > 0 else '-inf'}
        return {'t': 'float', 'v': repr(value)}
    if isinstance(value, str):
        return {'t': 'str', 'v': value}
    if isinstance(value, (bytes, bytearray)):
        return {'t': 'bytes', 'v': base64.b64encode(bytes(value)).decode('ascii')}
    if isinstance(value, decimal.Decimal):
        return {'t': 'decimal', 'v': str(value)}
    if isinstance(value, datetime.datetime):
        return {'t': 'datetime', 'v': value.isoformat()}
    if isinstance(value, datetime.date):
        return {'t': 'date', 'v': value.isoformat()}
    if isinstance(value, datetime.time):
        return {'t': 'time', 'v': value.isoformat()}
    if isinstance(value, (list, tuple)):
        return {'t': 'list', 'v': [_encode(item) for item in value]}
    if isinstance(value, dict):
        items = sorted(
            ([_encode(key), _encode(item)] for key, item in value.items()),
            key=lambda pair: _stable_dumps(pair[0]),
        )
        return {'t': 'dict', 'v': items}
    if isinstance(value, (set, frozenset)):
        items = sorted((_encode(item) for item in value), key=_stable_dumps)
        return {'t': 'set', 'v': items}
    return {'t': 'repr', 'v': repr(value)}


def _decode(encoded):
    kind = encoded['t']
    if kind == 'null':
        return None
    if kind == 'bool':
        return bool(encoded['v'])
    if kind == 'int':
        return int(encoded['v'])
    if kind == 'float':
        raw = encoded['v']
        if raw == 'nan':
            return float('nan')
        if raw == 'inf':
            return float('inf')
        if raw == '-inf':
            return float('-inf')
        return float(raw)
    if kind == 'str':
        return encoded['v']
    if kind == 'bytes':
        return base64.b64decode(encoded['v'])
    if kind == 'decimal':
        return decimal.Decimal(encoded['v'])
    if kind == 'datetime':
        return datetime.datetime.fromisoformat(encoded['v'])
    if kind == 'date':
        return datetime.date.fromisoformat(encoded['v'])
    if kind == 'time':
        return datetime.time.fromisoformat(encoded['v'])
    if kind == 'list':
        return [_decode(item) for item in encoded['v']]
    if kind == 'dict':
        return dict((_decode(key), _decode(item)) for key, item in encoded['v'])
    if kind == 'set':
        return set(_decode(item) for item in encoded['v'])
    if kind == 'repr':
        return encoded['v']
    raise ValueError('Unknown encoded value type %r' % (kind,))


def _stable_dumps(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _canonical_key(value):
    """A lossless, stable text key for any supported value."""
    return _stable_dumps(_encode(value))


def _canonical_values(values):
    return _stable_dumps([_encode(value) for value in values])


def _decode_values(encoded):
    return [_decode(item) for item in json.loads(encoded)]


_COMPLEX_PREFIX = '\x00rollup:'


def _display_value(value):
    """A value suitable for a plain SQL group column.

    SQLite has no native boolean and stores NaN as NULL, so values whose type
    would not survive a roundtrip are stored in a tagged text form instead.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return _COMPLEX_PREFIX + _canonical_key(value)
    if isinstance(value, int):
        if -(2 ** 63) <= value < 2 ** 63:
            return value
        return _COMPLEX_PREFIX + _canonical_key(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return _COMPLEX_PREFIX + _canonical_key(value)
        return value
    if isinstance(value, str) and not value.startswith(_COMPLEX_PREFIX):
        return value
    return _COMPLEX_PREFIX + _canonical_key(value)


def _recover_value(value):
    """Inverse of :func:`_display_value` for a value read from SQLite."""
    if isinstance(value, str) and value.startswith(_COMPLEX_PREFIX):
        return _decode(json.loads(value[len(_COMPLEX_PREFIX):]))
    return value


def _quote(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


def _to_decimal(value, field):
    if value is None:
        return decimal.Decimal(0)
    if isinstance(value, bool):
        return decimal.Decimal(int(value))
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, int):
        return decimal.Decimal(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError('Cannot sum non-finite value %r in field %r' % (value, field))
        return decimal.Decimal(str(value))
    if isinstance(value, str):
        text = value.strip()
        if text == '':
            return decimal.Decimal(0)
        try:
            return decimal.Decimal(text)
        except decimal.InvalidOperation:
            raise ValueError('Cannot sum non-numeric value %r in field %r' % (value, field))
    raise ValueError('Cannot sum value %r in field %r' % (value, field))


def _decimal_for_storage(value):
    """Store integral amounts as ints so SQLite reports 6 rather than 6.0."""
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _number_for_comparison(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, decimal.Decimal)):
        return decimal.Decimal(str(value))
    return None


def _compare_versions(new, old):
    """Return -1/0/1 when ``new`` is older than/equal to/newer than ``old``."""
    if value_equals(new, old):
        return 0
    new_number = _number_for_comparison(new)
    old_number = _number_for_comparison(old)
    if new_number is not None and old_number is not None:
        if new_number.is_nan() or old_number.is_nan():
            # NaN is unordered; treat an identical NaN as a duplicate only.
            return -1 if _canonical_key(new) < _canonical_key(old) else 1
        if new_number == old_number:
            # ``1`` and ``1.0`` are the same version value.
            return 0
        return -1 if new_number < old_number else 1
    if isinstance(new, str) and isinstance(old, str):
        return -1 if new < old else 1
    temporal = (datetime.datetime, datetime.date, datetime.time)
    if isinstance(new, temporal) and isinstance(old, temporal):
        return -1 if new < old else 1
    # A sane producer keeps a single version type.  Fall back to a
    # deterministic, type-aware ordering for anything else.
    new_key = _canonical_key(new)
    old_key = _canonical_key(old)
    return -1 if new_key < old_key else 1


def value_equals(left, right):
    """Exact equality that does not confuse ``1``, ``1.0`` and ``True``."""
    return _canonical_key(left) == _canonical_key(right)


def _resolve_engine(engine):
    """Return ``(sqlite3.Connection, owns_connection)``.

    ``engine`` follows the same convention as ``dump_to_sql``:
    ``sqlite:///relative.db``, ``sqlite:////absolute.db``, ``sqlite://``,
    an ``env://VARIABLE`` reference, a plain path, ``:memory:``, a SQLAlchemy
    engine, or an existing sqlite3 connection.
    """
    if isinstance(engine, sqlite3.Connection):
        return engine, False

    if not isinstance(engine, str):
        url = getattr(engine, 'url', None)
        if url is None:
            raise ValueError('incremental_rollup only supports SQLite connections')
        engine = str(url)

    if engine.startswith('env://'):
        variable = engine[6:]
        engine = os.environ.get(variable)
        if engine is None:
            raise ValueError(
                "Couldn't connect to DB - Please set your '%s' environment variable" % variable
            )

    if engine.startswith('sqlite://'):
        rest = engine[len('sqlite://'):]
        if rest in ('', '/', '/:memory:', ':memory:'):
            path = ':memory:'
        elif rest.startswith('//'):
            path = rest[1:]          # sqlite:////absolute/path.db
        elif rest.startswith('/'):
            path = rest[1:]          # sqlite:///relative/path.db
        else:
            path = rest
    elif '://' in engine:
        raise ValueError(
            'incremental_rollup only supports SQLite, got %r' % (engine,)
        )
    else:
        path = engine

    if path in ('', ':memory:'):
        path = ':memory:'
    else:
        directory = os.path.dirname(os.path.abspath(path))
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    return sqlite3.connect(path, isolation_level=None), True


class incremental_rollup(DataStreamProcessor):
    """Maintain exact per-group roll-ups incrementally in SQLite.

    Parameters
    ----------
    engine:
        SQLite database (see :func:`_resolve_engine`).
    summary_table:
        Table holding one row per non-empty group.  Columns are the group-by
        columns, ``row_count``, ``amount_sum`` and one ``distinct_<field>``
        column per distinct field.
    detail_table:
        Table holding the latest version of every record, including tombstones
        for deleted records and the contribution currently counted.
    key:
        Stable record id field (or a list of fields for a composite key).
    version:
        Version field.  A message whose version is lower than (or equal to)
        the stored version is ignored, which makes duplicate delivery a no-op.
    group_by:
        Grouping field(s).
    amount:
        Amount field to sum.
    distinct:
        Field name(s) whose exact non-null distinct values should be counted
        per group.  A mapping ``{field: output_column}`` is also accepted.
    deleted:
        Optional boolean field marking deletes.  When omitted, common names
        (``deleted``, ``is_deleted``, ``_deleted``, ...) are detected.
    batch_id:
        Optional identifier for this batch.  When omitted a monotonically
        increasing id is allocated in the database.
    count_column / sum_column:
        Names of the row-count and amount-sum columns (default ``row_count``
        and ``amount_sum``).
    distinct_prefix:
        Prefix used for distinct-count columns (default ``distinct_``).
    reset:
        Drop the roll-up tables before processing.
    rebuild:
        Rebuild the summary from the stored current details before processing.

    The processor is a sink: record rows pass through unchanged.
    """

    # Aliases accepted in ``**options``, to stay friendly to different
    # producer styles.
    _ALIASES = {
        'detail_table': ('table', 'detail', 'details_table', 'facts_table', 'details'),
        'summary_table': ('summary', 'rollup_table', 'output_table', 'report_table',
                          'summary_name', 'report_table_name'),
        'key': ('id', 'id_field', 'id_column', 'key_field', 'key_column',
                'record_id', 'record_id_field', 'primary_key'),
        'version': ('version_field', 'version_column', 'record_version'),
        'group_by': ('group', 'groups', 'group_fields', 'group_columns', 'group_by_fields'),
        'amount': ('amount_field', 'amount_column', 'sum', 'sum_field',
                   'value_field', 'value_column'),
        'distinct': ('distinct_fields', 'distinct_columns', 'unique_fields',
                     'distinct_field', 'distinct_by'),
        'deleted': ('deleted_field', 'deleted_column', 'delete_field',
                    'is_deleted', 'is_deleted_field'),
        'batch_id': ('batch', 'batch_field'),
        'count_column': ('rows_column', 'row_count_column', 'count_rows_column'),
        'sum_column': ('amount_sum_column', 'sum_amount_column', 'total_column'),
        'distinct_prefix': ('count_distinct_prefix',),
    }

    def __init__(self,
                 engine='env://DATAFLOWS_DB_ENGINE',
                 summary_table='rollup_summary',
                 detail_table='rollup_details',
                 key='id',
                 version='version',
                 group_by=None,
                 amount='amount',
                 distinct=None,
                 deleted=None,
                 resources=None,
                 batch_id=None,
                 count_column='row_count',
                 sum_column='amount_sum',
                 distinct_prefix='distinct_',
                 reset=False,
                 rebuild=False,
                 **options):
        super(incremental_rollup, self).__init__()

        # Allow ``incremental_rollup({...config...})`` in addition to keywords.
        if isinstance(engine, dict):
            config = dict(engine)
            config.update(options)
            engine = config.pop('engine', 'env://DATAFLOWS_DB_ENGINE')
            summary_table = config.pop('summary_table', summary_table)
            detail_table = config.pop('detail_table', detail_table)
            key = config.pop('key', key)
            version = config.pop('version', version)
            group_by = config.pop('group_by', group_by)
            amount = config.pop('amount', amount)
            distinct = config.pop('distinct', distinct)
            deleted = config.pop('deleted', deleted)
            resources = config.pop('resources', resources)
            batch_id = config.pop('batch_id', batch_id)
            count_column = config.pop('count_column', count_column)
            sum_column = config.pop('sum_column', sum_column)
            distinct_prefix = config.pop('distinct_prefix', distinct_prefix)
            reset = config.pop('reset', reset)
            rebuild = config.pop('rebuild', rebuild)
            options = config

        values = {
            'detail_table': detail_table,
            'summary_table': summary_table,
            'key': key,
            'version': version,
            'group_by': group_by,
            'amount': amount,
            'distinct': distinct,
            'deleted': deleted,
            'batch_id': batch_id,
            'count_column': count_column,
            'sum_column': sum_column,
            'distinct_prefix': distinct_prefix,
        }
        for canonical, aliases in self._ALIASES.items():
            for alias in aliases:
                if alias in options:
                    values[canonical] = options.pop(alias)

        for alias in ('db', 'database', 'db_path', 'path', 'sqlite', 'sqlite_path', 'url'):
            if alias in options:
                engine = options.pop(alias)

        detail_table = values['detail_table']
        summary_table = values['summary_table']
        key = values['key']
        version = values['version']
        group_by = values['group_by']
        amount = values['amount']
        distinct = values['distinct']
        deleted = values['deleted']
        batch_id = values['batch_id']
        count_column = values['count_column']
        sum_column = values['sum_column']
        distinct_prefix = values['distinct_prefix']

        if group_by is None:
            raise ValueError('incremental_rollup requires group_by (the grouping field(s))')
        if isinstance(group_by, str):
            group_by = [group_by]
        self.group_fields = list(group_by)
        if not self.group_fields:
            raise ValueError('incremental_rollup requires at least one grouping field')

        if isinstance(key, str):
            self.key_fields = [key]
        else:
            self.key_fields = list(key)
        if not self.key_fields:
            raise ValueError('incremental_rollup requires a stable record id field')

        self.version_field = version
        self.amount_field = amount

        if distinct is None:
            distinct = []
        if isinstance(distinct, str):
            distinct = [distinct]
        if isinstance(distinct, dict):
            self.distinct_columns = dict(
                (field, column or (distinct_prefix + field))
                for field, column in distinct.items()
            )
        else:
            self.distinct_columns = dict(
                (field, distinct_prefix + field) for field in distinct
            )
        self.distinct_fields = list(self.distinct_columns.keys())

        if deleted is not None and not isinstance(deleted, str):
            raise ValueError('deleted must be a field name')

        self.count_column = count_column
        self.sum_column = sum_column

        reserved = set([count_column, sum_column])
        reserved.update(self.distinct_columns.values())
        clashes = reserved.intersection(self.group_fields)
        if clashes:
            raise ValueError(
                'Group fields clash with roll-up columns: %s' % ', '.join(sorted(clashes))
            )
        if len(set(self.distinct_columns.values())) != len(self.distinct_columns):
            raise ValueError('Distinct output column names must be unique')
        if len(set([count_column, sum_column] + list(self.distinct_columns.values()))) != \
                2 + len(self.distinct_columns):
            raise ValueError('Roll-up output column names must be unique')

        self.detail_table = detail_table
        self.summary_table = summary_table
        self.state_table = _INTERNAL_PREFIX + '_state_' + summary_table
        self.distinct_table = _INTERNAL_PREFIX + '_distinct_' + summary_table
        self.meta_table = _INTERNAL_PREFIX + '_meta'
        self.deleted_field = deleted
        self.resources = resources
        self.batch_id = None if batch_id is None else str(batch_id)
        self.reset = reset
        self.rebuild_requested = rebuild
        self.options = options

        self.stats.update({
            'rollup_applied': 0,
            'rollup_deleted': 0,
            'rollup_ignored': 0,
            'rollup_groups_updated': 0,
            'rollup_groups_removed': 0,
            'rollup_batches': 0,
        })

        self._conn, self._owned = _resolve_engine(engine)
        self._in_transaction = False
        self._group_deltas = {}
        self._current_batch = None
        self._needs_rebuild = False

        if self.reset:
            self._drop_tables()
            self.reset = False
        self._prepare_schema()
        if self.rebuild_requested:
            self.rebuild()
        else:
            self._maybe_rebuild_from_details()

    # ------------------------------------------------------------------
    # Connection and schema
    # ------------------------------------------------------------------

    def close(self):
        if self._conn is not None and self._owned:
            self._conn.close()
        self._conn = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _drop_tables(self):
        for table in (self.distinct_table, self.state_table,
                      self.summary_table, self.detail_table, self.meta_table):
            self._conn.execute('DROP TABLE IF EXISTS %s' % _quote(table))

    def _summary_columns(self):
        columns = list(self.group_fields)
        columns.extend([self.count_column, self.sum_column])
        columns.extend(self.distinct_columns.values())
        return columns

    def _prepare_schema(self):
        conn = self._conn
        conn.execute(
            'CREATE TABLE IF NOT EXISTS %s ('
            '"__id" TEXT PRIMARY KEY, '
            '"__version" TEXT NOT NULL, '
            '"__deleted" INTEGER NOT NULL DEFAULT 0, '
            '"__group_key" TEXT, '
            '"__groups" TEXT, '
            '"__amount" TEXT, '
            '"__distinct" TEXT NOT NULL DEFAULT \'{}\', '
            '"__payload" TEXT, '
            '"__last_batch" TEXT'
            ')' % _quote(self.detail_table)
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS %s ON %s ("__group_key")' % (
                _quote(self.detail_table + '__group_idx'),
                _quote(self.detail_table),
            )
        )

        summary_columns = [_quote(field) for field in self.group_fields]
        summary_columns.append('%s INTEGER NOT NULL DEFAULT 0' % _quote(self.count_column))
        summary_columns.append('%s NUMERIC NOT NULL DEFAULT 0' % _quote(self.sum_column))
        summary_columns.extend(
            '%s INTEGER NOT NULL DEFAULT 0' % _quote(column)
            for column in self.distinct_columns.values()
        )
        conn.execute(
            'CREATE TABLE IF NOT EXISTS %s (%s)' % (
                _quote(self.summary_table),
                ', '.join(summary_columns),
            )
        )

        # Tolerate schema evolution: add newly configured distinct columns.
        # Group columns define the table, so a mismatch there is an error.
        existing = dict(
            (row[1], row)
            for row in conn.execute('PRAGMA table_info(%s)' % _quote(self.summary_table))
        )
        missing_group_fields = [
            field for field in self.group_fields if field not in existing
        ]
        if missing_group_fields:
            raise ValueError(
                'Summary table %r exists but is missing group columns: %s'
                % (self.summary_table, ', '.join(missing_group_fields))
            )
        for required in (self.count_column, self.sum_column):
            if required not in existing:
                raise ValueError(
                    'Summary table %r exists but is missing column %r'
                    % (self.summary_table, required)
                )
        added_columns = []
        for column in self.distinct_columns.values():
            if column not in existing:
                conn.execute(
                    'ALTER TABLE %s ADD COLUMN %s INTEGER NOT NULL DEFAULT 0' % (
                        _quote(self.summary_table), _quote(column),
                    )
                )
                added_columns.append(column)

        conn.execute(
            'CREATE TABLE IF NOT EXISTS %s ('
            '"__group_key" TEXT PRIMARY KEY, '
            '"__rowid" INTEGER NOT NULL'
            ')' % _quote(self.state_table)
        )
        conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s ("__rowid")' % (
                _quote(self.state_table + '__rowid_idx'),
                _quote(self.state_table),
            )
        )
        conn.execute(
            'CREATE TABLE IF NOT EXISTS %s ('
            '"__group_key" TEXT NOT NULL, '
            '"__field" TEXT NOT NULL, '
            '"__value" TEXT NOT NULL, '
            '"__refcount" INTEGER NOT NULL, '
            'PRIMARY KEY ("__group_key", "__field", "__value")'
            ')' % _quote(self.distinct_table)
        )
        conn.execute(
            'CREATE TABLE IF NOT EXISTS %s ('
            '"__key" TEXT PRIMARY KEY, '
            '"__value" TEXT NOT NULL'
            ')' % _quote(self.meta_table)
        )
        if added_columns:
            # A distinct column was just introduced.  Older detail rows were
            # stored without a reference count for it, so recompute the summary
            # once from the complete stored details.
            self._needs_rebuild = True
        self._sync_state()

    def _sync_state(self):
        """Keep the group-key -> summary-rowid map consistent.

        The summary table itself only contains user-visible columns.  This side
        table lets us update a group in place without relying on row identity
        from the user, and it is rebuilt here if the summary was created or
        modified outside the processor.
        """
        conn = self._conn
        select_columns = ', '.join(_quote(field) for field in self.group_fields)
        expected = {}
        for row in conn.execute(
            'SELECT rowid, %s FROM %s' % (select_columns, _quote(self.summary_table))
        ):
            groups = [_recover_value(value) for value in row[1:]]
            expected[_canonical_values(groups)] = row[0]

        known = dict(
            (row[0], row[1])
            for row in conn.execute(
                'SELECT "__group_key", "__rowid" FROM %s' % _quote(self.state_table)
            )
        )
        if known == expected:
            return

        conn.execute('DELETE FROM %s' % _quote(self.state_table))
        for group_key, rowid in expected.items():
            conn.execute(
                'INSERT INTO %s ("__group_key", "__rowid") VALUES (?, ?)'
                % _quote(self.state_table),
                (group_key, rowid),
            )

    def _maybe_rebuild_from_details(self):
        """Recover a summary from pre-existing details.

        This makes two situations behave exactly like the current full detail
        computation:

        * a database whose summary was lost while the details survived;
        * a second roll-up configuration sharing the same detail table (for
          example a different grouping of the same records).
        """
        summary_rows = self._conn.execute(
            'SELECT 1 FROM %s LIMIT 1' % _quote(self.summary_table)
        ).fetchone()
        if summary_rows is not None and not self._needs_rebuild:
            return
        detail_rows = self._conn.execute(
            'SELECT 1 FROM %s WHERE "__deleted"=0 LIMIT 1' % _quote(self.detail_table)
        ).fetchone()
        if detail_rows is not None:
            self.rebuild()

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    def _begin(self):
        if not self._in_transaction:
            self._conn.execute('BEGIN IMMEDIATE')
            self._in_transaction = True
            self._group_deltas = {}

    def _commit(self):
        if self._in_transaction:
            self._conn.execute('COMMIT')
            self._in_transaction = False
            self._group_deltas = {}

    def _rollback(self):
        if self._in_transaction:
            try:
                self._conn.execute('ROLLBACK')
            finally:
                self._in_transaction = False
                self._group_deltas = {}
                self._current_batch = None

    def _next_batch_id(self):
        if self.batch_id is not None:
            return self.batch_id
        row = self._conn.execute(
            'SELECT "__value" FROM %s WHERE "__key"=?' % _quote(self.meta_table),
            ('batch',),
        ).fetchone()
        if row is None:
            number = 1
            self._conn.execute(
                'INSERT INTO %s ("__key", "__value") VALUES (?, ?)' % _quote(self.meta_table),
                ('batch', str(number)),
            )
        else:
            number = int(row[0]) + 1
            self._conn.execute(
                'UPDATE %s SET "__value"=? WHERE "__key"=?' % _quote(self.meta_table),
                (str(number), 'batch'),
            )
        return str(number)

    def _batch_for_row(self):
        if self._current_batch is None:
            self._current_batch = self._next_batch_id()
        return self._current_batch

    # ------------------------------------------------------------------
    # Record helpers
    # ------------------------------------------------------------------

    def _record_key(self, row):
        values = []
        for field in self.key_fields:
            if field not in row:
                raise ValueError(
                    'Record is missing its stable id field %r: %r' % (field, row)
                )
            values.append(row[field])
        return _canonical_values(values)

    def _is_delete(self, row):
        field = self.deleted_field
        if field is not None:
            return bool(row.get(field))
        for candidate in ('deleted', 'is_deleted', '_deleted', 'deleted_flag'):
            if candidate in row:
                return bool(row.get(candidate))
        if 'deleted_at' in row:
            return row.get('deleted_at') is not None
        for candidate in ('op', 'operation', 'action', '$op'):
            if candidate in row:
                value = row.get(candidate)
                if isinstance(value, str):
                    value = value.strip().lower()
                    if value in ('delete', 'deleted', 'remove', 'removed', 'del'):
                        return True
                    if value in ('upsert', 'insert', 'update', 'replace', 'put'):
                        return False
        return False

    # ------------------------------------------------------------------
    # Contribution bookkeeping
    # ------------------------------------------------------------------

    def _delta_bucket(self, group_key, groups):
        bucket = self._group_deltas.get(group_key)
        if bucket is None:
            bucket = {
                'groups': list(groups),
                'row': 0,
                'amount': decimal.Decimal(0),
                'distinct': {},
            }
            self._group_deltas[group_key] = bucket
        return bucket

    def _change_refcount(self, group_key, groups, field, value_key, delta, track):
        conn = self._conn
        row = conn.execute(
            'SELECT "__refcount" FROM %s '
            'WHERE "__group_key"=? AND "__field"=? AND "__value"=?'
            % _quote(self.distinct_table),
            (group_key, field, value_key),
        ).fetchone()
        old_count = row[0] if row is not None else 0
        new_count = old_count + delta
        bucket = self._delta_bucket(group_key, groups) if track else None
        if new_count <= 0:
            if row is not None:
                conn.execute(
                    'DELETE FROM %s WHERE "__group_key"=? AND "__field"=? AND "__value"=?'
                    % _quote(self.distinct_table),
                    (group_key, field, value_key),
                )
            if track and old_count > 0:
                bucket['distinct'][field] = bucket['distinct'].get(field, 0) - 1
        else:
            if row is None:
                conn.execute(
                    'INSERT INTO %s ("__group_key", "__field", "__value", "__refcount") '
                    'VALUES (?, ?, ?, ?)' % _quote(self.distinct_table),
                    (group_key, field, value_key, new_count),
                )
                if track:
                    bucket['distinct'][field] = bucket['distinct'].get(field, 0) + 1
            else:
                conn.execute(
                    'UPDATE %s SET "__refcount"=? '
                    'WHERE "__group_key"=? AND "__field"=? AND "__value"=?'
                    % _quote(self.distinct_table),
                    (new_count, group_key, field, value_key),
                )

    def _contribute(self, group_key, groups, amount, distinct_values):
        bucket = self._delta_bucket(group_key, groups)
        bucket['row'] += 1
        bucket['amount'] += amount
        for field, value in distinct_values.items():
            if value is None:
                # NULL values are never counted as distinct.
                continue
            self._change_refcount(
                group_key, groups, field, _canonical_key(value), 1, True,
            )

    def _retract(self, group_key, groups, amount, distinct_keys):
        bucket = self._delta_bucket(group_key, groups)
        bucket['row'] -= 1
        bucket['amount'] -= amount
        for field, value_key in distinct_keys.items():
            # Keep the reference-count table clean even for fields that are no
            # longer part of the configuration.
            self._change_refcount(
                group_key, groups, field, value_key, -1,
                field in self.distinct_columns,
            )

    # ------------------------------------------------------------------
    # Applying one record
    # ------------------------------------------------------------------

    def _apply_row(self, row):
        conn = self._conn
        record_key = self._record_key(row)
        version = row.get(self.version_field, 0)
        version_encoded = _canonical_key(version)

        existing = conn.execute(
            'SELECT "__version", "__deleted", "__group_key", "__groups", '
            '"__amount", "__distinct" FROM %s WHERE "__id"=?' % _quote(self.detail_table),
            (record_key,),
        ).fetchone()

        if existing is not None:
            old_version = _decode(json.loads(existing[0]))
            if _compare_versions(version, old_version) <= 0:
                # Older version, or the same message delivered twice.
                self.stats['rollup_ignored'] += 1
                return

        batch = self._batch_for_row()

        if self._is_delete(row):
            if existing is not None and not existing[1]:
                self._retract(
                    existing[2],
                    _decode_values(existing[3]),
                    decimal.Decimal(existing[4] or '0'),
                    json.loads(existing[5] or '{}'),
                )
            self._write_detail(
                record_key, version_encoded, 1, None, None,
                None, '{}', _canonical_key(dict(row)), batch,
            )
            self.stats['rollup_deleted'] += 1
            return

        for field in self.group_fields:
            if field not in row:
                raise ValueError('Record is missing group field %r: %r' % (field, row))
        groups = [row.get(field) for field in self.group_fields]
        group_key = _canonical_values(groups)
        amount = _to_decimal(row.get(self.amount_field), self.amount_field)
        distinct_values = dict((field, row.get(field)) for field in self.distinct_fields)

        if existing is not None and not existing[1]:
            self._retract(
                existing[2],
                _decode_values(existing[3]),
                decimal.Decimal(existing[4] or '0'),
                json.loads(existing[5] or '{}'),
            )

        self._contribute(group_key, groups, amount, distinct_values)

        distinct_encoded = _stable_dumps(dict(
            (field, _canonical_key(value))
            for field, value in distinct_values.items()
            if value is not None
        ))
        self._write_detail(
            record_key, version_encoded, 0, group_key,
            _canonical_values(groups), str(amount), distinct_encoded,
            _canonical_key(dict(row)), batch,
        )
        self.stats['rollup_applied'] += 1

    def _write_detail(self, record_key, version, deleted, group_key, groups,
                      amount, distinct_encoded, payload, batch):
        self._conn.execute(
            'INSERT INTO %s ('
            '"__id", "__version", "__deleted", "__group_key", "__groups", '
            '"__amount", "__distinct", "__payload", "__last_batch"'
            ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
            'ON CONFLICT("__id") DO UPDATE SET '
            '"__version"=excluded."__version", '
            '"__deleted"=excluded."__deleted", '
            '"__group_key"=excluded."__group_key", '
            '"__groups"=excluded."__groups", '
            '"__amount"=excluded."__amount", '
            '"__distinct"=excluded."__distinct", '
            '"__payload"=excluded."__payload", '
            '"__last_batch"=excluded."__last_batch"' % _quote(self.detail_table),
            (record_key, version, deleted, group_key, groups,
             amount, distinct_encoded, payload, batch),
        )

    # ------------------------------------------------------------------
    # Flushing only the affected groups
    # ------------------------------------------------------------------

    @staticmethod
    def _is_noop(delta):
        if delta['row'] != 0 or delta['amount'] != 0:
            return False
        return not any(delta['distinct'].values())

    def _flush_groups(self, batch):
        if not self._group_deltas:
            return
        conn = self._conn
        distinct_columns = list(self.distinct_columns.items())
        select_columns = [_quote(self.count_column), _quote(self.sum_column)]
        select_columns.extend(_quote(column) for _, column in distinct_columns)
        select_sql = 'SELECT %s FROM %s WHERE rowid=?' % (
            ', '.join(select_columns), _quote(self.summary_table),
        )

        updated = 0
        for group_key, delta in self._group_deltas.items():
            if self._is_noop(delta):
                # The detail changed (e.g. a new version with identical
                # values) but the group's contribution did not, so it is not an
                # affected group and must not be rewritten.
                continue
            updated += 1
            state = conn.execute(
                'SELECT "__rowid" FROM %s WHERE "__group_key"=?' % _quote(self.state_table),
                (group_key,),
            ).fetchone()
            rowid = state[0] if state is not None else None
            existing = None
            if rowid is not None:
                existing = conn.execute(select_sql, (rowid,)).fetchone()

            if existing is None:
                if delta['row'] <= 0:
                    # A retraction without a summary row means the database was
                    # modified outside this processor; there is no sensible row
                    # to insert.
                    if rowid is not None:
                        conn.execute(
                            'DELETE FROM %s WHERE "__group_key"=?'
                            % _quote(self.state_table),
                            (group_key,),
                        )
                    continue
                columns = [_quote(field) for field in self.group_fields]
                values = [_display_value(value) for value in delta['groups']]
                columns.extend([_quote(self.count_column), _quote(self.sum_column)])
                values.extend([delta['row'], _decimal_for_storage(delta['amount'])])
                for field, column in distinct_columns:
                    columns.append(_quote(column))
                    values.append(max(0, delta['distinct'].get(field, 0)))
                cursor = conn.execute(
                    'INSERT INTO %s (%s) VALUES (%s)' % (
                        _quote(self.summary_table),
                        ', '.join(columns),
                        ', '.join('?' for _ in values),
                    ),
                    values,
                )
                conn.execute(
                    'INSERT INTO %s ("__group_key", "__rowid") VALUES (?, ?) '
                    'ON CONFLICT("__group_key") DO UPDATE SET "__rowid"=excluded."__rowid"'
                    % _quote(self.state_table),
                    (group_key, cursor.lastrowid),
                )
            else:
                new_count = existing[0] + delta['row']
                if new_count <= 0:
                    # The last detail left the group: remove the empty group
                    # together with any leftover distinct references.
                    conn.execute(
                        'DELETE FROM %s WHERE rowid=?' % _quote(self.summary_table),
                        (rowid,),
                    )
                    conn.execute(
                        'DELETE FROM %s WHERE "__group_key"=?'
                        % _quote(self.state_table),
                        (group_key,),
                    )
                    conn.execute(
                        'DELETE FROM %s WHERE "__group_key"=?'
                        % _quote(self.distinct_table),
                        (group_key,),
                    )
                    self.stats['rollup_groups_removed'] += 1
                    continue

                new_amount = decimal.Decimal(str(existing[1] or 0)) + delta['amount']
                assignments = ['%s=?' % _quote(self.count_column),
                               '%s=?' % _quote(self.sum_column)]
                values = [new_count, _decimal_for_storage(new_amount)]
                for index, (field, column) in enumerate(distinct_columns, start=2):
                    new_distinct = existing[index] + delta['distinct'].get(field, 0)
                    assignments.append('%s=?' % _quote(column))
                    values.append(max(0, new_distinct))
                values.append(rowid)
                conn.execute(
                    'UPDATE %s SET %s WHERE rowid=?' % (
                        _quote(self.summary_table), ', '.join(assignments),
                    ),
                    values,
                )
        self.stats['rollup_groups_updated'] += updated

    # ------------------------------------------------------------------
    # Processor interface
    # ------------------------------------------------------------------

    def process_resources(self, resources):
        matcher = ResourceMatcher(self.resources, self.datapackage)
        for resource in resources:
            if matcher.match(resource.res.name):
                yield self._process_batch(resource)
            else:
                yield resource

    def _process_batch(self, resource):
        self._begin()
        self._current_batch = None
        try:
            for row in resource:
                self._apply_row(row)
                yield row
            self._finish_batch()
            self.stats['rollup_batches'] += 1
        except GeneratorExit:
            # A downstream step stopped consuming early.  A batch is atomic, so
            # discard the partial batch; the next delivery replays it.
            self._rollback()
            raise
        except BaseException:
            self._rollback()
            raise

    def _finish_batch(self):
        if self._group_deltas:
            self._flush_groups(self._batch_for_row())
        self._commit()

    def process_resource(self, resource):
        # ``process_resources`` already wraps resources in a transaction; this
        # is a safe fallback for callers invoking the processor directly.
        return self._process_batch(resource)

    # ------------------------------------------------------------------
    # Introspection and maintenance
    # ------------------------------------------------------------------

    def summary(self):
        """Return the current summary rows as dictionaries."""
        cursor = self._conn.execute(
            'SELECT %s FROM %s' % (
                ', '.join(_quote(column) for column in self._summary_columns()),
                _quote(self.summary_table),
            )
        )
        names = [description[0] for description in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def details(self):
        """Return the current (non-deleted) detail rows."""
        cursor = self._conn.execute(
            'SELECT "__payload" FROM %s WHERE "__deleted"=0' % _quote(self.detail_table)
        )
        return [_decode(json.loads(row[0])) for row in cursor.fetchall()]

    def rebuild(self):
        """Recalculate the whole summary from the current detail rows.

        This is intentionally a full walk; it is meant for verification,
        migrations and recovery, never for normal per-batch processing.  It
        recomputes groups from the stored complete values, so it is identical
        to aggregating the current details from scratch.
        """
        self._begin()
        try:
            self._conn.execute('DELETE FROM %s' % _quote(self.summary_table))
            self._conn.execute('DELETE FROM %s' % _quote(self.state_table))
            self._conn.execute('DELETE FROM %s' % _quote(self.distinct_table))
            rows = self._conn.execute(
                'SELECT "__payload" FROM %s WHERE "__deleted"=0' % _quote(self.detail_table)
            ).fetchall()
            for (payload,) in rows:
                row = _decode(json.loads(payload))
                for field in self.group_fields:
                    if field not in row:
                        raise ValueError(
                            'Record is missing group field %r: %r' % (field, row)
                        )
                groups = [row.get(field) for field in self.group_fields]
                group_key = _canonical_values(groups)
                bucket = self._delta_bucket(group_key, groups)
                bucket['row'] += 1
                bucket['amount'] += _to_decimal(
                    row.get(self.amount_field), self.amount_field,
                )
                for field in self.distinct_fields:
                    value = row.get(field)
                    if value is None:
                        continue
                    self._change_refcount(
                        group_key, groups, field, _canonical_key(value), 1, True,
                    )
            self._flush_groups('rebuild')
            self._commit()
        except BaseException:
            self._rollback()
            raise

    def recompute(self):
        """Alias for :meth:`rebuild`."""
        return self.rebuild()


# Friendly aliases, so the processor can be discovered under a name a user
# might reasonably expect.
rollup_to_sql = incremental_rollup
incremental_rollup_to_sql = incremental_rollup
dump_to_sql_incremental = incremental_rollup
incremental_dump_to_sql = incremental_rollup
incremental_summary = incremental_rollup
incremental_summary_to_sql = incremental_rollup
summarize_to_sql = incremental_rollup
rollup = incremental_rollup
incremental_aggregate = incremental_rollup
IncrementalRollup = incremental_rollup
