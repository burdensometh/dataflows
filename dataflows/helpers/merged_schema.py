"""
Support for merged loading - loading several inputs as one logical table.

The schema of the merged table is decided for the whole batch of the selected inputs, and
not by the first input (or by the first rows / sample of it). The helpers in this module:

* collect the union of fields, ordered by first appearance across the inputs;
* unify each field's type across all inputs - nulls are ignored, ``integer`` is promoted to
  ``number``, and any other combination of types fails explicitly;
* spool already-read rows to temporary storage while the final schema is being determined,
  so inputs which can only be iterated once are supported, the number of rows is not limited
  by memory, and the data itself is never kept in RAM.
"""

import collections
import datetime
import decimal
import json
import os
import shutil
import tempfile

from tableschema import Field
from tableschema.exceptions import CastError
from tableschema.schema import _INFER_TYPE_ORDER, _TypeGuesser

from ..base.exceptions import ExplicitFieldError, IncompatibleTypeError

#: Types whose values depend on a format which should be preserved.
TEMPORAL_TYPES = ('date', 'datetime', 'time')


def type_priority(field_type):
    """Rank of a type in tableschema's inference order (lower is more specific)."""
    for index, type_rec in enumerate(_INFER_TYPE_ORDER):
        name = type_rec[0] if isinstance(type_rec, tuple) else type_rec
        if name == field_type:
            return index
    return len(_INFER_TYPE_ORDER)


def is_missing(value, missing_values):
    for missing in missing_values:
        if value == missing:
            return True
    return False


def resolve_type(matches, confidence=1):
    """
    Resolve the candidate type matches of one input's field into a single type/format.

    This replicates tableschema's own resolution logic, but works over a counter of matches
    instead of the full list of matches, so memory use is independent of the row count.
    """
    if not matches:
        return 'string', 'default'
    if len(matches) == 1:
        only = next(iter(matches))
        return only[0], only[1]
    max_count = max(matches.values())
    candidates = [
        match for match, count in matches.items()
        if count >= max_count * confidence
    ]
    candidates.sort(key=lambda match: match[2])
    return candidates[0][0], candidates[0][1]


class SchemaScanner(object):
    """
    Infer a single input's field types over *all* of its rows.

    Memory use is bounded by the number of fields and possible types, never by the number of
    rows: per field only a counter of candidate types is kept. The result is equivalent to
    running tableschema's full inference on the whole input, without sampling.
    """

    def __init__(self, guesser=None, missing_values=None):
        self.missing_values = list(missing_values if missing_values is not None else [''])
        self.guesser = guesser or _TypeGuesser(self.missing_values)
        self.field_order = []
        # field name -> Counter of (type, format, priority) candidates
        self.matches_by_field = collections.OrderedDict()

    def seed(self, field_names):
        """Register an input's fields (e.g. its headers) before any row is read."""
        for name in field_names or []:
            self._register(name)

    def _register(self, name):
        if name not in self.matches_by_field:
            self.matches_by_field[name] = collections.Counter()
            self.field_order.append(name)

    def scan(self, row):
        for name, value in row.items():
            self._register(name)
            if value is None or is_missing(value, self.missing_values):
                continue
            for match in self.guesser.cast(value):
                self.matches_by_field[name][match] += 1

    def scan_rows(self, rows):
        for row in rows:
            self.scan(row)
        return self

    def resolved_type(self, field_name):
        """
        The resolved ``(type, format)`` of a field in this input.

        ``None`` is returned when the field never held a non-missing value, so it doesn't
        constrain the merged type.
        """
        matches = self.matches_by_field.get(field_name)
        if not matches:
            return None
        return resolve_type(matches)


def unify_types(field_name, types_by_origin):
    """
    Unify the types resolved for one field across inputs.

    ``types_by_origin`` maps an input label to the ``(type, format)`` resolved for this
    field in that input; ``None`` marks an input which never saw a value for it.

    Nulls are ignored, ``integer`` is promoted to ``number``, and any other combination of
    types fails explicitly with :class:`IncompatibleTypeError`. Returns the unified type
    name, or ``None`` when no input saw a non-null value.
    """
    observed = collections.OrderedDict()
    any_seen = False
    for origin, resolved in types_by_origin.items():
        if resolved is None:
            continue
        field_type, field_format = resolved
        if field_type == 'any':
            # `any` doesn't constrain the field - explicit types win over it.
            any_seen = True
            continue
        observed.setdefault(field_type, (field_format, origin))
    if not observed:
        return 'any' if any_seen else None
    if len(observed) == 1:
        return next(iter(observed))
    if set(observed) == {'integer', 'number'}:
        return 'number'
    origins = {field_type: origin for field_type, (_format, origin) in observed.items()}
    raise IncompatibleTypeError(
        field_name, sorted(observed, key=type_priority), origins)


def unify_formats(field_type, resolved_formats):
    """
    Pick the format for a unified field type.

    Temporal formats are preserved when every input agreed on one; otherwise the field falls
    back to ``any``, which accepts the union of the observed formats.
    """
    if field_type not in TEMPORAL_TYPES:
        return 'default'
    formats = set(resolved_formats) - {None, 'default'}
    if len(formats) == 1:
        return formats.pop()
    if not formats:
        return 'default'
    return 'any'


def validate_declaration(name, descriptor):
    """Validate an explicit declaration eagerly, before any input is read."""
    descriptor = dict(descriptor)
    descriptor['name'] = name
    descriptor.setdefault('type', 'any')
    descriptor.setdefault('format', 'default')
    try:
        field = Field(descriptor)
        field.cast_value(None)
    except Exception as e:
        raise ExplicitFieldError(
            'Invalid explicit declaration for field {!r} ({!r}): {}'
            .format(name, descriptor, e)
        ) from e
    return descriptor


def collect_declarations(override_schema=None, override_fields=None):
    """
    Gather the explicit field information from the `load` options.

    Mirrors the semantics of a regular `load`:

    * ``override_schema['fields']`` fully replaces the field set (``replace_fields``);
    * ``override_fields`` patches individual inferred fields (including their ``type``);
    * other ``override_schema`` keys are preserved as extra schema metadata.

    Returns ``(declarations, patches, replace_fields, extra_schema)``.
    """
    declarations = {}
    patches = {}
    extra_schema = {}
    replace_fields = False
    for key, value in (override_schema or {}).items():
        if key == 'fields':
            replace_fields = True
            for descriptor in value:
                name = descriptor.get('name')
                if name is None:
                    raise ExplicitFieldError(
                        'A declared field is missing a name: {!r}'.format(descriptor))
                declarations[name] = validate_declaration(name, descriptor)
        elif key != 'missingValues':
            extra_schema[key] = value
    for name, descriptor in (override_fields or {}).items():
        validate_declaration(name, descriptor)
        patches[name] = dict(descriptor)
    return declarations, patches, replace_fields, extra_schema


def explicit_field_names(declarations, patches):
    """
    The fields whose type is explicitly declared, and must therefore be validated strictly.

    A full field declaration always counts; a patch counts when it sets the ``type``.
    """
    names = set(declarations or ())
    names.update(name for name, patch in (patches or {}).items() if 'type' in patch)
    return names


class MergedSchema(object):
    """
    The final schema shared by every input of a merged load.

    Every row of every input is converted to this schema, so a value's type never depends
    on the input it happened to come from. Fields missing from a row become ``None``.

    A value which doesn't match its final type fails loudly by default - it's never silently
    dropped or coerced. Passing an explicit ``on_error`` handler opts into the regular
    ``load`` error behaviours (drop / ignore / clear).
    """

    def __init__(self, fields, missing_values=None, stringify=False, explicit_fields=None):
        self.fields = [dict(f) for f in fields]
        self.missing_values = list(missing_values if missing_values is not None else [''])
        self.stringify = stringify
        self.explicit_fields = set(explicit_fields or ())
        self.field_names = [f['name'] for f in self.fields]
        self._casters = {
            f['name']: Field(f, missing_values=self.missing_values)
            for f in self.fields
        }

    def descriptor(self, extra=None):
        schema = {
            'fields': [dict(f) for f in self.fields],
            'missingValues': list(self.missing_values),
        }
        schema.update(extra or {})
        # The merged fields and missing values always win over extra metadata.
        schema['fields'] = [dict(f) for f in self.fields]
        schema['missingValues'] = list(self.missing_values)
        return schema

    def add_field(self, descriptor):
        """Append a field (used to add the extracted-missing-values field)."""
        descriptor = dict(descriptor)
        descriptor.setdefault('format', 'default')
        self.fields.append(descriptor)
        self.field_names.append(descriptor['name'])
        self._casters[descriptor['name']] = Field(
            descriptor, missing_values=self.missing_values)

    def convert(self, row, origin=None, index=None, on_error=None):
        """
        Cast a row onto the merged schema.

        Returns the converted row, or ``None`` when an ``on_error`` handler asked to drop
        it. Without a handler a mismatch raises :class:`ExplicitFieldError`.

        Explicitly declared fields are always validated strictly: a value which doesn't
        match its declared type fails, even when an ``on_error`` handler is provided.
        Other fields honour the handler (drop / ignore / clear). In ``CAST_TO_STRINGS`` mode
        every value is stringified, as in a regular load.
        """
        converted = {}
        okay = True
        for name in self.field_names:
            field = self._casters[name]
            value = row.get(name)
            strict = name in self.explicit_fields
            if self.stringify and not strict:
                if value is not None and not isinstance(value, str):
                    value = str(value)
                converted[name] = value
                continue
            try:
                converted[name] = field.cast_value(value)
            except CastError as e:
                if on_error is None or strict:
                    raise ExplicitFieldError(
                        'Row #{} of input {!r} does not match the final type {!r} of field '
                        '{!r} (value={!r}): {}'.format(
                            index, origin, field.type, name, value, e)
                    ) from e
                if not on_error(origin, row, index, e, field):
                    okay = False
                else:
                    # The handler may have fixed the value (e.g. `clear` sets it to None).
                    converted[name] = row.get(name)
        return converted if okay else None


def build_merged_schema(scans, declarations=None, patches=None, replace_fields=False,
                        missing_values=None, stringify=False, extra_schema=None):
    """
    Build the merged schema from the per-input scans.

    ``scans`` is an ordered list of ``(label, SchemaScanner)`` pairs, in input order.
    ``declarations`` replace inferred fields outright; ``patches`` add metadata to them.
    When ``replace_fields`` is set, the declared fields form the whole schema, in
    declaration order.

    Returns ``(schema, extra_schema)``.
    """
    declarations = declarations or {}
    patches = patches or {}
    missing_values = list(missing_values if missing_values is not None else [''])
    explicit = explicit_field_names(declarations, patches)

    ordered_names = []
    resolved_by_field = {}
    for label, scanner in scans:
        for name in scanner.field_order:
            resolved_by_field.setdefault(name, {})[label] = scanner.resolved_type(name)
            if name not in ordered_names:
                ordered_names.append(name)

    if replace_fields:
        ordered_names = list(declarations.keys())
        for name in patches:
            if name not in ordered_names:
                ordered_names.append(name)
    else:
        # Declarations for fields absent from every input are still part of the schema;
        # patches for unknown fields are ignored, as in a regular load.
        for name in declarations:
            if name not in ordered_names:
                ordered_names.append(name)

    fields = []
    for name in ordered_names:
        by_origin = resolved_by_field.get(name, {})
        resolved_formats = [resolved[1] for resolved in by_origin.values() if resolved]
        declaration = declarations.get(name)
        if declaration is not None:
            descriptor = dict(declaration)
            descriptor['name'] = name
        else:
            descriptor = {'name': name, 'type': unify_types(name, by_origin) or 'string'}
        patched = patches.get(name, {})
        descriptor.update(patched)
        descriptor['name'] = name
        if 'format' not in descriptor:
            # Temporal formats which every input agreed on are preserved; when the type was
            # overridden by a declaration or a patch, a format inferred for another type
            # would be wrong, so fall back to the default.
            inferred_type = descriptor.get('type', 'any')
            type_is_inferred = 'type' not in patched and declaration is None
            descriptor['format'] = unify_formats(
                inferred_type, resolved_formats if type_is_inferred else [])
        fields.append(descriptor)

    schema = MergedSchema(fields, missing_values=missing_values, stringify=stringify,
                          explicit_fields=explicit)
    return schema, extra_schema or {}


class RowSpool(object):
    """
    Temporary on-disk storage for the rows read during the schema scan.

    Rows are serialized into one temporary file per input, preserving native value types,
    so each input is iterated only once and memory use does not grow with the row count.
    """

    def __init__(self, label, directory, index=0):
        self.label = label
        # The index guarantees unique files even if sanitized labels collide.
        self._path = os.path.join(
            directory, 'rows-{:05d}-{}.jsonl'.format(index, _safe_filename(label)))
        self._file = None
        self.count = 0

    def open(self):
        self._file = open(self._path, 'w', encoding='utf-8')
        return self

    def write(self, row):
        self._file.write(json.dumps(row, ensure_ascii=False, default=_json_default))
        self._file.write('\n')
        self.count += 1

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None

    def rows(self):
        if self._file is not None:
            raise RuntimeError('Spool for {!r} is still open'.format(self.label))
        with open(self._path, encoding='utf-8') as f:
            for line in f:
                yield json.loads(line, object_hook=_json_object_hook)


_TYPE_MARKER = '__dataflows_merged_type__'


def _json_default(value):
    if isinstance(value, decimal.Decimal):
        return {_TYPE_MARKER: 'decimal', 'value': str(value)}
    if isinstance(value, datetime.datetime):
        return {_TYPE_MARKER: 'datetime', 'value': value.isoformat()}
    if isinstance(value, datetime.date):
        return {_TYPE_MARKER: 'date', 'value': value.isoformat()}
    if isinstance(value, datetime.time):
        return {_TYPE_MARKER: 'time', 'value': value.isoformat()}
    if isinstance(value, (set, tuple)):
        return list(value)
    if isinstance(value, bytes):
        return {_TYPE_MARKER: 'bytes', 'value': value.hex()}
    return str(value)


def _json_object_hook(obj):
    marker = obj.get(_TYPE_MARKER) if isinstance(obj, dict) and len(obj) == 2 else None
    if marker == 'decimal':
        return decimal.Decimal(obj['value'])
    if marker == 'datetime':
        return datetime.datetime.fromisoformat(obj['value'])
    if marker == 'date':
        return datetime.date.fromisoformat(obj['value'])
    if marker == 'time':
        return datetime.time.fromisoformat(obj['value'])
    if marker == 'bytes':
        return bytes.fromhex(obj['value'])
    return obj


def _safe_filename(label):
    safe = ''.join(c if (c.isalnum() or c in '-_.') else '_' for c in str(label))
    return safe or 'input'


class TempDirectory(object):
    """A temporary directory which is cleaned up explicitly or on garbage collection."""

    def __init__(self, path=None):
        self.name = path or tempfile.mkdtemp(prefix='dataflows-merge-')
        self._cleaned = False

    def cleanup(self):
        if not self._cleaned:
            self._cleaned = True
            shutil.rmtree(self.name, ignore_errors=True)

    def __del__(self):
        try:
            self.cleanup()
        except Exception:  # pragma: no cover - never fail during finalization
            pass


def strip_row(row):
    """Strip surrounding whitespace from string values, in place."""
    whitespace = set(' \t\n\r')
    for k, v in row.items():
        if v and isinstance(v, str) and (v[-1] in whitespace or v[0] in whitespace):
            row[k] = v.strip()
    return row
