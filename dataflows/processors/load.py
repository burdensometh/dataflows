import os
import glob
import warnings
import datetime

from datapackage import Package
from tabulator import Stream
from tableschema.schema import Schema
from .. import DataStreamProcessor
from ..base.exceptions import SourceLoadError, MergedSchemaError
from ..base.schema_validator import (
    schema_validator, ignore, drop, raise_exception, clear, wrap_handler,
)
from ..helpers.resource_matcher import ResourceMatcher
from ..helpers.merged_schema import (
    SchemaScanner, RowSpool, TempDirectory, build_merged_schema,
    collect_declarations, strip_row,
)

from .parsers import XMLParser, ExcelXMLParser, ExtendedSQLParser, GeoJsonParser


class StringsGuesser():
    def cast(self, value):
        return [('string', 'default', 0)]


class TypesGuesser():
    def cast(self, value):
        jts_type = {
            str: 'string',
            int: 'integer',
            float: 'number',
            list: 'array',
            dict: 'object',
            tuple: 'array',
            bool: 'boolean',
            datetime.datetime: 'datetime',
            datetime.date: 'date',
        }.get(type(value))
        ret = [('any', 'default', 0)]
        if jts_type is not None:
            ret.append(('jts_type', 'default', 1))
        return ret


class _MergedInput(object):
    """
    One input of a merged load.

    The row source may be opened lazily (``open``), so a batch of many files never holds
    more than one open stream at a time.
    """

    def __init__(self, label, name, rows=None, headers=None, format='csv',
                 stream=None, opener=None):
        self.label = label
        self.name = name
        self._rows = rows
        self.headers = headers
        self.format = format
        self.stream = stream
        self._opener = opener
        self.spool = None
        self.scanner = None

    def open(self):
        if self._rows is None:
            assert self._opener is not None
            self._rows, self.headers, self.format, self.stream = self._opener()
        return self

    @property
    def rows(self):
        if self._rows is None:
            raise RuntimeError('Input {!r} was not opened'.format(self.label))
        return self._rows

    def close(self):
        if self.stream is not None:
            try:
                self.stream.close()
            finally:
                self.stream = None
        close = getattr(self._rows, 'close', None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
        self._rows = iter(())

    def as_dict(self, row):
        """Normalize a row to a dict (datapackage resources may yield plain lists)."""
        if isinstance(row, dict):
            return dict(row)
        headers = self.headers or []
        return dict(zip(headers, row))


class load(DataStreamProcessor):

    INFER_STRINGS = 'strings'
    INFER_PYTHON_TYPES = 'pytypes'
    INFER_FULL = 'full'

    CAST_TO_STRINGS = 'strings'
    CAST_DO_NOTHING = 'nothing'
    CAST_WITH_SCHEMA = 'schema'

    ERRORS_IGNORE = ignore
    ERRORS_DROP = drop
    ERRORS_RAISE = raise_exception
    ERRORS_CLEAR = clear

    def __init__(self, load_source, name=None, resources=None, strip=True, limit_rows=None,
                 infer_strategy=None, cast_strategy=None,
                 override_schema=None, override_fields=None,
                 extract_missing_values=None,
                 deduplicate_headers=False,
                 deduplicate_headers_case_sensitive=True,
                 deduplicate_headers_format=' (%s)',
                 merge=False,
                 on_error=raise_exception,
                 **options):
        super(load, self).__init__()
        self.load_source = load_source

        self.name = name
        self.strip = strip
        self.limit_rows = limit_rows
        self.options = options
        self.resources = resources
        self.override_schema = override_schema
        self.override_fields = override_fields
        self.merge = merge
        self.deduplicate_headers = deduplicate_headers
        self.deduplicate_headers_case_sensitive = deduplicate_headers_case_sensitive
        self.deduplicate_headers_format = deduplicate_headers_format

        # Extract missing values
        self.extract_missing_values = None
        if extract_missing_values is not None:
            if isinstance(extract_missing_values, bool):
                extract_missing_values = {}
            extract_missing_values.setdefault('source', None)
            extract_missing_values.setdefault('target', 'missingValues')
            extract_missing_values.setdefault('values', [])
            if isinstance(extract_missing_values.get('source'), str):
                extract_missing_values['source'] = [extract_missing_values['source']]
            self.extract_missing_values = extract_missing_values

        self.load_dp = None
        self.resource_descriptors = []
        self.iterators = []

        if 'force_strings' in options:
            warnings.warn('force_strings is being deprecated, use infer_strategy & cast_strategy instead',
                          DeprecationWarning)
            if options['force_strings']:
                infer_strategy = self.INFER_STRINGS
                cast_strategy = self.CAST_TO_STRINGS

        if 'validate' in options:
            warnings.warn('validate is being deprecated, use cast_strategy & on_error instead',
                          DeprecationWarning)
            if options['validate']:
                cast_strategy = self.CAST_WITH_SCHEMA

        # Force strings from stream for the INFER_STRINGS strategy
        if infer_strategy == self.INFER_STRINGS:
            self.options['force_strings'] = True

        self.merged = bool(merge)
        self.infer_strategy = infer_strategy or self.INFER_FULL
        self.cast_strategy = cast_strategy or self.CAST_DO_NOTHING
        # In merged mode every row is converted to the final schema. A user-provided handler
        # is honoured for inferred fields; the default becomes an explicit schema error.
        self._merged_on_error = wrap_handler(on_error) if on_error is not raise_exception else None
        self.on_error = wrap_handler(on_error)
        self._merged_tmp = None
        self._merged_inputs = []
        self._merged_schema = None

        self.guesser = {
            self.INFER_FULL: None,
            self.INFER_PYTHON_TYPES: TypesGuesser,
            self.INFER_STRINGS: StringsGuesser,
        }[self.infer_strategy]

        self.caster = {
            self.CAST_DO_NOTHING: lambda res, it: it,
            self.CAST_WITH_SCHEMA: lambda res, it: schema_validator(res, it, on_error=on_error),
            self.CAST_TO_STRINGS: lambda res, it: self.stringer(it)
        }[self.cast_strategy]

    def process_datapackage(self, dp: Package):
        try:
            return self.safe_process_datapackage(dp)
        except MergedSchemaError:
            # Schema errors are the feature's own, explicit failures - keep them intact.
            raise
        except Exception as e:
            raise SourceLoadError('Failed to load source {!r} and options {!r}: {}'
                                  .format(self.load_source, self.options, e)) from e

    @classmethod
    def get_custom_parsers(cls, custom_parsers=None):
        custom_parsers = custom_parsers or dict()
        custom_parsers.setdefault('xml', XMLParser)
        custom_parsers.setdefault('excel-xml', ExcelXMLParser)
        custom_parsers.setdefault('sql', ExtendedSQLParser)
        custom_parsers.setdefault('geojson', GeoJsonParser)
        return custom_parsers

    def safe_process_datapackage(self, dp: Package):
        if self.merged:
            return self.merged_process_datapackage(dp)

        # If loading from datapackage & resource iterator:
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, resource_iterator = self.load_source
            resources = datapackage_descriptor['resources']
            resource_matcher = ResourceMatcher(self.resources, datapackage_descriptor)
            for resource_descriptor in datapackage_descriptor['resources']:
                if resource_matcher.match(resource_descriptor['name']):
                    self.resource_descriptors.append(resource_descriptor)
            self.iterators = (resource for resource, descriptor in zip(resource_iterator, resources)
                              if resource_matcher.match(descriptor['name']))

        # If load_source is string:
        else:
            # Handle Environment vars if necessary:
            if self.load_source.startswith('env://'):
                env_var = self.load_source[6:]
                self.load_source = os.environ.get(env_var)
                if self.load_source is None:
                    raise ValueError(f"Couldn't find value for env var '{env_var}'")

            # Loading from datapackage:
            if os.path.basename(self.load_source) == 'datapackage.json' or self.options.get('format') == 'datapackage':
                self.load_dp = Package(self.load_source)
                resource_matcher = ResourceMatcher(self.resources, self.load_dp)
                for resource in self.load_dp.resources:
                    if resource_matcher.match(resource.name):
                        self.resource_descriptors.append(resource.descriptor)
                        self.iterators.append(resource.iter(keyed=True, cast=True))

            # Loading for any other source
            else:
                path = os.path.basename(self.load_source)
                path = os.path.splitext(path)[0]
                descriptor = dict(path=self.name or path,
                                  profile='tabular-data-resource')
                self.resource_descriptors.append(descriptor)
                descriptor['name'] = self.name or path
                if 'encoding' in self.options:
                    descriptor['encoding'] = self.options['encoding']
                self.options['custom_parsers'] = self.get_custom_parsers(self.options.get('custom_parsers'))
                self.options.setdefault('ignore_blank_headers', True)
                if 'headers' not in self.options:
                    self.options.setdefault('skip_rows', [{'type': 'preset', 'value': 'auto'}])
                self.options.setdefault('headers', 1)
                self.options.setdefault('sample_size', 1000)
                stream: Stream = Stream(self.load_source, **self.options).open()
                if self.deduplicate_headers_case_sensitive:
                    duplication_test = len(stream.headers) != len(set(stream.headers))
                else:
                    lower_headers = [header.lower() for header in stream.headers]
                    duplication_test = len(lower_headers) != len(set(lower_headers))
                # duplication_test = len(stream.headers) != len(set(stream.headers))
                if duplication_test:
                    if not self.deduplicate_headers:
                        raise ValueError(
                            'Found duplicate headers.' +
                            'Use the `deduplicate_headers` flag (found headers=%r)' % stream.headers)
                    stream.headers = self.rename_duplicate_headers(
                        stream.headers, case_sensitive=self.deduplicate_headers_case_sensitive,
                        deduplicate_format=self.deduplicate_headers_format
                    )
                schema = Schema(self.override_schema or {}).infer(
                    stream.sample, headers=stream.headers,
                    confidence=1, guesser_cls=self.guesser)
                # restore schema field names to original headers
                for header, field in zip(stream.headers, schema['fields']):
                    field['name'] = header
                if self.override_schema:
                    schema.update(self.override_schema)
                if self.override_fields:
                    fields = schema.get('fields', [])
                    for field in fields:
                        field.update(self.override_fields.get(field['name'], {}))
                if self.extract_missing_values:
                    missing_values = schema.get('missingValues', [])
                    if not self.extract_missing_values['values']:
                        self.extract_missing_values['values'] = missing_values
                    schema['fields'].append({
                        'name': self.extract_missing_values['target'],
                        'type': 'object',
                        'format': 'default',
                        'values': self.extract_missing_values['values'],
                    })
                descriptor['schema'] = schema
                descriptor['format'] = self.options.get('format', stream.format)
                descriptor['path'] += '.{}'.format(stream.format)
                self.iterators.append(stream.iter(keyed=True))
        dp.descriptor.setdefault('resources', []).extend(self.resource_descriptors)
        return dp

    # ---------------------------------------------------------------- merged loading

    def merged_process_datapackage(self, dp: Package):
        """
        Load all selected inputs as one table, with one schema decided for the whole batch.

        Every input is fully scanned while its rows are spooled to temporary storage; the
        final schema (union of fields, unified types) is then fixed and each row is
        converted to it before being handed to the following flows.

        Only one input is open at a time, and only the per-field type counters are kept in
        memory until the schema is fixed - the batch size is bounded by disk, not by RAM.
        """
        merged_inputs = self._open_merged_inputs()
        try:
            missing_values = self._merged_missing_values()
            declarations, patches, replace_fields, extra_schema = \
                collect_declarations(self.override_schema, self.override_fields)
            stringify = self.cast_strategy == self.CAST_TO_STRINGS

            scans = []
            for position, merged_input in enumerate(merged_inputs):
                merged_input.open()
                scanner = SchemaScanner(
                    guesser=self.guesser() if self.guesser is not None else None,
                    missing_values=missing_values,
                )
                if merged_input.headers:
                    scanner.seed(merged_input.headers)
                spool = RowSpool(merged_input.label, self._merged_tmp.name,
                                 index=position).open()
                try:
                    for index, row in enumerate(merged_input.rows):
                        if self.limit_rows is not None and index >= self.limit_rows:
                            break
                        row = merged_input.as_dict(row)
                        if self.strip:
                            strip_row(row)
                        scanner.scan(row)
                        spool.write(row)
                finally:
                    spool.close()
                    merged_input.close()
                merged_input.spool = spool
                merged_input.scanner = scanner
                scans.append((merged_input.label, scanner))

            schema, extra_schema = build_merged_schema(
                scans,
                declarations=declarations,
                patches=patches,
                replace_fields=replace_fields,
                missing_values=missing_values,
                stringify=stringify,
                extra_schema=extra_schema,
            )
        except Exception:
            self._merged_cleanup()
            raise

        if self.extract_missing_values is not None:
            self._merged_add_missing_values_field(schema)
        self._merged_schema = schema
        self._merged_inputs = merged_inputs

        descriptor = self._merged_resource_descriptor(dp, merged_inputs, schema, extra_schema)
        dp.descriptor.setdefault('resources', []).append(descriptor)
        return dp

    def _merged_missing_values(self):
        schema = self.override_schema or {}
        missing_values = schema.get('missingValues')
        if missing_values is None:
            return ['']
        return list(missing_values)

    def _merged_add_missing_values_field(self, schema):
        target = self.extract_missing_values['target']
        if not self.extract_missing_values['values']:
            self.extract_missing_values['values'] = list(schema.missing_values)
        if target in schema.field_names:
            return
        schema.add_field({
            'name': target,
            'type': 'object',
            'format': 'default',
            'values': self.extract_missing_values['values'],
        })

    def _merged_resource_descriptor(self, dp, merged_inputs, schema, extra_schema):
        if self.name:
            name = self.name
        elif merged_inputs:
            name = merged_inputs[0].name
        else:
            name = 'merged'
        fmt = merged_inputs[0].format if merged_inputs else 'csv'
        descriptor = dict(
            name=name,
            path='{}.{}'.format(name, fmt or 'csv'),
            profile='tabular-data-resource',
            schema=schema.descriptor(extra_schema),
        )
        if fmt:
            descriptor['format'] = fmt
        return descriptor

    def _open_merged_inputs(self):
        """
        Open every selected input, returning descriptors and single-pass row sources.

        A list `source` stands for several inputs to merge. Each item is loaded like a
        regular `load` source (a path/URL, a datapackage, a (datapackage, resources) pair,
        or any iterable of rows). A list of dicts is a single in-memory input of rows,
        matching how a list source is normally understood.
        """
        self._merged_tmp = TempDirectory()
        sources = list(self.load_source) \
            if isinstance(self.load_source, list) and not self._is_row_list(self.load_source) \
            else [self.load_source]
        merged_inputs = []
        for source in sources:
            merged_inputs.extend(self._open_merged_source(source))
        if not merged_inputs:
            raise MergedSchemaError(
                'No inputs were selected for merged loading (source={!r}, resources={!r})'
                .format(self.load_source, self.resources))
        # Make labels unique (they show up in error messages and spool file names).
        seen = {}
        for merged_input in merged_inputs:
            label = merged_input.label
            if label in seen:
                seen[label] += 1
                merged_input.label = '{} #{}'.format(label, seen[label])
            else:
                seen[label] = 1
        self._merged_inputs = merged_inputs
        return merged_inputs

    def _open_merged_source(self, source):
        if isinstance(source, _MergedInput):
            return [source]
        if self._is_datapackage_tuple(source):
            return self._open_merged_tuple_source(source)
        if not isinstance(source, str):
            # Any other iterable of rows is a single in-memory input.
            return [_MergedInput(
                label=self.name or 'input',
                name=self.name or 'input',
                rows=iter(source),
                headers=None,
                format='csv',
            )]
        source = self._resolve_env_source(source)
        if os.path.basename(source) == 'datapackage.json' or \
                self.options.get('format') == 'datapackage':
            return self._open_merged_datapackage_source(source)
        return [
            self._open_merged_file_source(path)
            for path in self._expand_merged_path(source)
        ]

    @staticmethod
    def _is_datapackage_tuple(source):
        """A ``(datapackage_descriptor, resources_iterator)`` pair, as accepted by `load`."""
        return (
            isinstance(source, tuple) and len(source) == 2 and
            isinstance(source[0], dict) and 'resources' in source[0]
        )

    @staticmethod
    def _is_row_list(source):
        """Whether a list source is a list of rows (a single in-memory input)."""
        return len(source) > 0 and all(isinstance(item, dict) for item in source)

    def _expand_merged_path(self, source):
        """
        Expand a local path into the list of files it stands for.

        A directory stands for the files directly inside it, and a glob pattern for all
        matching files - both sorted, so the merged field order and error messages are
        deterministic. Remote sources and plain file names are used as-is.
        """
        if '://' in source:
            return [source]
        if os.path.isdir(source):
            matches = [os.path.join(source, name) for name in sorted(os.listdir(source))]
            return [path for path in matches if os.path.isfile(path)]
        if glob.has_magic(source):
            return sorted(path for path in glob.glob(source) if os.path.isfile(path))
        return [source]

    def _resolve_env_source(self, source):
        if not source.startswith('env://'):
            return source
        env_var = source[6:]
        resolved = os.environ.get(env_var)
        if resolved is None:
            raise ValueError(f"Couldn't find value for env var '{env_var}'")
        return resolved

    def _open_merged_tuple_source(self, source):
        datapackage_descriptor, resource_iterator = source
        resources = datapackage_descriptor['resources']
        matcher = ResourceMatcher(self.resources, datapackage_descriptor)
        inputs = []
        for resource_descriptor, rows in zip(resources, resource_iterator):
            if not matcher.match(resource_descriptor['name']):
                continue
            schema_fields = (resource_descriptor.get('schema') or {}).get('fields') or []
            inputs.append(_MergedInput(
                label=resource_descriptor['name'],
                name=resource_descriptor['name'],
                rows=rows,
                headers=[field['name'] for field in schema_fields] or None,
                format=resource_descriptor.get('format', 'csv'),
            ))
        return inputs

    def _open_merged_datapackage_source(self, source):
        self.load_dp = Package(source)
        matcher = ResourceMatcher(self.resources, self.load_dp)
        inputs = []
        for resource in self.load_dp.resources:
            if not matcher.match(resource.name):
                continue
            inputs.append(_MergedInput(
                label=resource.name,
                name=resource.name,
                rows=resource.iter(keyed=True, cast=True),
                headers=[field.name for field in resource.schema.fields]
                if resource.schema is not None else None,
                format=resource.descriptor.get('format', 'csv'),
            ))
        return inputs

    def _open_merged_file_source(self, source):
        path = os.path.splitext(os.path.basename(source))[0]
        return _MergedInput(
            label=path,
            name=self.name or path,
            opener=lambda: self._open_merged_stream(source),
        )

    def _open_merged_stream(self, source):
        """Open one file source; returns ``(rows, headers, format, stream)``."""
        options = dict(self.options)
        options['custom_parsers'] = self.get_custom_parsers(options.get('custom_parsers'))
        options.setdefault('ignore_blank_headers', True)
        if 'headers' not in options:
            options.setdefault('skip_rows', [{'type': 'preset', 'value': 'auto'}])
        options.setdefault('headers', 1)
        options.setdefault('sample_size', 1000)
        stream: Stream = Stream(source, **options).open()
        headers = list(stream.headers or [])
        deduplicated = self._merged_deduplicate_headers(headers)
        if deduplicated is not None:
            headers = deduplicated
            stream.headers = headers
        return stream.iter(keyed=True), headers, stream.format, stream

    def _merged_deduplicate_headers(self, headers):
        if self.deduplicate_headers_case_sensitive:
            has_duplicates = len(headers) != len(set(headers))
        else:
            lower_headers = [header.lower() for header in headers]
            has_duplicates = len(lower_headers) != len(set(lower_headers))
        if not has_duplicates:
            return None
        if not self.deduplicate_headers:
            raise ValueError(
                'Found duplicate headers.' +
                'Use the `deduplicate_headers` flag (found headers=%r)' % headers)
        return self.rename_duplicate_headers(
            headers,
            case_sensitive=self.deduplicate_headers_case_sensitive,
            deduplicate_format=self.deduplicate_headers_format,
        )

    def merged_process_resources(self, resources):
        """Pass through the previous flows' resources, then emit the merged table."""
        for resource in super(load, self).process_resources(resources):
            yield resource
        yield self._merged_rows()

    def _merged_rows(self):
        try:
            for merged_input in self._merged_inputs:
                origin = merged_input.label
                for index, row in enumerate(merged_input.spool.rows()):
                    if self.extract_missing_values is not None:
                        self._merged_extract_missing_values(row)
                    converted = self._merged_schema.convert(
                        row, origin=origin, index=index, on_error=self._merged_on_error)
                    if converted is not None:
                        yield converted
        finally:
            self._merged_cleanup()

    def _merged_extract_missing_values(self, row):
        source = self.extract_missing_values['source']
        target = self.extract_missing_values['target']
        values = self.extract_missing_values['values']
        mapping = {}
        if values:
            for key, value in row.items():
                if not source or key in source:
                    if value in values:
                        mapping[key] = value
        row[target] = mapping

    def _merged_cleanup(self):
        if self._merged_tmp is not None:
            self._merged_tmp.cleanup()
        for merged_input in self._merged_inputs:
            merged_input.close()

    def stripper(self, iterator):
        whitespace = set(' \t\n\r')
        for r in iterator:
            for k, v in r.items():
                if v and isinstance(v, str) and (v[-1] in whitespace or v[0] in whitespace):
                    r[k] = v.strip()
            yield r
            # yield dict(
            #     (k, v.strip()) if isinstance(v, str) else (k, v)
            #     for k, v in r.items()
            # )

    def limiter(self, iterator):
        count = 0
        for row in iterator:
            yield row
            count += 1
            if count >= self.limit_rows:
                break

    def stringer(self, iterator):
        for r in iterator:
            yield dict(
                (k, str(v)) if not isinstance(v, str) else (k, v)
                for k, v in r.items()
            )

    def missing_values_extractor(self, iterator):
        source = self.extract_missing_values['source']
        target = self.extract_missing_values['target']
        values = self.extract_missing_values['values']
        for row in iterator:
            mapping = {}
            if values:
                for key, value in row.items():
                    if not source or key in source:
                        if value in values:
                            mapping[key] = value
            row[target] = mapping
            yield row

    def process_resources(self, resources):
        if self.merged:
            yield from self.merged_process_resources(resources)
            return
        yield from super(load, self).process_resources(resources)
        for descriptor, it in zip(self.resource_descriptors, self.iterators):
            if self.extract_missing_values:
                it = self.missing_values_extractor(it)
            it = self.caster(descriptor, it)
            if self.strip:
                it = self.stripper(it)
            if self.limit_rows:
                it = self.limiter(it)
            yield it

    @staticmethod
    def rename_duplicate_headers(duplicate_headers, case_sensitive=True, deduplicate_format=' (%s)'):
        counter = {}
        headers = []
        header_keys = []
        for header in duplicate_headers:
            header_key = header
            if not case_sensitive:
                header_key = header_key.lower()
            header_keys.append(header_key)
            counter.setdefault(header_key, 0)
            counter[header_key] += 1
            if counter[header_key] > 1:
                if counter[header_key] == 2:
                    prev_index = header_keys.index(header_key) 
                    headers[prev_index] = ('%s' + deduplicate_format) % (headers[prev_index], 1)
                header = ('%s' + deduplicate_format) % (header, counter[header_key])
            headers.append(header)
        return headers
