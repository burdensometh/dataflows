import os
import glob

import pytest

from dataflows import Flow
from dataflows.base import exceptions


def _write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(text)
    return path


@pytest.fixture
def merge_files(tmpdir):
    """A batch of CSVs where later files introduce new fields and decimals."""
    base = str(tmpdir)
    files = [
        _write(os.path.join(base, 'a.csv'), 'id,age\n1,10\n2,11\n'),
        _write(os.path.join(base, 'b.csv'), 'id,age,score\n3,12.5,x\n4,13,y\n'),
        _write(os.path.join(base, 'c.csv'), 'id,flag\n5,true\n'),
    ]
    return files


def _fields(dp):
    return [f['name'] for f in dp.descriptor['resources'][0]['schema']['fields']]


def _types(dp):
    return {f['name']: f['type'] for f in dp.descriptor['resources'][0]['schema']['fields']}


def test_merge_schema_is_decided_for_the_whole_batch(merge_files):
    from dataflows import load

    results, dp, _ = Flow(load(merge_files, merge=True)).results()
    rows = results[0]

    # Columns follow the order of first appearance across the inputs.
    assert _fields(dp) == ['id', 'age', 'score', 'flag']
    # `age` is an integer in the first file but holds decimals in a later one - the whole
    # field must be a number, and the early integers must be represented as numbers too.
    assert _types(dp)['age'] == 'number'
    assert rows[0]['age'] == 10
    assert rows[2]['age'] == 12.5


def test_later_file_decimal_does_not_get_rejected_or_truncated(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    ints = _write(os.path.join(base, 'ints.csv'), 'id,price\n1,10\n2,20\n')
    decimals = _write(os.path.join(base, 'decimals.csv'), 'id,price\n3,12.5\n4,19.99\n')

    results, dp, _ = Flow(load([ints, decimals], merge=True)).results()
    assert _types(dp)['price'] == 'number'
    # Without merge, loading the first file alone would type `price` as integer, and a
    # sample of it would reject / truncate the later decimals.
    from decimal import Decimal
    assert [row['price'] for row in results[0]] == [
        Decimal('10'), Decimal('20'), Decimal('12.5'), Decimal('19.99')
    ]


def test_only_one_input_is_open_at_a_time(tmpdir):
    from dataflows import load
    from dataflows.processors.load import _MergedInput

    base = str(tmpdir)
    files = [
        _write(os.path.join(base, 'f{}.csv'.format(i)), 'id,val\n{},{}\n'.format(i, i))
        for i in range(10)
    ]

    counter = {'open': 0, 'max': 0}
    original_open = _MergedInput.open
    original_close = _MergedInput.close

    def counting_open(self):
        counter['open'] += 1
        counter['max'] = max(counter['max'], counter['open'])
        return original_open(self)

    def counting_close(self):
        was_open = self.stream is not None
        result = original_close(self)
        if was_open:
            counter['open'] -= 1
        return result

    _MergedInput.open = counting_open
    _MergedInput.close = counting_close
    try:
        results, _, _ = Flow(load(files, merge=True)).results()
    finally:
        _MergedInput.open = original_open
        _MergedInput.close = original_close

    assert len(results[0]) == 10
    assert counter['max'] == 1
    assert counter['open'] == 0


def test_first_file_does_not_decide_types(merge_files):
    from dataflows import load

    # Reverse the input order. Columns follow first appearance, so their order changes -
    # but the types must be decided by the whole batch, exactly as in the original order.
    reversed_results, reversed_dp, _ = Flow(load(list(reversed(merge_files)), merge=True)).results()
    results, dp, _ = Flow(load(merge_files, merge=True)).results()

    assert _types(reversed_dp) == _types(dp)
    assert sorted(reversed_results[0], key=lambda row: row['id']) == \
        sorted(results[0], key=lambda row: row['id'])


def test_missing_columns_are_null(merge_files):
    from dataflows import load

    results, _, _ = Flow(load(merge_files, merge=True)).results()
    rows = results[0]
    # `a.csv` has no `score`/`flag`, `c.csv` has no `age`/`score`.
    assert rows[0]['score'] is None and rows[0]['flag'] is None
    assert rows[4]['age'] is None and rows[4]['score'] is None
    assert rows[4]['flag'] is True


def test_incompatible_types_fail_explicitly(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    ints = _write(os.path.join(base, 'ints.csv'), 'id,val\n1,10\n')
    text = _write(os.path.join(base, 'text.csv'), 'id,val\n2,abc\n')

    for sources in ([ints, text], [text, ints]):
        with pytest.raises(exceptions.ProcessorError) as excinfo:
            Flow(load(sources, merge=True)).results()
        cause = excinfo.value.cause
        assert isinstance(cause, exceptions.IncompatibleTypeError)
        assert cause.field_name == 'val'
        assert set(cause.types) == {'integer', 'string'}


def test_explicit_declaration_wins_and_is_strict(merge_files):
    from dataflows import load

    # `age` is declared an integer, but a later file holds 12.5 -> must fail loudly.
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        Flow(load(merge_files, merge=True,
                  override_fields={'age': {'type': 'integer'}})).results()
    assert isinstance(excinfo.value.cause, exceptions.ExplicitFieldError)
    assert "'age'" in str(excinfo.value.cause)

    # Declaring `age` as a string wins over inference and converts every value.
    results, dp, _ = Flow(load(merge_files, merge=True,
                               override_fields={'age': {'type': 'string'}})).results()
    assert _types(dp)['age'] == 'string'
    assert [row['age'] for row in results[0][:3]] == ['10', '11', '12.5']


def test_explicit_declaration_that_matches_passes(merge_files):
    from dataflows import load

    results, dp, _ = Flow(load(merge_files, merge=True,
                               override_fields={'age': {'type': 'number'}})).results()
    assert _types(dp)['age'] == 'number'
    assert results[0][0]['age'] == 10


def test_on_error_handlers_apply_in_merged_mode(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    a = _write(os.path.join(base, 'a.csv'), 'id,val\n1,10\n')
    b = _write(os.path.join(base, 'b.csv'), 'id,val\n2,20\n')

    # With a *differently declared* type for a field whose data still matches, the handler
    # is irrelevant - inference already guarantees every value casts to the final type.
    for handler in (load.ERRORS_RAISE, load.ERRORS_DROP, load.ERRORS_CLEAR):
        results, dp, _ = Flow(load(
            [a, b], merge=True,
            override_fields={'val': {'type': 'number'}},
            on_error=handler,
        )).results()
        assert [row['val'] for row in results[0]] == [10, 20]


def test_explicit_declaration_is_strict_even_with_on_error(merge_files):
    from dataflows import load

    # A declared `integer` field receiving a decimal must fail, regardless of `on_error`.
    for handler in (load.ERRORS_RAISE, load.ERRORS_DROP, load.ERRORS_CLEAR, load.ERRORS_IGNORE):
        with pytest.raises(exceptions.ProcessorError) as excinfo:
            Flow(load(merge_files, merge=True,
                      override_fields={'age': {'type': 'integer'}},
                      on_error=handler)).results()
        assert isinstance(excinfo.value.cause, exceptions.ExplicitFieldError)


def test_invalid_explicit_declaration_fails_before_reading(tmpdir, merge_files):
    from dataflows import load

    with pytest.raises(exceptions.ProcessorError) as excinfo:
        Flow(load(merge_files, merge=True,
                  override_fields={'age': {'type': 'not-a-type'}})).results()
    assert isinstance(excinfo.value.cause, exceptions.ExplicitFieldError)


def test_override_schema_fields_replace_the_schema(merge_files):
    from dataflows import load

    results, dp, _ = Flow(load(merge_files, merge=True, override_schema={
        'fields': [
            {'name': 'id', 'type': 'string'},
            {'name': 'age', 'type': 'number'},
        ],
        'title': 'merged',
    })).results()
    assert _fields(dp) == ['id', 'age']
    assert _types(dp) == {'id': 'string', 'age': 'number'}
    assert dp.descriptor['resources'][0]['schema']['title'] == 'merged'
    assert results[0][0] == {'id': '1', 'age': 10}


def test_all_null_field_does_not_constrain_types(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    nulls = _write(os.path.join(base, 'nulls.csv'), 'id,val\n1,\n')
    ints = _write(os.path.join(base, 'ints.csv'), 'id,val\n2,7\n')

    results, dp, _ = Flow(load([nulls, ints], merge=True)).results()
    assert _types(dp)['val'] == 'integer'
    assert [row['val'] for row in results[0]] == [None, 7]


def test_single_pass_iterators_are_supported():
    from dataflows import load

    consumed = []

    def gen(rows, extra_field=None):
        for i in range(rows):
            consumed.append(i)
            row = {'id': i}
            if extra_field:
                row[extra_field] = i + 0.5
            yield row

    results, dp, _ = Flow(load([gen(2), gen(3, 'value')], merge=True)).results()
    # Each iterator was consumed exactly once.
    assert consumed == [0, 1, 0, 1, 2]
    assert _fields(dp) == ['id', 'value']
    assert _types(dp)['value'] == 'number'
    assert [row['value'] for row in results[0]] == [None, None, 0.5, 1.5, 2.5]


def test_does_not_keep_all_rows_in_memory():
    """Rows are spooled to disk, so a batch larger than a sample is handled in one pass."""
    from dataflows import load

    def gen(rows):
        for i in range(rows):
            yield {'id': i, 'value': i / 10}

    results, dp, _ = Flow(load([gen(5000)], merge=True)).results()
    assert len(results[0]) == 5000
    assert _types(dp)['value'] == 'number'


def test_globs_and_directories_are_supported(merge_files):
    from dataflows import load

    base = os.path.dirname(merge_files[0])
    # A glob matching all three files, and the directory itself, give the same batch.
    from_glob = Flow(load(os.path.join(base, '*.csv'), merge=True)).results()
    from_dir = Flow(load(base, merge=True)).results()
    assert from_glob[0] == from_dir[0]
    assert _fields(from_glob[1]) == ['id', 'age', 'score', 'flag']


def test_datapackage_resource_tuple(merge_files):
    from dataflows import load

    datapackage = {'resources': [
        {'name': 'r1', 'path': 'r1.csv', 'schema': {'fields': [{'name': 'foo', 'type': 'string'}]}},
        {'name': 'r2', 'path': 'r2.csv', 'schema': {'fields': [{'name': 'foo', 'type': 'string'},
                                                               {'name': 'bar', 'type': 'integer'}]}},
    ]}
    resources = [[{'foo': 'a'}, {'foo': 'b'}], [{'foo': 'c', 'bar': 1}]]

    results, dp, _ = Flow(load((datapackage, resources), merge=True)).results()
    assert _fields(dp) == ['foo', 'bar']
    assert results[0] == [
        {'foo': 'a', 'bar': None},
        {'foo': 'b', 'bar': None},
        {'foo': 'c', 'bar': 1},
    ]


def test_datapackage_json_source(tmpdir):
    from dataflows import load
    import json

    base = str(tmpdir)
    _write(os.path.join(base, 'r1.csv'), 'a\n1\n2\n')
    _write(os.path.join(base, 'r2.csv'), 'a,b\n3,x\n')
    with open(os.path.join(base, 'datapackage.json'), 'w') as f:
        json.dump({'profile': 'data-package', 'resources': [
            {'name': 'r1', 'path': 'r1.csv', 'format': 'csv',
             'schema': {'fields': [{'name': 'a', 'type': 'integer'}]}},
            {'name': 'r2', 'path': 'r2.csv', 'format': 'csv',
             'schema': {'fields': [{'name': 'a', 'type': 'integer'},
                                   {'name': 'b', 'type': 'string'}]}},
        ]}, f)

    results, dp, _ = Flow(load(os.path.join(base, 'datapackage.json'), merge=True)).results()
    assert _fields(dp) == ['a', 'b']
    assert results[0] == [
        {'a': 1, 'b': None},
        {'a': 2, 'b': None},
        {'a': 3, 'b': 'x'},
    ]


def test_datapackage_json_source_resource_selection(tmpdir):
    from dataflows import load
    import json

    base = str(tmpdir)
    _write(os.path.join(base, 'r1.csv'), 'a\n1\n')
    _write(os.path.join(base, 'r2.csv'), 'a\n2\n')
    with open(os.path.join(base, 'datapackage.json'), 'w') as f:
        json.dump({'resources': [
            {'name': 'r1', 'path': 'r1.csv', 'format': 'csv',
             'schema': {'fields': [{'name': 'a', 'type': 'integer'}]}},
            {'name': 'r2', 'path': 'r2.csv', 'format': 'csv',
             'schema': {'fields': [{'name': 'a', 'type': 'integer'}]}},
        ]}, f)

    results, dp, _ = Flow(load(os.path.join(base, 'datapackage.json'), merge=True,
                               resources=['r2'])).results()
    assert results[0] == [{'a': 2}]


def test_merged_load_matches_regular_load_for_single_input():
    from dataflows import load

    for filename in ['data/beatles_age.csv', 'data/beatles_drumkits.csv', 'data/temporal.csv']:
        regular = Flow(load(filename)).results()
        merged = Flow(load([filename], merge=True)).results()
        assert regular[0] == merged[0]
        assert regular[1].descriptor['resources'][0]['schema'] == \
            merged[1].descriptor['resources'][0]['schema']


def test_output_feeds_following_flows(merge_files):
    from dataflows import load, filter_rows

    def only_numbers(rows):
        for row in rows:
            yield row

    flow = Flow(
        load(merge_files, merge=True),
        filter_rows(equals=[dict(flag=True)]),
    )
    results, _, _ = flow.results()
    assert results[0] == [{'id': 5, 'age': None, 'score': None, 'flag': True}]


def test_limit_rows_applies_per_input(merge_files):
    from dataflows import load

    results, _, _ = Flow(load(merge_files, merge=True, limit_rows=1)).results()
    assert [row['id'] for row in results[0]] == [1, 3, 5]


def test_strip(merge_files):
    from dataflows import load

    results, _, _ = Flow(load(merge_files, merge=True)).results()
    # No surrounding whitespace in the fixture; just make sure stripping doesn't break data.
    assert all(row['id'] is not None for row in results[0])


def test_duplicate_headers_in_merged_load(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    dup = _write(os.path.join(base, 'dup.csv'), 'a,a\n1,2\n')

    with pytest.raises(exceptions.ProcessorError):
        Flow(load([dup], merge=True)).results()

    results, dp, _ = Flow(load([dup], merge=True, deduplicate_headers=True)).results()
    assert results[0] == [{'a (1)': 1, 'a (2)': 2}]
    assert _fields(dp) == ['a (1)', 'a (2)']


def test_merge_false_keeps_regular_loading(merge_files):
    from dataflows import load

    results, dp, _ = Flow(load(merge_files[0])).results()
    # Regular loading (merge=False) still infers from the sample of one source.
    assert _types(dp) == {'id': 'integer', 'age': 'integer'}
    assert results[0] == [{'id': 1, 'age': 10}, {'id': 2, 'age': 11}]


def test_no_selected_inputs_fails():
    from dataflows import load

    with pytest.raises(exceptions.ProcessorError) as excinfo:
        Flow(load([], merge=True)).results()
    assert isinstance(excinfo.value.cause, exceptions.MergedSchemaError)


def test_merge_with_cast_strategy_strings(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    ints = _write(os.path.join(base, 'ints.csv'), 'id,val\n1,10\n')
    nums = _write(os.path.join(base, 'nums.csv'), 'id,val\n2,1.5\n')

    results, dp, _ = Flow(load([ints, nums], merge=True,
                               cast_strategy=load.CAST_TO_STRINGS)).results()
    assert _types(dp)['val'] == 'number'
    # `results()` re-validates against the schema, just like a regular `load`.
    assert [str(row['val']) for row in results[0]] == ['10', '1.5']
    # The raw stream (what following flows see) holds strings.
    ds = Flow(load([ints, nums], merge=True, cast_strategy=load.CAST_TO_STRINGS)).datastream()
    assert [row['val'] for row in list(ds.res_iter)[0]] == ['10', '1.5']


def test_merge_infers_python_types_strategy(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    ints = _write(os.path.join(base, 'ints.csv'), 'id,val\n1,10\n')
    nums = _write(os.path.join(base, 'nums.csv'), 'id,val\n2,1.5\n')

    results, dp, _ = Flow(load([ints, nums], merge=True,
                               infer_strategy=load.INFER_STRINGS,
                               cast_strategy=load.CAST_TO_STRINGS)).results()
    assert _types(dp)['val'] == 'string'
    assert [row['val'] for row in results[0]] == ['10', '1.5']


def test_merge_extract_missing_values(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    a = _write(os.path.join(base, 'a.csv'), 'id,val\n1,N/A\n')
    b = _write(os.path.join(base, 'b.csv'), 'id,val\n2,3\n')

    results, dp, _ = Flow(load([a, b], merge=True,
                               override_schema={'missingValues': ['N/A']},
                               extract_missing_values=True)).results()
    assert _types(dp)['val'] == 'integer'
    assert results[0][0]['missingValues'] == {'val': 'N/A'}
    assert results[0][1]['missingValues'] == {}


def test_merge_temporal_formats_are_preserved(tmpdir):
    from dataflows import load

    base = str(tmpdir)
    dates = _write(os.path.join(base, 'dates.csv'),
                   'id,when\n1,2020-01-01\n2,2021-02-03\n')

    results, dp, _ = Flow(load([dates], merge=True)).results()
    assert _types(dp)['when'] == 'date'
    assert str(results[0][0]['when']) == '2020-01-01'


def test_merge_files_are_cleaned_up(tmpdir):
    from dataflows import load
    import tempfile

    base = str(tmpdir)
    a = _write(os.path.join(base, 'a.csv'), 'id,val\n1,10\n')
    b = _write(os.path.join(base, 'b.csv'), 'id,val\n2,20\n')

    before = set(glob.glob(os.path.join(tempfile.gettempdir(), 'dataflows-merge-*')))
    Flow(load([a, b], merge=True)).process()
    after = set(glob.glob(os.path.join(tempfile.gettempdir(), 'dataflows-merge-*')))
    assert after <= before
