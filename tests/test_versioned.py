import os
import json

import pytest

from dataflows import (
    Flow, dump_to_versioned_path, load_versioned, set_type, load,
)
from dataflows.base.exceptions import ProcessorError
from dataflows.processors.dumpers import versioning


def _read_descriptor(root, version='current'):
    path = os.path.join(root, versioning.resolve_version(root, version),
                        'datapackage.json')
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def _dump(root, *resources):
    return Flow(*resources, dump_to_versioned_path(root)).process()


def test_versioned_dump_publishes_current_symlink_and_index(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}], [{'a': 3}])

    current = os.path.join(root, 'current')
    assert os.path.islink(current)
    assert os.path.realpath(current) == os.path.realpath(
        os.path.join(root, 'versions', '1'))

    with open(os.path.join(root, 'index.json'), encoding='utf-8') as f:
        index = json.load(f)
    assert [entry['version'] for entry in index] == [1]
    entry = index[0]
    assert entry['count_of_rows'] == 3
    assert entry['hash']
    assert [(r['name'], r['count_of_rows']) for r in entry['resources']] == \
        [('res_1', 2), ('res_2', 1)]

    # Nothing is written at the root except version bookkeeping.
    assert not os.path.exists(os.path.join(root, 'res_1.csv'))
    assert os.path.exists(os.path.join(root, 'versions', '1', 'res_1.csv'))
    assert os.path.exists(
        os.path.join(root, 'versions', '1', 'datapackage.json'))


def test_empty_resources_are_saved_with_rowcount_and_hash(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1}], [])

    descriptor = _read_descriptor(root)
    assert descriptor['count_of_rows'] == 1
    names = []
    for resource in descriptor['resources']:
        names.append(resource['name'])
        # Every resource - even empty ones - keeps its true count and hash.
        assert 'count_of_rows' in resource
        assert resource['hash']
        assert resource['bytes'] is not None
    assert names == ['res_1', 'res_2']
    empty = descriptor['resources'][1]
    assert empty['count_of_rows'] == 0

    # The empty resource file exists (header only) and loads back as [].
    empty_path = os.path.join(root, 'current', empty['path'])
    assert os.path.exists(empty_path)
    results, _, _ = Flow(load_versioned(root)).results()
    assert results == [[{'a': 1}], []]


def test_failed_publish_keeps_previous_version(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1, 'b': 'v1'}])

    with pytest.raises(ProcessorError):
        Flow(
            [{'a': 'not-an-int', 'b': 'v2'}],
            set_type('a', type='integer'),
            dump_to_versioned_path(root),
        ).process()

    # The failed run never became a version and left no staging directory.
    assert os.listdir(os.path.join(root, 'versions')) == ['1']
    assert os.readlink(os.path.join(root, 'current')) == \
        os.path.join('versions', '1')
    assert not [n for n in os.listdir(root)
                if n.startswith(versioning.STAGING_PREFIX)]

    # Readers still see the last good version.
    results, _, _ = Flow(load_versioned(root)).results()
    assert results == [[{'a': 1, 'b': 'v1'}]]


def test_republish_creates_new_immutable_versions(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1}])
    _dump(root, [{'a': 2}, {'a': 3}])

    assert sorted(os.listdir(os.path.join(root, 'versions'))) == ['1', '2']
    assert os.readlink(os.path.join(root, 'current')) == \
        os.path.join('versions', '2')

    latest, _, _ = Flow(load_versioned(root)).results()
    assert latest == [[{'a': 2}, {'a': 3}]]

    # Old versions remain available for explicit reads (int and str).
    for version in (1, '1'):
        old, _, _ = Flow(load_versioned(root, version=version)).results()
        assert old == [[{'a': 1}]]

    with open(os.path.join(root, 'index.json'), encoding='utf-8') as f:
        assert [e['version'] for e in json.load(f)] == [1, 2]


def test_version_is_pinned_when_loading_starts(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'id': i, 'generation': 'v1'} for i in range(5)])

    # Start a reader: the version is pinned before any row is consumed.
    loader = load_versioned(root)
    datastream = Flow(loader).datastream()
    assert loader.pinned_version_id == '1'
    assert os.path.realpath(loader.resolved_version_dir).endswith(
        os.path.join('versions', '1'))

    # Republishing while the reader is still iterating must not affect it.
    _dump(root, [{'id': i, 'generation': 'v2'} for i in range(5)])
    _dump(root, [{'id': i, 'generation': 'v3'} for i in range(5)])
    assert os.readlink(os.path.join(root, 'current')) == \
        os.path.join('versions', '3')

    rows = [row for resource in datastream.res_iter for row in resource]
    assert {row['generation'] for row in rows} == {'v1'}
    assert len(rows) == 5

    # A freshly started reader pins the new current version.
    new_loader = load_versioned(root)
    results, _, _ = Flow(new_loader).results()
    assert new_loader.pinned_version_id == '3'
    assert results[0][0]['generation'] == 'v3'


def test_load_versioned_from_nested_path(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1}])

    nested = os.path.join(root, 'current')
    results, _, _ = Flow(load_versioned(nested)).results()
    assert results == [[{'a': 1}]]


def test_load_versioned_missing_version_and_root(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1}])

    with pytest.raises(ProcessorError) as exc_info:
        Flow(load_versioned(root, version=99)).results()
    assert isinstance(exc_info.value.__cause__, FileNotFoundError)

    with pytest.raises(ProcessorError) as exc_info:
        Flow(load_versioned(str(tmp_path / 'elsewhere'))).results()
    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


def test_versioned_dump_reuses_format_writers(tmp_path):
    root = str(tmp_path / 'pkg')
    Flow(
        [{'k': 1}, {'k': 2}],
        dump_to_versioned_path(root, format='json'),
    ).process()

    version_dir = os.path.join(root, 'versions', '1')
    assert os.path.exists(os.path.join(version_dir, 'res_1.json'))
    results, _, _ = Flow(load_versioned(root)).results()
    assert results == [[{'k': 1}, {'k': 2}]]


def test_plain_load_reads_published_descriptor(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1, 'b': 'x'}])

    descriptor = os.path.join(root, 'current', 'datapackage.json')
    results, _, _ = Flow(load(descriptor)).results()
    assert results == [[{'a': 1, 'b': 'x'}]]


def test_version_ids_advance_and_never_overwrite(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1}])

    with pytest.raises(ProcessorError):
        Flow(
            [{'a': 'bad'}],
            set_type('a', type='integer'),
            dump_to_versioned_path(root),
        ).process()

    # The failed run is discarded; the next publish takes the next free id
    # and never overwrites the existing version directory.
    _dump(root, [{'a': 2}])
    versions = sorted(os.listdir(os.path.join(root, 'versions')))
    assert versions == ['1', '2']
    first = _read_descriptor(root, version='1')
    assert first['resources'][0]['count_of_rows'] == 1
    results, _, _ = Flow(load_versioned(root)).results()
    assert results == [[{'a': 2}]]

    # Allocating directly while a version exists always picks a fresh id.
    assert versioning.next_version_id(root) == 3


def test_publish_failure_rolls_back_to_previous_version(tmp_path, monkeypatch):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1, 'b': 'v1'}])

    def broken_write_index(root_path, index):
        raise OSError('simulated index write failure')

    monkeypatch.setattr(versioning, 'write_index', broken_write_index)

    with pytest.raises(ProcessorError):
        _dump(root, [{'a': 2, 'b': 'v2'}])

    # The symlink was restored to version 1 and no version 2 is visible.
    assert os.readlink(os.path.join(root, 'current')) == \
        os.path.join('versions', '1')
    assert os.listdir(os.path.join(root, 'versions')) == ['1']
    assert not [n for n in os.listdir(root)
                if n.startswith(versioning.STAGING_PREFIX)]
    results, _, _ = Flow(load_versioned(root)).results()
    assert results == [[{'a': 1, 'b': 'v1'}]]

    # After the failure, publishing again still works and takes version 2.
    monkeypatch.undo()
    _dump(root, [{'a': 3, 'b': 'v3'}])
    assert os.readlink(os.path.join(root, 'current')) == \
        os.path.join('versions', '2')
    results, _, _ = Flow(load_versioned(root)).results()
    assert results == [[{'a': 3, 'b': 'v3'}]]


def test_multi_resource_failure_does_not_mix_files(tmp_path):
    root = str(tmp_path / 'pkg')
    _dump(root, [{'a': 1}], [{'a': 2}])

    # Failure happens while processing the second resource: res_1 of the new
    # run must not leak alongside the files of the published version.
    with pytest.raises(ProcessorError):
        Flow(
            [{'a': 10}],
            [{'a': 'bad'}],
            set_type('a', type='integer'),
            dump_to_versioned_path(root),
        ).process()

    version_one = os.path.join(root, 'versions', '1')
    files = sorted(os.listdir(version_one))
    assert files == ['datapackage.json', 'res_1.csv', 'res_2.csv']
    descriptor = _read_descriptor(root)
    assert [r['count_of_rows'] for r in descriptor['resources']] == [1, 1]
