"""Tests for the verifiable checkpoint mode.

The checkpoint processor accepts a caller-declared manifest of input files,
a transformation version and cleaning parameters. The prefix is skipped only
when every declaration matches a completed record (content hashes, never file
mtime), and the cached stream itself passes integrity verification.
"""
import datetime
import decimal
import json
import os
import shutil
import subprocess
import sys

import pytest

from dataflows import Flow, checkpoint
from dataflows.base.exceptions import CheckpointError, ProcessorError


ROWS = [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}]


@pytest.fixture
def workspace(tmp_path):
    input_file = tmp_path / 'data.csv'
    input_file.write_text('a,b\n1,x\n2,y\n')
    checkpoint_dir = tmp_path / 'cps'
    return tmp_path, input_file, checkpoint_dir


def make_source(counter):
    def source():
        counter['runs'] += 1
        for row in ROWS:
            yield dict(row)
    return source


def verified_flow(factory, checkpoint_dir, input_file, *, name='cp',
                  version='v1', parameters=None):
    return Flow(
        factory(),
        checkpoint(name, checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version=version,
                   parameters=parameters or {}),
    )


def test_skips_prefix_while_declarations_unchanged(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    flow = verified_flow(make_source(counter), checkpoint_dir, input_file)

    first, _, _ = flow.results()
    second, _, _ = flow.results()
    third, _, _ = flow.results()

    assert counter['runs'] == 1
    assert first == second == third == [ROWS]
    files = os.listdir(str(checkpoint_dir / 'cp'))
    assert {'stream.ndjson', 'checkpoint-metadata.json', 'checkpoint.lock'} <= set(files)


def test_input_content_change_forces_rebuild(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file).results()
    assert counter['runs'] == 1

    # mtime-only change must not invalidate the cache.
    stat = os.stat(str(input_file))
    os.utime(str(input_file), (stat.st_atime + 1000, stat.st_mtime + 1000))
    verified_flow(make_source(counter), checkpoint_dir, input_file).results()
    assert counter['runs'] == 1

    # A content change must invalidate it, regardless of timestamps.
    input_file.write_text('a,b\n1,x\n2,y\n3,z\n')
    verified_flow(make_source(counter), checkpoint_dir, input_file).results()
    assert counter['runs'] == 2


def test_version_change_forces_rebuild(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file,
                  version='v1').results()
    verified_flow(make_source(counter), checkpoint_dir, input_file,
                  version='v2').results()
    assert counter['runs'] == 2


def test_parameters_change_forces_rebuild(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file,
                  parameters={'strip': True}).results()
    verified_flow(make_source(counter), checkpoint_dir, input_file,
                  parameters={'strip': False}).results()
    assert counter['runs'] == 2


def test_parameter_comparison_is_content_based(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    Flow(make_source(counter)(), checkpoint('cp', checkpoint_path=str(checkpoint_dir),
                                            input_files=[str(input_file)],
                                            transformation_version='v1',
                                            parameters={'nested': {'b': 2, 'a': 1}, 'list': [3, 4]})
         ).results()
    # Same content, different dict ordering / literal identity.
    results, _, _ = Flow(
        checkpoint('cp', checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version='v1',
                   parameters={'nested': {'a': 1, 'b': 2}, 'list': [3, 4]})
    ).results()
    assert counter['runs'] == 1
    assert results == [ROWS]


def test_named_inputs_and_order_independence(workspace):
    tmp_path, input_file, checkpoint_dir = workspace
    other = tmp_path / 'other.csv'
    other.write_text('z\n9\n')
    counter = {'runs': 0}
    Flow(make_source(counter)(),
         checkpoint('cp', checkpoint_path=str(checkpoint_dir),
                    input_files=[{'name': 'main', 'path': str(input_file)},
                                 str(other)],
                    transformation_version='v1')).results()
    Flow(checkpoint('cp', checkpoint_path=str(checkpoint_dir),
                    input_files=[str(other),
                                 {'name': 'main', 'path': str(input_file)}],
                    transformation_version='v1')).results()
    assert counter['runs'] == 1


def test_removed_input_file_is_detected(workspace):
    tmp_path, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file).results()
    os.unlink(str(input_file))
    with pytest.raises(CheckpointError) as excinfo:
        verified_flow(make_source(counter), checkpoint_dir, input_file).results()
    assert 'does not exist' in str(excinfo.value)


def test_nonserializable_parameters_rejected(workspace):
    _, input_file, checkpoint_dir = workspace
    with pytest.raises(CheckpointError):
        Flow(make_source({'runs': 0})(),
             checkpoint('cp', checkpoint_path=str(checkpoint_dir),
                        input_files=[str(input_file)],
                        transformation_version='v1',
                        parameters={'bad': object()})).results()


def test_types_schema_and_rows_round_trip_through_cache(workspace):
    _, input_file, checkpoint_dir = workspace
    typed_rows = [
        {'i': 5, 'f': 1.5, 's': 'hi', 'd': datetime.date(2020, 1, 2),
         'dec': decimal.Decimal('3.14'), 'b': True},
        {'i': 6, 'f': 2.5, 's': 'ho', 'd': datetime.date(2021, 3, 4),
         'dec': decimal.Decimal('0.001'), 'b': False},
    ]

    def typed_source():
        for row in typed_rows:
            yield dict(row)

    fresh, fresh_dp, _ = verified_flow(typed_source, checkpoint_dir,
                                       input_file, name='typed').results()
    cached, cached_dp, _ = Flow(
        checkpoint('typed', checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version='v1')).results()

    assert fresh == cached == [typed_rows]
    assert fresh_dp.descriptor == cached_dp.descriptor
    assert isinstance(cached[0][0]['d'], datetime.date)
    assert isinstance(cached[0][0]['dec'], decimal.Decimal)
    fields = cached_dp.descriptor['resources'][0]['schema']['fields']
    types = {field['name']: field['type'] for field in fields}
    assert types == {'i': 'integer', 'f': 'number', 's': 'string',
                     'd': 'date', 'dec': 'number', 'b': 'boolean'}


def test_multiple_resources_round_trip(workspace):
    _, input_file, checkpoint_dir = workspace

    def first():
        yield {'k': 1}
        yield {'k': 2}

    def second():
        yield {'m': 'a'}

    Flow(first(), second(),
         checkpoint('multi', checkpoint_path=str(checkpoint_dir),
                    input_files=[str(input_file)],
                    transformation_version='v1')).results()
    results, dp, _ = Flow(
        checkpoint('multi', checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version='v1')).results()
    assert results == [[{'k': 1}, {'k': 2}], [{'m': 'a'}]]
    assert len(dp.resources) == 2


def test_failed_rebuild_keeps_previous_cache(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file).results()
    stream_path = str(checkpoint_dir / 'cp' / 'stream.ndjson')
    metadata_path = str(checkpoint_dir / 'cp' / 'checkpoint-metadata.json')
    good_stream = open(stream_path).read()
    good_metadata = open(metadata_path).read()

    # Invalidate via input change, then fail during schema inference.
    input_file.write_text('a,b\n9,z\n')

    def failing_fast():
        yield {'a': 1}
        raise RuntimeError('boom during inference')

    with pytest.raises(ProcessorError) as excinfo:
        verified_flow(failing_fast, checkpoint_dir, input_file).results()
    assert 'boom during inference' in str(excinfo.value)

    # Fail again, this time after inference while rows are consumed.
    def failing_late():
        for i in range(150):
            yield {'a': i}
        raise RuntimeError('boom during rows')

    with pytest.raises(ProcessorError) as excinfo:
        verified_flow(failing_late, checkpoint_dir, input_file).results()
    assert 'boom during rows' in str(excinfo.value)

    # The old complete cache is untouched and no partial file remains.
    assert open(stream_path).read() == good_stream
    assert open(metadata_path).read() == good_metadata
    assert not os.path.exists(stream_path + '.active')
    assert sorted(os.listdir(str(checkpoint_dir / 'cp'))) == [
        'checkpoint-metadata.json', 'checkpoint.lock', 'stream.ndjson']

    # Once the input matches again, the preserved cache is reused.
    input_file.write_text('a,b\n1,x\n2,y\n')
    results, _, _ = Flow(
        checkpoint('cp', checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version='v1')).results()
    assert results == [ROWS]
    assert counter['runs'] == 1


def test_partial_consumption_does_not_commit(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    flow = Flow(
        make_source(counter)(),
        checkpoint('partial', checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version='v1'))
    datastream = flow.datastream()
    consumed = 0
    with pytest.raises(RuntimeError):
        for resource in datastream.res_iter:
            for _ in resource:
                consumed += 1
                if consumed == 1:
                    raise RuntimeError('consumer stopped early')

    base = str(checkpoint_dir / 'partial')
    assert not os.path.exists(os.path.join(base, 'stream.ndjson'))
    assert not os.path.exists(os.path.join(base, 'stream.ndjson.active'))
    assert not os.path.exists(os.path.join(base, 'checkpoint-metadata.json'))


def test_corrupted_stream_is_detected_and_rebuilt(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file, name='corrupt').results()
    stream_path = str(checkpoint_dir / 'corrupt' / 'stream.ndjson')
    with open(stream_path, 'a') as file:
        file.write('{"tampered": true}\n')

    results, _, _ = verified_flow(make_source(counter), checkpoint_dir,
                                  input_file, name='corrupt').results()
    assert results == [ROWS]
    assert counter['runs'] == 2


def test_tampered_metadata_is_detected_and_rebuilt(workspace):
    _, input_file, checkpoint_dir = workspace
    counter = {'runs': 0}
    verified_flow(make_source(counter), checkpoint_dir, input_file,
                  name='tampered').results()
    metadata_path = str(checkpoint_dir / 'tampered' / 'checkpoint-metadata.json')
    metadata = json.load(open(metadata_path))
    metadata['stream_sha256'] = 'deadbeef'
    json.dump(metadata, open(metadata_path, 'w'))

    results, _, _ = verified_flow(make_source(counter), checkpoint_dir,
                                  input_file, name='tampered').results()
    assert results == [ROWS]
    assert counter['runs'] == 2


def test_legacy_usage_remains_unchanged(workspace):
    tmp_path, _, _ = workspace
    legacy_dir = tmp_path / 'legacy'
    Flow([{'foo': 'bar'}],
         checkpoint('lc', checkpoint_path=str(legacy_dir))).results()
    results, _, _ = Flow(
        checkpoint('lc', checkpoint_path=str(legacy_dir))).results()
    assert results == [[{'foo': 'bar'}]]
    assert not os.path.exists(
        str(legacy_dir / 'lc' / 'checkpoint-metadata.json'))
    assert not os.path.exists(
        str(legacy_dir / 'lc' / 'checkpoint.lock'))


def _concurrency_worker(argv):
    checkpoint_dir, input_file, mode = argv
    from dataflows import Flow, checkpoint

    def source():
        for i in range(50):
            yield {'a': i}

    declaration = dict(checkpoint_path=checkpoint_dir,
                       input_files=[input_file],
                       transformation_version='v1',
                       parameters={'k': 7})
    if mode == 'write':
        results, _, _ = Flow(source(), checkpoint('cc', **declaration)).results()
        assert results == [[{'a': i} for i in range(50)]]
    else:
        results, _, _ = Flow(checkpoint('cc', **declaration)).results()
        # A reader arriving before the first build sees an empty stream set.
        assert not results or results[0] in ([], [{'a': i} for i in range(50)])


def test_concurrent_writers_commit_single_complete_version(workspace, monkeypatch):
    _, input_file, checkpoint_dir = workspace
    pytest.importorskip('fcntl')
    # Re-exec this module in child processes so the worker is picklable by
    # the spawn/fork starters without depending on pytest internals.
    env = dict(os.environ)
    env['DATAFLOWS_CP_WORKER'] = json.dumps(
        [str(checkpoint_dir), str(input_file), 'write'])
    code = (
        'import json, os, sys;'
        'sys.path.insert(0, %r);'
        'from tests.test_checkpoint import _concurrency_worker;'
        '_concurrency_worker(json.loads(os.environ["DATAFLOWS_CP_WORKER"]))'
        % os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    processes = [subprocess.Popen([sys.executable, '-I', '-c', code], env=env)
                 for _ in range(4)]
    assert [process.wait() for process in processes] == [0, 0, 0, 0]

    base = str(checkpoint_dir / 'cc')
    files = os.listdir(base)
    assert 'stream.ndjson' in files
    assert 'checkpoint-metadata.json' in files
    assert not any(name.endswith('.active') for name in files)

    # The surviving version passes full verification on the next run.
    results, _, _ = Flow(
        checkpoint('cc', checkpoint_path=str(checkpoint_dir),
                   input_files=[str(input_file)],
                   transformation_version='v1',
                   parameters={'k': 7})).results()
    assert results == [[{'a': i} for i in range(50)]]


def test_concurrent_readers_see_only_complete_versions(workspace):
    _, input_file, checkpoint_dir = workspace
    pytest.importorskip('fcntl')
    env = dict(os.environ)
    base_code = (
        'import json, os, sys;'
        'sys.path.insert(0, %r);'
        'from tests.test_checkpoint import _concurrency_worker;'
        '_concurrency_worker(json.loads(os.environ["DATAFLOWS_CP_WORKER"]))'
        % os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    readers = []
    for _ in range(6):
        env['DATAFLOWS_CP_WORKER'] = json.dumps(
            [str(checkpoint_dir), str(input_file), 'read'])
        readers.append(subprocess.Popen([sys.executable, '-I', '-c', base_code],
                                        env=dict(env)))
    env['DATAFLOWS_CP_WORKER'] = json.dumps(
        [str(checkpoint_dir), str(input_file), 'write'])
    writer = subprocess.Popen([sys.executable, '-I', '-c', base_code], env=dict(env))

    assert writer.wait() == 0
    assert [process.wait() for process in readers] == [0] * len(readers)
    assert not any(name.endswith('.active')
                   for name in os.listdir(str(checkpoint_dir / 'cc')))
