import pytest
from sqlalchemy import create_engine, text

from dataflows import Flow, load_changes, dump_to_sql, set_primary_key
from dataflows.base.exceptions import ProcessorError, ChangeConflictError


def read_table(db_path, table):
    engine = create_engine('sqlite:///{}'.format(db_path))
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(text('select * from {}'.format(table)))]


def run(data, db_path, **kwargs):
    kwargs.setdefault('engine', 'sqlite:///{}'.format(db_path))
    flow = Flow(data, load_changes('current', **kwargs))
    return flow.process()


def versions_by_key(db_path, key='id'):
    return dict((row[key], row['__version__']) for row in read_table(db_path, 'current__versions'))


def test_load_changes_basic(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='alice', version=1, op='upsert', batch_id='b1'),
        dict(id=2, name='bob', version=1, op='upsert', batch_id='b1'),
        dict(id=1, name='alice2', version=2, op='upsert', batch_id='b2'),
    ]
    _, stats = run(data, db, keys=['id'])

    assert read_table(db, 'current') == [
        dict(id=1, name='alice2'),
        dict(id=2, name='bob'),
    ]
    assert versions_by_key(db) == {1: 2, 2: 1}
    assert read_table(db, 'current__batches') == [
        dict(source='default', batch_id='b1'),
        dict(source='default', batch_id='b2'),
    ]
    assert stats['load_changes']['batches_committed'] == 2
    assert stats['load_changes']['records_applied'] == 3


def test_load_changes_out_of_order_and_replayed(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='v3', version=3, op='upsert', batch_id='b1'),
        dict(id=1, name='v1', version=1, op='upsert', batch_id='b1'),  # stale, same batch
        dict(id=1, name='v2', version=2, op='upsert', batch_id='b2'),  # stale, later batch
        dict(id=1, name='v3', version=3, op='upsert', batch_id='b2'),  # replay of stored version
        dict(id=1, name='v5', version=5, op='upsert', batch_id='b3'),
        dict(id=1, name='v4', version=4, op='upsert', batch_id='b3'),  # stale again
    ]
    _, stats = run(data, db, keys=['id'])

    assert read_table(db, 'current') == [dict(id=1, name='v5')]
    assert versions_by_key(db) == {1: 5}
    counters = stats['load_changes']
    assert counters['records_applied'] == 2
    assert counters['records_stale'] == 3
    assert counters['records_replayed'] == 1


def test_load_changes_within_batch_ordering(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),  # duplicate within one batch
        dict(id=2, name='x', version=1, op='upsert', batch_id='b1'),
        dict(id=2, name='x', version=2, op='delete', batch_id='b1'),   # upsert+delete in one batch
        dict(id=3, name='y', version=1, op='delete', batch_id='b1'),
        dict(id=3, name='z', version=2, op='upsert', batch_id='b1'),   # delete+revive in one batch
    ]
    _, stats = run(data, db, keys=['id'])

    assert read_table(db, 'current') == [
        dict(id=1, name='a'),
        dict(id=3, name='z'),
    ]
    assert versions_by_key(db) == {1: 1, 2: 2, 3: 2}
    assert stats['load_changes']['records_replayed'] == 1


def test_load_changes_same_version_different_content_errors(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=5, op='upsert', batch_id='b1'),
        dict(id=1, name='b', version=5, op='upsert', batch_id='b2'),
    ]
    with pytest.raises(ProcessorError) as excinfo:
        run(data, db, keys=['id'])
    assert isinstance(excinfo.value.cause, ChangeConflictError)

    # the conflicting batch was not committed - state is exactly the first batch
    assert read_table(db, 'current') == [dict(id=1, name='a')]
    assert versions_by_key(db) == {1: 5}
    assert read_table(db, 'current__batches') == [dict(source='default', batch_id='b1')]


def test_load_changes_failed_batch_is_fully_rolled_back(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),
        # batch b2 fails on its second record, so none of it may be committed:
        dict(id=2, name='x', version=1, op='upsert', batch_id='b2'),
        dict(id=1, name='b', version=1, op='upsert', batch_id='b2'),  # conflicts with b1
        dict(id=3, name='y', version=1, op='upsert', batch_id='b2'),
    ]
    with pytest.raises(ProcessorError) as excinfo:
        run(data, db, keys=['id'])
    assert isinstance(excinfo.value.cause, ChangeConflictError)

    assert read_table(db, 'current') == [dict(id=1, name='a')]
    assert versions_by_key(db) == {1: 1}
    assert read_table(db, 'current__batches') == [dict(source='default', batch_id='b1')]


def test_load_changes_delete_and_no_resurrection(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=3, op='upsert', batch_id='b1'),
        dict(id=1, name='a', version=5, op='delete', batch_id='b2'),
        dict(id=1, name='a', version=4, op='upsert', batch_id='b3'),   # stale update, must not resurrect
        dict(id=1, name='a', version=5, op='delete', batch_id='b4'),   # replayed delete is fine
        dict(id=2, name='x', version=9, op='delete', batch_id='b4'),   # delete of unknown key
        dict(id=2, name='x', version=8, op='upsert', batch_id='b5'),   # older than the tombstone
        dict(id=1, name='b', version=6, op='upsert', batch_id='b6'),   # newer version revives legitimately
    ]
    _, stats = run(data, db, keys=['id'])

    assert read_table(db, 'current') == [dict(id=1, name='b')]
    assert versions_by_key(db) == {1: 6, 2: 9}
    deleted = dict((row['id'], row['__deleted__']) for row in read_table(db, 'current__versions'))
    assert deleted == {1: False, 2: True}  # id=2 remains a tombstone
    counters = stats['load_changes']
    assert counters['records_stale'] == 2
    assert counters['records_replayed'] == 1


def test_load_changes_restart_continues_processing(tmp_path):
    db = tmp_path / 'changes.db'
    first = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),
        dict(id=2, name='x', version=1, op='upsert', batch_id='b2'),
    ]
    _, stats = run(first, db, keys=['id'])
    assert stats['load_changes']['batches_committed'] == 2

    # after a restart the source redelivers everything (out of order,
    # with duplicates) plus one new batch
    second = [
        dict(id=2, name='x', version=1, op='upsert', batch_id='b2'),  # already committed
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),  # already committed
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),  # duplicate redelivery
        dict(id=1, name='a', version=1, op='upsert', batch_id='b3'),  # replay vs persisted state
        dict(id=3, name='y', version=1, op='upsert', batch_id='b3'),  # new batch
        dict(id=2, name='z', version=2, op='upsert', batch_id='b3'),
        dict(id=2, name='x', version=1, op='upsert', batch_id='b3'),  # stale vs persisted state
    ]
    _, stats = run(second, db, keys=['id'])

    assert read_table(db, 'current') == [
        dict(id=1, name='a'),
        dict(id=2, name='z'),
        dict(id=3, name='y'),
    ]
    counters = stats['load_changes']
    assert counters['batches_skipped'] == 2
    assert counters['batches_committed'] == 1
    assert counters['records_replayed'] == 1
    assert counters['records_stale'] == 1
    assert read_table(db, 'current__batches') == [
        dict(source='default', batch_id='b1'),
        dict(source='default', batch_id='b2'),
        dict(source='default', batch_id='b3'),
    ]


def test_load_changes_retry_after_failure(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),
        dict(id=1, name='b', version=1, op='upsert', batch_id='b2'),  # conflicts, b2 fails
    ]
    with pytest.raises(ProcessorError):
        run(data, db, keys=['id'])

    # the source fixes the conflicting record and redelivers from the start
    fixed = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),  # skipped
        dict(id=1, name='b', version=2, op='upsert', batch_id='b2'),  # now a proper newer version
    ]
    _, stats = run(fixed, db, keys=['id'])

    assert read_table(db, 'current') == [dict(id=1, name='b')]
    assert versions_by_key(db) == {1: 2}
    counters = stats['load_changes']
    assert counters['batches_skipped'] == 1
    assert counters['batches_committed'] == 1


def test_load_changes_cleaning_via_normal_flow(tmp_path):
    db = tmp_path / 'changes.db'

    def clean(row):
        row['name'] = row['name'].strip().title()

    data = [
        dict(id=1, name='  alice ', version=1, op='upsert', batch_id='b1'),
        dict(id=2, name=' bob', version=1, op='upsert', batch_id='b1'),
    ]
    Flow(
        data,
        clean,
        load_changes('current', keys=['id'], engine='sqlite:///{}'.format(db)),
    ).process()

    assert read_table(db, 'current') == [
        dict(id=1, name='Alice'),
        dict(id=2, name='Bob'),
    ]


def test_load_changes_composite_key(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, sub='x', name='a', version=1, op='upsert', batch_id='b1'),
        dict(id=1, sub='y', name='b', version=1, op='upsert', batch_id='b1'),
        dict(id=1, sub='x', name='c', version=2, op='upsert', batch_id='b2'),
    ]
    run(data, db, keys=['id', 'sub'])

    rows = sorted(read_table(db, 'current'), key=lambda row: row['sub'])
    assert rows == [
        dict(id=1, sub='x', name='c'),
        dict(id=1, sub='y', name='b'),
    ]
    versions = dict(
        ((row['id'], row['sub']), row['__version__'])
        for row in read_table(db, 'current__versions')
    )
    assert versions == {(1, 'x'): 2, (1, 'y'): 1}


def test_load_changes_keys_from_primary_key(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),
    ]
    Flow(
        data,
        set_primary_key(['id']),
        load_changes('current', engine='sqlite:///{}'.format(db)),
    ).process()
    assert read_table(db, 'current') == [dict(id=1, name='a')]


def test_load_changes_requires_keys(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(id=1, name='a', version=1, op='upsert', batch_id='b1'),
    ]
    with pytest.raises(ProcessorError):
        run(data, db)


def test_load_changes_custom_field_names_and_source(tmp_path):
    db = tmp_path / 'changes.db'
    data = [
        dict(key=1, name='a', seq=7, kind='u', batch='b1'),
        dict(key=1, name='a', seq=8, kind='d', batch='b2'),
    ]
    run(data, db, keys=['key'],
        version_field='seq', op_field='kind', batch_field='batch',
        source='collector-1',
        upsert_op='u', delete_op='d')

    assert read_table(db, 'current') == []
    assert versions_by_key(db, key='key') == {1: 8}
    assert read_table(db, 'current__batches') == [dict(source='collector-1', batch_id='b1'),
                                                  dict(source='collector-1', batch_id='b2')]


def test_dump_to_sql_still_works(tmp_path):
    # the plain SQL dumper is untouched and keeps working side by side
    db = tmp_path / 'plain.db'
    data = [dict(x=1, y='a'), dict(x=2, y='b')]
    Flow(
        data,
        dump_to_sql(dict(output_table={'resource-name': 'res_1'}),
                    engine='sqlite:///{}'.format(db)),
    ).process()
    assert read_table(db, 'output_table') == data
