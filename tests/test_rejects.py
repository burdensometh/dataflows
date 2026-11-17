import os

import pytest

from dataflows import Flow, reject_rows, dump_to_sql, set_type, filter_rows
from dataflows.processors.rejects import (
    RejectionStore,
    STATUS_ACCEPTED,
    STATUS_FILTERED,
    STATUS_REJECTED,
    SOURCE_ID_COLUMN,
)


@pytest.fixture
def paths(tmpdir):
    return {
        'ledger': str(tmpdir.join('ledger.db')),
        'warehouse': str(tmpdir.join('warehouse.db')),
    }


def _dump(engine, table='people', resource='res_1'):
    return dump_to_sql({table: {'resource-name': resource}},
                       engine='sqlite:///' + engine)


def _people():
    return [
        dict(name='alice', age='10'),
        dict(name='bob', age='oops'),
        dict(name='carol', age='12'),
    ]


def test_only_clean_rows_reach_sqlite(paths):
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    rows, dp, _ = Flow(
        _people(),
        rejects,
        _dump(paths['warehouse']),
    ).results()

    assert rows[0] == [
        dict(name='alice', age=10),
        dict(name='carol', age=12),
    ]

    rejected = rejects.store.rejected()
    assert len(rejected) == 1
    record = rejected[0]
    assert record['id'] == 'res_1:2'
    assert record['original'] == dict(name='bob', age='oops')
    assert record['stage_name'] == 'set_type'
    assert record['stage_index'] == 1
    assert record['error_type'] == 'ValidationError'
    assert 'oops' in record['reason']
    assert record['current_reason'] == record['reason']

    import sqlite3
    connection = sqlite3.connect(paths['warehouse'])
    try:
        assert connection.execute(
            'select name from people order by name').fetchall() == [('alice',), ('carol',)]
    finally:
        connection.close()


def test_correct_and_replay_selected_row(paths):
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
        add_id_field='source_id',
    )
    Flow(_people(), rejects, _dump(paths['warehouse'])).process()

    # Only the selected rejected row is reprocessed; the clean rows are untouched.
    rejects.store.correct('res_1:2', age='42')
    report = rejects.replay(['res_1:2'])
    assert report == [{'id': 'res_1:2', 'status': STATUS_ACCEPTED, 'result_count': 1}]

    assert rejects.store.status('res_1:2') == STATUS_ACCEPTED
    assert rejects.store.reason('res_1:2') is None
    assert rejects.store.results('res_1:2') == [
        dict(name='bob', age=42, source_id='res_1:2')
    ]
    warehouse = rejects.target_results('res_1:2')
    assert warehouse == [dict(name='bob', age=42, source_id='res_1:2',
                              **{SOURCE_ID_COLUMN: 'res_1:2',
                                 'dataflows_source_seq': 0})]

    # A repeated replay is a no-op for an accepted row...
    assert rejects.replay(['res_1:2']) == [
        {'id': 'res_1:2', 'status': 'skipped', 'reason': 'already accepted'}
    ]
    # ...and can never duplicate rows even when forced.
    rejects.replay(['res_1:2'], force=True)
    assert len(rejects.target_results('res_1:2')) == 1


def test_expansion_is_committed_atomically(paths):
    def expand(rows):
        for row in rows:
            yield dict(row)
            yield dict(row)

    rejects = reject_rows(
        [set_type('age', type='integer'), expand],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    rows, *_ = Flow(_people(), rejects).results()
    # Each accepted source row expanded to two output rows.
    assert len(rows[0]) == 4

    rejects.store.correct('res_1:2', age='7')
    assert rejects.replay(['res_1:2']) == [
        {'id': 'res_1:2', 'status': STATUS_ACCEPTED, 'result_count': 2}
    ]
    assert len(rejects.target_results('res_1:2')) == 2

    # Idempotency: replaying again replaces the two rows, not appends them.
    rejects.replay(['res_1:2'], force=True)
    assert len(rejects.target_results('res_1:2')) == 2


def test_partial_expansion_is_never_emitted(paths):
    def half_expand(rows):
        for row in rows:
            yield dict(row, part=1)
            if row['name'] == 'bob':
                raise ValueError('boom after the first part')
            yield dict(row, part=2)

    rejects = reject_rows(
        [set_type('age', type='integer'), half_expand],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    data = [
        dict(name='alice', age='10'),
        dict(name='bob', age='11'),   # casts fine, but expansion explodes
        dict(name='carol', age='12'),
    ]
    rows, *_ = Flow(data, rejects).results()
    assert [row['part'] for row in rows[0]] == [1, 2, 1, 2]
    assert [row['name'] for row in rows[0]] == ['alice', 'alice', 'carol', 'carol']

    rejected = rejects.store.rejected()
    assert [record['id'] for record in rejected] == ['res_1:2']
    assert rejected[0]['reason'] == 'boom after the first part'
    assert rejected[0]['stage_name'] == 'half_expand'
    assert rejects.target_results('res_1:2') == []


def test_failed_replay_leaves_no_rows_and_refreshes_reason(paths):
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    Flow(_people(), rejects).process()

    rejects.store.correct('res_1:2', age='still-bad')
    report = rejects.replay(['res_1:2'])
    assert report[0]['status'] == STATUS_REJECTED
    assert report[0]['stage_name'] == 'set_type'
    assert 'still-bad' in report[0]['reason']

    assert rejects.store.status('res_1:2') == STATUS_REJECTED
    assert 'still-bad' in rejects.store.reason('res_1:2')
    assert rejects.target_results('res_1:2') == []
    assert rejects.store.results('res_1:2') == []

    # A previously accepted row that is corrected into an invalid value is
    # rejected again *and* its old warehouse rows are removed.
    rejects.store.correct('res_1:2', age='9')
    rejects.replay(['res_1:2'])
    assert len(rejects.target_results('res_1:2')) == 1

    rejects.store.correct('res_1:2', age='nope')
    rejects.replay(['res_1:2'], force=True)
    assert rejects.store.status('res_1:2') == STATUS_REJECTED
    assert rejects.target_results('res_1:2') == []


def test_filtered_rows_are_not_written_to_the_target(paths):
    rejects = reject_rows(
        [set_type('age', type='integer'),
         filter_rows(condition=lambda row: row['age'] < 11)],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    rows, *_ = Flow(_people(), rejects).results()
    assert rows[0] == [dict(name='alice', age=10)]

    rejects.store.correct('res_1:2', age='9')
    assert rejects.replay(['res_1:2']) == [
        {'id': 'res_1:2', 'status': STATUS_ACCEPTED, 'result_count': 1}
    ]
    assert len(rejects.target_results('res_1:2')) == 1


def test_replay_of_filtered_out_row_records_filtered_status(paths):
    rejects = reject_rows(
        [set_type('age', type='integer'),
         filter_rows(condition=lambda row: row['name'] != 'bob')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    Flow(_people(), rejects).process()

    # Bob never failed, but force-replay him with a filter that removes him.
    rejects.store.record_rejection(
        'manual:1', 'res_1', 2, dict(name='bob', age='oops'), 'manually queued',
        resource_descriptor={'name': 'res_1', 'schema': {'fields': [
            {'name': 'name', 'type': 'string', 'format': 'default'},
            {'name': 'age', 'type': 'string', 'format': 'default'},
        ]}})
    rejects.store.correct('manual:1', age='11')
    report = rejects.replay(['manual:1'])
    assert report == [{'id': 'manual:1', 'status': STATUS_FILTERED, 'result_count': 0}]
    assert rejects.store.status('manual:1') == STATUS_FILTERED
    assert rejects.target_results('manual:1') == []


def test_stable_ids_from_natural_key(paths):
    data = [
        dict(sku='A1', qty='5'),
        dict(sku='A2', qty='bad'),
    ]
    rejects = reject_rows(
        [set_type('qty', type='integer')],
        db=paths['ledger'],
        id_field='sku',
        target=dict(path=paths['warehouse'], table='things'),
    )
    Flow(data, rejects).results()
    assert [record['id'] for record in rejects.store.rejected()] == ['res_1:sku=A2']

    rejects.store.correct('res_1:sku=A2', qty='8')
    assert rejects.replay(['res_1:sku=A2'])[0]['status'] == STATUS_ACCEPTED
    assert rejects.target_results('res_1:sku=A2')[0]['qty'] == 8


def test_configuration_errors_are_not_swallowed(paths):
    from dataflows import exceptions

    def broken(rows):
        for row in rows:
            yield row
        raise KeyError('missing-config')

    rejects = reject_rows([broken], db=paths['ledger'])
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        Flow(_people(), rejects).results()
    assert isinstance(excinfo.value.cause, KeyError)
    assert rejects.store.rejected() == []


def test_correction_survives_reprocessing_the_whole_source(paths):
    """Correcting + replaying never requires rerunning the entire dataset."""
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    Flow(_people(), rejects).process()
    rejects.store.correct('res_1:2', age='8')
    assert rejects.replay(['res_1:2'], steps=[set_type('age', type='integer')]) == [
        {'id': 'res_1:2', 'status': STATUS_ACCEPTED, 'result_count': 1}
    ]
    # The clean rows were not reprocessed: only one warehouse row exists.
    assert rejects.target_results('res_1:1') == []
    assert len(rejects.target_results('res_1:2')) == 1


def test_replay_does_not_duplicate_bulk_written_rows(paths):
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
        add_id_field='source_id',
    )
    Flow(_people(), rejects, _dump(paths['warehouse'])).process()

    rejects.store.correct('res_1:2', age='5')
    assert rejects.replay(['res_1:2'])[0]['status'] == STATUS_ACCEPTED
    rejects.replay(['res_1:2'], force=True)
    rejects.replay(['res_1:2'], force=True)

    import sqlite3
    connection = sqlite3.connect(paths['warehouse'])
    try:
        assert connection.execute(
            'select name, age, source_id from people order by name').fetchall() == [
                ('alice', 10, 'res_1:1'),
                ('bob', 5, 'res_1:2'),
                ('carol', 12, 'res_1:3'),
        ]
    finally:
        connection.close()


def test_source_row_ids_stay_stable_across_runs(paths):
    first = reject_rows([set_type('age', type='integer')], db=paths['ledger'])
    Flow(_people(), first).process()
    first.store.close()

    # A second, independent run over the same source must produce the same id.
    second = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    Flow(_people(), second).process()
    second.store.correct('res_1:2', age='3')
    second.replay(['res_1:2'])
    assert second.target_results('res_1:2')[0]['age'] == 3


def test_rerunning_with_fixed_source_clears_the_rejection(paths):
    first = reject_rows([set_type('age', type='integer')], db=paths['ledger'])
    Flow([dict(name='a', age='x')], first).process()
    assert first.store.reason('res_1:1') is not None

    second = reject_rows([set_type('age', type='integer')], db=paths['ledger'])
    Flow([dict(name='a', age='7')], second).process()
    record = second.store.get('res_1:1')
    assert record['status'] == STATUS_ACCEPTED
    assert record['current_reason'] is None
    assert record['last_reason'] is not None


def test_store_queries_and_corrections(paths):
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    Flow(_people(), rejects).process()
    store = rejects.store

    assert [record['id'] for record in store.rejected()] == ['res_1:2']
    assert store.rejected()[0]['original'] == dict(name='bob', age='oops')

    corrected = store.correct('res_1:2', name='Bobby')
    assert corrected == dict(name='Bobby', age='oops')
    assert store.effective_row('res_1:2') == dict(name='Bobby', age='oops')
    assert store.get('res_1:2')['corrected'] == dict(name='Bobby', age='oops')

    store.clear_correction('res_1:2')
    assert store.effective_row('res_1:2') == dict(name='bob', age='oops')

    store.forget('res_1:2')
    assert store.rejected() == []


def test_record_accepted_keeps_full_accounting(paths):
    rejects = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        record_accepted=True,
    )
    Flow(_people(), rejects).results()
    statuses = {record['id']: record['status'] for record in rejects.store.query()}
    assert statuses == {
        'res_1:1': STATUS_ACCEPTED,
        'res_1:2': STATUS_REJECTED,
        'res_1:3': STATUS_ACCEPTED,
    }
    assert rejects.store.ids(status=STATUS_ACCEPTED) == ['res_1:1', 'res_1:3']


def test_existing_ledger_is_reused(paths):
    first = reject_rows([set_type('age', type='integer')], db=paths['ledger'])
    Flow(_people(), first).process()
    assert first.store.rejected()[0]['attempts'] == 1
    first.store.close()

    second = reject_rows(
        [set_type('age', type='integer')],
        db=paths['ledger'],
        target=dict(path=paths['warehouse'], table='people'),
    )
    Flow(_people(), second).process()
    record = second.store.get('res_1:2')
    assert record['attempts'] == 2
    second.store.close()

    with RejectionStore(paths['ledger']) as reopened:
        assert [r['id'] for r in reopened.rejected()] == ['res_1:2']
