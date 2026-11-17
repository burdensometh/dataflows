import random
import sqlite3

import pytest

from dataflows import Flow
from dataflows.processors.incremental_rollup import (
    _canonical_key,
    _canonical_values,
    incremental_rollup,
)


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / 'rollup.db')


def make_processor(db_path, **kwargs):
    options = dict(
        summary_table='daily',
        detail_table='detail',
        key='id',
        version='version',
        group_by='group',
        amount='amount',
        distinct=['customer'],
    )
    options.update(kwargs)
    return incremental_rollup('sqlite:///' + db_path, **options)


def run(db_path, rows, **kwargs):
    processor = make_processor(db_path, **kwargs)
    Flow(rows, processor).process()
    processor.close()


def summary(db_path):
    connection = sqlite3.connect(db_path)
    try:
        return {
            row[0]: row[1:]
            for row in connection.execute(
                'SELECT "group", row_count, amount_sum, distinct_customer '
                'FROM daily'
            )
        }
    finally:
        connection.close()


def detail_versions(db_path):
    connection = sqlite3.connect(db_path)
    try:
        return {
            row[0]: (row[1], row[2])
            for row in connection.execute(
                'SELECT "__id", "__version", "__deleted" FROM detail'
            )
        }
    finally:
        connection.close()


def test_basic_aggregation(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=10, customer='c1'),
        dict(id=2, version=1, group='a', amount=5, customer='c2'),
        dict(id=3, version=1, group='b', amount=7, customer='c1'),
    ])
    assert summary(db_path) == {
        'a': (2, 15, 2),
        'b': (1, 7, 1),
    }


def test_update_retracts_old_contribution(db_path):
    run(db_path, [dict(id=1, version=1, group='a', amount=10, customer='c1')])
    run(db_path, [dict(id=1, version=2, group='a', amount=4, customer='c2')])
    assert summary(db_path) == {'a': (1, 4, 1)}


def test_older_version_is_ignored(db_path):
    run(db_path, [dict(id=1, version=3, group='a', amount=10, customer='c1')])
    run(db_path, [dict(id=1, version=2, group='b', amount=999, customer='zzz')])
    assert summary(db_path) == {'a': (1, 10, 1)}
    assert detail_versions(db_path) == {
        _canonical_values([1]): (_canonical_key(3), 0),
    }


def test_same_version_is_ignored(db_path):
    rows = [dict(id=1, version=3, group='a', amount=10, customer='c1')]
    run(db_path, rows)
    run(db_path, rows)
    assert summary(db_path) == {'a': (1, 10, 1)}


def test_group_change_moves_contribution(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=10, customer='c1'),
        dict(id=2, version=1, group='a', amount=5, customer='c2'),
    ])
    run(db_path, [dict(id=1, version=2, group='b', amount=3, customer='c1')])
    assert summary(db_path) == {
        'a': (1, 5, 1),
        'b': (1, 3, 1),
    }


def test_empty_group_is_removed(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=10, customer='c1'),
        dict(id=2, version=1, group='b', amount=5, customer='c2'),
    ])
    run(db_path, [dict(id=2, version=2, group='b', amount=5, customer='c2', deleted=True)])
    assert summary(db_path) == {'a': (1, 10, 1)}

    group_b = _canonical_values(['b'])
    connection = sqlite3.connect(db_path)
    try:
        leftovers = connection.execute(
            'SELECT COUNT(*) FROM __dataflows_rollup_distinct_daily '
            'WHERE "__group_key"=?', (group_b,)
        ).fetchone()[0]
        state_rows = connection.execute(
            'SELECT COUNT(*) FROM __dataflows_rollup_state_daily '
            'WHERE "__group_key"=?', (group_b,)
        ).fetchone()[0]
    finally:
        connection.close()
    assert leftovers == 0
    assert state_rows == 0


def test_null_is_not_counted_as_distinct(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=1, customer=None),
        dict(id=2, version=1, group='a', amount=1, customer=None),
        dict(id=3, version=1, group='a', amount=1, customer='c1'),
    ])
    assert summary(db_path) == {'a': (3, 3, 1)}


def test_delete_of_unknown_record_is_a_noop(db_path):
    run(db_path, [dict(id=1, version=1, group='a', amount=1, customer='c1')])
    run(db_path, [dict(id=99, version=4, group='a', amount=1, customer='c1', deleted=True)])
    assert summary(db_path) == {'a': (1, 1, 1)}


def test_resurrect_deleted_record(db_path):
    run(db_path, [dict(id=1, version=1, group='a', amount=2, customer='c1')])
    run(db_path, [dict(id=1, version=2, group='a', amount=2, customer='c1', deleted=True)])
    assert summary(db_path) == {}
    run(db_path, [dict(id=1, version=3, group='a', amount=2, customer='c1')])
    assert summary(db_path) == {'a': (1, 2, 1)}


def test_repeated_delivery_is_idempotent(db_path):
    rows = [
        dict(id=1, version=1, group='a', amount=10, customer='c1'),
        dict(id=2, version=1, group='a', amount=5, customer='c2'),
    ]
    run(db_path, rows)
    before = summary(db_path)
    run(db_path, rows)
    run(db_path, rows)
    assert summary(db_path) == before


def test_reopening_equals_full_recomputation(db_path):
    for batch in (
        [dict(id=1, version=1, group='a', amount=10, customer='c1')],
        [dict(id=2, version=1, group='a', amount=5, customer='c2')],
        [dict(id=1, version=2, group='b', amount=30, customer='c3')],
    ):
        run(db_path, batch)
    expected = summary(db_path)

    # Merely re-opening the database must change nothing.
    processor = make_processor(db_path)
    processor.close()
    assert summary(db_path) == expected


def test_rebuild_matches_full_computation(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=10, customer='c1'),
        dict(id=2, version=1, group='b', amount=5, customer='c1'),
    ])
    run(db_path, [dict(id=1, version=2, group='b', amount=10, customer='c2')])
    expected = summary(db_path)

    processor = make_processor(db_path)
    processor.rebuild()
    processor.close()
    assert summary(db_path) == expected


def test_lost_summary_is_recovered_from_details(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=10, customer='c1'),
        dict(id=2, version=1, group='b', amount=5, customer='c2'),
    ])
    expected = summary(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute('DELETE FROM daily')
        connection.execute('DELETE FROM __dataflows_rollup_state_daily')
        connection.execute('DELETE FROM __dataflows_rollup_distinct_daily')
        connection.commit()
    finally:
        connection.close()

    processor = make_processor(db_path)
    processor.close()
    assert summary(db_path) == expected


def full_computation(current):
    expected = {}
    for record in current.values():
        if record['deleted']:
            continue
        bucket = expected.setdefault(record['group'], [0, 0, set()])
        bucket[0] += 1
        bucket[1] += record['amount']
        if record['customer'] is not None:
            bucket[2].add(record['customer'])
    return dict(
        (group, (count, amount, len(customers)))
        for group, (count, amount, customers) in expected.items()
    )


def test_random_batches_match_full_computation(db_path):
    rng = random.Random(1234)
    groups = ['a', 'b', 'c']
    customers = ['c1', 'c2', 'c3', None]
    current = {}

    for _ in range(30):
        rows = []
        for _ in range(rng.randint(1, 6)):
            record_id = rng.randint(1, 12)
            previous = current.get(record_id)
            version = (previous['version'] if previous else 0) + rng.randint(1, 3)
            deleted = rng.random() < 0.25
            record = dict(
                id=record_id,
                version=version,
                group=rng.choice(groups),
                amount=rng.randint(0, 50),
                customer=rng.choice(customers),
                deleted=deleted,
            )
            current[record_id] = record
            rows.append(dict(record))
        run(db_path, rows)
        assert summary(db_path) == full_computation(current)


def test_only_affected_groups_are_written(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=1, customer='c1'),
        dict(id=2, version=1, group='b', amount=1, customer='c1'),
    ])
    connection = sqlite3.connect(db_path)
    connection.execute('CREATE TABLE audit (group_name TEXT)')
    connection.execute(
        'CREATE TRIGGER audit_update AFTER UPDATE ON daily '
        'BEGIN INSERT INTO audit VALUES (NEW."group"); END'
    )
    connection.commit()
    connection.close()

    # Only group 'a' changes in this batch.
    run(db_path, [dict(id=1, version=2, group='a', amount=9, customer='c1')])
    # A new version with identical values does not touch the summary at all.
    run(db_path, [dict(id=2, version=2, group='b', amount=1, customer='c1')])

    connection = sqlite3.connect(db_path)
    try:
        touched = [row[0] for row in connection.execute('SELECT * FROM audit')]
    finally:
        connection.close()
    assert touched == ['a']


def test_failed_batch_is_atomic(db_path):
    run(db_path, [dict(id=1, version=1, group='a', amount=1, customer='c1')])
    before = summary(db_path)

    with pytest.raises(Exception):
        run(db_path, [
            dict(id=2, version=1, group='a', amount=5, customer='c2'),
            dict(id=3, version=1, group='a', amount='not-a-number', customer='c3'),
        ])

    assert summary(db_path) == before
    assert detail_versions(db_path) == {
        _canonical_values([1]): (_canonical_key(1), 0),
    }


def test_op_field_marks_deletion(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=3, customer='c1'),
        dict(id=2, version=1, group='a', amount=4, customer='c2'),
    ])
    run(db_path, [dict(id=1, version=2, group='a', amount=3, customer='c1', op='delete')])
    assert summary(db_path) == {'a': (1, 4, 1)}


def test_composite_key(db_path):
    run(db_path, [
        dict(id=1, region='n', version=1, group='a', amount=1, customer='c1'),
        dict(id=1, region='s', version=1, group='a', amount=2, customer='c2'),
    ], key=['id', 'region'])
    assert summary(db_path) == {'a': (2, 3, 2)}


def test_adding_a_distinct_field_rebuilds_from_details(db_path):
    run(db_path, [
        dict(id=1, version=1, group='a', amount=1, customer='c1'),
        dict(id=2, version=1, group='a', amount=1, customer='c1'),
    ])
    assert summary(db_path) == {'a': (2, 2, 1)}

    processor = make_processor(
        db_path,
        distinct={'customer': 'distinct_customer', 'ticket': 'distinct_ticket'},
    )
    Flow([dict(id=3, version=1, group='a', amount=1, customer='c1', ticket='t1')],
         processor).process()
    assert processor.summary() == [
        dict(group='a', row_count=3, amount_sum=3,
             distinct_customer=1, distinct_ticket=1),
    ]
    processor.close()


def test_decimal_amounts_are_exact(db_path):
    for batch in (
        [dict(id=1, version=1, group='a', amount=0.1, customer='c1')],
        [dict(id=2, version=1, group='a', amount=0.2, customer='c1')],
    ):
        run(db_path, batch)
    assert summary(db_path) == {'a': (2, 0.3, 1)}
