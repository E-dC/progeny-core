import pytest
import sqlite3
import datetime

TABLE_COLUMNS = ['pid', 'expiry_datetime', 'session_name', 'username', 'port']

soon_to_expire = int(datetime.datetime.now().timestamp())

no_expire = int(
    (datetime.datetime.now() + datetime.timedelta(days=10)).timestamp()
)

mock_processes = [
    (1234, soon_to_expire, 'abc-123', 'abc', 8070),
    (1235, no_expire,      'xyz-123', 'xyz', 8071)
]
@pytest.fixture(scope='module')
def mock_db():
    con = sqlite3.connect(
        "file:test_registry?mode=memory&cache=shared",
        uri=True)

    with con:
        con.executescript(
            '''
            CREATE TABLE registry(pid, expiry_datetime, session_name, username, port);
            '''
        )
        con.executemany(
            '''
            INSERT INTO registry(pid, expiry_datetime, session_name, username, port)
            VALUES (?, ?, ?, ?, ?)
            ''', mock_processes
        )

    return con

@pytest.mark.registry
@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({'pid': 1234, 'keys': 'all'},
         {'pid': 1234, 'expiry_datetime': soon_to_expire,
          'session_name': 'abc-123', 'username': 'abc', 'port': 8070}
        )
    ])
def test_find_running_instance(mock_db, kwargs, expected):
    r = mock_db.execute('SELECT * FROM registry WHERE pid=1234').fetchone()
    res = dict(zip(
        ['pid', 'expiry_datetime', 'session_name', 'username', 'port'],
        r)
    )
    assert res == expected

@pytest.mark.registry
@pytest.mark.parametrize(
    "kwargs",
    [
        {'pid': 1111, 'expiry_datetime': no_expire,
         'session_name': 'uvw-123', 'username': 'uvw', 'port': 8075}
    ])
def test_insert_process_in_registry(mock_db, kwargs):

    v = tuple([kwargs[k] for k in TABLE_COLUMNS])
    stmt = ', '.join(
        ['?' for i in range(len(v))]
    )

    with mock_db:
        mock_db.execute(
            f'INSERT INTO registry VALUES ({stmt});', v)

    res = mock_db.execute(
        'SELECT * FROM registry WHERE pid=?', (kwargs['pid'],)).fetchone()
    assert res


@pytest.mark.registry
@pytest.mark.parametrize(
    "kwargs",
    [
        {'pid': 1234}
    ])
def test_remove_process_from_registry(mock_db, kwargs):

    conditions = [
            (k, v) for (k, v) in kwargs.items()
            if k in set(TABLE_COLUMNS) - {'expiry_datetime'}
    ]
    stmt = ' AND '.join(
        [f'{k} = ?' for k, v in conditions]
    )
    stmt_values = tuple([v for (k, v) in conditions])

    with mock_db:
        mock_db.execute(
            f'DELETE FROM registry WHERE {stmt}', stmt_values)

    res = mock_db.execute(
        f'SELECT * FROM registry WHERE {stmt}', stmt_values).fetchall()
    assert not res
