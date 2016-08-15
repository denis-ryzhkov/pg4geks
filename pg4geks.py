
'''
pg4geks - PostgreSQL for Gevent kept Simple

Provides:
* db(sql, *values).row/rows/affected
* id = db_insert(table_name, _return='id', name=value,..)
* affected = db_update(table_name, where=dict(name=value, item_in=tuple_of_values), name=value,..)
* result = db_transaction(code)
* raise db_rollback
* db("""ALTER TYPE "my_type" ADD VALUE 'my_value'""", autocommit=True)  # Avoid "cannot run inside a transaction block".
* db('SELECT * FROM "table" WHERE "name" LIKE %s', escape_like(fragment))
* connection pool blocks only for the first connection - for quick deploy
* auto reconnect and retry
* optional log of each query

Usage:

    sudo apt-get install --yes gcc libevent-dev libpq-dev python-dev
    sudo pip install pg4geks

    from pg4geks import db, db_config, db_insert, db_update, db_transaction
    db_config(name='test', user='user', password='password')
    # Defaults: host='127.0.0.1', port=5432, pool_size=10, pool_block=1, patch_psycopg2_with_gevent=True, log=None

    row = db('SELECT "column" FROM "table" WHERE "id" = %s', id).row
    assert row is None or row.column == row['column']

    return db('SELECT * FROM "table" WHERE "related_id" IN %s AND "parent_id" = %s', tuple(related_ids), parent_id).rows
    # Please note that "tuple()" should be used with "IN %s", to keep "list []" for PostgreSQL Array operations.
    # http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types

    return [
        processed(row)
        for row in db('SELECT * FROM "table" LIMIT 10')
    ]  # Please note that no ").rows" is required for iteration.

    try:
        def code():
            db('INSERT INTO "table1" ("quantity") VALUES (%s)', -100)
            db('INSERT INTO "table2" ("quantity") VALUES (%s)', +1/0)

            if error:
                raise db_rollback

            return result
        result = db_transaction(code)

    except db_rollback:
        pass  # Or not.

    id = db_insert('table',
        related_id=related_id,
        parent_id=parent_id,
        _return='id',
    )

    assert db_update('table',
        related_id=None,
        where=dict(id=id),  # Or: id=tuple(ids_to_update)
    ) == 1

    # See tests for more usage examples.

pg4geks version 0.2.1
Copyright (C) 2013-2016 by Denis Ryzhkov <denisr@denisr.com>
MIT License, see http://opensource.org/licenses/MIT
'''

### prepare for test(): become cooperative

if __name__ == '__main__':
    import gevent.monkey
    gevent.monkey.patch_all()

### import

from adict import adict
import gevent
from gevent.local import local
from gevent.queue import Queue, Empty
from gevent.socket import wait_read, wait_write
from psycopg2 import connect, Error, extensions, OperationalError, ProgrammingError
from psycopg2.extras import RealDictCursor
import sys

### init

_db_config = {}
_db_pool = Queue()
_local = local()
_log = None

class db_rollback(Exception):
    pass

def _gevent_wait_callback(conn, timeout=None):
    # From https://github.com/gevent/gevent/blob/master/examples/psycopg2_pool.py#L19
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise OperationalError('Bad result from poll: %r' % state)

def _connect(N):
    for _ in xrange(N):
        _db_pool.put(connect(**_db_config))

### db_config

def db_config(name, user, password, host='127.0.0.1', port=5432, pool_size=10, pool_block=1, patch_psycopg2_with_gevent=True, log=None, **kwargs):

    ### _db_config

    _db_config.clear()
    _db_config.update(database=name, user=user, password=password, host=host, port=port, **kwargs)

    ### pool

    global _db_pool
    connections_to_create = pool_size - _db_pool.qsize()

    if connections_to_create > 0:  # Create connections.
        if pool_block > connections_to_create:
            pool_block = connections_to_create
        _connect(pool_block)
        gevent.spawn(_connect, connections_to_create - pool_block)

    else:  # Delete connections.
        for _ in xrange(-connections_to_create):
            try:
                db_conn = _db_pool.get(block=False)
            except Empty:
                break
            try:
                db_conn.close()
            except Exception:
                pass

    ### patch_psycopg2_with_gevent

    if patch_psycopg2_with_gevent:
        extensions.set_wait_callback(_gevent_wait_callback)

    ### log

    global _log
    _log = log

### db_transaction

def db_transaction(code, initial_seconds=0.1, max_seconds=10.0, autocommit=False):

    ### if aready inside a transaction

    if hasattr(_local, 'db_conn'):
        return code()

    ### create transaction

    db_conn = _local.db_conn = _db_pool.get(block=True)
    try:  # Always return to the pool in "finally".
        db_conn.autocommit = autocommit

        ### retry

        while 1:
            try:
                result = code()
                db_conn.commit()
                return result

            ### error

            except Exception:
                e_type, e_value, e_traceback = sys.exc_info()
                e_repr = repr(e_value)

                ### rollback

                try:
                    db_conn.rollback()
                except Exception:
                    pass

                ### reconnect

                if (
                    'connection has been closed unexpectedly' in e_repr or
                    'connection already closed' in e_repr
                ):
                    seconds_before_reconnect = initial_seconds
                    while 1:

                        try:
                            db_conn.close()
                        except Exception:
                            pass

                        try:
                            db_conn = _local.db_conn = connect(**_db_config)
                        except Exception:
                            seconds_before_reconnect = min(max_seconds, seconds_before_reconnect * 2)
                        else:
                            break

                        gevent.sleep(seconds_before_reconnect)
                    continue

                ### raise other errors

                raise e_type, e_value, e_traceback

    ### return connection to the pool

    finally:
        _db_pool.put(db_conn)
        del _local.db_conn

### db

class db(object):

    ### execute and fetch

    def __init__(self, sql, *values, **params):
        def code():
            with _local.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:

                if _log:
                    _log((sql, values))

                cursor.execute(sql, values)
                # Use "INSERT ... RETURNING" instead of "cursor.lastrowid":
                # http://pythonhosted.org/psycopg2/cursor.html#cursor.lastrowid

                affected = cursor.rowcount
                try:
                    rows = [adict(row) for row in cursor.fetchall()] if affected else []
                except ProgrammingError as e:
                    if 'no results to fetch' in repr(e):
                        rows = []
                    else:
                        raise

                return rows, affected

        self.rows, self.affected = db_transaction(code, **params)
        # "db_transaction(code)" is just "code()" if already inside a transaction.

        self.row = self.rows[0] if self.rows else None

    ### iterate

    def __iter__(self):
        return iter(self.rows)

### db_insert

def db_insert(table_name, _return=None, **names_values):
    row = db('INSERT INTO "{table_name}" {names_values}{_return}'.format(
        table_name=table_name,
        names_values=(
            '({names}) VALUES ({placeholders})'.format(
                names=', '.join('"{name}"'.format(name=name) for name in names_values.keys()),
                placeholders=', '.join(['%s'] * len(names_values)),
            ) if names_values else 'DEFAULT VALUES'
        ),
        _return=(' RETURNING "{_return}"'.format(_return=_return) if _return else ''),
    ), *names_values.values()).row
    return row[_return] if _return else None

### db_update

def db_update(table_name, where, **names_values):
    assert isinstance(where, dict), type(where)
    return db('UPDATE "{table_name}" SET {names_values_sql} WHERE {where_sql}'.format(
        table_name=table_name,
        names_values_sql=', '.join('"{name}" = %s'.format(name=name) for name in names_values.keys()),
        where_sql=' AND '.join('"{name}" {op} %s'.format(name=name, op='IN' if isinstance(value, tuple) else '=') for name, value in where.items()),
    ), *(names_values.values() + where.values())).affected

### escape_like

def escape_like(fragment, prefix='%', postfix='%'):
    return u'{prefix}{fragment}{postfix}'.format(
        prefix=prefix,
        fragment=fragment
            .replace('\\', '\\\\')
            .replace('%', '\\%')
            .replace('_', '\\_'),
        postfix=postfix,
    )

### test

def test():

    pool_size = 2
    db_config(name='test', user='user', password='password', pool_size=pool_size, pool_block=pool_size)  # Use your database credentials!

    ### async, reconnect

    '''
    [0.0 seconds] Testing async pool (connections: 2), reconnect and retry:
    [0.0 seconds] Greenlet 139815404809104 tries to "SELECT pg_sleep(5)".
    [0.0 seconds] Greenlet 139815404810064 tries to "SELECT pg_sleep(5)".
    [0.0 seconds] Greenlet 139815404808464 tries to "SELECT pg_sleep(5)".
    [0.0 seconds] Greenlet 139815401537616 tries to "SELECT pg_sleep(5)".
    [3.16 seconds] Restarted PostgreSQL in 3.16118597984 seconds.
    # 4.18 seconds - Reconnected to PostgreSQL, trying to sleep again.
    [9.18 seconds] Greenlet 139815404809104 stopped sleeping.
    [9.18 seconds] Greenlet 139815404810064 stopped sleeping.
    # Pool of 2 connections is ready for pending queries.
    [14.18 seconds] Greenlet 139815404808464 stopped sleeping.
    [14.18 seconds] Greenlet 139815401537616 stopped sleeping.
    [14.18 seconds] All greenlets are done.
    '''

    import gevent, time
    from subprocess import Popen
    from thread import get_ident

    sleep_seconds = 5
    start = time.time()

    def log(text):
        print('[{seconds} seconds] {text}'.format(seconds=round(time.time() - start, 2), text=text))

    log('Testing async pool (connections: {pool_size}), reconnect and retry:'.format(pool_size=pool_size))

    def restart_postgresql():
        start = time.time()
        Popen('sudo service postgresql restart', shell=True).communicate()
        log('Restarted PostgreSQL in {seconds} seconds.'.format(seconds=(time.time() - start)))

    def sleep_at_postgresql():
        greenlet_id = get_ident()
        log('Greenlet {greenlet_id} tries to "SELECT pg_sleep({seconds})".'.format(greenlet_id=greenlet_id, seconds=sleep_seconds))
        db('SELECT pg_sleep(%s)', sleep_seconds)
        log('Greenlet {greenlet_id} stopped sleeping.'.format(greenlet_id=greenlet_id))

    gevent.joinall([
        gevent.spawn(sleep_at_postgresql)
        for _ in xrange(pool_size * 2)
    ] + [
        gevent.spawn(restart_postgresql),
    ])
    log('All greenlets are done.')

    ### other

    print('\nTesting other features...')

    ### db

    db('DROP TABLE IF EXISTS "test_item"')

    db('''
        CREATE TABLE "test_item" (
            "id" serial NOT NULL PRIMARY KEY,
            "parent_id" integer
        )
    ''')

    ### db_insert

    item1_id = db_insert('test_item',
        _return='id',
    )

    item2_id = db_insert('test_item',
        parent_id=item1_id,
        _return='id',
    )

    ### db_insert result, db_transaction, db_rollback, db rows iteration

    raised = False
    try:
        def code():
            assert db('SELECT "id" FROM "test_item" WHERE id = %s', item2_id).row.id == item2_id
            db('DELETE FROM "test_item" WHERE "id" = %s', item2_id)
            assert db('SELECT "id" FROM "test_item" WHERE id = %s', item2_id).row is None
            raise db_rollback
        db_transaction(code)

    except db_rollback:
        raised = True
    assert raised

    items = db('SELECT "id" FROM "test_item" WHERE "id" IN %s', (item1_id, item2_id))
    assert set(item.id for item in items) == set([item1_id, item2_id])

    ### db_transaction result, db_update

    def code():
        return db('SELECT "parent_id" FROM "test_item" WHERE "id" = %s', item2_id).row.parent_id
    assert db_transaction(code) == item1_id

    assert db_update('test_item',
        parent_id=None,
        where=dict(id=item2_id),
    ) == 1
    assert db('SELECT "parent_id" FROM "test_item" WHERE "id" = %s', item2_id).row.parent_id is None

    ### db_update "IN %s"

    db_update('test_item',
        parent_id=0,
        where=dict(id=(item1_id, item2_id)),
    )
    items = db('SELECT "parent_id" FROM "test_item" WHERE "id" IN %s', (item1_id, item2_id))
    assert [item.parent_id for item in items] == [0, 0]

    ### "cannot run inside a transaction block"

    db('DROP TYPE IF EXISTS "my_type"')
    db("""CREATE TYPE "my_type" AS ENUM ('aaa', 'bbb')""")
    raised = False
    try:
        db("""ALTER TYPE "my_type" ADD VALUE 'my_value'""")
    except Exception as e:
        raised = True
        assert 'cannot run inside a transaction block' in repr(e)
    assert raised
    db("""ALTER TYPE "my_type" ADD VALUE 'my_value'""", autocommit=True)

    ### escape_like

    assert escape_like('\? item_percent = 42%') == r'%\\? item\_percent = 42\%%'

    ### log

    state = adict(logged=False)
    def log(query):
        sql, values = query
        assert sql == 'UPDATE "test_item" SET "parent_id" = %s WHERE "id" = %s'
        assert values == (42, 1)
        state.logged = True

    db_config(name=_db_config['database'], user=_db_config['user'], password=_db_config['password'], pool_size=_db_pool.qsize(), log=log)
    db_update('test_item', parent_id=42, where=dict(id=item1_id))
    assert state.logged

    print('OK')

if __name__ == '__main__':
    test()
