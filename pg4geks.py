
'''
pg4geks - PostgreSQL for Gevent kept Simple.

Provides:
* db(sql, *values).row|s
* id = db_insert(**kw)
* db_update(**kw)
* with db_transaction
* raise db_rollback
* patch, log, pool, reconnect, retry.

Usage:

    sudo apt-get install --yes gcc libevent-dev libpq-dev python-dev
    sudo pip install pg4geks

    from pg4geks import db, db_config, db_transaction
    db_config(name='test', user='user', password='password')
    # Defaults: host='127.0.0.1', port=5432, pool_size=10, patch_psycopg2_with_gevent=True, log=None

    row = db('SELECT column FROM table WHERE id = %s', id).row
    assert row.column == row['column'] or row is None

    return db('SELECT * FROM table WHERE related_id IN %s AND parent_id = %s', tuple(related_ids), parent_id).rows
    # Please note that tuple() should be used with IN %s, to keep list [] for PostgreSQL Array operations.
    # http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types

    return [
        processed(row)
        for row in db('SELECT * FROM table LIMIT 10')
    ] # Please note that no ').rows' is required on iteration.

    try:

        with db_transaction():
            db('INSERT INTO table1 (quantity) VALUES (%s)', -100)
            db('INSERT INTO table2 (quantity) VALUES (%s)', +1/0)

            if error:
                raise db_rollback
    except db_rollback:
        pass # Or not.

    id = db_insert('table',
        related_id=related_id,
        parent_id=parent_id,
        _return='id',
    )

    db_update('table',
        related_id=None,
        where=dict(id=id),
    )

pg4geks version 0.1.1
Copyright (C) 2013-2014 by Denis Ryzhkov <denisr@denisr.com>
MIT License, see http://opensource.org/licenses/MIT
'''

#### prepare for test(): become cooperative

if __name__ == '__main__':
    import gevent.monkey
    gevent.monkey.patch_all()

#### import

from adict import adict
from contextlib import contextmanager
import gevent, gevent.local, gevent.queue
from gevent.socket import wait_read, wait_write
from psycopg2 import connect, Error, extensions, OperationalError, ProgrammingError
from psycopg2.extras import RealDictCursor
from Queue import Empty

#### db_config, _db_pool, patch_psycopg2_with_gevent, log

_db_config = {}
_db_pool = gevent.queue.Queue()
_log = None

def _gevent_wait_callback(conn, timeout=None):
    # From https://github.com/SiteSupport/gevent/blob/master/examples/psycopg2_pool.py
    while True:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise OperationalError('Bad result from poll: %r' % state)

def db_config(name, user, password, host='127.0.0.1', port=5432, pool_size=10, patch_psycopg2_with_gevent=True, log=None, **kwargs):

    #### _db_config

    _db_config.clear()
    _db_config.update(database=name, user=user, password=password, host=host, port=port, **kwargs)

    #### pool

    global _db_pool
    connections_to_create = pool_size - _db_pool.qsize()

    if connections_to_create > 0: # Create connections.
        for _ in xrange(connections_to_create):
            _db_pool.put(connect(**_db_config))

    else: # Delete connections.
        for _ in xrange(-connections_to_create):
            try:
                db_connection = _db_pool.get(block=False)
            except Empty:
                break
            try:
                db_connection.close()
            except Exception:
                pass

    #### patch_psycopg2_with_gevent

    if patch_psycopg2_with_gevent:
        extensions.set_wait_callback(_gevent_wait_callback)

    #### log

    global _log
    _log = log

#### db_transaction

_db_transaction = gevent.local.local()

@contextmanager
def db_transaction():

    # Do not use auto wraping of each api action into db_transaction(), for not to limit concurrency of [db]stateless actions with pool_size.

    if hasattr(_db_transaction, 'db_connection'):
        yield # Already inside a transaction.

    else: # Create transaction.
        _db_transaction.db_connection = _db_pool.get(block=True)
        try:
            yield

        except Exception:
            if not _db_transaction.db_connection.closed:
                _db_transaction.db_connection.rollback()
            raise

        else:
            _db_transaction.db_connection.commit()

        finally:
            # Broken connections are reconnected inside db(), so just return it to pool here.
            _db_pool.put(_db_transaction.db_connection)
            del _db_transaction.db_connection

class db_rollback(Exception):
    pass

#### db

_reconnect_sleep_seconds = 1

class db(object):

    #### execute and fetch

    def __init__(self, sql, *values):
        with db_transaction(): # Noop if already inside a transaction. Anyway now _db_transaction.db_connection is ready to be used with correct transaction borders.

            done = False
            while not done:
                try:
                    cursor = _db_transaction.db_connection.cursor(cursor_factory=RealDictCursor)
                    try:

                        if _log:
                            _log((sql, values))

                        cursor.execute(sql, values)
                        # Use INSERT ... RETURNING instead of cursor.lastrowid: http://pythonhosted.org/psycopg2/cursor.html#cursor.lastrowid

                        try:
                            self.rows = [adict(row) for row in cursor.fetchall()] if cursor.rowcount else []
                        except ProgrammingError as e:
                            if 'no results to fetch' in repr(e):
                                self.rows = []
                            else:
                                raise

                        self.row = self.rows[0] if self.rows else None
                        done = True

                    finally:
                        cursor.close()

                #### reconnect and retry

                except Error as e:
                    repr_e = repr(e)
                    if not (
                        'connection has been closed unexpectedly' in repr_e or
                        'connection already closed' in repr_e
                    ):
                        raise

                    reconnected = False
                    while not reconnected:
                        try:
                            _db_transaction.db_connection = connect(**_db_config)

                        except Exception as e:
                            if 'connection failed' not in repr(e):
                                raise
                            gevent.sleep(_reconnect_sleep_seconds)

                        else:
                            reconnected = True

    #### iterate

    def __iter__(self):
        return iter(self.rows)

#### db_insert

def db_insert(table_name, _return=None, **names_values):
    row = db('INSERT INTO {table_name} {names_values} {_return}'.format(
        table_name=table_name,
        names_values=(
            '({names}) VALUES ({placeholders})'.format(
                names=', '.join(names_values.keys()),
                placeholders=', '.join(['%s'] * len(names_values)),
            ) if names_values else 'DEFAULT VALUES'
        ),
        _return=('RETURNING {_return}'.format(_return=_return) if _return else ''),
    ), *names_values.values()).row
    return row[_return] if _return else None

#### db_update

def db_update(table_name, where, **names_values):
    assert isinstance(where, dict), type(where)
    where_sql = ' AND '.join('{name} = %s'.format(name=name) for name in where.keys())
    db('UPDATE {table_name} SET {names_values_sql} WHERE {where_sql}'.format(
        table_name=table_name,
        names_values_sql=', '.join('{name} = %s'.format(name=name) for name in names_values.keys()),
        where_sql=where_sql,
    ), *(names_values.values() + where.values()))

#### test

def test():

    pool_size = 2
    db_config(name='test', user='user', password='password', pool_size=pool_size) # Use your database credentials!

    ####

    '''
    [0 seconds] Testing async pool, reconnect and retry.
    [0 seconds] Pool of 2 connections.
    [0 seconds] Started sleeping at postgresql for 5 seconds.
    [0 seconds] Started sleeping at postgresql for 5 seconds.
    [0 seconds] Started sleeping at postgresql for 5 seconds.
    [0 seconds] Started sleeping at postgresql for 5 seconds.
    [ ok ] Restarting PostgreSQL 9.1 database server: main.
    [5 seconds] Restarted postgresql in 5.10631895065 seconds.
    [10 seconds] Done sleeping.
    [10 seconds] Done sleeping.
    [15 seconds] Done sleeping.
    [15 seconds] Done sleeping.
    [15 seconds] Done all.
    '''

    import gevent, time
    from subprocess import Popen

    sleep_seconds = 5
    start = time.time()

    def log(text):
        print('[{seconds} seconds] {text}'.format(seconds=int(time.time() - start), text=text))

    log('Testing async pool, reconnect and retry.')
    log('Pool of {pool_size} connections.'.format(pool_size=pool_size))

    def restart_postgresql():
        start = time.time()
        Popen('sudo service postgresql restart', shell=True).communicate()
        log('Restarted postgresql in {seconds} seconds.'.format(seconds=(time.time() - start)))

    def sleep_at_postgresql():
        log('Started sleeping at postgresql for {seconds} seconds.'.format(seconds=sleep_seconds))
        db('SELECT pg_sleep(%s)', sleep_seconds)
        log('Done sleeping.')

    gevent.joinall([
        gevent.spawn(sleep_at_postgresql)
        for _ in xrange(pool_size * 2)
    ] + [
        gevent.spawn(restart_postgresql),
    ])
    log('Done all.')

    ####

    print('\nTesting db_insert(), transaction rollback and commit, IN %s, iteration, db_update().')

    db('''
        CREATE TABLE IF NOT EXISTS test_item (
            id serial NOT NULL PRIMARY KEY,
            parent_id integer
        )
    ''')

    db('DELETE FROM test_item')

    item1_id = db_insert('test_item',
        _return='id',
    )

    item2_id = db_insert('test_item',
        parent_id=item1_id,
        _return='id',
    )

    try:
        with db_transaction():
            assert db('SELECT id FROM test_item WHERE id = %s', item2_id).row.id == item2_id
            db('DELETE FROM test_item WHERE id = %s', item2_id)
            assert db('SELECT id FROM test_item WHERE id = %s', item2_id).row is None
            raise db_rollback
    except db_rollback:
        pass

    items = db('SELECT id FROM test_item WHERE id IN %s', (item1_id, item2_id))
    assert set(item.id for item in items) == set([item1_id, item2_id])

    assert db('SELECT parent_id FROM test_item WHERE id = %s', item2_id).row.parent_id == item1_id
    db_update('test_item',
        parent_id=None,
        where=dict(id=item2_id),
    )
    assert db('SELECT parent_id FROM test_item WHERE id = %s', item2_id).row.parent_id is None

    print('OK')

if __name__ == '__main__':
    test()
