pg4geks
=======

[PostgreSQL][] for [Gevent][] kept [Simple][].
[PostgreSQL]: http://www.postgresql.org/
[Gevent]: http://www.gevent.org/
[Simple]: http://en.wikipedia.org/wiki/KISS_principle

Provides:
* `db(sql, *values).row|s`
* `id = db_insert(**kw)`
* `db_update(**kw)`
* `with db_transaction`
* `raise db_rollback`
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
