pg4geks
=======

[PostgreSQL][] for [Gevent][] kept [Simple][]
[PostgreSQL]: https://www.postgresql.org/
[Gevent]: http://www.gevent.org/
[Simple]: https://en.wikipedia.org/wiki/KISS_principle

Provides:
* `db(sql, *values).row/rows/affected`
* `id = db_insert(table_name, _return='id', name=value,..)`
* `affected = db_update(table_name, where=dict(name=value, item_in=tuple_of_values), name=value,..)`
* `result = db_transaction(code)`
* `raise db_rollback`
* `db("""ALTER TYPE "my_type" ADD VALUE 'my_value'""", autocommit=True)  # Avoid "cannot run inside a transaction block".`
* `db('SELECT * FROM "table" WHERE "name" LIKE %s', escape_like(fragment))`
* connection pool blocks only for the first connection - for quick deploy
* auto reconnect and retry
* optional log of each query

Usage:

    sudo apt-get install --yes gcc libevent-dev libpq-dev python-dev
    sudo pip install pg4geks

    from pg4geks import db, db_config, db_insert, db_update, db_transaction
    db_config(name='test', user='user', password='password')
    # Defaults: host='127.0.0.1', port=5432, pool_size=10, patch_psycopg2_with_gevent=True, log=None

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

pg4geks version 0.2.3  
Copyright (C) 2013-2017 by Denis Ryzhkov <denisr@denisr.com>  
MIT License, see http://opensource.org/licenses/MIT
