dynamo_db_dict
==============

Simple Python interface to [Amazon DynamoDB](http://aws.amazon.com/dynamodb/),  
adding some dict-like sugar to [`boto.dynamodb.layer2`](http://boto.cloudhackers.com/en/latest/ref/dynamodb.html#module-boto.dynamodb.layer2).

Usage:

    pip install dynamo_db_dict
    from dynamo_db_dict import dynamo_db

    db = dynamo_db(aws_access_key_id='YOUR KEY HERE', aws_secret_access_key='YOUR SECRET KEY HERE') # or via: os.environ, ~/.boto, /etc/boto.cfg
    # Set table_name_prefix='YOUR_PROJECT_NAME_' if you use the same DynamoDB account for several projects.

    # Either create table "user" with hash_key "email" via AWS concole, or via inherited db.create_table(...).
    db.user['john@example.com'] = dict(first_name='John', last_name='Johnson') # Put. No need to repeat "email" in dict(...).
    john = db.user['john@example.com'] # Get.
    assert john == dict(email='john@example.com', first_name='John', last_name='Johnson') # Complete item, with "email".
    assert john['first_name'] == 'John' # Key access.
    assert john.first_name == 'John' # Attr access.
    del db.user['john@example.com'] # Delete.

dynamo_db_dict version 0.2.4  
Copyright (C) 2012 by Denis Ryzhkov <denis@ryzhkov.org>  
MIT License, see http://opensource.org/licenses/MIT
