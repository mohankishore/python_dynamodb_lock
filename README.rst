====================
Python DynamoDB Lock
====================


.. image:: https://img.shields.io/pypi/v/python_dynamodb_lock.svg
        :target: https://pypi.python.org/pypi/python_dynamodb_lock

.. image:: https://img.shields.io/travis/mohankishore/python_dynamodb_lock.svg
        :target: https://travis-ci.org/mohankishore/python_dynamodb_lock

.. image:: https://readthedocs.org/projects/python-dynamodb-lock/badge/?version=latest
        :target: https://python-dynamodb-lock.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based `AmazonDynamoDBLockClient <https://github.com/awslabs/dynamodb-lock-client>`_
library, and supports both coarse-grained and fine-grained locking.

* Free software: Apache Software License 2.0
* Documentation: https://python-dynamodb-lock.readthedocs.io
* Source Code: https://github.com/mohankishore/python_dynamodb_lock


Features
--------

* Acquire named locks - with configurable retry semantics
* Periodic heartbeat/update for the locks to keep them alive
* Auto-release the locks if there is no heartbeat for a configurable lease-duration
* Notify an app-callback function if the lock is stolen, or gets too close to lease expiry
* Store arbitrary application data along with the locks
* Uses monotonically increasing clock to avoid issues due to clock skew and/or DST etc.
* Auto-delete the database entries after a configurable expiry-period


Consistency Notes
-----------------

Note that while the lock itself can offer fairly strong consistency guarantees, it does NOT
participate in any kind of distributed transaction.

For example, you may wish to acquire a lock for some customer-id "xyz", and then make some changes
to the corresponding database entry for this customer-id, and then release the lock - thereby
guaranteeing that only one process changes any given customer-id at a time.

While the happy path looks okay, consider a case where the application changes take a long time,
and some errors/gc-pauses prevent the heartbeat from updating the lock. Then, some other client
can assume the lock to be abandoned, and start processing the same customer in parallel. The original
lock-client will recognize that its lock has been "stolen" and will let the app know through a callback
event, but the app may have already committed its changes to the database. This can only be solved by
having the application changes and the lock-release be part of a single distributed transaction - which,
as indicated earlier, is NOT supported.

That said, in most cases, where the heartbeat is not expected to get delayed beyond the lock's lease
duration, the implementation should work just fine.

Refer to an excellent post by Martin Kleppmann on this subject:
https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html

Credits
-------

* AmazonDynamoDBLockClient: https://github.com/awslabs/dynamodb-lock-client
* Cookiecutter: https://github.com/audreyr/cookiecutter
* Cookiecutter Python: https://github.com/audreyr/cookiecutter-pypackage

