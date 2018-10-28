Usage
=====

To use Python DynamoDB Lock in a project::

    from python_dynamodb_lock.python_dynamodb_lock import *


Basic Usage
-----------

You would typically create (and shutdown) the DynamoDBLockClient at the application startup
and shutdown::

    # get a reference to the DynamoDB resource
    dynamodb_resource = boto3.resource('dynamodb')

    # create the lock-client
    lock_client = DynamoDBLockClient(dynamodb_resource)

    ...

    # close the lock_client
    lock_client.close()


Then, you would wrap the lock acquisition and release around the code-block that needs to be
protected by a mutex::

    # acquire the lock
    lock = lock_client.acquire_lock('my_key')

    # ... app logic that requires the lock ...

    # release the lock after you are done
    lock.release()


Both the lock_client constructor and the acquire_lock method support numerous arguments to help
control/customize the behavior. Please look at the :doc:`API documentation <./python_dynamodb_lock>`
for more details.


Context Management
------------------
The DynamoDBLock class implements the context-management interface and you can auto-release the
lock by doing something like this::

    with lock_client.acquire_lock('my_key'):
        # ... app logic that requires the lock ...


Table Creation
--------------
The DynamoDBLockClient provides a helper class-method to create the table in DynamoDB::

    # get a reference to the DynamoDB client
    ddb_client = boto3.client('dynamodb')

    # create the table
    DynamoDBLockClient.create_dynamodb_table(ddb_client)

The above code snippet will create a table with the default name, partition/sort-key column-names,
read/write througput, but the method supports optional parameters to configure all of these.

That said, you can always create the table offline (e.g. using the AWS console) and use whatever
table and column names you wish. Please do remember to setup the TTL attribute to enable auto-deleting
of old/abandoned locks.


Error-Handling
--------------

There are a lot of things that can go wrong when dealing with distributed systems - the library
tries to strike the right balance between hiding these errors, and allowing the library to handle
specific kinds of errors as needed. Let's go through the different use-cases one at a time.


Lock Acquisition
~~~~~~~~~~~~~~~~

This is a synchronous use-case where the caller is waiting till it receives a lock. In this case,
most of the errors are wrapped inside a DynamoDBError and raised up to the caller. The key error
scenarios are the following:

* **Some other client holds the lock**
    * This is not treated as real error scenario. This client would just wait for a configurable
      retry_period, and then try to acquire the lock again.
* **Race-condition amongst multiple lock-clients waiting to acquire lock**
    * Whenever the "old" lock is released (or expires), there may be multiple "new" clients trying
      to grab the lock - in which case, one of those would succeed, and the rest of them would get
      a DynamoDB's ConditionalUpdateException. This is also not treated as a real error scenario, and
      the client would just wait for the retry_period and then try again.
* **This client goes over the configurable retry_timeout period**
    * After repeated retry attempts, this client might eventually go over the retry_timeout period
      (as provided by the caller) - then, a DynamoDBLockError with code == ACQUIRE_TIMEOUT will be thrown.
* **Any other error/exception**
    * Any other error would be wrapped inside a DynamoDBLockError with code == UNKNOWN_ERROR and raised
      to the caller.


Lock Release
~~~~~~~~~~~~

While this is also a synchronous use-case, in most cases, by the time this method is called, the caller
would have already committed his application-data changes, and would not have real rollback options.
Therefore, this method defaults to the best_effort mode, where it will try to release the lock properly,
but will log and swallow any exceptions encountered in the process. But, for the callers that are interested
in being notified of the errors, they can pass in best_effort=False and have all the errors wrapped inside
a DynamoDBLockError and raised up to them. The specific error scenarios could be one of the below:

* **This client does not own the lock**
    * This can happen if the caller tries to use this client to release a lock owned by some other client.
      The client will raise a DynamoDBLockError with code == LOCK_NOT_OWNED.
* **The lock was stolen by some other client**
    * This should typically not happen unless someone messes with the back-end DynamoDB table directly. The
      client will raise a DynamoDBLockError with code == LOCK_STOLEN.
* **Any other error/exception**
    * Any other error would be wrapped inside a DynamoDBLockError with code == UNKNOWN_ERROR and raised
      to the caller.


Lock Heartbeat
~~~~~~~~~~~~~~

This is an asynchronous use-case, where the caller is not directly available to handle any errors. To handle
any error scenarios encountered while sending a heartbeat for a given lock, the client allows the caller to
pass in an app_callback function at the time of acquiring the lock.

* **The lock was stolen by some other client**
    * This should typically not happen unless someone messes with the back-end DynamoDB table directly. The
      client will call the app_callback with code == LOCK_STOLEN. The callback is expected to terminate the
      related application processing and rollback any changes made under this lock's protection.
* **The lock has entered the danger zone**
    * If the send_heartbeat call for a given lock fails multiple times, the lock could go over the configurable
      safe_period. The client will call the app_callback with code == LOCK_IN_DANGER. The callback is expected
      to complete/terminate the related application processing, and call the lock.release() as soon as possible.

Note: it is worth noting that the client spins up two separate threads - one to send out the heartbeats, and
another one to check the lock-statuses. For whatever reason, if the send_heartbeat calls start hanging or
taking too long, the other thread will allow the client to notify the app about the locks getting into the
danger-zone. The actual app_callbacks are executed on a dedicated ThreadPoolExecutor.


Client Close
~~~~~~~~~~~~

By default, the lock_client.close() will NOT release all the locks - as releasing the locks prematurely while the
application is still making changes assuming that it has the lock can be dangerous. As soon as a lock is released
by this client, some other client may pick it up, and the associated app may start processing the underlying
business entity in parallel.

It is highly recommended that the application manage its shutdown-lifecycle such that all the worker threads
operating under these locks are first terminated (committed or rolled-back), the corresponding locks released
(one at a time - by each worker thread), and then the lock_client.close() method is called. Alternatively, consider
letting the process die without releasing all the locks - they will be auto-released when their lease runs out
after a while.

That said, if the caller does wish to release all locks when closing the lock_client, it can pass in release_locks=True
argument when invoking the close() method. Please note that all the locks are released in the best_effort mode -
i.e. all the errors will be logged and swallowed.


Process Termination
~~~~~~~~~~~~~~~~~~~

A sudden process termination would leave the locks frozen with the values as of their last heartbeat. These locks
will go through one of the following scenarios:

* **Eventual expiry - as per the TTL attribute**
    * Each lock has a TTL attribute (named 'expiry_time' by default) - which stores the timestamp (as epoch) after
      which it is eligible for auto-deletion by DynamoDB. This deletion does not have a fixed SLA - but will likley
      happen over the next 24 hours after the lock expires.
* **Some other client tries to acquire the lock**
    * The client will treat the lock as an active lock - and will wait for a period equal to its lease_duration from
      the point it first sees the lock. This does need the acquire_lock call to be made with a retry_period larger
      than the lease_duration of the lock - otherwise, the acquire_lock call will timeout before the lease expires.


Throughput Provisioning
-----------------------

Whenever using DynamoDB, you have to think about how much read and write throughput you need to provision for your
table. The DynamoDBLockClient makes the following calls to DynamoDB:

* **acquire_lock**
    * ``get_item``: at least once per lock, and more often if there is lock contention and the lock_client needs to
      retry multiple times before acquiring the lock.
    * ``put_item``: typically once per lock - whenever the lock becomes available.
    * ``update_item``: should be fairly rare - only needed when this client needs to take over an abandoned lock.
    * So, the write throughput should be directly proportional to the applications need to acquire locks, but the
      read throughput is a little harder to predict - it can be more sensitive to the lock contention at runtime.
* **release_lock**
    * ``delete_item``: once per lock
    * So, assuming that every lock that is acquired will be released, this is also directly proportional to the
      application's lock acquition TPS.
* **send_heartbeat**
    * ``update_item``: the lock client supports a deterministic model where the caller can pass in a TPS value, and
      the client will honor the same when making the heartbeat calls. Alternatively, the client also supports an
      "adaptive" mode (the default), where it will take all the active locks at the beginning of each heartbeat_period
      and spread their individual heartbeat calls evenly across the whole period.


Differences from Java implementation
------------------------------------

As indicated before, this library derives most of its design from the
`dynamo-db-lock <https://github.com/awslabs/dynamodb-lock-client>`_ (Java) module. This section goes over few details
where this library goes a slightly different way:

* **Added suport for DynadmoDB TTL attribute**
    * Since Feb 2017, DynamoDB supports having the tables designate one of the attributes as a TTL attribute -
      containing an epoch timestamp value. Once the current time goes past that value, that row becomes eligible
      for automated deletion by DynamoDB. These deletes do not incur any additional costs and help keep the table
      clean of old/stale entries.
* **Dropped support for lock retention after release**
    * The java library supports an additional lock-attribute called "deleteOnRelease" - which allows the caller to
      control whether the lock, on its release, should be deleted or just marked as released. This python module
      drops that flexibility, and always deletes the lock on release. The idea is to not try and treat the lock
      table as a general purpose data-store, and treat it as a persistent representation of the "currently active
      locks".
* **Dropped support for BLOB data field**
    * The java library supports a byte[] field called 'data' in addition to supporting arbitrary named fields to
      be stored along with any lock. This python module drops that additional data field - with the understanding
      that any additional data that the app wishes to store, can be passed in as part of the additional_attributes
      map/dict that is already supported.
* **Separate lock classes to represent local vs remote locks**
    * The java library uses the same LockItem class to represent both the locks created/acquired by this client as
      well as the locks loaded from the database (currently held by other clients). This results in confusing
      overloading of fields e.g. the "lookupTime" is overloaded to store the "lastUpdatedTime" for the locks owned
      by this client, and the "lastLookupTime" for the locks owned by other clients.
* **Added support for explicit and adaptive heartbeat TPS**
    * The java library would fire off the heartbeat updates for all the active locks one-after-another - as fast as
      it can, and then wait till the end ot the heartbeat_period, and then do the same thing over. This can result
      in significant write TPS is the application has a lot (say ~100) active locks. This python module allows the
      caller to specific an explicit TPS value, or use an adaptive mode - where the heartbeats are evenly spread
      over the whole heatbeat_period.
* **Different callback model**
    * The java library creates a different thread for each lock that wishes to support "session-monitors". This
      python module uses a single thread (separate from the one used to send heartbeats) to periodically check that
      the locks are being "heartbeat"-ed and if needed, use a ThreadPoolExecutor to invoke the app_callbacks.
* **Uses retry_period/retry_timeout arguments instead of refreshPeriod/additionalTimeToWait**
    * Though the logic is pretty much the same, the names are a little clearer about the intent - the "retry_period"
      controls how long the client waits before retrying a previously failed lock acquisition, and "retry_timeout"
      controls how long the client keeps retrying before giving up and raising an error.
* **Simplified sort-key handling**
    * The java library goes to great lengths to support the caller's ability to use a simple hash-partitioned table
      as well as a hash-and-range partitioned table. This python module drops the support for hash-partitioned
      tables, and instead chooses to use a default sort-key of '-' to simplify the implementation.
* **Lock release best_effort mode**
    * The java library defaults to best_effort == False, whereas this python module defaults to True. i.e. trying
      to release a lock without choosing an explicit "best_effort" setting, could result in Exceptions being
      thrown in Java, but would be silently logged+swallowed in Python.
* **Releasing all locks on client code**
    * The java library will always try to release all locks when closing the lock_client. This python module will
      default to NOT releasing the locks on lock_client closure - but does support an optional argument called
      "release_locks" that will allow the caller to request lock releases. The idea behind this is that it is not
      a safe operation to release the locks without considering the application threads that could continue to
      process under the assumption that they hold a lock on the underlying business entity. Making the caller
      request the lock-release explicitly is meant to encourage them to try and wind up the application processing
      first and release the locks first, before trying to close the lock_client.
* **Dropped/Missing support for AWS RequestMetricCollector**
    * The java library has pervasive support for collecting the AWS request metrics. This python module does not
      (yet) support this capability.

