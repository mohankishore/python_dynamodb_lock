# -*- coding: utf-8 -*-

"""
This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based AmazonDynamoDBLockClient library, and supports both coarse-grained
and fine-grained locking.

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
"""

from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import datetime
from decimal import Decimal
import logging
import socket
import time
import threading
from urllib.parse import quote
import uuid

# module level logger
logger = logging.getLogger(__name__)


class DynamoDBLockClient:
    """
    Provides distributed locks using DynamoDB's support for conditional reads/writes.
    """

    # default values for class properties
    _DEFAULT_TABLE_NAME = 'DynamoDBLockTable'
    _DEFAULT_PARTITION_KEY_NAME = 'lock_key'
    _DEFAULT_SORT_KEY_NAME = 'sort_key'
    _DEFAULT_HEARTBEAT_PERIOD = datetime.timedelta(seconds=5)
    _DEFAULT_SAFE_PERIOD = datetime.timedelta(seconds=20)
    _DEFAULT_LEASE_DURATION = datetime.timedelta(seconds=30)
    _DEFAULT_EXPIRY_PERIOD = datetime.timedelta(hours=1)
    _DEFAULT_HEARTBEAT_TPS = 5
    _DEFAULT_APP_CALLBACK_THREADPOOL_SIZE = 5
    # for optional create-table method
    _DEFAULT_READ_CAPACITY = 5
    _DEFAULT_WRITE_CAPACITY = 5

    # to help make the sort-key optional
    _DEFAULT_SORT_KEY_VALUE = '-'

    # DynamoDB "hard-coded" column names
    _COL_OWNER_NAME = 'owner_name'
    _COL_LEASE_DURATION = 'lease_duration'
    _COL_RECORD_VERSION_NUMBER = 'record_version_number'
    _COL_EXPIRY_TIME = 'expiry_time'


    def __init__(self,
                 dynamodb_resource,
                 table_name=_DEFAULT_TABLE_NAME,
                 partition_key_name=_DEFAULT_PARTITION_KEY_NAME,
                 sort_key_name=_DEFAULT_SORT_KEY_NAME,
                 owner_name=None,
                 heartbeat_period=_DEFAULT_HEARTBEAT_PERIOD,
                 safe_period=_DEFAULT_SAFE_PERIOD,
                 lease_duration=_DEFAULT_LEASE_DURATION,
                 expiry_period=_DEFAULT_EXPIRY_PERIOD,
                 heartbeat_tps=_DEFAULT_HEARTBEAT_TPS,
                 app_callback_executor=None
                 ):
        """
        :param boto3.ServiceResource dynamodb_resource: mandatory argument
        :param str table_name: defaults to 'DynamoDBLockTable'
        :param str partition_key_name: defaults to 'lock_key'
        :param str sort_key_name: defaults to 'sort_key'
        :param str owner_name: defaults to hostname + uuid
        :param datetime.timedelta heartbeat_period: How often to update DynamoDB to note that the
                instance is still running. It is recommended to make this at least 4 times smaller
                than the leaseDuration. Defaults to 5 seconds.
        :param datetime.timedelta safe_period: How long is it okay to go without a heartbeat before
                considering a lock to be in "danger". Defaults to 20 seconds.
        :param datetime.timedelta lease_duration: The length of time that the lease for the lock
                will be granted for. i.e. if there is no heartbeat for this period of time, then
                the lock will be considered as expired. Defaults to 30 seconds.
        :param datetime.timedelta expiry_period: The fallback expiry timestamp to allow DynamoDB
                to cleanup old locks after a server crash. This value should be significantly larger
                than the lease_duration to ensure that clock-skew etc. are not an issue. Defaults
                to 1 hour.
        :param int heartbeat_tps: The number of heartbeats to execute per second (per node) - this
                will have direct correlation to DynamoDB provisioned throughput for writes. Defaults to
                5 per second.
        :param ThreadPoolExecutor app_callback_executor: The executor to be used for invoking the
                app_callbacks in case of un-expected errors. Defaults to a ThreadPoolExecutor with a
                maximum of 5 threads.
        """
        self.uuid = uuid.uuid4().hex
        self.dynamodb_resource = dynamodb_resource
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name
        self.owner_name = owner_name or (socket.getfqdn() + self.uuid)
        self.heartbeat_period = heartbeat_period
        self.safe_period = safe_period
        self.lease_duration = lease_duration
        self.expiry_period = expiry_period
        self.heartbeat_tps = heartbeat_tps
        self.app_callback_executor = app_callback_executor or ThreadPoolExecutor(
            max_workers=self._DEFAULT_APP_CALLBACK_THREADPOOL_SIZE,
            thread_name_prefix='DynamoDBLockClient-CB-' + self.uuid + "-"
        )
        # additional properties
        self._locks = {}
        self._shutting_down = False
        self._dynamodb_table = dynamodb_resource.Table(table_name)
        # and, initialization
        self._start_background_thread()
        logger.info('Created: %s', str(self))


    def _start_background_thread(self):
        """
        Creates and starts a daemon thread - that calls DynamoDBLockClient.run() method.
        """
        self._background_thread = threading.Thread(
            name='DynamoDBLockClient-HB-' + self.uuid,
            target=self.run
        )
        self._background_thread.daemon = True
        self._background_thread.start()
        logger.info('Started the background thread: %s', str(self._background_thread))


    def run(self):
        """
        Keeps renewing the leases for the locks owned by this client - till the client is closed.

        The method has a while loop that wakes up on a periodic basis (as defined by the heartbeat_period)
        and invokes the send_heartbeat() method on each lock.
        """
        # e.g. 5 TPS => each loop should take an average of 0.2 seconds (200ms)
        avg_loop_time = 1.0 / self.heartbeat_tps

        while not self._shutting_down:
            logger.info('Starting a heartbeat loop')
            start_time = time.time()
            count = 0

            for uid, lock in self._locks.copy().items():
                count += 1
                self.send_heartbeat(lock)
                # After each lock, sleep a little (if needed) to honor the heartbeat_tps
                curr_loop_end_time = time.time()
                next_loop_start_time = start_time + count * avg_loop_time
                if curr_loop_end_time < next_loop_start_time:
                    time.sleep( next_loop_start_time - curr_loop_end_time )

            # After all the locks have been "heartbeat"-ed, sleep before the next run (if needed)
            logger.info('Finished the heartbeat loop')
            end_time = time.time()
            next_start_time = start_time + self.heartbeat_period.total_seconds()
            if end_time < next_start_time and not self._shutting_down:
                time.sleep( next_start_time - end_time )


    def send_heartbeat(self, lock):
        """
        Renews the lease for the given lock.

        It actually just switches the record_version_number on the existing lock - which tells
        all other clients waiting for this lock that the current owner is still alive, and they
        effectively reset their timers (to wait for lease_duration from the time they see this
        new record_version_number).

        As this method is called on a background thread, it uses the app_callback to let the
        (lock requestor) app know when there are significant events in the lock lifecycle. There
        are two such events:

        1) LOCK_STOLEN
            When the heartbeat process finds that someone else has taken over the lock,
            or it has been released/deleted without the lock-client's knowledge. In this case, the
            app_callback should just try to abort its processing and roll back any changes it had
            made with the assumption that it owned the lock. This is not a normal occurrance and
            should only happen if someone manually changes/deletes the data in DynamoDB.
        2) LOCK_IN_DANGER
            When the heartbeat for a given lock has failed multiple times, and it is
            now in danger of going past its lease-duration without a successful heartbeat - at which
            point, any other client waiting to acquire the lock will consider it abandoned and take
            over. In this case, the app_callback should try to expedite the processing,  either
            commit or rollback its changes quickly, and release the lock.

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        logger.info('Sending a DynamoDBLock heartbeat: %s', lock.unique_identifier)
        with lock.thread_lock:
            try:
                # the ddb-lock might have been released while waiting for the thread-lock
                if lock.unique_identifier not in self._locks: return

                old_record_version_number = lock.record_version_number
                new_record_version_number = str(uuid.uuid4())
                new_expiry_time = int(time.time() + self.expiry_period.total_seconds())

                # first, try to update the database
                self._dynamodb_table.update_item(
                    Key={
                        self.partition_key_name: lock.partition_key,
                        self.sort_key_name: lock.sort_key
                    },
                    UpdateExpression='SET #rvn = :new_rvn, #et = :new_et',
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn',
                    ExpressionAttributeNames={
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                        '#rvn': self._COL_RECORD_VERSION_NUMBER,
                        '#et': self._COL_EXPIRY_TIME,
                    },
                    ExpressionAttributeValues={
                        ':old_rvn': old_record_version_number,
                        ':new_rvn': new_record_version_number,
                        ':new_et':  new_expiry_time,
                    }
                )

                # if successful, update the in-memory lock representations
                lock.record_version_number = new_record_version_number
                lock.expiry_time = new_expiry_time
                lock.last_updated_time = time.time()
                logger.debug('Successfully sent the heartbeat: %s', lock.unique_identifier)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # someone else stole our lock!
                    logger.warning('LockStolenError while sending heartbeat: %s', lock.unique_identifier)
                    # let's drop it from our in-memory collection as well
                    del self._locks[lock.unique_identifier]
                    # callback - the app should abort its processing; no need to release
                    self._call_app_callback(lock, DynamoDBLockError.LOCK_STOLEN)
                else:
                    logger.warning('ClientError while sending heartbeat: %s', lock.unique_identifier, exc_info=True)
            except Exception as e:
                logger.warning('Unexpected error while sending heartbeat: %s', lock.unique_identifier, exc_info=True)
            finally:
                # Note: the lock might have been released locally by the conditional-exception above
                if lock.unique_identifier not in self._locks: return
                # if the lock is in danger, invoke the app-callback
                safe_period_end_time = lock.last_updated_time + self.safe_period.total_seconds()
                if not time.time() < safe_period_end_time:
                    logger.warning('LockInDanger while sending heartbeat: %s', lock.unique_identifier)
                    # callback - the app should abort its processing, and release the lock
                    self._call_app_callback(lock, DynamoDBLockError.LOCK_IN_DANGER)


    def _call_app_callback(self, lock, code):
        """
        Utility function to route the app_callback through the thread-pool-executor

        :param DynamoDBLock lock: the lock for which the event is being fired
        :param str code: the notification event-type
        """
        self.app_callback_executor.submit(lock.app_callback, code, lock)


    def acquire_lock(self,
                     partition_key,
                     sort_key=_DEFAULT_SORT_KEY_VALUE,
                     retry_period=datetime.timedelta(seconds=2),
                     retry_timeout=datetime.timedelta(seconds=5),
                     additional_attributes=None,
                     app_callback=None,
                     ):
        """
        Acquires a distributed DynaomDBLock for the given key(s).

        If the lock is currently held by a different client, then this client will keep retrying on
        a periodic basis. In that case, a few different things can happen:

        1) The other client releases the lock - basically deleting it from the database
            Which would allow this client to try and insert its own record instead.
        2) The other client dies, and the lock stops getting updated by the heartbeat thread.
            While waiting for a lock, this client keeps track of the local-time whenever it sees the lock's
            record-version-number change. From that point-in-time, it needs to wait for a period of time
            equal to the lock's lease duration before concluding that the lock has been abandoned and try
            to overwrite the database entry with its own lock.
        3) This client goes over the max-retry-timeout-period
            While waiting for the other client to release the lock (or for the lock's lease to expire), this
            client may go over the retry_timeout period (as provided by the caller) - in which case, a
            DynamoDBLockError with code == ACQUIRE_TIMEOUT will be thrown.
        4) Race-condition amongst multiple lock-clients waiting to acquire lock
            Whenever the "old" lock is released (or expires), there may be multiple "new" clients trying
            to grab the lock - in which case, one of those would succeed, and the rest of them would get
            a "conditional-update-exception". This is just logged and swallowed internally - and the
            client moves on to another sleep-retry cycle.
        5) Any other error/exception
            Would be wrapped inside a DynamoDBLockError and raised to the caller.

        :param str partition_key: The primary lock identifier
        :param str sort_key: Forms a "composite identifier" along with the partition_key. Defaults to '-'
        :param datetime.timedelta retry_period: If the lock is not immediately available, how long
                should we wait between retries? Defaults to 2 seconds.
        :param datetime.timedelta retry_timeout: If the lock is not available for an extended period,
                how long should we keep trying before giving up and timing out? Defaults to 5 seconds.
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        :param Callable app_callback: Callback function that can be used to notify the app of lock entering
                the danger period, or an unexpected release
        :rtype: DynamoDBLock
        :return: A distributed lock instance
        """
        logger.info('Trying to acquire lock for: %s, %s', partition_key, sort_key)
        new_lock = DynamoDBLock(
            partition_key=partition_key,
            sort_key=sort_key,
            owner_name=self.owner_name,
            lease_duration=self.lease_duration.total_seconds(),
            record_version_number=str( uuid.uuid4() ),
            expiry_time=int(time.time() + self.expiry_period.total_seconds()),
            additional_attributes=additional_attributes,
            app_callback=app_callback,
            lock_client=self,
        )

        start_time = time.time()
        retry_timeout_time = start_time + retry_timeout.total_seconds()
        retry_count = 0
        last_record_version_number = None
        last_version_fetch_time = -1.0
        while True:
            if self._shutting_down:
                raise DynamoDBLockError(DynamoDBLockError.CLIENT_SHUTDOWN, 'Client already shut down')

            try:
                # need to bump up the expiry time - to account for the sleep between tries
                new_lock.last_updated_time = time.time()
                new_lock.expiry_time = int(time.time() + self.expiry_period.total_seconds())

                logger.debug('Checking the database for existing owner: %s', new_lock.unique_identifier)
                existing_lock = self._get_lock_from_dynamodb(partition_key, sort_key)

                if existing_lock is None:
                    logger.debug('No existing lock - attempting to add one: %s', new_lock.unique_identifier)
                    self._add_new_lock_to_dynamodb(new_lock)
                    logger.debug('Added to the DDB. Adding to in-memory map: %s', new_lock.unique_identifier)
                    self._locks[new_lock.unique_identifier] = new_lock
                    logger.info('Successfully added a new lock: %s', str(new_lock))
                    return new_lock
                else:
                    if existing_lock.record_version_number != last_record_version_number:
                        logger.debug('Existing lock\'s record_version_number changed: %s, %s, %s',
                                     new_lock.unique_identifier,
                                     last_record_version_number,
                                     existing_lock.record_version_number)
                        # if the record_version_number changes, the lock gets a fresh lease of life
                        # keep track of the time we first saw this record_version_number
                        last_record_version_number = existing_lock.record_version_number
                        last_version_fetch_time = time.time()
                    else:
                        logger.debug('Existing lock\'s record_version_number has not changed: %s, %s',
                                     new_lock.unique_identifier,
                                     last_record_version_number)
                        # if the record_version_number has not changed for more than lease_duration period,
                        # it basically means that the owner thread/process has died.
                        last_version_elapsed_time = time.time() - last_version_fetch_time
                        if last_version_elapsed_time > existing_lock.lease_duration:
                            logger.warning('Existing lock\'s lease has expired: %s', str(existing_lock))
                            self._overwrite_existing_lock_in_dynamodb(new_lock, last_record_version_number)
                            logger.debug('Added to the DDB. Adding to in-memory map: %s', new_lock.unique_identifier)
                            self._locks[new_lock.unique_identifier] = new_lock
                            logger.info('Successfully updated with the new lock: %s', str(new_lock))
                            return new_lock
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    logger.info('Someone else beat us to it - just log-it, sleep and retry: %s', new_lock.unique_identifier)
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))
            except Exception as e:
                raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))

            # sleep and retry
            retry_count += 1
            curr_loop_end_time = time.time()
            next_loop_start_time = start_time + retry_count * retry_period.total_seconds()
            if next_loop_start_time > retry_timeout_time:
                raise DynamoDBLockError(DynamoDBLockError.ACQUIRE_TIMEOUT, 'acquire_lock() timed out: ' + new_lock.unique_identifier)
            elif next_loop_start_time > curr_loop_end_time:
                logger.info('Sleeping before a retry: %s', new_lock.unique_identifier)
                time.sleep(next_loop_start_time - curr_loop_end_time)


    def release_lock(self, lock, best_effort=True):
        """
        Releases the given lock - by deleting it from the database.

        It allows the caller app to indicate whether it wishes to be informed of all errors/exceptions,
        or just have the lock-client swallow all of them. A typical usage pattern would include acquiring
        the lock, making app changes, and releasing the lock. By the time the app is releasing the lock,
        it would generally be too late to respond to any errors encountered during the release phase - but,
        the app may still wish to get informed and log it somewhere of offline re-conciliation/follow-up.

        :param DynamoDBLock lock: The lock instance that needs to be released
        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to True.
        """
        logger.info('Releasing the lock: %s', str(lock))

        # if this client did not create the lock being released
        if lock.unique_identifier not in self._locks:
            if best_effort:
                logger.warning('Lock not owned by this client: %s', str(lock))
                return
            else:
                raise DynamoDBLockError(DynamoDBLockError.LOCK_NOT_OWNED, 'Lock is not owned by this client')

        with lock.thread_lock:
            try:
                # first, remove from in-memory locks - will stop the heartbeats
                # even if the database call fails, it will auto-release after the lease expires
                del self._locks[lock.unique_identifier]

                # then, remove it from the database
                self._dynamodb_table.delete_item(
                    Key={
                        self.partition_key_name: lock.partition_key,
                        self.sort_key_name: lock.sort_key
                    },
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn',
                    ExpressionAttributeNames={
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                        '#rvn': self._COL_RECORD_VERSION_NUMBER,
                    },
                    ExpressionAttributeValues={
                        ':rvn': lock.record_version_number,
                    }
                )

                logger.info('Successfully released the lock: %s', lock.unique_identifier)
            except ClientError as e:
                if best_effort:
                    logger.warning('DynamoDb error while releasing lock: %s', lock.unique_identifier, exc_info=True)
                elif e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # Note: this is slightly different from the Java impl - which would just returns false
                    raise DynamoDBLockError(DynamoDBLockError.LOCK_STOLEN, 'Lock was stolen by someone else')
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))
            except Exception as e:
                if best_effort:
                    logger.warning('Unknown error while releasing lock: %s', lock.unique_identifier, exc_info=True)
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))


    def _get_lock_from_dynamodb(self, partition_key, sort_key):
        """
        Loads the lock from the database - or returns None if not available.

        :rtype: BaseDynamoDBLock
        """
        logger.debug('Getting the lock from dynamodb for: %s, %s', partition_key, sort_key)
        result = self._dynamodb_table.get_item(
            Key={
                self.partition_key_name: partition_key,
                self.sort_key_name: sort_key
            },
            ConsistentRead=True
        )
        if 'Item' in result:
            return self._get_lock_from_item( result['Item'] )
        else:
            return None


    def _add_new_lock_to_dynamodb(self, lock):
        """
        Adds a new lock into the database - while checking that it does not exist already.

        :param DynamoDBLock lock: The lock instance that needs to be added to the database.
        """
        logger.debug('Adding a new lock: %s', str(lock))
        self._dynamodb_table.put_item(
            Item=self._get_item_from_lock(lock),
            ConditionExpression='NOT(attribute_exists(#pk) AND attribute_exists(#sk))',
            ExpressionAttributeNames={
                '#pk': self.partition_key_name,
                '#sk': self.sort_key_name,
            },
        )


    def _overwrite_existing_lock_in_dynamodb(self, lock, record_version_number):
        """
        Overwrites an existing lock in the database - while checking that the version has not changed.

        :param DynamoDBLock lock: The new lock instance that needs to overwrite the old one in the database.
        :param str record_version_number: The version-number for the old lock instance in the database.
        """
        logger.debug('Overwriting existing-rvn: %s with new lock: %s', record_version_number,  str(lock))
        self._dynamodb_table.put_item(
            Item=self._get_item_from_lock(lock),
            ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn',
            ExpressionAttributeNames={
                '#pk': self.partition_key_name,
                '#sk': self.sort_key_name,
                '#rvn': self._COL_RECORD_VERSION_NUMBER,
            },
            ExpressionAttributeValues={
                ':old_rvn': record_version_number,
            }
        )


    def _get_lock_from_item(self, item):
        """
        Converts a DynamoDB 'Item' dict to a BaseDynamoDBLock instance

        :param dict item: The DynamoDB 'Item' dict object to be de-serialized.
        :rtype: BaseDynamoDBLock
        """
        logger.debug('Get lock from item: %s', str(item))
        lock = BaseDynamoDBLock(
            partition_key=item.pop(self.partition_key_name),
            sort_key=item.pop(self.sort_key_name),
            owner_name=item.pop(self._COL_OWNER_NAME),
            lease_duration=float(item.pop(self._COL_LEASE_DURATION)),
            record_version_number=item.pop(self._COL_RECORD_VERSION_NUMBER),
            expiry_time=int(item.pop(self._COL_EXPIRY_TIME)),
            additional_attributes=item
        )
        return lock


    def _get_item_from_lock(self, lock):
        """
        Converts a BaseDynamoDBLock (or subclass) instance to a DynamoDB 'Item' dict

        :param BaseDynamoDBLock lock: The lock instance to be serialized.
        :rtype: dict
        """
        logger.debug('Get item from lock: %s', str(lock))
        item = lock.additional_attributes.copy()
        item.update({
            self.partition_key_name: lock.partition_key,
            self.sort_key_name: lock.sort_key,
            self._COL_OWNER_NAME: lock.owner_name,
            self._COL_LEASE_DURATION: Decimal.from_float(lock.lease_duration),
            self._COL_RECORD_VERSION_NUMBER: lock.record_version_number,
            self._COL_EXPIRY_TIME: lock.expiry_time
        })
        return item


    def _release_all_locks(self):
        """
        Iterates over all the locks and releases each one.
        """
        logger.info('Releasing all locks: %d', len(self._locks))
        for uid, lock in self._locks.copy().items():
            self.release_lock(lock, best_effort=True)
            # TODO: should we fire app-callback to indicate the force-release
            # self._call_app_callback(lock, DynamoDBLockError.LOCK_STOLEN)


    def close(self, release_locks=False):
        """
        Shuts down the background thread - and releases all locks if so asked.

        By default, this method will NOT release all the locks - as releasing the locks while
        the application is still making changes assuming that it has the lock can be dangerous.
        As soon as a lock is released by this client, some other client may pick it up, and the
        associated app may start processing the underlying business entity in parallel.

        It is recommended that the application manage its shutdown-lifecycle such that all the
        worker threads operating under these locks are first terminated (committed or rolled-back),
        the corresponding locks released (one at a time - by each worker thread), and then the
        lock_client.close() method is called. Alternatively, consider letting the process die
        without releasing all the locks - they will be auto-released when their lease runs out
        after a while.

        :param bool release_locks: if True, releases all the locks. Defaults to False.
        """
        if self._shutting_down: return
        logger.info('Shutting down')
        self._shutting_down = True
        self._background_thread.join()
        if release_locks: self._release_all_locks()


    def __str__(self):
        """
        Returns a readble string representation of this instance.
        """
        return '%s::%s' % (self.__class__.__name__, self.__dict__)


    @classmethod
    def create_dynamodb_table(cls,
                              dynamodb_client,
                              table_name=_DEFAULT_TABLE_NAME,
                              partition_key_name=_DEFAULT_PARTITION_KEY_NAME,
                              sort_key_name=_DEFAULT_SORT_KEY_NAME,
                              read_capacity=_DEFAULT_READ_CAPACITY,
                              write_capacity=_DEFAULT_WRITE_CAPACITY):

        """
        Helper method to create the DynamoDB table
        
        :param boto3.DynamoDB.Client dynamodb_client: mandatory argument
        :param str table_name: defaults to 'DynamoDBLockTable'
        :param str partition_key_name: defaults to 'lock_key'
        :param str sort_key_name: defaults to 'sort_key'
        :param int read_capacity: the max TPS for strongly-consistent reads; defaults to 5
        :param int write_capacity: the max TPS for write operations; defaults to 5
        :return: 
        """
        logger.info("Creating the lock table: %s", table_name)
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': partition_key_name,
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': sort_key_name,
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': partition_key_name,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': sort_key_name,
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity
            },
        )
        cls._wait_for_table_to_be_active(dynamodb_client, table_name)

        logger.info("Updating the table with time_to_live configuration")
        dynamodb_client.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': cls._COL_EXPIRY_TIME
            }
        )
        cls._wait_for_table_to_be_active(dynamodb_client, table_name)


    @classmethod
    def _wait_for_table_to_be_active(cls, dynamodb_client, table_name):
        logger.info("Waiting till the table becomes ACTIVE")
        while True:
            response = dynamodb_client.describe_table(TableName=table_name)
            status = response.get('Table', {}).get('TableStatus', 'UNKNOWN')
            logger.info("Table status: %s", status)
            if status == 'ACTIVE':
                break
            else:
                time.sleep(2)



class BaseDynamoDBLock:
    """
    Represents a distributed lock - as stored in DynamoDB.

    Typically used within the code to represent a lock held by some other lock-client.
    """

    def __init__(self,
                 partition_key,
                 sort_key,
                 owner_name,
                 lease_duration,
                 record_version_number,
                 expiry_time,
                 additional_attributes
                 ):
        """
        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param str owner_name: The owner name - typically from the lock_client
        :param float lease_duration: The lease duration in seconds - typically from the lock_client
        :param str record_version_number: A "liveness" indicating GUID - changes with every heartbeat
        :param int expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        """
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.owner_name = owner_name
        self.lease_duration = lease_duration
        self.record_version_number = record_version_number
        self.expiry_time = expiry_time
        self.additional_attributes = additional_attributes or {}
        # additional properties
        self.unique_identifier = quote(partition_key) + '|' + quote(sort_key)


    def __str__(self):
        """
        Returns a readble string representation of this instance.
        """
        return '%s::%s' % (self.__class__.__name__, self.__dict__)



class DynamoDBLock(BaseDynamoDBLock):
    """
    Represents a lock that is owned by a local DynamoDBLockClient instance.
    """

    def __init__(self,
                 partition_key,
                 sort_key,
                 owner_name,
                 lease_duration,
                 record_version_number,
                 expiry_time,
                 additional_attributes,
                 app_callback,
                 lock_client,
                 ):
        """
        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param str owner_name: The owner name - typically from the lock_client
        :param float lease_duration: The lease duration - typically from the lock_client
        :param str record_version_number: Changes with every heartbeat - the "liveness" indicator
        :param int expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock

        :param Callable app_callback: Callback function that can be used to notify the app of lock entering
                the danger period, or an unexpected release
        :param DynamoDBLockClient lock_client: The client that "owns" this lock
        """
        BaseDynamoDBLock.__init__(self,
                                  partition_key,
                                  sort_key,
                                  owner_name,
                                  lease_duration,
                                  record_version_number,
                                  expiry_time,
                                  additional_attributes
                                  )
        self.app_callback = app_callback
        self.lock_client = lock_client
        # additional properties
        self.last_updated_time = time.time()
        self.thread_lock = threading.RLock()


    def __enter__(self):
        """
        No-op - returns itself
        """
        logger.debug('Entering: %s', self.unique_identifier)
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        """
        Releases the lock - with best_effort=True
        """
        logger.debug('Exiting: %s', self.unique_identifier)
        self.release(best_effort=True)
        return True


    def release(self, best_effort=True):
        """
        Calls the lock_client.release_lock(self, True) method

        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to True.
        """
        logger.debug('Releasing: %s', self.unique_identifier)
        self.lock_client.release_lock(self, best_effort)



class DynamoDBLockError(Exception):
    """
    Wrapper for all kinds of errors that might occur during the acquire and release calls.
    """

    # code-constants
    CLIENT_SHUTDOWN = 'CLIENT_SHUTDOWN'
    ACQUIRE_TIMEOUT = 'ACQUIRE_TIMEOUT'
    LOCK_NOT_OWNED = 'LOCK_NOT_OWNED'
    LOCK_STOLEN = 'LOCK_STOLEN'
    LOCK_IN_DANGER = 'LOCK_IN_DANGER'
    UNKNOWN = 'UNKNOWN'


    def __init__(self,
                 code='UNKNOWN',
                 message='Unknown error'
                 ):
        Exception.__init__(self)
        self.code = code
        self.message = message


    def __str__(self):
        return "%s: %s - %s" % (self.__class__.__name__, self.code, self.message)

