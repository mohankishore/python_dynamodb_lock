# -*- coding: utf-8 -*-

"""
This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based AmazonDynamoDBLockClient library, and supports both coarse-grained
and fine-grained locking.
"""

from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import datetime
import logging
import socket
import time
import threading
import urllib
import uuid

# module level logger
logger = logging.getLogger(__name__)


class DynamoDBLockClient:
    """
    Provides distributed locks using DynamoDB's support for conditional reads/writes.
    """

    # default values for class properties
    _DEFAULT_TABLE_NAME = 'lockTable'
    _DEFAULT_PARTITION_KEY_NAME = 'key'
    _DEFAULT_SORT_KEY_NAME = 'sort_key'
    _DEFAULT_HEARTBEAT_PERIOD = datetime.timedelta(seconds=5)
    _DEFAULT_SAFE_PERIOD = datetime.timedelta(seconds=20)
    _DEFAULT_LEASE_DURATION = datetime.timedelta(seconds=30)
    _DEFAULT_EXPIRY_PERIOD = datetime.timedelta(hours=1)
    _DEFAULT_HEARTBEAT_TPS = 5
    _DEFAULT_APP_CALLBACK_EXECUTOR = ThreadPoolExecutor(max_workers=5, thread_name_prefix='DynamoDBLockClient-CB-')


    def __init__(self,
                 dynamodb_client,
                 table_name=_DEFAULT_TABLE_NAME,
                 partition_key_name=_DEFAULT_PARTITION_KEY_NAME,
                 sort_key_name=_DEFAULT_SORT_KEY_NAME,
                 owner_name=None,
                 heartbeat_period=_DEFAULT_HEARTBEAT_PERIOD,
                 safe_period=_DEFAULT_SAFE_PERIOD,
                 lease_duration=_DEFAULT_LEASE_DURATION,
                 expiry_period=_DEFAULT_EXPIRY_PERIOD,
                 heartbeat_tps=_DEFAULT_HEARTBEAT_TPS,
                 app_callback_executor=_DEFAULT_APP_CALLBACK_EXECUTOR
                 ):
        """
        :param boto3.DynamoDB.Client dynamodb_client: mandatory argument
        :param str table_name: defaults to 'lockTable'
        :param str partition_key_name: defaults to 'key'
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
        :param ThreadPoolExecutor : The executor to be used for invoking the app_callbacks in case of
                un-expected errors. Defaults to a ThreadPoolExecutor with maximum of 5 threads. 
        """
        self.uuid = uuid.uuid4()
        self.dynamodb_client = dynamodb_client
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name
        self.owner_name = owner_name if owner_name else socket.getfqdn() + self.uuid
        self.heartbeat_period = heartbeat_period
        self.safe_period = safe_period
        self.lease_duration = lease_duration
        self.expiry_period = expiry_period
        self.heartbeat_tps = heartbeat_tps
        self.app_callback_executor = app_callback_executor
        # additional properties
        self._locks = {}
        self._shutting_down = False
        # and, initialization
        self._start_background_thread()
        logger.info('Created: %s', str(self))


    def _start_background_thread(self):
        """
        Creates and starts a daemon thread - that calls DynamoDBLockClient.run() method.
        """
        self._background_thread = threading.Thread(
            name='DynamoDBLockClient-HB-' + self.uuid,
            target=self
        )
        self._background_thread.daemon = True
        self._background_thread.start()
        logger.info('Started the background thread: %s', str(self._background_thread))


    def run(self):
        """
        Keeps renewing the leases for the locks owned by this client - till the client is closed.

        The method has a while loop that wakes up on a periodic basis (as defined by the heartbeat_period)
        and invokes the _send_heartbeat() method on each lock.
        """
        # A more useful local representation of the heartbeat_tps field
        # e.g. 5 TPS => a max of 1 loop every 200ms
        min_loop_time = 1.0 / self.heartbeat_tps

        while not self._shutting_down:
            logger.info('Starting a heartbeat loop')
            start_time = datetime.datetime.utcnow()
            count = 0

            for uid, lock in self._locks.items():
                count += 1
                self._send_heartbeat(lock)
                # After each lock, sleep a little (if needed) to honor the heartbeat_tps
                elapsed_time = (datetime.datetime.utcnow() - start_time).total_seconds()
                if elapsed_time < min_loop_time * count:
                    time.sleep(min_loop_time * count - elapsed_time)

            # After all the locks have been "heartbeat"-ed, sleep before the next run (if needed)
            elapsed_time = (datetime.datetime.utcnow() - start_time)
            if elapsed_time < self.heartbeat_period and not self._shutting_down:
                time.sleep((self.heartbeat_period - elapsed_time).total_seconds())


    def _send_heartbeat(self, lock):
        """
        Renews the lease for the given lock.

        It actually just switches the record_version_number on the existing lock - which tells
        all other clients waiting for this lock that the current owner is still alive, and they
        effectively reset their timers (to wait for lease_duration from the time they see this
        new record_version_number)

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        logger.info('Sending a DynamoDBLock heartbeat: %s', lock.unique_identifier)
        with lock.thread_lock:
            try:
                # the ddb-lock might have been released while waiting for the thread-lock
                if not self._locks.has_key(lock.unique_identifier): return

                old_record_version_number = lock.record_version_number
                new_record_version_number = str(uuid.uuid4())
                # for expiry related calculations, we use the time module (better epoch-seconds support)
                # for everything else, we use the datetime module (better period/timedelta support)
                new_expiry_time = time.time() + self.expiry_period.total_seconds()

                # first, try to update the database
                self.dynamodb_client.update_item(
                    TableName=self.table_name,
                    Key={
                        self.partition_key_name: {'S': lock.partition_key},
                        self.sort_key_name: {'S': lock.sort_key}
                    },
                    UpdateExpression='SET #rvn = :new_rvn, #et = :new_et',
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn',
                    ExpressionAttributeNames={
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                        '#rvn': 'record_version_number',
                        '#et': 'expiry_time',
                    },
                    ExpressionAttributeValues={
                        ':old_rvn': {'S': old_record_version_number},
                        ':new_rvn': {'S': new_record_version_number},
                        ':new_et':  {'N': new_expiry_time}
                    }
                )

                # if successful, update the in-memory lock representations
                lock.record_version_number = new_record_version_number
                lock.expiry_time = new_expiry_time
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # someone else stole our lock!
                    logger.warning('LockNotOwnerError while sending heartbeat: %s', lock.unique_identifier)
                    # let's drop it from our in-memory collection as well
                    del self._locks[lock.unique_identifier]
                    # callback - the app should abort its processing; no need to release
                    self._call_app_callback(lock, DynamoDBLockError.LOCK_STOLEN)
                else:
                    logger.warning('ClientError while sending heartbeat: %s', lock.unique_identifier, exc_info=True)
            except:
                logger.warning('Unexpected error while sending heartbeat: %s', lock.unique_identifier, exc_info=True)
            finally:
                # Note: the lock might have been released locally by the conditional-exception above
                if not self._locks.has_key(lock.unique_identifier): return
                # if the lock is in danger, invoke the app-callback
                last_update_time = lock.expiry_time - self.expiry_period.total_seconds()
                is_lock_safe = time.time() < (last_update_time + self.safe_period.total_seconds())
                if not is_lock_safe:
                    logger.warning('LockNotSafe while sending heartbeat: %s', lock.unique_identifier)
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
                     sort_key=None,
                     retry_period=datetime.timedelta(seconds=2),
                     retry_timeout=datetime.timedelta(seconds=5),
                     additional_attributes={},
                     app_callback=None,
                     ):
        """
        Acquires a distributed DynaomDBLock for the given key(s).

        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param datetime.timedelta retry_period: If the lock is not immediately available, how long
                should we wait between retries? Defaults to 2 seconds.
        :param datetime.timedelta retry_timeout: If the lock is not available for an extended period,
                how long should we keep trying before giving up and timing out? Defaults to 5 seconds.
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        :param Callable app_callback: Callback function that can be used to notify the app of lock entering 
                the danger period, or an unexpected release
        :rtype: BaseDynamoDBLock
        :return: A distributed lock instance
        """
        logger.info('Trying to acquire lock for: %s, %s', partition_key, sort_key)
        new_lock = DynamoDBLock(
            partition_key=partition_key,
            sort_key=sort_key,
            owner_name=self.owner_name,
            lease_duration_in_seconds=self.lease_duration.total_seconds(),
            record_version_number=str( uuid.uuid4() ),
            expiry_time=time.time() + self.expiry_period.total_seconds(),
            additional_attributes=additional_attributes,
            app_callback=app_callback,
            lock_client=self,
        )

        retry_timeout_time = datetime.datetime.utcnow() + retry_timeout
        last_record_version_number = None
        last_version_fetch_time = None
        while True:
            if self._shutting_down:
                raise DynamoDBLockError(DynamoDBLockError.CLIENT_SHUTDOWN, 'Client already shut down')

            try:
                # need to bump up the expiry time - to account for the sleep between tries
                new_lock.expiry_time = time.time() + self.expiry_period.total_seconds(),

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
                        last_version_fetch_time = datetime.datetime.utcnow()
                    else:
                        logger.debug('Existing lock\'s record_version_number has not changed: %s, %s',
                                     new_lock.unique_identifier,
                                     last_record_version_number)
                        # if the record_version_number has not changed for more than lease_duration period,
                        # it basically means that the owner thread/process has died.
                        last_version_elapsed_time = datetime.datetime.utcnow() - last_version_fetch_time
                        if last_version_elapsed_time > existing_lock.lease_duration:
                            logger.warning('Existing lock\'s lease has expired: %s', str(existing_lock))
                            self._overwrite_existing_lock_in_dynamodb(new_lock, last_record_version_number)
                            logger.debug('Added to the DDB. Adding to in-memory map: %s', new_lock.unique_identifier)
                            self._locks[new_lock.unique_identifier] = new_lock
                            logger.info('Successfully updated with the new lock: %s', str(new_lock))
                            return new_lock
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    logger.info('Someone else beat us to it - just log-it, sleep and retry')
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))
            except Exception as e:
                raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))

            # sleep and retry
            if datetime.datetime.utcnow() + retry_period < retry_timeout_time:
                time.sleep(retry_period.total_seconds())
            else:
                raise DynamoDBLockError(DynamoDBLockError.ACQUIRE_TIMEOUT, 'acquire_lock() timed out')


    def release_lock(self, lock, best_effort=True):
        """
        Releases the given lock - by deleting it from the database.

        :param DynamoDBLock lock: The lock instance that needs to be released
        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to True.
        """
        logger.info('Releasing the lock: %s', str(lock))
        with lock.thread_lock:
            try:
                # if this client did not create the lock being released
                if lock.unique_identifier not in self._locks:
                    if best_effort:
                        logger.warn('Lock not owned by this client: %s', str(lock))
                    else:
                        raise DynamoDBLockError(DynamoDBLockError.LOCK_NOT_OWNED, 'Lock is not owned by this client')

                # first, remove from in-memory locks - will stop the heartbeats
                # even if the database call fails, it will auto-release after the lease expires
                del self._locks[lock.unique_identifier]

                # then, remove it from the database
                self.dynamodb_client.delete_item(
                    TableName=self.table_name,
                    Key={
                        self.partition_key_name: {'S': lock.partition_key},
                        self.sort_key_name: {'S': lock.sort_key}
                    },
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn',
                    ExpressionAttributeNames={
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                        '#rvn': 'record_version_number',
                    },
                    ExpressionAttributeValues={
                        ':rvn': {'S': lock.record_version_number},
                    }
                )

                logger.info('Successfully released the lock: %s', lock.unique_identifier)
            except ClientError as e:
                if best_effort:
                    logger.warn('Lock was stolen by someone else: %s', lock.unique_identifier)
                elif e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # Note: this is slightly different from the Java impl - which would just returns false
                    raise DynamoDBLockError(DynamoDBLockError.LOCK_STOLEN, 'Lock was stolen by someone else')
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))
            except Exception as e:
                if (best_effort):
                    logger.warn('Unknown error while releasing lock: %s, %s', lock.unique_identifier, str(e))
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(e))


    def _get_lock_from_dynamodb(self, partition_key, sort_key=None):
        """
        Loads the lock from the database - or returns None if not available. 

        :rtype: BaseDynamoDBLock
        """
        logger.debug('Getting the lock from dynamodb for: %s, %s', partition_key, sort_key)
        result = self.dynamodb_client.get_item(
            TableName=self.table_name,
            Key={
                self.partition_key_name: {'S': partition_key},
                self.sort_key_name: {'S': sort_key}
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
        self.dynamodb_client.put_item(
            TableName=self.table_name,
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
        self.dynamodb_client.put_item(
            TableName=self.table_name,
            Item=self._get_item_from_lock(lock),
            ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn',
            ExpressionAttributeNames={
                '#pk': self.partition_key_name,
                '#sk': self.sort_key_name,
                '#rvn': 'record_version_number',
            },
            ExpressionAttributeValues={
                ':old_rvn': {'S': record_version_number},
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
            owner_name=item.pop('owner_name'),
            lease_duration_in_seconds=item.pop('lease_duration_in_seconds'),
            record_version_number=item.pop('record_version_number'),
            expiry_time=item.pop('expiry_time'),
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
            self.partition_key_name: {'S': lock.partition_key},
            self.sort_key_name: {'S': lock.sort_key},
            'owner_name': {'S': lock.owner_name},
            'lease_duration_in_seconds': {'N': lock.lease_duration_in_seconds},
            'record_version_number': {'S': lock.record_version_number},
            'expiry_time': { 'N': lock.expiry_time }
        })
        return item


    def _release_all_locks(self):
        """
        Iterates over all the locks and releases each one.
        
        It is highly recommended that the application never invoke this in production.
        Instead, the application should keep track of all the threads that are actually
        performing work "under" these locks, and have each of them abort and release the
        respective lock.
        """
        logger.debug('Releasing all locks')
        for uid, lock in self._locks.items():
            self.release_lock(lock, best_effort=True)


    def close(self, release_locks=False):
        """
        Shuts down the background thread - and releases all locks if so asked.
        
        :param bool release_locks: if True, releases all the locks. Defaults to False.
        """
        logger.debug('Shutting down')
        self._shutting_down = True
        self._background_thread.join()
        if release_locks: self._release_all_locks()


    def __str__(self):
        """
        Returns a readble string representation of this instance. 
        """
        return '%s::%s' % (self.__class__.__name__, self.__dict__)



class BaseDynamoDBLock:
    """
    Represents a distributed lock - as stored in DynamoDB.
    
    Typically used within the code to represent a lock held by some other lock-client.
    """

    def __init__(self,
                 partition_key,
                 sort_key,
                 owner_name,
                 lease_duration_in_seconds,
                 record_version_number,
                 expiry_time,
                 additional_attributes
                 ):
        """
        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param str owner_name: The owner name - typically from the lock_client
        :param float lease_duration_in_seconds: The lease duration - typically from the lock_client
        :param str record_version_number: Changes with every heartbeat - the "liveness" indicator
        :param float expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        """
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.owner_name = owner_name
        self.lease_duration_in_seconds = lease_duration_in_seconds
        self.record_version_number = record_version_number
        self.expiry_time = expiry_time
        self.additional_attributes = additional_attributes
        # additional properties
        self.unique_identifier = urllib.quote(partition_key) + '|' + urllib.quote(sort_key)
        self.lease_duration = datetime.timedelta(seconds=lease_duration_in_seconds)


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
                 lease_duration_in_seconds,
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
        :param float lease_duration_in_seconds: The lease duration - typically from the lock_client
        :param str record_version_number: Changes with every heartbeat - the "liveness" indicator
        :param float expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        
        :param Callable app_callback: Callback function that can be used to notify the app of lock entering 
                the danger period, or an unexpected release
        :param DynamoDBLockClient lock_client: The client that "owns" this lock
        """
        BaseDynamoDBLock.__init__(self,
                                  partition_key,
                                  sort_key,
                                  owner_name,
                                  lease_duration_in_seconds,
                                  record_version_number,
                                  expiry_time,
                                  additional_attributes
                                  )
        self.app_callback = app_callback
        self.lock_client = lock_client
        # additional properties
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

