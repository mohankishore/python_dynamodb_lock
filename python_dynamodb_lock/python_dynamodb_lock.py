# -*- coding: utf-8 -*-

"""
This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based AmazonDynamoDBLockClient library, and supports both coarse-grained
and fine-grained locking.
"""

from botocore.exceptions import ClientError
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
    _DEFAULT_LEASE_DURATION = datetime.timedelta(seconds=30)
    _DEFAULT_HEARTBEAT_PERIOD = datetime.timedelta(seconds=10)
    _DEFAULT_EXPIRY_PERIOD = datetime.timedelta(hours=1)


    def __init__(self,
                 dynamodb_client,
                 table_name=_DEFAULT_TABLE_NAME,
                 partition_key_name=_DEFAULT_PARTITION_KEY_NAME,
                 sort_key_name=_DEFAULT_SORT_KEY_NAME,
                 owner_name=None,
                 lease_duration=_DEFAULT_LEASE_DURATION,
                 heartbeat_period=_DEFAULT_HEARTBEAT_PERIOD,
                 expiry_period=_DEFAULT_EXPIRY_PERIOD
                 ):
        """
        :param boto3.DynamoDB.Client dynamodb_client: mandatory argument
        :param str table_name: defaults to 'lockTable'
        :param str partition_key_name: defaults to 'key'
        :param str sort_key_name: defaults to 'sort_key'
        :param str owner_name: defaults to hostname + uuid
        :param datetime.timedelta lease_duration: The length of time that the lease for the lock
                will be granted for. i.e. if there is no heartbeat for this period of time, then
                the lock will be considered as expired. Defaults to 30 seconds.
        :param datetime.timedelta heartbeat_period: How often to update DynamoDB to note that the
                instance is still running. It is recommended to make this at least 3 times smaller
                than the leaseDuration. Defaults to 10 seconds.
        :param datetime.timedelta expiry_period: The fallback expiry timestamp to allow DynamoDB
                to cleanup old locks after a server crash. This value should be significantly larger
                than the lease_duration to ensure that clock-skew etc. are not an issue.
        """
        logger.debug('Creating a new instance of DynamoDBLockClient')
        self.uuid = uuid.uuid4()
        self.dynamodb_client = dynamodb_client
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name
        self.owner_name = owner_name if owner_name else socket.getfqdn() + self.uuid
        self.lease_duration = lease_duration
        self.heartbeat_period = heartbeat_period
        self.expiry_period = expiry_period
        # additional properties
        self.locks = {}
        self.shutting_down = False
        self.background_thread = self._start_background_thread()


    def _start_background_thread(self):
        """
        Creates and starts a daemon thread - that calls DynamoDBLockClient.run() method.
        """
        logger.debug('Starting a background thread')
        background_thread = threading.Thread(
            name='DynamoDBLockClient-' + self.uuid,
            target=self
        )
        background_thread.daemon = True
        background_thread.start()
        return background_thread


    def run(self):
        """
        Keeps renewing the leases for the locks owned by this client - till the client is closed.

        The method has a while loop that wakes up on a periodic basis (as defined by the heartbeat_period)
        and invokes the send_heartbeat() method on each lock.
        """
        logger.debug('Kicking off the while loop')
        while not self.shutting_down:
            start_time = datetime.datetime.utcnow()
            for uid, lock in self.locks.items():
                self.send_heartbeat(lock)
            end_time = datetime.datetime.utcnow()

            elapsed_time = (end_time - start_time)
            if elapsed_time < self.heartbeat_period and not self.shutting_down:
                time.sleep((self.heartbeat_period - elapsed_time).total_seconds())


    def send_heartbeat(self, lock):
        """
        Renews the lease for the given lock.

        It actually just switches the record_version_number on the existing lock - which tells
        all other clients waiting for this lock that the current owner is still alive, and they
        effectively reset their timers (to wait for lease_duration from the time they see this
        new record_version_number)

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        logger.debug('Sending a heartbeat for: %s', str(lock))
        with lock.thread_lock:
            try:
                self.dynamodb_client.update_item(
                    TableName=self.table_name,
                    Key={
                        self.partition_key_name: {'S': lock.partition_key},
                        self.sort_key_name: {'S': lock.sort_key}
                    },
                    UpdateExpression='SET #rvn = :new_rvn, #et = :et',
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn',
                    ExpressionAttributeNames={
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                        '#rvn': 'record_version_number',
                        '#et': 'expiry_time',
                    },
                    ExpressionAttributeValues={
                        ':new_rvn': {'S': str(uuid.uuid4())},
                        ':old_rvn': {'S': lock.record_version_number},
                        ':et': { 'N': time.time() + self.expiry_period.total_seconds() }
                    }
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # TODO: need to handle the case where the lock was "stolen" by someone else...
                    print()
                else:
                    print()
            except:
                print()


    def acquire_lock(self,
                     partition_key,
                     sort_key=None,
                     retry_period=datetime.timedelta(seconds=2),
                     retry_timeout=datetime.timedelta(seconds=5),
                     additional_attributes={}
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
        :rtype: DynamoDBLock
        :return: A distributed lock instance
        """
        logger.debug('Trying to acquire a lock for: %s, %s', partition_key, sort_key)
        start_time = datetime.datetime.utcnow()
        new_lock = DynamoDBLock(
            lock_client=self,
            partition_key=partition_key,
            sort_key=sort_key,
            owner_name=self.owner_name,
            lease_duration_in_seconds=self.lease_duration.total_seconds(),
            record_version_number=str( uuid.uuid4() ),
            expiry_time=time.time() + self.expiry_period.total_seconds(),
            additional_attributes=additional_attributes
        )
        last_record_version_number = None
        last_version_fetch_time = None
        while True:
            if self.shutting_down:
                print('TODO: raise SomeShuttingDownError')

            try:
                existing_lock = self._get_lock_from_dynamodb(partition_key, sort_key)

                if existing_lock is None:
                    new_lock.expiry_time = time.time() + self.expiry_period.total_seconds(),
                    self._add_new_lock_to_dynamodb(new_lock)
                else:
                    if existing_lock.record_version_number != last_record_version_number:
                        # if the record_version_number changes, the lock gets a fresh lease of life
                        # keep track of the time we first saw this record_version_number
                        last_record_version_number = existing_lock.record_version_number
                        last_version_fetch_time = datetime.datetime.utcnow()
                    else:
                        # if the record_version_number has not changed for more than lease_duration period,
                        # it basically means that the owner thread/process has died.
                        last_version_elapsed_time = datetime.datetime.utcnow() - last_version_fetch_time
                        if last_version_elapsed_time > existing_lock.lease_duration:
                            new_lock.expiry_time = time.time() + self.expiry_period.total_seconds(),
                            self._overwrite_existing_lock_in_dynamodb(new_lock, last_record_version_number)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    print('Someone else beat us to it - just log-it, sleep and retry')
                else:
                    print('Some random DDB error - just log-it, sleep and retry')
            # TODO: should we catch-all, and return None (instead of error-ing out?)

            # sleep and retry
            total_elapsed_time = datetime.datetime.utcnow() - start_time
            if total_elapsed_time + retry_period < retry_timeout:
                time.sleep(retry_period.total_seconds())
            else:
                print('TODO: raise SomeTimeoutError')


    def release_lock(self, lock, best_effort=False):
        """
        Releases the given lock - by deleting it from the database.

        :param DynamoDBLock lock: The lock instance that needs to be released
        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to False.
        """
        logger.debug('Releasing the lock: %s', str(lock))
        with lock.thread_lock:
            try:
                del self.locks[lock.unique_identifier]
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
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # TODO: need to handle the case where the lock was "stolen" by someone else...
                    print()
                elif best_effort:
                    print()
                else:
                    print()
            except:
                if (best_effort):
                    print()
                else:
                    print()


    def _get_lock_from_dynamodb(self, partition_key, sort_key=None):
        """
        Loads the lock from the database - or returns None if not available. 

        :rtype: DynamoDBLock
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
        if ('Item' in result):
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
        Converts a DynamoDB 'Item' dict to a DynamoDBLock instance

        :param dict item: The DynamoDB 'Item' dict object to be de-serialized.
        :rtype: DynamoDBLock
        """
        logger.debug('Get lock from item: %s', str(item))
        lock = DynamoDBLock(
            lock_client=self,
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
        Converts a DynamoDBLock instance to a DynamoDB 'Item' dict 

        :param DynamoDBLock lock: The lock instance to be serialized.
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
        """
        logger.debug('Releasing all locks')
        for uid, lock in self.locks.items():
            self.release_lock(lock)


    def close(self):
        """
        Shuts down the background thread and releases all locks.
        """
        logger.debug('Shutting down')
        self.shutting_down = True
        self.background_thread.join()
        self._release_all_locks()


    def __str__(self):
        """
        Returns a readble string representation of this instance. 
        """
        return '%s::%s' % (self.__class__.__name__, self.__dict__)



class DynamoDBLock:
    """
    Represents a distributed lock - stored in DynamoDB.
    """

    def __init__(self,
                 lock_client,
                 partition_key,
                 sort_key,
                 owner_name,
                 lease_duration_in_seconds,
                 record_version_number,
                 expiry_time,
                 additional_attributes={}
                 ):
        """
        :param DynamoDBLockClient lock_client: The client that "owns" this lock
        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param str owner_name: The owner name - typically from the lock_client
        :param float lease_duration_in_seconds: The lease duration - typically from the lock_client
        :param str record_version_number: Changes with every heartbeat - the "liveness" indicator
        :param float expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        """
        logger.debug('Creating a new instance of DynamoDBLock')
        self.lock_client = lock_client
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.owner_name = owner_name
        self.lease_duration_in_seconds = lease_duration_in_seconds
        self.record_version_number = record_version_number
        self.expiry_time = expiry_time
        self.additional_attributes = additional_attributes
        # additional properties
        self.thread_lock = threading.RLock()
        self.unique_identifier = urllib.quote(partition_key) + '|' + urllib.quote(sort_key)
        self.lease_duration = datetime.timedelta(seconds=lease_duration_in_seconds)


    def release(self, best_effort=False):
        """
        Calls the lock_client.release_lock(self, True) method

        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to False.
        """
        self.lock_client.release_lock(self, best_effort)


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


    def __str__(self):
        """
        Returns a readble string representation of this instance. 
        """
        return '%s::%s' % (self.__class__.__name__, self.__dict__)

