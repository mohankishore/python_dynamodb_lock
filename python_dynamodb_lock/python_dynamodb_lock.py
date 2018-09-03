# -*- coding: utf-8 -*-

"""
This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based AmazonDynamoDBLockClient library, and supports both coarse-grained
and fine-grained locking.
"""

import boto3
from botocore.exceptions import ClientError
import datetime
import socket
import time
import threading
import urllib
import uuid



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


    def __init__(self,
                 dynamodb_client,
                 table_name = _DEFAULT_TABLE_NAME,
                 partition_key_name = _DEFAULT_PARTITION_KEY_NAME,
                 sort_key_name = _DEFAULT_SORT_KEY_NAME,
                 owner_name = None,
                 lease_duration = _DEFAULT_LEASE_DURATION,
                 heartbeat_period = _DEFAULT_HEARTBEAT_PERIOD
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
        """
        self.uuid = uuid.uuid4()
        self.dynamodb_client = dynamodb_client
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name
        self.owner_name = owner_name if owner_name else socket.getfqdn() + self.uuid
        self.lease_duration = lease_duration
        self.heartbeat_period = heartbeat_period
        self.background_thread = self._start_background_thread()
        self.locks = {}
        self.shutting_down = False


    def _start_background_thread(self):
        """
        Creates and starts a daemon thread - that calls DynamoDBLockClient.run() method.
        """
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
        while (not self.shutting_down):
            start_time = datetime.datetime.utcnow()
            for id, lock in self.locks.items():
                self.send_heartbeat(lock)
            end_time = datetime.datetime.utcnow()

            elapsed_time = (end_time - start_time)
            if elapsed_time < self.heartbeat_period and not self.shutting_down:
                time.sleep( (self.heartbeat_period - elapsed_time).total_seconds() )


    def send_heartbeat(self, lock):
        """
        Renews the lease for the given lock.

        It actually just switches the record_version_number on the existing lock - which tells
        all other clients waiting for this lock that the current owner is still alive, and they
        effectively reset their timers (to wait for lease_duration from the time they see this
        new record_version_number)

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        with lock.thread_lock:
            try:
                self.dynamodb_client.update_item(
                    TableName=self.table_name,
                    Key={
                        self.partition_key_name: { 'S': lock.partition_key },
                        self.sort_key_name: { 'S': lock.sort_key }
                    },
                    UpdateExpression='SET #rvn = :new_rvn',
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn',
                    ExpressionAttributeNames={
                        '#rvn': 'record_version_number',
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                    },
                    ExpressionAttributeValues={
                        ':new_rvn': { 'S': str(uuid.uuid4()) },
                        ':old_rvn': { 'S': lock.record_version_number },
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
                     sort_key = None,
                     retry_period = datetime.timedelta(seconds=2),
                     retry_timeout = datetime.timedelta(seconds=5),
                     additional_attributes = {}
                     ):
        """
        Acquires a distributed DynaomDBLock for the given key(s).

        :param str partition_key:
        :param str sort_key:
        :param datetime.timedelta retry_period:
        :param datetime.timedelta retry_timeout:
        :param dict additional_attributes:
        :rtype: DynamoDBLock
        :return: A distributed lock instance
        """
        start_time = datetime.datetime.utcnow()
        last_record_version_number = None
        last_version_fetch_time = None
        while (True):
            existing_lock = DynamoDBLock() # TODO: get from db
            if (existing_lock is None or existing_lock.is_expired):
                print('TODO: try to put a new lock')
            else:
                if (existing_lock.record_version_number != last_record_version_number):
                    # keep track of the first time we saw this record_version_number
                    last_version_fetch_time = datetime.datetime.utcnow()
                else:
                    last_version_elapsed_time = datetime.datetime.utcnow() - last_version_fetch_time
                    # if the record_version_number has not changed for more than lease_duration period,
                    # it basically means that the owner thread/process has died.
                    if (last_version_elapsed_time > self.lease_duration):
                        print('TODO: try to put a new lock')
            total_elapsed_time = datetime.datetime.utcnow() - start_time
            # check if we have enough time to sleep and try again
            if (total_elapsed_time + retry_period < retry_timeout):
                time.sleep(retry_period.total_seconds())
            else:
                print('TODO: raise SomeError')


    def release_lock(self,
                     lock,
                     best_effort = False
                     ):
        """
        Releases the given lock - by deleting it from the database.

        :param DynamoDBLock lock: The lock instance that needs to be released
        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to False.
        """
        with lock.thread_lock:
            try:
                del self.locks[lock.unique_identifier]
                self.dynamodb_client.delete_item(
                    TableName=self.table_name,
                    Key={
                        self.partition_key_name: { 'S': lock.partition_key },
                        self.sort_key_name: { 'S': lock.sort_key }
                    },
                    ConditionExpression='attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn',
                    ExpressionAttributeNames={
                        '#rvn': 'record_version_number',
                        '#pk': self.partition_key_name,
                        '#sk': self.sort_key_name,
                    },
                    ExpressionAttributeValues={
                        ':rvn': { 'S': lock.record_version_number },
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


    def _release_all_locks(self):
        """
        Iterates over all the locks and releases each one.
        """
        for id, lock in self.locks.items():
            self.release_lock(lock)


    def close(self):
        """
        Shuts down the background thread and releases all locks.
        """
        self.shutting_down = True
        self.background_thread.join()
        self._release_all_locks()


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Calls the close() method
        """
        self.close()



class DynamoDBLock:
    """
    Represents a distributed lock - stored in DynamoDB.
    """

    def __init__(self,
                 record_version_number,
                 partition_key,
                 sort_key = None,
                 additional_attributes = {}
                 ):
        """
        :param str record_version_number:
        :param str partition_key:
        :param str sort_key:
        :param dict additional_attributes:
        """
        self.thread_lock = threading.RLock()
        self.record_version_number = record_version_number
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.additional_attributes = additional_attributes
        self.unique_identifier = urllib.quote(partition_key) + '|' + urllib.quote(sort_key)
