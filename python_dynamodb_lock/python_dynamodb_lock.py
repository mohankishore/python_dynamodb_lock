# -*- coding: utf-8 -*-

# TODO
"""
module comment
"""
import boto3
import botocore
import datetime
import socket
import time
import threading
import uuid


class DynamoDBLockClient:
    # TODO
    """
    class comment
    """

    DEFAULT_TABLE_NAME = 'lockTable'
    DEFAULT_PARTITION_KEY_NAME = 'key'
    DEFAULT_SORT_KEY_NAME = 'sort_key'
    DEFAULT_LEASE_DURATION = datetime.timedelta(seconds=30)
    DEFAULT_HEARTBEAT_PERIOD = datetime.timedelta(seconds=10)

    def __init__(self,
                 dynamodb_client,
                 table_name = DEFAULT_TABLE_NAME,
                 partition_key_name = DEFAULT_PARTITION_KEY_NAME,
                 sort_key_name = DEFAULT_SORT_KEY_NAME,
                 owner_name = None,
                 lease_duration = DEFAULT_LEASE_DURATION,
                 heartbeat_period = DEFAULT_HEARTBEAT_PERIOD
                 ):
        """
        The constructor that support default values for all/most of the arguments.
        
        :param dynamodb_client: boto3.DynamoDB.Client - mandatory argument
        :param table_name: str - defaults to 'lockTable' 
        :param partition_key_name: str - defaults to 'key'
        :param sort_key_name: str - defaults to 'sort_key'
        :param owner_name: str - defaults to hostname + uuid
        :param lease_duration: datetime.timedelta - The length of time that the lease for the lock 
                will be granted for. i.e. if there is no heartbeat for this period of time, then 
                the lock will be considered as expired. Defaults to 30 seconds.
        :param heartbeat_period: datetime.timedelta - How often to update DynamoDB to note that the 
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
            name='DynamoDBLockClient-' + uuid,
            target=self
        )
        background_thread.daemon = True
        background_thread.start()
        return background_thread

    def run(self):
        """
        Iterates over all the locks owned by this lock-client, and renews their leases.
        """
        while (True):
            if self.shutting_down: return None

            start_time = datetime.datetime.utcnow()
            for lock in self.locks.itervalues():
                self.send_heartbeat(lock)
            end_time = datetime.datetime.utcnow()

            if self.shutting_down: return None

            elapsed_time = (end_time - start_time)
            if elapsed_time < self.heartbeat_period:
                time.sleep( (self.heartbeat_period - elapsed_time).total_seconds() )

    def send_heartbeat(self, lock):
        """
        Renews the lease for the given lock
        
        :param lock: DynamoDBLock instance - that needs its lease to be renewed 
        """
        with lock.lock:
            try:
                # TODO actual impl
                print
                # update
                # table
                # keys
                # SET #ld = :ld, #rvn = :newRvn
                # attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn AND #on = :on
                # attribute-names
                # attribute-values
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # TODO: need to handle the case where the lock was "stolen" by someone else...
                    print
                else:
                    print
            except:
                print


    def close(self):
        """
        Shuts down the background thread and releases all locks. 
        """
        self.shutting_down = True
        self.background_thread.join()
        self.release_all_locks()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Calls the close() method
        """
        self.close()

    def acquire_lock(self,
                     partition_key,
                     sort_key = None,
                     retry_period = datetime.timedelta(seconds=2),
                     retry_timeout = datetime.timedelta(seconds=5),
                     additional_attributes = {}
                     ):
        """"""

    def release_lock(self,
                     lock,
                     best_effort = False
                     ):
        """"""
        with lock.lock:
            try:
                del self.locks[lock.uuid]
                # TODO actual impl
                # delete
                # table
                # keys
                # attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn AND #on = :on
                # attribute-names
                # attribute-values
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # TODO: need to handle the case where the lock was "stolen" by someone else...
                    print
                else:
                    # if not best_effort: raise error
                    print
            except:
                print


    # TODO: make this private?
    def release_all_locks(self):
        """"""
        for lock in self.locks.itervalues():
            self.release_lock(lock)

    # get_lock
    # get_all_locks



class DynamoDBLock:
    # TODO
    """
    class comment
    """

    def __init__(self,
                 partition_key,
                 sort_key = None,
                 retry_period = datetime.timedelta(seconds=2),
                 retry_timeout = datetime.timedelta(seconds=5),
                 additional_attributes = {}
                 ):
        # TODO
        """"""
