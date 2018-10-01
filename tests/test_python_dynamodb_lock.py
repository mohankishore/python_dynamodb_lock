#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `python_dynamodb_lock` package."""


import unittest
from unittest import mock
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


from python_dynamodb_lock.python_dynamodb_lock import *


class TestDynamoDBLockClient(unittest.TestCase):
    """Tests for `python_dynamodb_lock` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        self.app_callbacks = []
        self.ddb_resource = mock.MagicMock(name='ddb_resource')
        self.lock_client = DynamoDBLockClient(
            self.ddb_resource,
            heartbeat_period=datetime.timedelta(milliseconds=100),
            safe_period=datetime.timedelta(milliseconds=600),
            lease_duration=datetime.timedelta(milliseconds=1000),
            expiry_period=datetime.timedelta(milliseconds=5000),
        )
        # switch the table reference; easier than patching etc.
        self.ddb_table = mock.MagicMock(name='ddb_table')
        self.lock_client._dynamodb_table = self.ddb_table

    def tearDown(self):
        """Tear down test fixtures, if any."""
        self.lock_client.close()

    def app_callback(self, code, lock):
        """Just adds the (code, lock) tuple to an in-memory array"""
        self.app_callbacks.append( (code, lock) )


    # __init__ tests

    def test_minimal_args_client(self):
        minimal_args_client = DynamoDBLockClient(self.ddb_table)
        self.assertIsNotNone(minimal_args_client)


    # send_heartbeat tests

    def test_background_threads(self):
        heartbeat_sender = self.lock_client._heartbeat_sender_thread
        self.assertIsNotNone(heartbeat_sender)
        self.assertTrue(heartbeat_sender.isDaemon())
        self.assertTrue(heartbeat_sender.isAlive())
        heartbeat_checker = self.lock_client._heartbeat_checker_thread
        self.assertIsNotNone(heartbeat_checker)
        self.assertTrue(heartbeat_checker.isDaemon())
        self.assertTrue(heartbeat_checker.isAlive())
        # now, close the client
        self.lock_client.close()
        # and check that the threads are also shutdown
        self.assertFalse(heartbeat_sender.isAlive())
        self.assertFalse(heartbeat_checker.isAlive())


    def test_send_heartbeat_success(self):
        self.ddb_table.update_item = mock.MagicMock('update_item')
        self.lock_client.acquire_lock('key')
        time.sleep(200/1000) # 200 millis
        self.ddb_table.update_item.assert_called()


    def test_send_heartbeat_lock_stolen(self):
        self.ddb_table.update_item = mock.MagicMock('update_item')
        self.lock_client.acquire_lock('key', app_callback=self.app_callback)
        time.sleep(200/1000)
        self.ddb_table.update_item.side_effect = ClientError({
            'Error': { 'Code': 'ConditionalCheckFailedException' }
        }, 'update_item')
        time.sleep(200/1000)
        self.ddb_table.update_item.side_effect = None
        self.assertEqual(len(self.lock_client._locks), 0)
        self.assertEqual(len(self.app_callbacks), 1)
        (code, lock) = self.app_callbacks.pop(0)
        self.assertEqual(code, DynamoDBLockError.LOCK_STOLEN)


    def test_send_heartbeat_ddb_error(self):
        self.ddb_table.update_item = mock.MagicMock('update_item')
        self.lock_client.acquire_lock('key', app_callback=self.app_callback)
        time.sleep(200/1000)
        self.ddb_table.update_item.side_effect = ClientError({
            'Error': { 'Code': 'SomeOtherDynamoDBError' }
        }, 'update_item')
        time.sleep(200/1000)
        self.ddb_table.update_item.side_effect = None
        self.assertEqual(len(self.app_callbacks), 0) # ignore other DDB Errors


    def test_send_heartbeat_other_error(self):
        self.ddb_table.update_item = mock.MagicMock('update_item')
        self.lock_client.acquire_lock('key', app_callback=self.app_callback)
        time.sleep(200/1000)
        self.ddb_table.update_item.side_effect = RuntimeError('TestError')
        time.sleep(200/1000)
        self.ddb_table.update_item.side_effect = None
        self.assertEqual(len(self.app_callbacks), 0) # ignore other Runtime Errors


    # this tests the heartbeat_checker_thread and the app_callback_executor
    def test_send_heartbeat_in_danger(self):
        self.ddb_table.update_item = mock.MagicMock('update_item')
        self.ddb_table.update_item.side_effect = RuntimeError('TestError')
        self.lock_client.acquire_lock('key', app_callback=self.app_callback)
        time.sleep(850/1000)
        self.ddb_table.update_item.side_effect = None
        self.assertTrue(len(self.app_callbacks) == 1)
        (code, lock) = self.app_callbacks.pop(0)
        self.assertEqual(code, DynamoDBLockError.LOCK_IN_DANGER)


    # acquire_lock tests

    def test_acquire_lock_success(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        lock = self.lock_client.acquire_lock('key')
        self.assertIsNotNone(lock)
        self.ddb_table.get_item.assert_called()
        self.ddb_table.put_item.assert_called()
        locks = self.lock_client._locks
        self.assertEqual(len(locks), 1)
        self.assertTrue(lock.unique_identifier in locks)
        self.assertEqual(locks[lock.unique_identifier], lock)


    def test_acquire_lock_after_close(self):
        self.lock_client.close()
        try:
            self.lock_client.acquire_lock('key')
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.CLIENT_SHUTDOWN)


    def test_acquire_lock_after_release(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.get_item.side_effect = [
            # first call, return a valid item
            {
                'Item': {
                    'lock_key': 'key',
                    'sort_key': '-',
                    'owner_name': 'owner',
                    'lease_duration': 0.3,
                    'record_version_number': 'xyz',
                    'expiry_time': 100,
                }
            },
            # second call, act as if its been deleted
            {}
        ]
        start_time = time.monotonic()
        lock = self.lock_client.acquire_lock('key', retry_period=datetime.timedelta(milliseconds=100))
        end_time = time.monotonic()
        self.assertIsNotNone(lock)
        self.assertTrue((end_time - start_time) * 1000 >= 100)


    def test_acquire_lock_after_lease_expires(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.get_item.side_effect = lambda **kwargs: {
            'Item': {
                'lock_key': 'key',
                'sort_key': '-',
                'owner_name': 'owner',
                'lease_duration': 0.3,
                'record_version_number': 'xyz',
                'expiry_time': 100,
            }
        }
        start_time = time.monotonic()
        lock = self.lock_client.acquire_lock('key', retry_period=datetime.timedelta(milliseconds=100))
        end_time = time.monotonic()
        self.assertIsNotNone(lock)
        self.assertTrue((end_time - start_time) * 1000 >= 300)


    def test_acquire_lock_retry_timeout(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.get_item.side_effect = lambda **kwargs: {
            'Item': {
                'lock_key': 'key',
                'sort_key': '-',
                'owner_name': 'owner',
                'lease_duration': 0.6,
                'record_version_number': 'xyz',
                'expiry_time': 100,
            }
        }
        start_time = time.monotonic()
        try:
            self.lock_client.acquire_lock(
                'key',
                retry_period=datetime.timedelta(milliseconds=100),
                retry_timeout=datetime.timedelta(milliseconds=300))
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            end_time = time.monotonic()
            self.assertEqual(e.code, DynamoDBLockError.ACQUIRE_TIMEOUT)
            self.assertTrue((end_time - start_time) * 1000 >= 200)
            # at 220ms, it would error out, instead of sleeping for another 100ms

    def test_acquire_lock_race_condition(self):
        # test the get-none, put-error, retry, get-none, put-success case
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        self.ddb_table.get_item.side_effect = lambda **kwargs: {}
        self.ddb_table.put_item.side_effect = [
            ClientError({
                'Error': { 'Code': 'ConditionalCheckFailedException' }
            }, 'put_item'),
            {}
        ]
        start_time = time.monotonic()
        lock = self.lock_client.acquire_lock('key', retry_period=datetime.timedelta(milliseconds=100))
        end_time = time.monotonic()
        self.assertIsNotNone(lock)
        self.assertTrue((end_time - start_time) * 1000 >= 100)

    def test_acquire_lock_ddb_error(self):
        # test the get-none, put-error
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        self.ddb_table.get_item.side_effect = lambda **kwargs: {}
        self.ddb_table.put_item.side_effect = ClientError({
            'Error': { 'Code': 'SomeOtherDynamoDBError' }
        }, 'put_item')
        try:
            self.lock_client.acquire_lock('key', retry_period=datetime.timedelta(milliseconds=100))
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.UNKNOWN)

    def test_acquire_lock_other_error(self):
        # test the get-none, put-error
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        self.ddb_table.get_item.side_effect = lambda **kwargs: {}
        self.ddb_table.put_item.side_effect = RuntimeError('TestError')
        try:
            self.lock_client.acquire_lock('key', retry_period=datetime.timedelta(milliseconds=100))
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.UNKNOWN)


    # release_lock tests

    def test_release_lock_success(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        lock = self.lock_client.acquire_lock('key')
        self.lock_client.release_lock(lock)
        self.assertTrue(lock.unique_identifier not in self.lock_client._locks)
        self.ddb_table.delete_item.assert_called_once()


    def test_release_lock_not_owned(self):
        other_lock_client = DynamoDBLockClient(self.ddb_table)
        other_lock = other_lock_client.acquire_lock('key')
        try:
            self.lock_client.release_lock(other_lock, best_effort=False)
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.LOCK_NOT_OWNED)


    def test_release_lock_after_stolen(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        self.ddb_table.delete_item.side_effect = [
            ClientError({
                'Error': { 'Code': 'ConditionalCheckFailedException' }
            }, 'delete_item'),
        ]
        lock = self.lock_client.acquire_lock('key')
        try:
            self.lock_client.release_lock(lock, best_effort=False)
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.LOCK_STOLEN)
            self.assertTrue(lock.unique_identifier not in self.lock_client._locks)

    def test_release_lock_ddb_error(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        self.ddb_table.delete_item.side_effect = [
            ClientError({
                'Error': { 'Code': 'SomeOtherDynamoDBError' }
            }, 'delete_item'),
        ]
        lock = self.lock_client.acquire_lock('key')
        try:
            self.lock_client.release_lock(lock, best_effort=False)
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.UNKNOWN)
            self.assertTrue(lock.unique_identifier not in self.lock_client._locks)

    def test_release_lock_other_error(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        self.ddb_table.delete_item.side_effect = RuntimeError('TestError')
        lock = self.lock_client.acquire_lock('key')
        try:
            self.lock_client.release_lock(lock, best_effort=False)
            self.fail('Expected an error')
        except DynamoDBLockError as e:
            self.assertEqual(e.code, DynamoDBLockError.UNKNOWN)
            self.assertTrue(lock.unique_identifier not in self.lock_client._locks)


    # release_lock tests - with best_effort=True

    def test_best_effort_release_lock_not_owned(self):
        other_lock_client = DynamoDBLockClient(self.ddb_table)
        other_lock = other_lock_client.acquire_lock('key')
        self.lock_client.release_lock(other_lock)


    def test_best_effort_release_lock_after_stolen(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        self.ddb_table.delete_item.side_effect = [
            ClientError({
                'Error': { 'Code': 'ConditionalCheckFailedException' }
            }, 'delete_item'),
        ]
        lock = self.lock_client.acquire_lock('key')
        self.lock_client.release_lock(lock)
        self.assertTrue(lock.unique_identifier not in self.lock_client._locks)

    def test_best_effort_release_lock_ddb_error(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        self.ddb_table.delete_item.side_effect = [
            ClientError({
                'Error': { 'Code': 'SomeOtherDynamoDBError' }
            }, 'delete_item'),
        ]
        lock = self.lock_client.acquire_lock('key')
        self.lock_client.release_lock(lock)
        self.assertTrue(lock.unique_identifier not in self.lock_client._locks)

    def test_best_effort_release_lock_other_error(self):
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        self.ddb_table.delete_item.side_effect = RuntimeError('TestError')
        lock = self.lock_client.acquire_lock('key')
        self.lock_client.release_lock(lock)
        self.assertTrue(lock.unique_identifier not in self.lock_client._locks)


    # lock-to-item serialize/deserialize tests

    def test_lock_to_item(self):
        lock = BaseDynamoDBLock('p', 's', 'o', 5, 'r', 10, { 'k': 'v'})
        item = self.lock_client._get_item_from_lock(lock) or {}
        self.assertEqual(item[self.lock_client._partition_key_name], 'p')
        self.assertEqual(item[self.lock_client._sort_key_name], 's')
        self.assertEqual(item['owner_name'], 'o')
        self.assertEqual(item['lease_duration'], 5)
        self.assertEqual(item['record_version_number'], 'r')
        self.assertEqual(item['expiry_time'], 10)
        self.assertEqual(item['k'], 'v')


    def test_item_to_lock(self):
        item = {
            self.lock_client._partition_key_name: 'p2',
            self.lock_client._sort_key_name: 's2',
            'owner_name': 'o2',
            'lease_duration': 52,
            'record_version_number': 'r2',
            'expiry_time': 102,
            'k2': 'v2'
        }
        lock = self.lock_client._get_lock_from_item(item)
        self.assertEqual(lock.partition_key, 'p2')
        self.assertEqual(lock.sort_key, 's2')
        self.assertEqual(lock.owner_name, 'o2')
        self.assertEqual(lock.lease_duration, 52)
        self.assertEqual(lock.record_version_number, 'r2')
        self.assertEqual(lock.expiry_time, 102)
        self.assertDictEqual(lock.additional_attributes, { 'k2': 'v2' })


    # close() tests

    def test_close_without_release_locks(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        lock = self.lock_client.acquire_lock('key')
        self.lock_client.close()
        self.assertTrue(lock.unique_identifier in self.lock_client._locks)


    def test_close_with_release_locks(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        lock = self.lock_client.acquire_lock('key')
        self.lock_client.close(release_locks=True)
        self.assertFalse(lock.unique_identifier in self.lock_client._locks)


    # context-manager methods

    def test_lock_with_enter_exit(self):
        self.ddb_table.get_item = mock.MagicMock('get_item')
        self.ddb_table.put_item = mock.MagicMock('put_item')
        self.ddb_table.delete_item = mock.MagicMock('delete_item')
        with self.lock_client.acquire_lock('key') as lock:
            self.assertIsNotNone(lock)
            self.ddb_table.get_item.assert_called()
            self.ddb_table.put_item.assert_called()
            print('Lock: %s' % (str(lock)))
        self.ddb_table.delete_item.assert_called()

