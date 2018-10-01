# hack to enable relative imports
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# import the module/classes under test
from python_dynamodb_lock.python_dynamodb_lock import *

# import other dependencies
import argparse
import boto3

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

"""
This file is meant to be used after downloading and setting up a local DynamoDB instance.
Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
"""


def create_table(args):
    # DynamoDB
    ddb_client = boto3.client(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        aws_access_key_id='non-blank',
        aws_secret_access_key='non-blank',
        region_name='non-blank'
    )
    # Create Table
    DynamoDBLockClient.create_dynamodb_table(
        ddb_client,
        table_name=args.table_name,
        partition_key_name=args.partition_key_name,
        sort_key_name=args.sort_key_name,
        ttl_attribute_name=args.ttl_attribute_name,
        read_capacity=args.read_capacity,
        write_capacity=args.write_capacity
    )


def acquire_lock(args):
    # the one mandatory argument for acquiring a lock
    if not args.partition_key:
        raise RuntimeError('Need to provide --partition_key')

    # DynamoDB
    ddb_resource = boto3.resource(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        aws_access_key_id='non-blank',
        aws_secret_access_key='non-blank',
        region_name='non-blank'
    )
    # The Lock-Client
    lock_client = DynamoDBLockClient(
        ddb_resource,
        table_name=args.table_name,
        partition_key_name=args.partition_key_name,
        sort_key_name=args.sort_key_name,
        ttl_attribute_name=args.ttl_attribute_name,
        heartbeat_period=datetime.timedelta(seconds=args.heartbeat_period),
        safe_period=datetime.timedelta(seconds=args.safe_period),
        lease_duration=datetime.timedelta(seconds=args.lease_duration),
    )
    # The Lock itself
    lock = lock_client.acquire_lock(
        args.partition_key,
        sort_key=args.sort_key,
        retry_period=datetime.timedelta(seconds=args.retry_period),
        retry_timeout=datetime.timedelta(seconds=args.retry_timeout),
        app_callback=_app_callback
    )
    # And, hold as needed
    if args.hold_period > 0:
        time.sleep(args.hold_period)
        lock.release()
    else:
        while True: time.sleep(60)


def _app_callback(code, lock):
    logger.warning('CALLBACK: %s - %s', code, str(lock))


# basically, the main method
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Test app to verify the python_dynamodb_lock module',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        '--create_table',
        help='Create the DynamoDB table',
        action='store_true',
    )
    mode.add_argument(
        '--acquire_lock',
        help='Acquire a named lock',
        action='store_true',
    )

    common_group = parser.add_argument_group(title='(common arguments)')
    common_group.add_argument(
        '--table_name',
        help='Name of the DynamoDB table',
        required=False,
        default=DynamoDBLockClient._DEFAULT_TABLE_NAME
    )
    common_group.add_argument(
        '--partition_key_name',
        help='Name of the partition-key column',
        required=False,
        default=DynamoDBLockClient._DEFAULT_PARTITION_KEY_NAME
    )
    common_group.add_argument(
        '--sort_key_name',
        help='Name of the sort-key column',
        required=False,
        default=DynamoDBLockClient._DEFAULT_SORT_KEY_NAME
    )
    common_group.add_argument(
        '--ttl_attribute_name',
        help='Name of the TTL attribute',
        required=False,
        default=DynamoDBLockClient._DEFAULT_TTL_ATTRIBUTE_NAME
    )

    create_table_group = parser.add_argument_group(title='--create_table')
    create_table_group.add_argument(
        '--read_capacity',
        help='The max TPS for strongly-consistent reads',
        required=False,
        default=DynamoDBLockClient._DEFAULT_READ_CAPACITY,
        type=int
    )
    create_table_group.add_argument(
        '--write_capacity',
        help='The max TPS for write operations',
        required=False,
        default=DynamoDBLockClient._DEFAULT_WRITE_CAPACITY,
        type=int
    )

    acquire_lock_group = parser.add_argument_group(title='--acquire_lock')
    acquire_lock_group.add_argument(
        '--partition_key',
        help='Value for the partition-key column',
    )
    acquire_lock_group.add_argument(
        '--sort_key',
        help='Value for the sort-key column',
        required=False,
        default=DynamoDBLockClient._DEFAULT_SORT_KEY_VALUE
    )
    acquire_lock_group.add_argument(
        '--retry_period',
        help='How long to wait between retries?',
        required=False,
        default=2,
        type=int
    )
    acquire_lock_group.add_argument(
        '--retry_timeout',
        help='How long to retry before giving up?',
        required=False,
        default=10,
        type=int
    )
    acquire_lock_group.add_argument(
        '--hold_period',
        help='How long to hold the lock before releasing it?',
        required=False,
        default=-1,
        type=int
    )
    acquire_lock_group.add_argument(
        '--heartbeat_period',
        help='How long to wait between consecutive heartbeat-all-locks?',
        required=False,
        default=DynamoDBLockClient._DEFAULT_HEARTBEAT_PERIOD.total_seconds(),
        type=int
    )
    acquire_lock_group.add_argument(
        '--safe_period',
        help='How long can a lock go without a successful heartbeat - before it is deemed to be in danger?',
        required=False,
        default=DynamoDBLockClient._DEFAULT_SAFE_PERIOD.total_seconds(),
        type=int
    )
    acquire_lock_group.add_argument(
        '--lease_duration',
        help='How long can a lock go without a successful heartbeat - before it is deemed to be abandoned?',
        required=False,
        default=DynamoDBLockClient._DEFAULT_LEASE_DURATION.total_seconds(),
        type=int
    )

    args = parser.parse_args()
    if args.create_table:
        create_table(args)
    elif args.acquire_lock:
        acquire_lock(args)
    else:
        logger.error('Invalid arguments: %s', str(args))

