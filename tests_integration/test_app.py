# hack to enable relative imports
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# import the module/classes under test
from python_dynamodb_lock.python_dynamodb_lock import *

# import other dependencies
import boto3

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def do_main():
    ddb_resource = boto3.resource('dynamodb',
                              endpoint_url='http://localhost:8000',
                              aws_access_key_id='non-blank',
                              aws_secret_access_key='non-blank',
                              region_name='non-blank')
    lock_client = DynamoDBLockClient(ddb_resource)
    lock = lock_client.acquire_lock('key-4', retry_timeout=datetime.timedelta(seconds=60))
    time.sleep(25)
    lock.release()


# basically, the main method
if __name__ == "__main__":
    do_main()
