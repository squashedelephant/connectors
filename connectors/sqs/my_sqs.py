
from boto3 import resource
from botocore.exception import ClientError
from logging import getLogger
from sys import exc_info
from traceback import extract_tb

from connectors.sqs.error import SQSError
from connectors.sqs.message import SQSMessage

log = getLogger(__name__)

class SQSConnector:
    """
    as many BS and MS will communicate with AWS SQS, centralize access
    with this library
    mandatory args:
        region = AWS region
        access_key = AWS access key id
        secret_key = AWS secret access key
    """
    def __init__(self,
                 region=None,
                 access_key=None,
                 secret_key=None):
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key

    def _connect(self):
        """
        reach AWS SQS via credentials
        unable to identify retry, timeout mechanisms, it is always online
        """
        try:
            self.sqs = resource('sqs',
                                region_name=self.region,
                                aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key) 
            return
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return SQSError.unknown_exception(backtrace, str(e))

    def _set_queue(self, queue):
        """
        obtain id for existing AWS SQS 
        queue: AWS SQS queue
        """
        err_msg = self._connect()
        if err_msg:
            return err_msg
        try:
            if self.mq:
                return
            else:
                self.mq = self.sqs.get_queue_by_name(QueueName=queue)
            return
        except ClientError as e:
            if str(e).find('InvalidClientTokenId') >= 0:
                return SQSError.credentials_expired(self.region,
                                                    self.access_key,
                                                    self.secret_key)
            elif str(e).find('SignatureDoesNotMatch') >= 0:
                return SQSError.clock_skew(queue)
            elif str(e).find('NonExistentQueue') >= 0:
                return SQSError.no_such_queue(queue)

    def create(self, queue):
        """
        create a new AWS SQS 
        ignore if already exists
        queue: AWS SQS queue
        """
        err_msg = self._connect()
        if err_msg:
            return err_msg
        try:
            self.mq = self.sqs.create_queue(QueueName=queue,
                                            Attributes=self.attributes)
            return SQSMessage.aqueue_created()
        except ClientError as e:
            if str(e).find('InvalidClientTokenId') >= 0:
                return SQSError.credentials_expired(self.region,
                                                    self.access_key,
                                                    self.secret_key)
            elif str(e).find('SignatureDoesNotMatch') >= 0:
                return SQSError.clock_skew(queue)
        except QueueAlreadyExists as e:
            err_msg = self._set_queue(queue)
            if err_msg:
                return err_msg
            return SQSMessage.queue_created()

    def validate_item(self, item):
        """
        validate item format
        mandatory components:
            body: string
            metadata: JSON object
        """
        if isinstance(item, dict):
            mandatory_components = ['body', 'metadata']
            for key in mandatory_components:
                if key not in item:
                    return Error.invalid_item(item)
            return
        else:
            return Error.invalid_item(item)

    def insert(self, queue, item):
        """
        insert item into SQS queue
        queue: SQS queue
        item: dict with keys: body, metadata
        """
        err_msg = self.validate_item(item)
        if err_msg:
            return err_msg
        err_msg = self._set_queue(queue)
        if err_msg:
            return err_msg
        try:
            self.result = self.mq.send_message(MessageBody=item['body'],
                                               MessageAttributes=item['metadata'])    
            id = self.result.get('MessageId')
            md5 = self.result.get('MD5OfMessageBody')
            return SQSMessage.item_submitted(item, id, md5)
        except ClientError as e:
            if str(e).find('InvalidClientTokenId') >= 0:
                return SQSError.credentials_expired(self.region,
                                                    self.access_key,
                                                    self.secret_key)
            elif str(e).find('SignatureDoesNotMatch') >= 0:
                return SQSError.clock_skew(queue)

    def queue_lease_expired(cls, queue):
        pass

    def delete(self, queue, item):
        pass

    def get(self, queue):
        """
        retrieve one item from SQS queue and remove from the queue
        queue: SQS queue
        """
        err_msg = self._set_queue(queue)
        if err_msg:
            return err_msg
        try:
            self.item = mq.receive_messages(MaxNumberOfMessages=1,
                                            VisibilityTimeout=10
                                            WaitTimeSeconds=10):
            metadata = self.item[0].message_attributes.get('metadata')
            body = self.item[0].body
            self.item[0].delete()
            return SQSMessage.item_received(body, metadata)
        except ClientError as e:
            if str(e).find('InvalidClientTokenId') >= 0:
                return SQSError.credentials_expired(self.region,
                                                    self.access_key,
                                                    self.secret_key)
            elif str(e).find('SignatureDoesNotMatch') >= 0:
                return SQSError.clock_skew(queue)

