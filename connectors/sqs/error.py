
class SQSError:
    @classmethod
    def credentials_expired(cls, region, access_key, secret_key):
        """ 
        Wrapper for ClientError exception 
        region = AWS region
        access_key = AWS access key id
        secret_key = AWS secret access key
        """
        credentials = {'aws_region': region,
                       'aws_access_key_id': access_key,
                       'aws_secret_access_key': secret_key}
        err_msg = 'Credentials {} expired'.format(credentials)
        return {'data': [],
                'status_code': 4001,
                'reason': err_msg}

    @classmethod
    def clock_skew(cls, queue):
        """ 
        Wrapper for ClientError exception 
        queue = AWS SQS queue
        """
        err_msg = 'container time out of sync with SQS queue: {}'.format(queue)
        return {'data': [],
                'status_code': 4002,
                'reason': err_msg}

    @classmethod
    def queue_lease_expired(cls, queue):
        """ 
        Wrapper for ClientError exception 
        queue = AWS SQS queue
        """
        err_msg = 'Requested lease time expired'
        return {'data': [],
                'status_code': 4003,
                'reason': err_msg}

    @classmethod
    def no_such_queue(cls, queue):
        """
        Wrapper for NonSuchQueue exception 
        """
        err_msg = 'queue: {} does not exist'.format(queue)
        return {'data': [],
                'status_code': 4004,
                'reason': err_msg}

    @classmethod
    def invalid_item(cls, item):
        """
        Wrapper for item to place in queue
        mandatory keys in item:
            body: string
            metadata: JSON object
        """
        err_msg = 'item: {} has invalid format'.format(item)
        return {'data': [],
                'status_code': 4005,
                'reason': err_msg}

    @classmethod
    def unknown_exception(cls, bt, s):
        """
        Wrapper for unknown generic exception
        """
        return {'data': [],
                'status_code': 4099,
                'reason': {'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}

