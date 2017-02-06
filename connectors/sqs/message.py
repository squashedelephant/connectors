
class SQSMessage:
    @classmethod
    def queue_created(cls):
        """
        Wrapper when new SQS queue created
        """
        return {'data': [],
                'reason': 'queue created successfully',
                'status_code': 4006}

    @classmethod
    def item_submitted(cls, item, id, md5):
        """
        Wrapper when new item submitted to the queue
        """
        reason = 'MessageId: {} sent with signature: {}'.format(id,
                                                                md5)
        return {'data': item,
                'reason': reason,
                'status_code': 4007}

    @classmethod
    def item_received(cls, body, metadata):
        """
        Wrapper when item retrieved from the queue
        """
        reason = 'Message retrieved with metadata: {}'.format(metadata)
        return {'data': body,
                'reason': reason,
                'status_code': 4008}

    @classmethod
    def no_item_found(cls):
        """
        Wrapper when no item received
        """
        return {'data': [],
                'reason': 'No item retrieved',
                'status_code': 4009}
