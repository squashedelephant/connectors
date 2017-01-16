
class ElasticSearchError:

    @classmethod
    def no_host_available(cls, host, port):
        return {'data': [],
                'status_code': 3001,
                'reason': 'Unable to reach {}:{}'.format(host, port)}

    @classmethod
    def unable_to_create_index(cls, index):
        err_msg = 'Unable to create index: {}'.format(index)
        return {'data': [],
                'status_code': 3002,
                'reason': err_msg}

    @classmethod
    def missing_index(cls, index):
        return {'data': [],
                'status_code': 3003,
                'reason': 'Index: {} not created yet'.format(index)}

    @classmethod
    def invalid_request(cls, s):
        return {'data': [],
                'status_code': 3004,
                'reason': 'Invalid query: {}'.format(s)}

    @classmethod
    def unknown_exception(cls, bt, s):
        return {'data': [],
                'status_code': 3005,
                'reason': {'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}

class ElasticSearchReadError:
    @classmethod
    def unknown_exception(cls, dsl, fields, bt, s):
        return {'data': [],
                'status_code': 3005,
                'reason': {'dsl: {}'.format(dsl),
                           'fields: {}'.format(fields),
                           'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}

class ElasticSearchWriteError:
    @classmethod
    def unknown_exception(cls, doc_id, body, bt, s):
        return {'data': [],
                'status_code': 3005,
                'reason': {'doc_id: {}'.format(doc_id),
                           'body: {}'.format(body),
                           'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}


