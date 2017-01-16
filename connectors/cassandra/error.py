
class CassandraError:
    @classmethod
    def no_host_available(cls, hosts):
        """
        Wrapper for NoHostAvailable exception 
        """
        err_msg = 'Unable to reach Cassandra at {}:9042'.format(hosts)
        return {'data': [],
                'status_code': 2001,
                'reason': err_msg}

    @classmethod
    def operation_timeout(cls, s):
        """
        Wrapper for OperationTimeout exception 
        """
        return {'data': [],
                'status_code': 2002,
                'reason': s}

    @classmethod
    def invalid_request(cls, keyspace, s):
        """
        Wrapper for InvalidRequest exception 
        """
        err_msg = 'Keyspace: {} not loaded'.format(keyspace)
        if s.find('Keyspace') >= 0:
            return {'data': [],
                    'status_code': 2003,
                    'reason': err_msg}
        else:
            return {'data': [],
                    'status_code': 2004,
                    'reason': s}

    @classmethod
    def unknown_exception(cls, bt, s):
        """
        Wrapper for unknown generic exception 
        """
        return {'data': [],
                'status_code': 2005,
                'reason': {'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}

class CassandraReadError:
    @classmethod
    def unknown_exception(cls, sql, v, bt, s):
        """
        Wrapper for unknown generic exception 
        """
        return {'data': [],
                'status_code': 2005,
                'reason': {'sql: {}'.format(sql),
                           'values: {}'.format(v),
                           'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}

class CassandraWriteError:
    @classmethod
    def unknown_exception(cls, sql, v, bt, s):
        """
        Wrapper for unknown generic exception 
        """
        return {'data': [],
                'status_code': 2005,
                'reason': {'sql: {}'.format(sql),
                           'values: {}'.format(v),
                           'backtrace: {}'.format(bt),
                           'e: {}'.format(s)}}
