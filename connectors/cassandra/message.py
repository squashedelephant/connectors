
class CassandraWrite:
    @classmethod
    def object_created(cls):
        """
        Wrapper when new object created
        """
        return {'data': [],
                'reason': 'object created successfully',
                'status_code': 2006}

    @classmethod
    def one_row_found(cls, row):
        """
        Wrapper when only one row detected
        """
        return {'data': row,
                'reason': 'OK',
                'status_code': 2007}

    @classmethod
    def many_rows_found(cls, rows):
        """
        Wrapper when multiple rows detected
        """
        return {'data': rows,
                'reason': 'OK',
                'status_code': 2008}

class CassandraRead:
    @classmethod
    def no_rows_found(cls):
        """
        Wrapper when no rows detected
        """
        return {'data': [],
                'reason': 'No rows found',
                'status_code': 2009}

    @classmethod
    def one_row_found(cls, row):
        """
        Wrapper when only one row detected
        """
        return {'data': row,
                'reason': 'OK',
                'status_code': 2010}

    @classmethod
    def many_rows_found(cls, rows):
        """
        Wrapper when multiple rows detected
        """
        return {'data': rows,
                'reason': 'OK',
                'status_code': 2011}

