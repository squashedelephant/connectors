
from cassandra import ConsistencyLevel as CL, InvalidRequest, OperationTimedOut
from cassandra.cqlengine import connection
from cassandra.policies import ConstantReconnectionPolicy, RetryPolicy
from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy
from logging import getLogger
from sys import exc_info
from traceback import extract_tb

from connectors.cassandra.error import CassandraError, CassandraReadError
from connectors.cassandra.error import CassandraWriteError
from connectors.cassandra.message import CassandraRead, CassandraWrite

log = getLogger(__name__)

class CQLConnector:
    """
    as many MS will communicate with Cassandra, centralize access
    with this library
    mandatory args:
        hosts = list of 1+ nodes in Cassandra cluster
        port = native
        keyspace = service specific keyspace
        cql_version = CQL language version
        local_env = True or False
        connect_timeout = seconds allowed to try connection
    """

    def __init__(self,
                 hosts=[],
                 port=9042,
                 keyspace=None,
                 cql_version='3.4.0',
                 local_env=False,
                 connect_timeout=5):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.cql_version = cql_version
        self.local_env = local_env
        self.connect_timeout = connect_timeout
        self.cluster = None
        self.session = None

    def _local_connect(self):
        """
        assumes single node Cassandra cluster
        """
        connection.setup(hosts=self.hosts,
                         default_keyspace=self.keyspace,
                         consistency=CL.ONE,
                         port=self.port,
                         cql_version=self.cql_version,
                         lazy_connect=False,
                         retry_connect=True,
                         compression=True,
                         auth_provider=None,
                         load_balancing_policy=RoundRobinPolicy(),
                         protocol_version=4,
                         executor_threads=2,
                         reconnection_policy=ConstantReconnectionPolicy(3.0, 5),
                         default_retry_policy=RetryPolicy(),
                         conviction_policy_factory=None,
                         metrics_enabled=False,
                         connection_class=None,
                         ssl_options=None,
                         sockopts=None,
                         max_schema_agreement_wait=10,
                         control_connection_timeout=2.0,
                         idle_heartbeat_interval=30,
                         schema_event_refresh_window=2,
                         topology_event_refresh_window=10,
                         connect_timeout=self.connect_timeout)
        return

    def _production_connect(self):
        """
        assumes multiple node Cassandra cluster
        """
        connection.setup(hosts=self.hosts,
                         default_keyspace=self.keyspace,
                         consistency=CL.LOCAL_QUORUM,
                         port=self.port,
                         cql_version=self.cql_version,
                         lazy_connect=True,
                         retry_connect=False,
                         compression=True,
                         auth_provider=None,
                         load_balancing_policy=DCAwareRoundRobinPolicy(
                             local_dc='dc1',
                             used_hosts_per_remote_dc=0),
                         protocol_version=4,
                         executor_threads=2,
                         reconnection_policy=None,
                         default_retry_policy=None,
                         conviction_policy_factory=None,
                         metrics_enabled=False,
                         connection_class=None,
                         ssl_options=None,
                         sockopts=None,
                         max_schema_agreement_wait=10,
                         control_connection_timeout=2.0,
                         idle_heartbeat_interval=30,
                         schema_event_refresh_window=2,
                         topology_event_refresh_window=10,
                         connect_timeout=self.connect_timeout)
        return

    def _connect(self):
        """
        settings differ depending on cluster selected
        """
        try:
            if self.local_env:
                self._local_connect()
            else:
                self._production_connect()
            self.session = connection.get_session()
            return
        except connection.NoHostAvailable as e:
            return CassandraError.no_host_available(self.hosts)
        except OperationTimedOut as e:
            return CassandraError.operation_timeout(str(e))
        except InvalidRequest as e:
            return CassandraError.invalid_request(self.keyspace, str(e))
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return CassandraError.unknown_exception(backtrace, str(e))

    def _disconnect(self):
        """
        despite the object names, this action truly disables only the executor
        threads of the existing session closing the session and does not bring
        down the Cassandra cluster itself
        """
        try:
            self.session.cluster.shutdown()
            self.session.shutdown()
            return
        except connection.NoHostAvailable as e:
            return CassandraError.no_host_available(self.hosts)
        except OperationTimedOut as e:
            return CassandraError.operation_timeout(str(e))
        except InvalidRequest as e:
            return CassandraError.invalid_request(self.keyspace, str(e))
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return CassandraError.unknown_exception(backtrace, str(e))

    def _get_uuid_columns(self, values):
        """
        walk elements of list or keys of dict identifying columns
        with data that should be formatted as type UUID
        """
        uuid_columns = []
        for key in values:
            if key.endswith('_id'):
                uuid_columns.append(key)
        return uuid_columns
        
    def _format_row_data(self, resultset, uuid_columns):
        """
        convert UUID to str(UUID) before return row
        """
        rows = []
        columns = resultset.column_names
        for i in range(len(resultset.current_rows)):
            row = {}
            for column in range(len(columns)):
                if columns[column] in uuid_columns:
                    row[columns[column]] = str(resultset[i][columns[column]])
                else:
                    row[columns[column]] = resultset[i][columns[column]]
            rows.append(row)
        return rows

    def write(self, sql=None, values=None):
        """
        process any Cassandra CQL DML statement that changes data
        ie. insert, upsert, update, delete
        mandatory args
        sql = Cassandra CQL statement or statement template
        ie. if: CREATE TABLE T(id bigint, name text);
            sql = "INSERT INTO T(id, name) VALUES({}, {});".format(id, name)
        optional args
        values = dictionary of non integer or string values inserted by template
        ie. if: CREATE TABLE T(id uuid, deleted boolean);
            sql = "INSERT INTO T(id, deleted) VALUES(%(id)s, %(deleted)s);"
            values = {'id': UUID(id), 'deleted': True}
        """
        rows = []
        err_msg = self._connect()
        if err_msg:
            return err_msg
        try:
            if self.local_env:
                statement = connection.SimpleStatement(
                    sql, consistency_level=CL.ONE)
            else:
                statement = connection.SimpleStatement(
                    sql, consistency_level=CL.LOCAL_QUORUM)
            if values:
                resultset = self.session.execute(statement, values)
            else:
                resultset = self.session.execute(statement)
            if len(resultset.current_rows) == 0:
                return CassandraWrite.object_created()
            else:
                uuid_columns = self._get_uuid_columns(values)
                while True:
                    if not resultset.has_more_pages:
                        rows = self._format_row_data(resultset, uuid_columns)
                        break
                    rows.append(self._format_row_data(resultset, uuid_columns))
                    resultset.next()
            err_msg = self._disconnect()
            if err_msg:
                return err_msg
            if len(rows) == 1:
                return CassandraWrite.one_row_found(rows[0])
            else:
                return CassandraWrite.many_rows_found(rows)
        except connection.NoHostAvailable as e:
            return CassandraError.no_host_available(self.hosts)
        except OperationTimedOut as e:
            return CassandraError.operation_timeout(str(e))
        except InvalidRequest as e:
            return CassandraError.invalid_request(self.keyspace, str(e))
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return CassandraWriteError.unknown_exception(sql, values, backtrace, str(e))

    def new_read(self, sql=None):
        """
        because CQL table search is restricted to columns defined in the primary
        and cluster keys, writes will be duplicated in ElasticSearch and all
        reads will occur only from ElasticSearch as any column may be chosen
        
        """
        pass

    def read(self, sql=None, values=None):
        """
        process any Cassandra CQL DQL statement 
        ie. select
        mandatory args
        sql = Cassandra CQL statement or statement template
        ie. if: CREATE TABLE T(id bigint, name text);
            sql = "SELECT name FROM T WHERE id={};".format(id)
        optional args
        values = dictionary of non integer or string values inserted by template
        ie. if: CREATE TABLE T(id uuid, deleted boolean);
            sql = "SELECT INTO T(id, deleted) VALUES(%(id)s, %(deleted)s);"
            values = {'id': UUID(id), 'deleted': True}
        """
        rows = []
        err_msg = self._connect()
        if err_msg:
            return err_msg
        try:
            if self.local_env:
                statement = connection.SimpleStatement(
                    sql, consistency_level=CL.ONE)
            else:
                statement = connection.SimpleStatement(
                    sql, consistency_level=CL.LOCAL_QUORUM)
            if values:
                resultset = self.session.execute(statement, values)
            else:
                resultset = self.session.execute(statement)
            if len(resultset.current_rows) == 0:
                return CassandraRead.no_rows_found()
            else:
                uuid_columns = self._get_uuid_columns(values)
                while True:
                    if not resultset.has_more_pages:
                        rows = self._format_row_data(resultset, uuid_columns)
                        break
                    rows.append(self._format_row_data(resultset, uuid_columns))
                    resultset.next()
            err_msg = self._disconnect()
            if err_msg:
                return err_msg
            if len(rows) == 1:
                return CassandraRead.one_row_found(rows[0])
            else:
                return CassandraRead.many_rows_found(rows)
        except connection.NoHostAvailable as e:
            return CassandraError.no_host_available(self.hosts)
        except OperationTimedOut as e:
            return CassandraError.operation_timeout(str(e))
        except InvalidRequest as e:
            return CassandraError.invalid_request(self.keyspace, str(e))
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return CassandraReadError.unknown_exception(sql, values, backtrace, str(e))
