package styx.data.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import styx.data.db.Database;
import styx.data.db.DatabaseTransaction;

class JdbcDatabase implements Database {

    private final String url;
    private final Connection connection;
    private final Map<String, PreparedStatement> statements = new HashMap<>();

    JdbcDatabase(String url) {
        try {
            this.url = url;
            this.connection = DriverManager.getConnection(url);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new JdbcException("Failed to open JDBC connection for " + url, e);
        }
    }

    @Override
    public void close() {
        try {
            // TODO (semantics): where to close connection, is styx.data.db.Database a resource or a service!?
            // TODO (JDBC spec): do we have to close the statements?
            connection.close();
        } catch (SQLException e) {
            throw new JdbcException("Failed to close JDBC connection for " + url, e);
        }
    }

    @Override
    public DatabaseTransaction openReadTransaction() {
        return openWriteTransaction();
    }

    @Override
    public DatabaseTransaction openWriteTransaction() {
        return new JdbcTransaction(url, connection, statements);
    }
}
