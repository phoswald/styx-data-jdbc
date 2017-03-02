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

    // TODO: support multiple concurrent transactions (and thus, JDBC connections)!
    //
    // A stateless implementation would be even better, but DataSource is a tricky interface to implement
    // and opening a new connection for each transaction breaks tests for in-memory databases with volatile state.

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
            try {
                closeStatements();
            } finally {
                connection.close();
            }
        } catch (SQLException e) {
            throw new JdbcException("Failed to close JDBC connection for " + url, e);
        }
    }

    private void closeStatements() throws SQLException {
        // Probably not necessary as statements should be closed when the connection is closed,
        // but some drivers are known to be leaky (especially Oracle).
        for(PreparedStatement statement : statements.values()) {
            statement.close();
        }
        statements.clear();
    }

    @Override
    public DatabaseTransaction openReadTransaction() {
        return new JdbcTransaction(url, connection, statements, true);
    }

    @Override
    public DatabaseTransaction openWriteTransaction() {
        return new JdbcTransaction(url, connection, statements, false);
    }
}
