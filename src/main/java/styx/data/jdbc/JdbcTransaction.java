package styx.data.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import styx.data.db.DatabaseTransaction;
import styx.data.db.Path;
import styx.data.db.Row;

class JdbcTransaction implements DatabaseTransaction {

    private final String url;
    private final Connection connection;
    private final Map<String, PreparedStatement> statements;
    private final boolean readOnly;
    private boolean commit;

    JdbcTransaction(String url, Connection connection, Map<String, PreparedStatement> statements, boolean readOnly) {
        this.url = url;
        this.connection = connection;
        this.statements = statements;
        this.readOnly = readOnly;
        this.commit = false;
        try {
            checkAndCreateTable();
        } catch (SQLException e) {
            close();
            throw new JdbcException("Failed to check or create database table for " + url, e);
        }
    }

    @Override
    public void close() {
        try {
            if(!readOnly && commit) {
                connection.commit();
            } else {
                connection.rollback();
            }
        } catch (SQLException e) {
            throw new JdbcException("Failed to commit or rollback JDBC transaction for " + url, e);
        }
    }

    @Override
    public void markCommit() {
        commit = true;
    }

    @Override
    public Stream<Row> selectAll() {
        try {
            PreparedStatement statement = makePreparedStatement("SELECT PARENT_, KEY_, SUFFIX_, VALUE_ FROM STYX_DATA ORDER BY PARENT_, KEY_");
            List<Row> rows = new ArrayList<>();
            try(ResultSet result = statement.executeQuery()) {
                while(result.next()) {
                    rows.add(createRow(result));
                }
            }
            return rows.stream();
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute SELECT query.", e);
        }
    }

    @Override
    public Optional<Row> selectSingle(Path parent, String key) {
        try {
            PreparedStatement statement = makePreparedStatement("SELECT PARENT_, KEY_, SUFFIX_, VALUE_ FROM STYX_DATA WHERE PARENT_=? AND KEY_=?");
            statement.setString(1, parent.encode());
            statement.setString(2, key);
            try(ResultSet result = statement.executeQuery()) {
                if(result.next()) {
                    return Optional.of(createRow(result));
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute SELECT query.", e);
        }
    }

    @Override
    public Stream<Row> selectChildren(Path parent) {
        try {
            PreparedStatement statement = makePreparedStatement("SELECT PARENT_, KEY_, SUFFIX_, VALUE_ FROM STYX_DATA WHERE PARENT_=? ORDER BY PARENT_, KEY_");
            statement.setString(1, parent.encode());
            List<Row> rows = new ArrayList<>();
            try(ResultSet result = statement.executeQuery()) {
                while(result.next()) {
                    rows.add(createRow(result));
                }
            }
            return rows.stream();
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute SELECT statement.", e);
        }
    }

    @Override
    public Stream<Row> selectDescendants(Path parent) {
        try {
            PreparedStatement statement = makePreparedStatement("SELECT PARENT_, KEY_, SUFFIX_, VALUE_ FROM STYX_DATA WHERE PARENT_ LIKE ? ORDER BY PARENT_, KEY_"); // ORDER BY KEY_, SUFFIX_
            statement.setString(1, parent.encode() + "%");
            List<Row> rows = new ArrayList<>();
            try(ResultSet result = statement.executeQuery()) {
                while(result.next()) {
                    rows.add(createRow(result));
                }
            }
            return rows.stream().sorted(Row.ITERATION_ORDER);
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute SELECT statement.", e);
        }
    }

    @Override
    public int allocateSuffix(Path parent) {
        try {
            PreparedStatement statement = makePreparedStatement("SELECT MAX(SUFFIX_) FROM STYX_DATA WHERE PARENT_ = ?");
            statement.setString(1, parent.encode());
            try(ResultSet result = statement.executeQuery()) {
               if(result.next()) {
                   return result.getInt(1) + 1;
               }
           }
           return 1;
       } catch (SQLException e) {
           throw new JdbcException("Failed to execute SELECT statement.", e);
       }
    }

    @Override
    public void insert(Row row) {
        if(readOnly) {
            throw new IllegalStateException();
        }
        try {
            PreparedStatement statement = makePreparedStatement("INSERT INTO STYX_DATA (PARENT_, KEY_, SUFFIX_, VALUE_) VALUES (?,?,?,?)");
            statement.setString(1, row.parent().encode());
            statement.setString(2, row.key());
            if(row.suffix() == 0) {
                statement.setNull(3, Types.INTEGER);
            } else {
                statement.setInt(3, row.suffix());
            }
            statement.setString(4, row.value());
            statement.execute();
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute INSERT statement.", e);
        }
    }

    @Override
    public void deleteAll() {
        if(readOnly) {
            throw new IllegalStateException();
        }
        try {
            try(Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM STYX_DATA");
            }
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute DELETE statement.", e);
        }
    }

    @Override
    public void deleteSingle(Path parent, String key) {
        if(readOnly) {
            throw new IllegalStateException();
        }
        try {
            PreparedStatement statement = makePreparedStatement("DELETE FROM STYX_DATA WHERE PARENT_=? AND KEY_=?");
            statement.setString(1, parent.encode());
            statement.setString(2, key);
            statement.execute();
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute DELETE statement.", e);
        }
    }

    @Override
    public void deleteDescendants(Path parent) {
        if(readOnly) {
            throw new IllegalStateException();
        }
        try {
            PreparedStatement statement = makePreparedStatement("DELETE FROM STYX_DATA WHERE PARENT_ LIKE ?");
            statement.setString(1, parent.encode() + "%");
            statement.execute();
        } catch (SQLException e) {
            throw new JdbcException("Failed to execute DELETE statement.", e);
        }
    }

    private void checkAndCreateTable() throws SQLException {
        boolean exists = false;
        String tableName = "STYX_DATA";
        if(url.startsWith("jdbc:postgresql:")) {
            tableName = tableName.toLowerCase();
        }
        try(ResultSet result = connection.getMetaData().getTables(null, null, tableName, null)) {
            while(result.next()) {
                exists = true;
            }
        }
        if(!exists) {
            try(Statement statement = connection.createStatement()) {
                String tableCollation = "";
                String columnCollation = "";
                if(url.startsWith("jdbc:sqlite:")) {
                    statement.execute("PRAGMA case_sensitive_like = true;");
                }
                if(url.startsWith("jdbc:mysql:")) {
                    tableCollation = " CHARSET utf8 COLLATE utf8_bin";
                }
                if(url.startsWith("jdbc:postgresql:")) {
                    columnCollation = " COLLATE \"C\"";
                }
                statement.execute(
                        "CREATE TABLE STYX_DATA(\n"+
                        "  PARENT_ VARCHAR(100) NOT NULL"+columnCollation+",\n"+
                        "  KEY_ VARCHAR(500) NOT NULL"+columnCollation+",\n"+
                        "  SUFFIX_ INT,\n"+
                        "  VALUE_ VARCHAR(10000),\n"+
                        "  PRIMARY KEY (PARENT_, KEY_)\n"+
                        ")" + tableCollation);
            }
        }
    }

    private PreparedStatement makePreparedStatement(String sql) throws SQLException {
        PreparedStatement statement = statements.get(sql);
        if(statement == null) {
            statement = connection.prepareStatement(sql);
            statements.put(sql, statement);
        }
        return statement;
    }

    private Row createRow(ResultSet result) throws SQLException {
        return new Row(Path.decode(result.getString(1)), result.getString(2), result.getInt(3), result.getString(4));
    }
}
