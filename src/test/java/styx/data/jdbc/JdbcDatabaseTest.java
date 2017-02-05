package styx.data.jdbc;

import styx.data.db.GenericDatabaseTest;

public class JdbcDatabaseTest extends GenericDatabaseTest {

    public JdbcDatabaseTest() {
        super(new JdbcDatabase("jdbc:h2:mem:"));
    }
}
