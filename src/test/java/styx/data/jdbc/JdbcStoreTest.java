package styx.data.jdbc;

import styx.data.GenericStoreTest;

public class JdbcStoreTest extends GenericStoreTest {

    public JdbcStoreTest() {
        super("jdbc:h2:mem:");
    }
}
