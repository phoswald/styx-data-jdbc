package styx.data.jdbc;

import java.util.Arrays;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import styx.data.db.GenericDatabaseTest;

@RunWith(Parameterized.class)
public class JdbcDatabaseTest extends GenericDatabaseTest {

    public JdbcDatabaseTest(String url) {
        super(new JdbcDatabase(url));
    }

    @Parameters(name="{0}")
    public static Iterable<Object[]> getParameters() {
        return Arrays.asList(
                new Object[] { "jdbc:h2:mem:" },
                new Object[] { "jdbc:derby:memory:test;create=true" },
                new Object[] { "jdbc:sqlite::memory:" });
    }
}