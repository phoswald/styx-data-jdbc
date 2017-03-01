package styx.data.jdbc;

import java.util.Arrays;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import styx.data.GenericStoreTest;

@RunWith(Parameterized.class)
public class JdbcStoreTest extends GenericStoreTest {

    public JdbcStoreTest(String url) {
        super(url);
    }

    @Parameters(name="{0}")
    public static Iterable<Object[]> getParameters() {
        return Arrays.asList(
                new Object[] { "jdbc:h2:mem:" },
                new Object[] { "jdbc:derby:memory:test;create=true" },
                new Object[] { "jdbc:sqlite::memory:" },
                new Object[] { "jdbc:mysql://localhost/styx_test?user=root&password=sesam&useSSL=false" },
                new Object[] { "jdbc:postgresql://localhost/styx_test?user=postgres&password=sesam" });
    }
}
