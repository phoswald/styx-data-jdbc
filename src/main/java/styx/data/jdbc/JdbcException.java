package styx.data.jdbc;

public class JdbcException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JdbcException(String message, Throwable cause) {
        super(message, cause);
    }
}
