package ir.neo.asyncjobexecutor.config;

/** Connection-pool configuration for one database. */
public class DatabaseConfig {

    private String jdbcUrl;
    private String username;
    private String password;
    private int maximumPoolSize;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    void validate(String propertyName) {
        requireText(jdbcUrl, propertyName + ".jdbcUrl");
        requireText(username, propertyName + ".username");
        if (password == null) {
            throw new IllegalArgumentException(propertyName + ".password must not be null");
        }
        if (maximumPoolSize <= 0) {
            throw new IllegalArgumentException(propertyName + ".maximumPoolSize must be positive");
        }
    }

    private static void requireText(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(propertyName + " must not be blank");
        }
    }
}
