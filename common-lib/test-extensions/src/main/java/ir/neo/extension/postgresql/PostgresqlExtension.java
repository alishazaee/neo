package ir.neo.extension.postgresql;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** JUnit 5 extension that starts one shared PostgreSQL container before all tests. */
public class PostgresqlExtension implements BeforeAllCallback {

    @Container
    static final PostgreSQLContainer<?> POSTGRESQL_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
                    .withDatabaseName("test")
                    .withUsername("test")
                    .withPassword("test");

    public static PostgreSQLContainer<?> getPostgresqlContainer() {
        return POSTGRESQL_CONTAINER;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        if (!POSTGRESQL_CONTAINER.isRunning()) {
            POSTGRESQL_CONTAINER.start();
            Runtime.getRuntime().addShutdownHook(new Thread(POSTGRESQL_CONTAINER::stop));
        }
    }
}
