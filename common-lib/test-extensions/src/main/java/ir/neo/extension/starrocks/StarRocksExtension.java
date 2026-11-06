package ir.neo.extension.starrocks;

import java.time.Duration;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** JUnit 5 extension that starts one shared StarRocks all-in-one container before all tests. */
public class StarRocksExtension implements BeforeAllCallback {

    private static final int QUERY_PORT = 9030;
    private static final String IMAGE = "starrocks/allin1-ubuntu:3.5.21";

    @Container
    static final GenericContainer<?> STARROCKS_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(IMAGE))
                    .withExposedPorts(QUERY_PORT)
                    .waitingFor(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofMinutes(3));

    public static GenericContainer<?> getStarRocksContainer() {
        return STARROCKS_CONTAINER;
    }

    public static String getJdbcUrl() {
        return "jdbc:mysql://"
                + STARROCKS_CONTAINER.getHost()
                + ":"
                + STARROCKS_CONTAINER.getMappedPort(QUERY_PORT);
    }

    public static String getUsername() {
        return "root";
    }

    public static String getPassword() {
        return "";
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        if (!STARROCKS_CONTAINER.isRunning()) {
            STARROCKS_CONTAINER.start();
            Runtime.getRuntime().addShutdownHook(new Thread(STARROCKS_CONTAINER::stop));
        }
    }
}
