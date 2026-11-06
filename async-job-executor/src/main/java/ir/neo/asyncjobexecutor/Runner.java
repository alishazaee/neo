package ir.neo.asyncjobexecutor;

import ir.neo.asyncjobexecutor.config.ApplicationConfig;
import ir.neo.asyncjobexecutor.config.ConfigReader;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command-line entry point for an async-job executor. */
public final class Runner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);

    private Runner() {
    }

    public static void main(String[] arguments) {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("Expected exactly one argument: the application YAML path");
        }

        Thread.setDefaultUncaughtExceptionHandler((thread, failure) -> {
            LOGGER.error("Uncaught exception in thread {}", thread.getName(), failure);
        });

        Path configurationPath = Path.of(arguments[0]);
        LOGGER.info("Starting async-job executor with configuration {}", configurationPath);
        ApplicationConfig configuration = ConfigReader.load(configurationPath);
        ExecutorApplication application = ExecutorApplication.create(configuration);
        Runtime.getRuntime().addShutdownHook(new Thread(application::close, "async-job-executor-shutdown"));

        try {
            LOGGER.info("Async-job executor started successfully for {}", configuration.getJobType());
            application.run();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOGGER.info("Async-job executor was interrupted");
        } finally {
            application.close();
        }
    }
}
