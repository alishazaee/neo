package ir.neo.asyncjobs.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts the asynchronous-jobs REST service. */
@SpringBootApplication
public class ApplicationLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationLauncher.class);

    public static void main(String[] args) {
        LOGGER.info("Starting async-jobs-rest application...");
        SpringApplication.run(ApplicationLauncher.class, args);
        LOGGER.info("Async-jobs-rest started successfully");
    }
}
