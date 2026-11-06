package ir.neo.asyncjobexecutor.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/** Loads async-job executor configuration from YAML. */
public final class ConfigReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class);

    private ConfigReader() {
    }

    public static ApplicationConfig load(Path configurationPath) {
        try (InputStream stream = Files.newInputStream(configurationPath)) {
            ApplicationConfig configuration = new Yaml().loadAs(stream, ApplicationConfig.class);
            if (configuration == null) {
                throw new IllegalArgumentException("Configuration file must not be empty");
            }
            configuration.validate();
            LOGGER.info("Loaded async-job executor configuration from {}", configurationPath);
            return configuration;
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Could not read async-job executor configuration from " + configurationPath,
                    exception
            );
        }
    }
}
