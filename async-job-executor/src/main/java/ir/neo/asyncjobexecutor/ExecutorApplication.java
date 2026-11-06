package ir.neo.asyncjobexecutor;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import ir.neo.asyncjobexecutor.config.ApplicationConfig;
import ir.neo.asyncjobexecutor.config.DatabaseConfig;
import ir.neo.asyncjobexecutor.regionaltravel.RegionalTravelExecutor;
import ir.neo.asyncjobexecutor.regionaltravel.RegionalTravelQueryRepository;
import ir.neo.asyncjobexecutor.regionaltravel.RegionalTravelResultRepository;
import ir.neo.asyncjobexecutor.regionaluser.RegionalUserExecutor;
import ir.neo.asyncjobexecutor.regionaluser.RegionalUserQueryRepository;
import ir.neo.asyncjobexecutor.regionaluser.RegionalUserResultRepository;
import ir.neo.asyncjobs.repository.CommonAsyncJobRepository;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns one configured async-job executor and its database connections. */
public class ExecutorApplication implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorApplication.class);

    private final ApplicationConfig configuration;
    private final HikariDataSource postgresDataSource;
    private final HikariDataSource starRocksDataSource;
    private final AsyncJobExecutor executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private ExecutorApplication(
            ApplicationConfig configuration,
            HikariDataSource postgresDataSource,
            HikariDataSource starRocksDataSource,
            AsyncJobExecutor executor
    ) {
        this.configuration = configuration;
        this.postgresDataSource = postgresDataSource;
        this.starRocksDataSource = starRocksDataSource;
        this.executor = executor;
    }

    public static ExecutorApplication create(ApplicationConfig configuration) {
        HikariDataSource postgresDataSource = createDataSource("async-job-postgres", configuration.getPostgres());
        HikariDataSource starRocksDataSource = null;
        try {
            starRocksDataSource = createDataSource("async-job-starrocks", configuration.getStarRocks());

            DSLContext postgres = DSL.using(postgresDataSource, SQLDialect.POSTGRES);
            DSLContext starRocks = DSL.using(starRocksDataSource, SQLDialect.MYSQL);
            AsyncJobExecutor executor = createExecutor(configuration, postgres, starRocks);
            return new ExecutorApplication(
                    configuration,
                    postgresDataSource,
                    starRocksDataSource,
                    executor
            );
        } catch (RuntimeException exception) {
            if (starRocksDataSource != null) {
                starRocksDataSource.close();
            }
            postgresDataSource.close();
            throw exception;
        }
    }

    public void run() throws InterruptedException {
        while (running.get()) {
            boolean jobExecuted = executor.executeNextSubmittedJob();
            if (!jobExecuted) {
                Thread.sleep(configuration.getPollIntervalMillis());
            }
        }
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) {
            return;
        }
        LOGGER.info("Stopping async-job executor for {}", configuration.getJobType());
        executor.close();
        starRocksDataSource.close();
        postgresDataSource.close();
    }

    /** Creates the executor selected by the configured job type. */
    static AsyncJobExecutor createExecutor(
            ApplicationConfig configuration,
            DSLContext postgres,
            DSLContext starRocks
    ) {
        CommonAsyncJobRepository jobRepository = new CommonAsyncJobRepository(postgres);
        return switch (configuration.getJobType()) {
            case REGIONAL_TRAVELS -> new RegionalTravelExecutor(
                    jobRepository,
                    new RegionalTravelQueryRepository(starRocks),
                    new RegionalTravelResultRepository(postgres),
                    configuration
            );
            case REGIONAL_USERS -> new RegionalUserExecutor(
                    jobRepository,
                    new RegionalUserQueryRepository(starRocks),
                    new RegionalUserResultRepository(postgres),
                    configuration
            );
        };
    }

    private static HikariDataSource createDataSource(String poolName, DatabaseConfig database) {
        HikariConfig poolConfiguration = new HikariConfig();
        poolConfiguration.setPoolName(poolName);
        poolConfiguration.setJdbcUrl(database.getJdbcUrl());
        poolConfiguration.setUsername(database.getUsername());
        poolConfiguration.setPassword(database.getPassword());
        poolConfiguration.setMaximumPoolSize(database.getMaximumPoolSize());
        return new HikariDataSource(poolConfiguration);
    }

}
