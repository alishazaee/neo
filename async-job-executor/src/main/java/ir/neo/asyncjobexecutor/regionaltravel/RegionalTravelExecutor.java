package ir.neo.asyncjobexecutor.regionaltravel;

import static ir.neo.travelcube.model.TravelCubeConstants.GEOHASH_PRECISION;

import com.google.common.collect.Lists;
import ir.neo.asyncjobexecutor.AbstractAsyncJobExecutor;
import ir.neo.asyncjobexecutor.JobStatistics;
import ir.neo.asyncjobexecutor.config.ApplicationConfig;
import ir.neo.asyncjobs.db.tables.records.AsyncJobsRecord;
import ir.neo.asyncjobs.model.asyncjob.JobType;
import ir.neo.asyncjobs.repository.CommonAsyncJobRepository;
import ir.neo.common.geohash.GeohashCoverageGenerator;
import ir.neo.regionaltravel.model.RegionalTravelRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes jobs that find travel between two geographic regions. */
public class RegionalTravelExecutor extends AbstractAsyncJobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegionalTravelExecutor.class);

    private final RegionalTravelQueryRepository travelQueryRepository;
    private final RegionalTravelResultRepository resultRepository;

    public RegionalTravelExecutor(
            CommonAsyncJobRepository jobRepository,
            RegionalTravelQueryRepository travelQueryRepository,
            RegionalTravelResultRepository resultRepository,
            ApplicationConfig configuration
    ) {
        super(jobRepository, configuration);

        this.travelQueryRepository = travelQueryRepository;
        this.resultRepository = resultRepository;
    }

    @Override
    public synchronized boolean executeNextSubmittedJob() {
        if (isClosed()) {
            return false;
        }

        AsyncJobsRecord submittedJob = jobRepository.claimOldestSubmittedJob(JobType.REGIONAL_TRAVELS);

        if (submittedJob == null) {
            LOGGER.debug("No submitted regional-travel job is available");
            return false;
        }

        long jobId = submittedJob.getId();

        LOGGER.info("Executing regional-travel job {}", jobId);

        try {
            RegionalTravelRequest request = parseRequest(
                    submittedJob.getRequestParameters(),
                    RegionalTravelRequest.class
            );
            validateTimeRange(request.startTime(), request.endTime());

            Set<String> sourceRegionGeohashes =
                    GeohashCoverageGenerator.cover(
                            request.sourceRegionWkt(),
                            GEOHASH_PRECISION
                    );

            Set<String> destinationRegionGeohashes =
                    GeohashCoverageGenerator.cover(
                            request.destinationRegionWkt(),
                            GEOHASH_PRECISION
                    );

            JobStatistics jobStatistics = new JobStatistics();

            executeBatchedQueries(
                    jobId,
                    sourceRegionGeohashes,
                    destinationRegionGeohashes,
                    request,
                    jobStatistics
            );

            finishJob(jobId, jobStatistics);

            LOGGER.info(
                    "Finished regional-travel job {} with {} results",
                    jobId,
                    jobStatistics.getTotalRows()
            );

            return true;

        } catch (RuntimeException exception) {
            markJobFailed(jobId);
            throw exception;
        }
    }

    private void executeBatchedQueries(
            long jobId,
            Set<String> sourceRegionGeohashes,
            Set<String> destinationRegionGeohashes,
            RegionalTravelRequest request,
            JobStatistics jobStatistics
    ) {
        List<List<String>> sourceGeohashBatches = Lists.partition(new ArrayList<>(sourceRegionGeohashes),
                configuration.getGeohashesPerBatch()
        );

        List<CompletableFuture<Void>> batchExecutions =
                sourceGeohashBatches.stream()
                        .map(batch -> queryAsync(
                                () -> travelQueryRepository.findTravels(
                                        Set.copyOf(batch),
                                        destinationRegionGeohashes,
                                        toUtcDateTime(request.startTime()),
                                        toUtcDateTime(request.endTime())
                                ),
                                jobStatistics
                        ).thenCompose(results ->
                                saveResultsAsync(
                                        jobId,
                                        results,
                                        resultRepository::save,
                                        jobStatistics
                                )
                        ))
                        .toList();

        CompletableFuture.allOf(batchExecutions.toArray(CompletableFuture[]::new)).join();
    }
}
