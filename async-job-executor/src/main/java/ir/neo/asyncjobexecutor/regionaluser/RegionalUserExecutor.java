package ir.neo.asyncjobexecutor.regionaluser;

import static ir.neo.travelcube.model.TravelCubeConstants.GEOHASH_PRECISION;

import com.google.common.collect.Lists;
import ir.neo.asyncjobexecutor.AbstractAsyncJobExecutor;
import ir.neo.asyncjobexecutor.JobStatistics;
import ir.neo.asyncjobexecutor.config.ApplicationConfig;
import ir.neo.asyncjobs.db.tables.records.AsyncJobsRecord;
import ir.neo.asyncjobs.model.asyncjob.JobType;
import ir.neo.asyncjobs.repository.CommonAsyncJobRepository;
import ir.neo.common.geohash.GeohashCoverageGenerator;
import ir.neo.regionaluser.model.RegionalUserRequest;
import ir.neo.regionaluser.model.RegionalUserResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes jobs that aggregate users present inside a geographic region. */
public class RegionalUserExecutor extends AbstractAsyncJobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegionalUserExecutor.class);

    private final RegionalUserQueryRepository userQueryRepository;
    private final RegionalUserResultRepository resultRepository;

    public RegionalUserExecutor(
            CommonAsyncJobRepository jobRepository,
            RegionalUserQueryRepository userQueryRepository,
            RegionalUserResultRepository resultRepository,
            ApplicationConfig configuration
    ) {
        super(jobRepository, configuration);
        this.userQueryRepository = userQueryRepository;
        this.resultRepository = resultRepository;
    }

    @Override
    public synchronized boolean executeNextSubmittedJob() {
        if (isClosed()) {
            return false;
        }

        AsyncJobsRecord submittedJob = jobRepository.claimOldestSubmittedJob(JobType.REGIONAL_USERS);
        if (submittedJob == null) {
            LOGGER.debug("No submitted regional-user job is available");
            return false;
        }

        long jobId = submittedJob.getId();
        LOGGER.info("Executing regional-user job {}", jobId);

        try {
            RegionalUserRequest request = parseRequest(
                    submittedJob.getRequestParameters(),
                    RegionalUserRequest.class
            );
            validateTimeRange(request.startTime(), request.endTime());
            Set<String> regionGeohashes = GeohashCoverageGenerator.cover(
                    request.regionWkt(),
                    GEOHASH_PRECISION
            );

            JobStatistics jobStatistics = new JobStatistics();
            List<RegionalUserResult> results = executeBatchedQueries(regionGeohashes, request, jobStatistics);
            saveResultsAsync(jobId, results, resultRepository::save, jobStatistics).join();
            finishJob(jobId, jobStatistics);

            LOGGER.info("Finished regional-user job {} with {} users", jobId, results.size());
            return true;
        } catch (RuntimeException exception) {
            markJobFailed(jobId);
            throw exception;
        }
    }

    private List<RegionalUserResult> executeBatchedQueries(
            Set<String> regionGeohashes,
            RegionalUserRequest request,
            JobStatistics jobStatistics
    ) {
        List<List<String>> geohashBatches = Lists.partition(
                new ArrayList<>(regionGeohashes),
                configuration.getGeohashesPerBatch()
        );
        List<CompletableFuture<List<RegionalUserResult>>> queries = geohashBatches.stream()
                .map(batch -> queryAsync(
                        () -> userQueryRepository.findUsers(
                                Set.copyOf(batch),
                                toUtcDateTime(request.startTime()),
                                toUtcDateTime(request.endTime())
                        ),
                        jobStatistics
                ))
                .toList();

        CompletableFuture.allOf(queries.toArray(CompletableFuture[]::new)).join();
        Map<String, Long> countsByUser = queries.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toMap(
                        RegionalUserResult::userId,
                        RegionalUserResult::presenceCount,
                        Math::addExact
                ));

        return countsByUser.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(toRegionalUserResult())
                .toList();
    }

    private static Function<Map.Entry<String, Long>, RegionalUserResult> toRegionalUserResult() {
        return entry -> new RegionalUserResult(entry.getKey(), entry.getValue());
    }
}
