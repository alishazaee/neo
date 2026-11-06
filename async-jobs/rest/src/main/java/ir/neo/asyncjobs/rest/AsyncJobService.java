package ir.neo.asyncjobs.rest;

import ir.neo.asyncjobs.model.asyncjob.AsyncJobRequest;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobResponse;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobSearchRequest;
import ir.neo.common.pagination.Page;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/** Coordinates asynchronous-job operations. */
@Service
public class AsyncJobService {

    private final AsyncJobRestRepository restRepository;
    private final int maxAllowedOpenJobsCount;

    public AsyncJobService(AsyncJobRestRepository restRepository,
                           @Value("${async-jobs.max-allowed-open-jobs-count}") int maxAllowedOpenJobsCount) {
        this.restRepository = restRepository;
        this.maxAllowedOpenJobsCount = maxAllowedOpenJobsCount;
    }

    public Long submit(AsyncJobRequest asyncJobRequest) {
        long openJobs = restRepository.countOpenJobs(asyncJobRequest.jobType());
        if (openJobs >= maxAllowedOpenJobsCount) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "The maximum number of open jobs for this job type has been reached"
            );
        }
        return restRepository.create(asyncJobRequest);
    }

    public Page<AsyncJobResponse> search(AsyncJobSearchRequest searchRequest, int pageSize, int pageNumber) {
        return restRepository.search(searchRequest, pageSize, pageNumber);
    }

    public AsyncJobResponse retrieve(long jobId) {
        AsyncJobResponse asyncJob = restRepository.findById(jobId);
        if (asyncJob == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Async job not found");
        }
        return asyncJob;
    }

    public void updateTitle(long jobId, String jobTitle) {
        int updatedJobs = restRepository.updateTitle(jobId, jobTitle);
        if (updatedJobs == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Async job not found");
        }
    }

}
