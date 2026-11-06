package ir.neo.asyncjobexecutor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe execution statistics stored in an async job's metadata. */
public class JobStatistics {

    private final AtomicLong writeDuration = new AtomicLong(0);
    private final AtomicLong queryDuration = new AtomicLong(0);
    private final AtomicInteger totalRows = new AtomicInteger(0);

    public void incrementWriteDuration(long duration) {
        writeDuration.addAndGet(duration);
    }

    public void incrementQueryDuration(long duration) {
        queryDuration.addAndGet(duration);
    }

    public void incrementTotalRows(int rows) {
        totalRows.addAndGet(rows);
    }

    public long getWriteDuration() {
        return writeDuration.get();
    }

    public long getQueryDuration() {
        return queryDuration.get();
    }

    public int getTotalRows() {
        return totalRows.get();
    }
}
