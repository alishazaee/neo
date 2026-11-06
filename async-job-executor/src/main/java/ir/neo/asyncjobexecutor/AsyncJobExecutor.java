package ir.neo.asyncjobexecutor;

/** Executes submitted jobs of one configured async-job type. */
public interface AsyncJobExecutor extends AutoCloseable {

    boolean executeNextSubmittedJob();

    @Override
    void close();
}
