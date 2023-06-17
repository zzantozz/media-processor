package rds.mediaprocessor;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;

import static rds.mediaprocessor.MainBuildCatalog.DB_BATCH_SIZE;

public class DifferentDbBatcher implements FileEventInserter {
    private final int queueCapacity = DB_BATCH_SIZE + (DB_BATCH_SIZE / 10);
    private final BlockingQueue<Object> triggerQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<QueuedEvent> insertQueue = new LinkedBlockingQueue<>(queueCapacity);
    private final BasicDataSource dataSource;
    private final ExecutorService batchSender;
    private final ExecutorService queueMonitor;
    private final int batchSize = 200;
    private long lastBatchTriggered = -1;

    public DifferentDbBatcher(BasicDataSource dataSource, ExecutorService batchSender, ExecutorService queueMonitor) {
        this.dataSource = dataSource;
        this.batchSender = batchSender;
        this.queueMonitor = queueMonitor;
    }

    @Override
    public int getCurrentQueuedInserts() {
        return insertQueue.size();
    }

    @Override
    public int getQueueCapacity() {
        return queueCapacity;
    }

    public Runnable getPeriodicBatchTrigger(long pollMillis) {
        return () -> {
            try {
                if (triggerQueue.poll(pollMillis, TimeUnit.MILLISECONDS) != null) {
                    triggerQueue.clear();
                    doBatch();
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException("Unexpected interrupt", e);
            }
        };
    }

    private static class QueuedEvent {
        public final MainBuildCatalog.FileEvent fileEvent;
        public final long queueTime = System.currentTimeMillis();

        private QueuedEvent(MainBuildCatalog.FileEvent fileEvent) {
            this.fileEvent = fileEvent;
        }
    }

    public void addToBatch(MainBuildCatalog.FileEvent event) {
        try {
            if (insertQueue.size() >= batchSize) {
                triggerQueue.add(new Object());
            }
            boolean success = insertQueue.offer(new QueuedEvent(event), 10, TimeUnit.SECONDS);
            if (!success) {
                throw new IllegalStateException("No room on insert queue after waiting");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt", e);
        }
    }

    /**
     * Triggers a batch to be executed in the specified executor. If it's single-threaded, that ensures only one batch
     * can every run at a time, which is probably desirable. This method is safe to call at any time and will insert
     * anywhere from 0 to batchSize events. If everything is running efficiently, every invocation will insert the full
     * batch size.
     */
    private Future<Void> doBatch() {
        return batchSender.submit(() -> {
            final long now = System.currentTimeMillis();
            if (lastBatchTriggered != -1) {
                long timeSinceLastBatch = now - lastBatchTriggered;
                MainBuildCatalog.Stats.addBatchDelay(timeSinceLastBatch);
            }
            lastBatchTriggered = now;
            int count = 0;
            try (Connection connection = dataSource.getConnection()) {
                String stmt = "insert into file_events(event_type, file_path, time, storage_location, sha1) " +
                        "values(?, ?, ?, 'UNUSED!!', ?);";
                PreparedStatement insertStatement = connection.prepareStatement(stmt);
                Statement txStatement = connection.createStatement();
                txStatement.execute("begin");
                while (count < batchSize && !insertQueue.isEmpty()) {
                    QueuedEvent queuedEvent = insertQueue.poll();
                    long queueDelay = System.currentTimeMillis() - queuedEvent.queueTime;
                    MainBuildCatalog.Stats.addQueueDelay(queueDelay);
                    MainBuildCatalog.FileEvent fileEvent = queuedEvent.fileEvent;
                    insertStatement.setString(1, fileEvent.eventType);
                    insertStatement.setString(2, fileEvent.relPath);
                    insertStatement.setLong(3, now);
                    insertStatement.setString(4, fileEvent.sha1Hex);
                    insertStatement.addBatch();
                    count += 1;
                }
                System.out.println("insert: " + insertStatement);
                int[] updates = insertStatement.executeBatch();
                txStatement.execute("end");
//            System.out.println("Updates in batch: " + Arrays.stream(updates)
//                    .mapToObj(Integer::toString)
//                    .collect(Collectors.joining(",")));
                MainBuildCatalog.Stats.insertsCompleted.getAndAdd(updates.length);
            } catch (SQLException e) {
                throw new IllegalStateException("Error doing batch insert", e);
            }
            long batchTime = System.currentTimeMillis() - now;
            MainBuildCatalog.Stats.addBatchTime(batchTime);
            return null;
        });
    }

    @Override
    public void flush() {
        flushAllRemaining();
    }

    public void flushAllRemaining() {
        while (!insertQueue.isEmpty()) {
            try {
                doBatch().get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Unexpected interrupt", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Error in final batch cleanup", e);
            } catch (TimeoutException e) {
                throw new IllegalStateException("Timed out waiting for batch to finish", e);
            }
        }
    }
}
