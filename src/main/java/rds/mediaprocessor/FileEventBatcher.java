package rds.mediaprocessor;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static rds.mediaprocessor.MainBuildCatalog.DB_BATCH_SIZE;

public class FileEventBatcher implements Runnable, FileEventInserter {
    private boolean inputsActive = true;
    private final int queueCapacity = DB_BATCH_SIZE + (DB_BATCH_SIZE / 10);
    private final BlockingQueue<MainBuildCatalog.FileEvent> queuedInserts = new LinkedBlockingQueue<>(queueCapacity);
    private final long insertTimestamp;
    private final BasicDataSource dataSource;
    private final int queuePollTimeoutMillis;

    public FileEventBatcher(long insertTimestamp, BasicDataSource dataSource, Map<String, String> settings) {
        this.insertTimestamp = insertTimestamp;
        this.dataSource = dataSource;
        queuePollTimeoutMillis = Integer.parseInt(
                settings.getOrDefault("FileEventBatcher.queuePollTimeoutMillis", "1000"));
    }

    @Override
    public int getCurrentQueuedInserts() {
        return queuedInserts.size();
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    @Override
    public void addToBatch(MainBuildCatalog.FileEvent event) {
        try {
            boolean success = queuedInserts.offer(event, 10, TimeUnit.SECONDS);
            if (!success) {
                throw new IllegalStateException("No room on insert queue after waiting");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt", e);
        }
    }

    @Override
    public void run() {
        // Only create the connection here, so that if this thing isn't actually running, it doesn't connect.
        final Connection connection;
        final PreparedStatement beginTxStatement;
        final PreparedStatement endTxStatement;
        final PreparedStatement insertStatement;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open db connection", e);
        }
        try {
            String stmt = "insert into file_events(event_type, file_path, time, storage_location, sha1) " +
                    "values(?, ?, ?, 'Desk archive', ?);";
            beginTxStatement = connection.prepareStatement("begin");
            endTxStatement = connection.prepareStatement("end");
            insertStatement = connection.prepareStatement(stmt);
        } catch (SQLException e) {
            throw new IllegalStateException("Error creating sql statements", e);
        }
        while (true) {
            List<MainBuildCatalog.FileEvent> toHandle = new ArrayList<>();
            while (toHandle.size() < DB_BATCH_SIZE) {
                try {
                    MainBuildCatalog.FileEvent fileInfo = queuedInserts.poll(queuePollTimeoutMillis, TimeUnit.MILLISECONDS);
                    if (fileInfo == null) {
                        break;
                    } else {
                        toHandle.add(fileInfo);
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Unexpected interrupt", e);
                }
            }
            if (!toHandle.isEmpty()) {
                long start = System.currentTimeMillis();
                try {
                    beginTxStatement.execute();
                } catch (SQLException e) {
                    throw new IllegalStateException("Failed to start new transaction", e);
                }
                for (MainBuildCatalog.FileEvent fileEvent : toHandle) {
                    try {
//                        System.out.println("Insert " + fileInfo.relPath + " at " + now);
                        insertStatement.setString(1, fileEvent.eventType);
                        insertStatement.setString(2, fileEvent.relPath);
                        insertStatement.setLong(3, insertTimestamp);
                        insertStatement.setString(4, fileEvent.sha1Hex);
                        insertStatement.addBatch();
                    } catch (SQLException e) {
                        throw new IllegalStateException("Error populating insert statement", e);
                    }
                }
                try {
                    int[] updates = insertStatement.executeBatch();
                    endTxStatement.execute();
//                            System.out.println("Updates in batch: " + Arrays.stream(updates)
//                                    .mapToObj(Integer::toString)
//                                    .collect(Collectors.joining(",")));
                    MainBuildCatalog.Stats.insertsCompleted.getAndAdd(toHandle.size());
                } catch (SQLException e) {
                    throw new RuntimeException("Error executing batch insert", e);
                }
                long elapsed = System.currentTimeMillis() - start;
                double timeEach = elapsed / (double) toHandle.size();
                System.out.println("Inserted " + toHandle.size() + " rows in " + elapsed + " ms (" + timeEach + " ms each)");
            } else {
//                System.out.println(getClass().getSimpleName() + " is idle!");
                // It's tempting to put this in the while condition, but I want to make sure we only break from the loop
                // after we've timed out waiting on the queue.
                if (!inputsActive) {
                    System.out.println(getClass().getSimpleName() + " terminating; total inserts: "
                            + MainBuildCatalog.Stats.insertsCompleted);
                    break;
                }
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IllegalStateException("Error closing connection used for inserts", e);
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not yet written");
    }

    public void finishUp() {
        inputsActive = false;
    }
}
