package rds.mediaprocessor;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static rds.mediaprocessor.MainBuildCatalog.DB_BATCH_SIZE;

public class FileEventBatcher implements Runnable {
    private boolean inputsActive = true;
    private final Statement txStatement;
    private final PreparedStatement insertStatement;
    private final int queueCapacity = DB_BATCH_SIZE + (DB_BATCH_SIZE / 10);
    private final BlockingQueue<MainBuildCatalog.FileEvent> queuedInserts = new LinkedBlockingQueue<>(queueCapacity);
    private final long insertTimestamp;
    private int queuePollTimeoutMillis;

    public FileEventBatcher(long insertTimestamp, BasicDataSource dataSource, Map<String, String> settings) {
        this.insertTimestamp = insertTimestamp;
        queuePollTimeoutMillis = Integer.parseInt(
                settings.getOrDefault("FileEventBatcher.queuePollTimeoutMillis", "1000"));
        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open db connection", e);
        }
        try {
            String stmt = "insert into file_events(event_type, file_path, time, storage_location, sha1) " +
                    "values(?, ?, ?, 'Desk archive', ?);";
            txStatement = connection.createStatement();
            insertStatement = connection.prepareStatement(stmt);
        } catch (SQLException e) {
            throw new IllegalStateException("Error creating sql statements", e);
        }
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public BlockingQueue<MainBuildCatalog.FileEvent> getQueuedInserts() {
        return queuedInserts;
    }

    @Override
    public void run() {
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
                    txStatement.execute("begin");
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
                    txStatement.execute("end");
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
                System.out.println(getClass().getSimpleName() + " is idle!");
                // It's tempting to put this in the while condition, but I want to make sure we only break from the loop
                // after we've timed out waiting on the queue.
                if (!inputsActive) {
                    break;
                }
            }
        }
    }

    public void finishUp() {
        inputsActive = false;
    }
}
