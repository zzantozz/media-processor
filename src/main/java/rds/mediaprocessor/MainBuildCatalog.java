package rds.mediaprocessor;

import org.apache.commons.dbcp.BasicDataSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MainBuildCatalog {
    public static final int DB_BATCH_SIZE = 200;

    static class Stats {
        public static final AtomicInteger insertsCompleted = new AtomicInteger();
        public static final AtomicInteger insertsQueued = new AtomicInteger();
        private static final AtomicLong cumulativeQueueDelay = new AtomicLong();
        private static final AtomicInteger queueDelayCount = new AtomicInteger();
        private static final AtomicLong cumulativeBatchDelay = new AtomicLong();
        private static final AtomicInteger batchDelayCount = new AtomicInteger();
        private static final AtomicLong cumulativeBatchTime = new AtomicLong();
        private static final AtomicInteger batchTimeCount = new AtomicInteger();

        private Stats() {
        }

        public static void addQueueDelay(long queueDelay) {
            queueDelayCount.incrementAndGet();
            cumulativeQueueDelay.addAndGet(queueDelay);
        }

        public static double getAverageQueueDelay() {
            return cumulativeQueueDelay.get() / (double) queueDelayCount.get();
        }

        public static void addBatchDelay(long timeBetweenBatches) {
            batchDelayCount.incrementAndGet();
            cumulativeBatchDelay.addAndGet(timeBetweenBatches);
        }

        public static double getAverageBatchDelay() {
            return cumulativeBatchDelay.get() / (double) batchDelayCount.get();
        }

        public static void addBatchTime(long batchTime) {
            batchTimeCount.incrementAndGet();
            cumulativeBatchTime.addAndGet(batchTime);
        }

        public static double getAverageBatchTime() {
            return cumulativeBatchTime.get() / (double) batchTimeCount.get();
        }
    }

    static class FileEvent {
        public final String eventType;
        public final String relPath;
        public final String sha1Hex;

        public FileEvent(String eventType, FileInfo info) {
            this.eventType = eventType;
            relPath = info.relPath;
            sha1Hex = info.sha1Hex;
        }

        public FileEvent(String eventType, String relPath, String sha1Hex) {
            this.eventType = eventType;
            this.relPath = relPath;
            this.sha1Hex = sha1Hex;
        }
    }

    static class FileInfo {
        public final String relPath;
        public final String sha1Hex;

        public FileInfo(String relPath, String sha1Hex) {
            this.relPath = relPath;
            this.sha1Hex = sha1Hex;
        }

        @Override
        public String toString() {
            return "FileInfo{" +
                    "relPath='" + relPath + '\'' +
                    ", sha1Hex='" + sha1Hex + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        final Path dbLocation = Paths.get("java-test.db");
        final Path rootDir = Paths.get("D:\\archive\\pics");
        buildCatalog(dbLocation, rootDir);
    }

    public static void buildCatalog(Path rootDir, Path dbLocation) throws Exception {
        buildCatalog(rootDir, dbLocation, new HashMap<>());
    }

    public static void buildCatalog(Path rootDir, Path dbLocation, Map<String, String> settings) throws Exception {
        final long now = System.currentTimeMillis();
        final BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:sqlite:" + dbLocation);
        dataSource.setMinIdle(1);
        dataSource.setMaxIdle(2);
        dataSource.setMaxActive(10);
        dataSource.setMaxWait(5000);
        dataSource.setValidationQuery("select 1");
        dataSource.setConnectionInitSqls(List.of(
                "pragma busy_timeout=10000"
        ));
        dataSource.setPoolPreparedStatements(true);
        final FileEventBatcher fileEventBatcher = new FileEventBatcher(now, dataSource, settings);
        // A single thread executor will ensure batches can't be sent concurrently.
        ExecutorService batchSender = Executors.newSingleThreadExecutor();
        ScheduledExecutorService batchScheduler = Executors.newSingleThreadScheduledExecutor();
        final DifferentDbBatcher differentDbBatcher = new DifferentDbBatcher(dataSource, batchSender, batchScheduler);
        // This feels a little wonky. I'm scheduling something on a fixed delay that itself polls for a set amount of
        // time. I want the turnaround on it to be pretty short because currently, this is the amount of time I have to
        // wait for the "idle" state later on. This mainly shows itself in tests, where each test has to do this
        // multiple times.
        batchScheduler.scheduleWithFixedDelay(
                differentDbBatcher.getPeriodicBatchTrigger(50), 0, 50, TimeUnit.MILLISECONDS);
        String whichInserter = "new";
        FileEventInserter inserterToUse;
        if ("old".equals(whichInserter)) {
            inserterToUse = fileEventBatcher;
            batchSender.submit(fileEventBatcher);
        } else if ("new".equals(whichInserter)) {
            inserterToUse = differentDbBatcher;
        } else {
            throw new IllegalStateException("can't pick a batcher");
        }
        ScheduledExecutorService statsReportingExecutor = Executors.newSingleThreadScheduledExecutor();
        statsReportingExecutor.scheduleWithFixedDelay(() -> {
            String report = " *** Stats update ***\n" +
                    " ***\n" +
                    " *** Inserts queued         : " + Stats.insertsQueued + "\n" +
                    " *** Inserts completed      : " + Stats.insertsCompleted + "\n" +
                    " *** Avg queued time        : " + Stats.getAverageQueueDelay() + "\n" +
                    " *** Avg time btwn batches  : " + Stats.getAverageBatchDelay() + "\n" +
                    " *** Avg time per batch ins : " + Stats.getAverageBatchTime() + "\n" +
                    " *** Insert queue usage     : " + inserterToUse.getCurrentQueuedInserts() + "/" + inserterToUse.getQueueCapacity() + "\n" +
                    " *** Active connections     : " + dataSource.getNumActive() + "\n" +
                    " *** Idle connections       : " + dataSource.getNumIdle() + "\n" +
                    " ***\n";
            System.out.print(report);
        }, 5, 5, TimeUnit.SECONDS);
        new FileSystemScanner(dataSource, inserterToUse).scan(rootDir);
        // Next up is scanning the db to see if it contains files that no longer exist. To do that, we need the db to be
        // fully updated, meaning everything queued in the batcher needs to be sent to the database.
        inserterToUse.flush();
        new DatabaseScanner(dataSource, inserterToUse).scan(rootDir);
        // Forcibly terminate the batch scheduler because we're going to clean up manually next.
        batchScheduler.shutdownNow();
        if ("old".equals(whichInserter)) {
            fileEventBatcher.finishUp();
        } else if ("new".equals(whichInserter)) {
            differentDbBatcher.flushAllRemaining();
        }
        // Now we can shut down the other executors used, since all the work is done.
        batchSender.shutdown();
        statsReportingExecutor.shutdownNow();
        if (!batchSender.awaitTermination(15, TimeUnit.SECONDS)) {
            System.out.println("Batcher sender didn't shut down completely!");
        }
        dataSource.close();
    }
}
