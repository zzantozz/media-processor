package rds.mediaprocessor;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static rds.mediaprocessor.DbNames.*;
import static rds.mediaprocessor.DbNames.FileEventTable.event_type;

public class MainBuildCatalog {
    public static final int DB_BATCH_SIZE = 200;

    static class Stats {
        public static final AtomicInteger insertsCompleted = new AtomicInteger();
        public static final AtomicInteger insertsQueued = new AtomicInteger();

        private Stats() {
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
        ForkJoinPool.commonPool().submit(fileEventBatcher);
        ScheduledExecutorService statsReportingExecutor = Executors.newSingleThreadScheduledExecutor();
        statsReportingExecutor.scheduleWithFixedDelay(() -> {
            String report = " *** Stats update ***\n" +
                    " ***\n" +
                    " *** Inserts queued    : " + Stats.insertsQueued + "\n" +
                    " *** Inserts completed : " + Stats.insertsCompleted + "\n" +
                    " *** Insert queue usage: " + fileEventBatcher.getQueuedInserts().size() + "/" + fileEventBatcher.getQueueCapacity() + "\n" +
                    " *** Active connections: " + dataSource.getNumActive() + "\n" +
                    " *** Idle connections  : " + dataSource.getNumIdle() + "\n" +
                    " ***\n";
            System.out.print(report);
        }, 5, 5, TimeUnit.SECONDS);
        try (Stream<Path> stream = Files.walk(rootDir)) {
            stream
//                    .parallel()
                    .filter(Files::isRegularFile)
                    .map(path -> {
                        Path relPath = rootDir.relativize(path);
                        return new FileInfo(relPath.toString(), sha1(path));
                    })
                    .forEach(info -> {
                        try (Connection connection = dataSource.getConnection();
                             PreparedStatement findExistingStatement = connection.prepareStatement(
                                     "select event_type, sha1 from file_events where file_path = ? order by time desc limit 1;")) {
                            findExistingStatement.setString(1, info.relPath);
                            ResultSet resultSet = findExistingStatement.executeQuery();
                            FileEvent fileEvent = null;
                            if (resultSet.next()) {
                                String eventType = resultSet.getString(FileEventTable.event_type);
                                String sha1 = resultSet.getString(FileEventTable.sha1);
                                if (EventTypes.delete.equals(eventType)) {
                                    System.out.println(info.relPath + " is re-created");
                                    fileEvent = new FileEvent(EventTypes.create, info);
                                } else if (!sha1.equals(info.sha1Hex)) {
                                    System.out.println(info.relPath + " is updated");
                                    fileEvent = new FileEvent(EventTypes.update, info);
                                } else {
//                                    System.out.println(info.relPath + " is seen before and unchanged");
                                }
                            } else {
//                                System.out.println(info.relPath + " is newly discovered");
                                fileEvent = new FileEvent(EventTypes.create, info);
                            }
                            resultSet.close();
                            if (fileEvent != null) {
                                boolean offer = fileEventBatcher.getQueuedInserts().offer(fileEvent, 10, TimeUnit.SECONDS);
                                if (!offer) {
                                    throw new IllegalStateException("Failed to queue file info!");
                                }
                                Stats.insertsQueued.incrementAndGet();
                            }
                        } catch (SQLException e) {
                            throw new IllegalStateException("SQL failure", e);
                        } catch (InterruptedException e) {
                            throw new IllegalStateException("Unexpected interrupt", e);
                        }
                    });
        }
        fileEventBatcher.finishUp();
        statsReportingExecutor.shutdownNow();
        if (!ForkJoinPool.commonPool().awaitQuiescence(15, TimeUnit.SECONDS)) {
            System.out.println("Fork join pool didn't shut down completely!");
        }
        dataSource.close();
    }

    @Deprecated
    // Build hashing functions that inspect only image and video content, ignoring metadata. An image is the same even
    // if its timestamp changes, if the image data remains the same. Or... it might be good to know both.
    public static String sha1(Path path) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            DigestInputStream digester = new DigestInputStream(Files.newInputStream(path), md);
            IOUtils.copy(digester, new NullOutputStream());
            return Hex.encodeHexString(md.digest());
        } catch (NoSuchAlgorithmException | IOException e) {
//            throw new RuntimeException("Error while getting checksum for " + path, e);
            System.out.println("Error while getting checksum for " + path);
            e.printStackTrace();
            // Need to keep going here, but how?
            return "Failed to get checksum; file corrupt?";
        }
    }
}
