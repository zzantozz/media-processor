package rds.mediaprocessor;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Scans a directory and updates a database to reflect its current state.
 */
public class FileSystemScanner {
    private final BasicDataSource dataSource;
    private final FileEventInserter fileEventInserter;

    public FileSystemScanner(BasicDataSource dataSource, FileEventInserter fileEventInserter) {
        this.dataSource = dataSource;
        this.fileEventInserter = fileEventInserter;
    }

    public void scan(Path directory) throws Exception {
        try (Stream<Path> stream = Files.walk(directory)) {
            stream
//                    .parallel()
                    .filter(Files::isRegularFile)
                    .map(path -> {
                        Path relPath = directory.relativize(path);
                        return new MainBuildCatalog.FileInfo(relPath.toString(), sha1(path));
                    })
                    .forEach(info -> {
                        try (Connection connection = dataSource.getConnection();
                             PreparedStatement findExistingStatement = connection.prepareStatement(
                                     "select event_type, sha1 from file_events where file_path = ? order by time desc limit 1;")) {
                            findExistingStatement.setString(1, info.relPath);
                            ResultSet resultSet = findExistingStatement.executeQuery();
                            MainBuildCatalog.FileEvent fileEvent = null;
                            if (resultSet.next()) {
                                String eventType = resultSet.getString(DbNames.FileEventTable.event_type);
                                String sha1 = resultSet.getString(DbNames.FileEventTable.sha1);
                                if (DbNames.EventTypes.delete.equals(eventType)) {
                                    System.out.println("File was re-created - " + info.relPath);
                                    fileEvent = new MainBuildCatalog.FileEvent(DbNames.EventTypes.create, info);
                                } else if (!sha1.equals(info.sha1Hex)) {
                                    System.out.println("File was updated - " + info.relPath);
                                    fileEvent = new MainBuildCatalog.FileEvent(DbNames.EventTypes.update, info);
                                } else {
//                                    System.out.println(info.relPath + " is seen before and unchanged");
                                }
                            } else {
//                                System.out.println(info.relPath + " is newly discovered");
                                fileEvent = new MainBuildCatalog.FileEvent(DbNames.EventTypes.create, info);
                            }
                            resultSet.close();
                            if (fileEvent != null) {
                                fileEventInserter.addToBatch(fileEvent);
                                MainBuildCatalog.Stats.insertsQueued.incrementAndGet();
                            }
                        } catch (SQLException e) {
                            throw new IllegalStateException("SQL failure", e);
                        }
                    });
        }
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
