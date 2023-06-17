package rds.mediaprocessor;

import org.apache.commons.dbcp.BasicDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.concurrent.TimeUnit;

import static rds.mediaprocessor.DbNames.*;

public class DatabaseScanner {
    private final BasicDataSource dataSource;
    private final FileEventInserter fileEventInserter;

    public DatabaseScanner(BasicDataSource dataSource, FileEventInserter fileEventInserter) {
        this.dataSource = dataSource;
        this.fileEventInserter = fileEventInserter;
    }

    public void scan(Path directory) {
        try (Connection connection = dataSource.getConnection()) {
            // Can I tweak this query to only return the last event per file? Maybe an "order by time desc" on the outer
            // query?
            String stmt = "select distinct file_path from file_events where file_path not in " +
                    "(select file_path from file_events where event_type = 'delete')";
            ResultSet resultSet = connection.createStatement().executeQuery(stmt);
            PreparedStatement findLastEvent = connection.prepareStatement(
                    "select sha1 from file_events where file_path = ? order by time desc limit 1");
            while (resultSet.next()) {
                String fileRelPath = resultSet.getString(FileEventTable.file_path);
                Path filePath = directory.resolve(fileRelPath);
                if (Files.exists(filePath)) {
                    System.out.println("File still exists - " + filePath);
                } else {
                    // This threw a huge number of events at the batcher too fast while tying up disk io, or something.
                    // The batcher took 10+ seconds to persist a batch, by which time an offer here had timed out.
                    System.out.println("File was deleted - " + filePath);
                    findLastEvent.setString(1, fileRelPath);
                    ResultSet lastEventRs = findLastEvent.executeQuery();
                    if (!lastEventRs.next()) {
                        throw new IllegalStateException("Something went wrong. There should be a result here.");
                    }
                    String sha1 = lastEventRs.getString(FileEventTable.sha1);
                    MainBuildCatalog.FileEvent fileEvent = new MainBuildCatalog.FileEvent(EventTypes.delete, fileRelPath, sha1);
                    fileEventInserter.addToBatch(fileEvent);
                    MainBuildCatalog.Stats.insertsQueued.incrementAndGet();
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to do db things", e);
        }
    }
}
