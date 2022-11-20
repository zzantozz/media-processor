package rds.mediaprocessor;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static rds.mediaprocessor.DbNames.*;

public class IntegrationTest {
    /**
     * A temp directory, freshly created for this test. All of the below directories and files are in this directory.
     */
    Path tempDirectory;

    /**
     * Two directories containing files for test purposes. They begin with exactly the same files.
     */
    private Path location1, location2;

    /**
     * The locations of the db files for the above directories.
     */
    private Path location1Db, location2Db;

    /**
     * The four test files. Each one starts off containing the string "v1". That makes it easy for tests to change it
     * to, for example, "v2" and then "v3" to test how changes are handled.
     */
    private Path testFile11, testFile12, testFile21, testFile22;

    /**
     * Some precalculated checksums of the strings used in test files here. For example, the first one is the checksum
     * of the string "v1".
     */
    private static final String CHECKSUM_OF_V1 = "a1047eab1035d58682a53557e0b2a75edbfd15fd";
    private static final String CHECKSUM_OF_V2 = "5a6df720540c20d95d530d3fd6885511223d5d20";

    /**
     * Settings to use for this test. Mainly this is for time-related things that will cause the tests to wait a long
     * time or fail because they run too fast.
     */
    private final Map<String, String> settings = Map.of(
            // The batcher queues db inserts for this long, and it makes the tests run as slow as this timeout is
            "FileEventBatcher.queuePollTimeoutMillis", "5"
    );

    /**
     * Creates an isolated directory for each test to work in. Inside this directory are two subdirectories and two
     * empty database files that are initialized with the create-schema.sql script. The two subdirectories each have
     * two, identical files in them, a test that builds a catalog of the two directories and compares them should see
     * no differences. This provides a baseline from which other tests can be built.
     */
    @BeforeEach
    public void createTempDir() throws Exception {
        tempDirectory = Files.createTempDirectory("mediaprocessor-it-");
        System.out.println("Using temp dir: " + tempDirectory);
        location1 = tempDirectory.resolve("test-files-1");
        location2 = tempDirectory.resolve("test-files-2");
        location1Db = tempDirectory.resolve("db1");
        location2Db = tempDirectory.resolve("db2");
        testFile11 = location1.resolve("file1");
        testFile12 = location1.resolve("file2");
        testFile21 = location2.resolve("file1");
        testFile22 = location2.resolve("file2");
        FileUtils.writeStringToFile(testFile11.toFile(), "v1", "UTF-8");
        FileUtils.writeStringToFile(testFile12.toFile(), "v1", "UTF-8");
        FileUtils.writeStringToFile(testFile21.toFile(), "v1", "UTF-8");
        FileUtils.writeStringToFile(testFile22.toFile(), "v1", "UTF-8");
        String initSql = FileUtils.readFileToString(new File("create-schema.sql"), "UTF-8");
        try (Connection connection1 = DriverManager.getConnection("jdbc:sqlite:" + location1Db);
             Connection connection2 = DriverManager.getConnection("jdbc:sqlite:" + location2Db)) {
            connection1.createStatement().execute(initSql);
            connection2.createStatement().execute(initSql);
        }
    }

    @AfterEach
    public void deleteTempDir() throws Exception {
        FileUtils.forceDelete(tempDirectory.toFile());
    }

    @Test
    void basicTestFixturesWork_noDifferences() throws Exception {
        MainBuildCatalog.buildCatalog(location1, location1Db);
        MainBuildCatalog.buildCatalog(location2, location2Db);
        List<MainReconcileThings.Diff> diffs = MainReconcileThings.reconcileCatalogs(location1Db, location2Db);
        assertThat(diffs, hasSize(0));
        for (Path db : List.of(location1Db, location2Db)) {
            try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + db)) {
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select count(*) from file_events");
                resultSet.next();
                assertThat(db + " should have events", resultSet.getInt(1), equalTo(2));
            }
        }
    }

    @Test
    void updatingOneFileInLeftDbIsNoticed() throws Exception {
        // Given I've cataloged both locations in their initial states.
        MainBuildCatalog.buildCatalog(location1, location1Db, settings);
        MainBuildCatalog.buildCatalog(location2, location2Db, settings);

        // When I update a file on the left hand side and re-catalog it
        FileUtils.writeStringToFile(testFile11.toFile(), "v2", "UTF-8");
        MainBuildCatalog.buildCatalog(location1, location1Db, settings);

        // Then the difference is noted
        List<MainReconcileThings.Diff> diffs = MainReconcileThings.reconcileCatalogs(location1Db, location2Db);
        assertThat(diffs, hasSize(1));
        MainReconcileThings.Diff diff = diffs.get(0);
        String relPath = location1.relativize(testFile11).toString();
        assertThat(diff.path, equalTo(relPath));
        assertThat(diff.lhs.state, equalTo(EventTypes.update));
        assertThat(diff.lhs.sha1, equalTo(CHECKSUM_OF_V1));
        assertThat(diff.rhs.state, equalTo(EventTypes.create));
        assertThat(diff.rhs.sha1, equalTo(CHECKSUM_OF_V2));
    }

    @Test
    void updatingOneFileInRightDbIsNoticed() throws Exception {
        // Given I've cataloged both locations in their initial states.
        MainBuildCatalog.buildCatalog(location1, location1Db, settings);
        MainBuildCatalog.buildCatalog(location2, location2Db, settings);

        // When I update a file on the right hand side and re-catalog it
        FileUtils.writeStringToFile(testFile21.toFile(), "v2", "UTF-8");
        MainBuildCatalog.buildCatalog(location2, location2Db, settings);

        // Then the difference is noted
        List<MainReconcileThings.Diff> diffs = MainReconcileThings.reconcileCatalogs(location1Db, location2Db);
        assertThat(diffs, hasSize(1));
        MainReconcileThings.Diff diff = diffs.get(0);
        String relPath = location2.relativize(testFile21).toString();
        assertThat(diff.path, equalTo(relPath));
        assertThat(diff.lhs.state, equalTo(EventTypes.create));
        assertThat(diff.lhs.sha1, equalTo(CHECKSUM_OF_V2));
        assertThat(diff.rhs.state, equalTo(EventTypes.update));
        assertThat(diff.rhs.sha1, equalTo(CHECKSUM_OF_V1));
    }
}
