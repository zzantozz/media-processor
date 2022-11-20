package rds.mediaprocessor;

import javax.print.attribute.standard.Sides;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static rds.mediaprocessor.DbNames.*;

public class MainReconcileThings {
    static class Db {
        public final String name;
        public final Path path;
        private Function<String, String> normalizer;

        public Db(String name, Path path, Function<String, String> normalizer) {
            this.name = name;
            this.path = path;
            this.normalizer = normalizer;
        }
    }

    public static void main(String[] args) throws Exception {
        Path catalog1Path = Paths.get("D:\\dev\\projects\\media-processor\\test.db");
        Path catalog2Path = Paths.get("D:\\dev\\projects\\media-processor\\java-test.db");
        System.out.println(reconcileCatalogs(catalog1Path, catalog2Path));
    }

    static class Diff {
        public final String path;
        public Side lhs;
        public Side rhs;

        public Diff(String path, Side lhs, Side rhs) {
            this.path = path;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        static class Side {
            public String state;
            public String sha1;

            public Side(String state, String sha1) {
                this.state = state;
                this.sha1 = sha1;
            }
        }
    }

    public static List<Diff> reconcileCatalogs(Path catalog1Path, Path catalog2Path) throws Exception {
        // acquire db from every node
        // build master list of represented files?
        // ... or just iterate everything in every db?
        // wait... when i fix something, how do the changes get back to the db owners?
        // how about a feedback loop?
        // this just looks, calculates discrepancies, and sends a report of what it thinks should happen
        // user manually fixes things, reducing size of next report
        // when patterns become obvious, automatic fixes can be put in place

        // new note to self: all i'm really doing here is comparing deep hashes across file systems
        // maybe there's a much simpler way to go here, like using hashdeep?
        // note to the note: it's not just a point in time snapshot. i have to remember history.
        // i have to remember that a file used to exist and doesn't anymore. that's the delete event, which is
        // what actually started this whole thing.
        // and that can't be seen just by comparing two hashdeeps. if a file exists in one place but not the other,
        // that could mean it's a new file that hasn't been synced yet just as well as it could mean an old file that's
        // been deleted!

        // with any approach, how to know when i've got a quorum?
        // to begin, probably just require a set number of nodes to report in, and report a problem if they don't

        List<Db> knownDbs = List.of(
                new Db("Amazon",
                        Paths.get("D:\\dev\\projects\\media-processor\\test.db"),
                        (db2Path) -> Paths.get("/mnt/d/amazon-drive/Amazon Drive/Backup/DESK/D/archive/pics", db2Path).toString()),
                new Db("Desk archive",
                        Paths.get("D:\\dev\\projects\\media-processor\\java-test.db"),
                        (db1Path) ->
                                db1Path.replaceAll("^/mnt/d/amazon-drive/Amazon Drive/Backup/DESK/D/archive/pics/", "")
                                        .replaceAll("/", "\\\\")
                ));
        Db db1;
        Db db2;
        db1 = knownDbs.stream()
                .filter(db -> db.path.equals(catalog1Path))
                .findFirst()
                .orElse(new Db("some db", catalog1Path, (s) -> s));
        db2 = knownDbs.stream()
                .filter(db -> db.path.equals(catalog2Path))
                .findFirst()
                .orElse(new Db("some db", catalog2Path, (s) -> s));
        final Connection connection1 = DriverManager.getConnection("jdbc:sqlite:" + db1.path);
        final Connection connection2 = DriverManager.getConnection("jdbc:sqlite:" + db2.path);
        PreparedStatement findFileInDb2Stmt = connection2.prepareStatement(
                "select * from file_events where file_path = ? order by time desc limit 1");
        ResultSet resultSet1 = connection1.createStatement().executeQuery("select * from file_events");
        final List<Diff> result = new ArrayList<>();
        while (resultSet1.next()) {
            String filePath1 = resultSet1.getString(FileEventTable.file_path);
            String filePath2 = db2.normalizer.apply(filePath1);
            findFileInDb2Stmt.setString(1, filePath2);
            ResultSet resultSet2 = findFileInDb2Stmt.executeQuery();
            String db1FileState = resultSet1.getString(FileEventTable.event_type);
            String db1FileSha1 = resultSet1.getString((FileEventTable.sha1));
            if (resultSet2.next()) {
                String db2FileState = resultSet2.getString(FileEventTable.event_type);
                String db2FileSha1 = resultSet2.getString((FileEventTable.sha1));
                Diff diff = new Diff(filePath1,
                        new Diff.Side(db1FileState, db1FileSha1),
                        new Diff.Side(db2FileState, db2FileSha1));
                if (db1FileState.equals(db2FileState)) {
                    if (db1FileSha1.equals(db2FileSha1)) {
                        System.out.println("Files are an exact match in both db's - " + filePath2);
                    } else {
                        System.out.println("File checksums don't match - " + filePath2);
                        result.add(diff);
                    }
                } else if (db1FileState.equals(EventTypes.delete)) {
                    System.out.println("File should be deleted from " + db2.name + " - " + filePath2);
                    result.add(diff);
                } else if (db2FileState.equals(EventTypes.delete)) {
                    System.out.println("File should be deleted from " + db1.name + " - " + filePath1);
                    result.add(diff);
                } else if (db1FileState.equals(EventTypes.create)) {
                    System.out.println("File is updated in " + db2.name + " but not " + db1.name + " - " + filePath1);
                    result.add(diff);
                } else if (db2FileState.equals(EventTypes.create)) {
                    System.out.println("File is updated in " + db1.name + " but not " + db2.name + " - " + filePath2);
                    result.add(diff);
                } else {
                    throw new IllegalStateException("Shouldn't be able to get here. What happened?");
                }
            } else {
//                System.out.println("File exists in " + db1.name + " but unknown in " + db2.name + " - " + filePath1);
                result.add(new Diff(filePath1,
                        new Diff.Side(db1FileState, db1FileSha1),
                        new Diff.Side("none", "none")));
            }
        }
        return result;
    }
}
