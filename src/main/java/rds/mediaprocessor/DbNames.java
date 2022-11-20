package rds.mediaprocessor;

public class DbNames {
    public static class EventTypes {
        public static final String delete = "delete";
        public static final String create = "create";
        public static final String update = "update";
    }
    public static class FileEventTable {
        public static final String TABLE_NAME = "file_events";
        public static final String event_type = "event_type";
        public static final String file_path = "file_path";
        public static final String time = "time";
        public static final String storage_location = "storage_location";
        public static final String sha1 = "sha1";
    }
}
