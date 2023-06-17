package rds.mediaprocessor;

public interface FileEventInserter {
    void addToBatch(MainBuildCatalog.FileEvent event);

    int getCurrentQueuedInserts();

    int getQueueCapacity();

    void flush();
}
