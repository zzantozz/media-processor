-- Creates the db schema expected by the scripts here.
--
-- Usage:
--   sqlite3 test.db < create-schema.sql

-- Creates an events table to store discrete file events, like "create", "update", and "delete". Rows should be treated
-- as immutable because they just represent discrete events at a point in time.
create table file_events (
    file_path text not null,
    time integer not null,
    event_type text not null,
    storage_location text not null,
    sha1 text not null, -- might not know sha1 on delete, but we can look it up from most recent event
    primary key (file_path, time)
);
