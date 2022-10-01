# Problem

Amazon is photo storage source of truth.

Amazon is temporary video source of truth. Video storage is limited, so videos are removed from Amazon once transferred
to desktop.

Photos and videos sync from Amazon drive to desktop.
They sync from desktop to Plex server.
They used to sync from desktop to Google Drive, but they no longer allow unlimited photo storage, so need to fix that.

When a photo is deleted from Amazon, the delete should trickle down through all other things.
Nice to have: same when a photo is deleted from Plex.
Nicer to have? Same when a photo is deleted anywhere? With confirmation?
Same for videos, excluding removal from Amazon or Google because they don't live there permanently.

# Solution

Have to start by cataloging existing files initially.

Have separate lists for each storage location.

Keep track of whether a storage location is authoritative for photos and videos. "Authoritative" means: if a file is
removed from a location that is authoritative for that type of file, it should be removed from all locations.

- Amazon is authoritative for photos, not videos.

- Google isn't authoritative for any.

- Desk and Plex are authoritative for both.

So: db schema looks like

- path: relative path from root of whichever storage location; primary key probably?

- location: one of the locations: "Amazon", "Plex", etc.; probably part of the primary key, too.

- last_seen: timestamp for last time this file was seen in a scan

- sha1: sha1sum of the file for comparison later

Periodically refresh the db by scanning all locations.

...


what's the actual algorithm tho?

1. each scan updates "last seen" ts on record for photo. Then can query for "last seen" more than X days ago and assume
   those have been deleted. Keep a "last successful scan" time as well because if no scans have run, can't consider them
   deleted. If "last seen" > X days ago and "last seen" < "last successful scan" and "location" is authoritative,
   execute delete logic.

2. Before scan, mark all db entries as "stale". For each scanned file, mark as "not stale". After complete successful
   scan, all "stale" entries can be considered deleted. If "location" is authoritative, execute delete logic.

3. Scans don't store data. Just inspect each file in all locations. (Probably use temp db to keep from re-processing
   duplicates.) For each file, checksum it, check for it in all other locations. If file exists in non-authoritative
   location but doesn't exist in any authoritative location, execute delete logic? No. This could mean it's just been
   added to the pipeline, like a video file added to Amazon right now. Has to be time-sensitive. When did last sync
   happen?

Missing part of the picture here... Multiple possible states for a file:

- Brand new, just uploaded to Amazon, so only in the one location; could be authoritative or not.

- Fully synced: exists in all locations.

- Pruned: is a video that's been removed from Amazon and Google. But what if it's only removed from one?

- Deleted: file used to exist on an authoritative source but has been removed.

- Out of sync: File hasn't made it all the way through the pipeline for some reason. Probably Desk was off.

Everything needs to reach "fully synced" before I can start doing anything with it, I think. It's hard to know from a
snapshot of the system state what the right thing to do is.

# Solution 2

Maybe start conservative, and try to identify "fully synced" state. Only do anything with files that have reached that
state. How to track state? Need event sourcing? I have to know that it's been fully synced before, regardless of current
state. Maybe it's not one state but multiple attributes?

Can individual scanners update different location info, then another process compares all location info? How to sync and
lock so the comparator doesn't see inconsistent state?

Or... should one scanner know all locations and check each file in all locations as it goes?

I think an event stream/event sourcing is a must. Other processes that migrate and clean up video files need to notify
this process about those files, and this process should keep record that it used to exist but was pruned. Pruning
happens per location, not globally, so it's not a file state!

With true event sourcing, could even store file content upon automated delete so it's recoverable...

How, then, to know current state? Process entire stream for given file? Could work, as long as I don't store every "it
exists" as an event. Kafka? How efficient to read sparse events like this? I suppose this is where the temporary
"current state" storage comes in.

So... sqlite db holds all "current state", but is only a disposable cache. Can be fully rebuilt by processing event
stream, and *is* periodically rebuilt to ensure integrity.

- Scanners aren't so much updating stateful db as emitting events: found a file in a location; did it exist already? if
  so, did content change? if not, do nothing; otherwise, emit appropriate event

- also need scanner for "previous state", which is the db; has to follow similar logic to location scanners, but mainly
  looking for files that have changed/disappeared

Hmm... so when a scanner notes the presence or absence of a file, what happens? Ideally, should consult the event
history of that file to determine if there's a change, and if there is, emit an event. That means scanners need a
reliable "current state". No way to get around this. Either calculate on the fly from event stream, or keep db cache
for performance (probably this, but maybe try the other way first).

So... A scanner is scanning either a location or the file history. When scanning locations, must be able to get last
recorded state of a found file. If no last recorded, emit "new file". If last recorded differs, emit "changed file".
If everything matches, emit nothing. Whens scanning history, sort of the inverse because history can't have new files,
but could have a file that disappeared or changed.

So three event types for event stream:

- file added: a file has been added to any location (new file detected by location scanner)

- file changed: a file has been altered in any location (history scanner found that a file content has changed;
  location scanner doesn't need to care about this, right?)

- file rename: is this different that change?

- file deleted: file has been deleted from an authoritative location (history scanner found that a file that used to
  exist has gone away)

## Location scanner

- Has its own sqlite db that remembers files it's seen before - path and checksum at least, should be able to determine
  new file vs move. Db can be deleted and rebuilt from event stream!

- Periodically scans its location and emits events for new or moved files. Old file entries can TTL if not seen
  again for X days or the last time everything was reconciled. Need a central, coordinator db/process for this.

## History scanner

I think this can only work from an established history, meaning a concrete db? Tho still the db must be able to be
rebuilt from event stream.

# On further thought...

In Dropbox, if a file is added to one place, it's added to all, and if it's deleted from one place, it's deleted from
all. It also doesn't matter one bit whether the file has been synced to everything yet, or even whether all the storage
locations are powered on at the moment.

I think the answer here is a central event stream that *any* (authoritative?) source can send events to for file
creation and deletion. Then all other sources read from the stream when they're able to and enact the same operations.

A stream event could have:

- type: create or delete, or even update maybe

- path: relative file path

- checksum: hash of file content for verification

The files themselves can be synced by the current set of scripts. Each location has its own data store, built from the
event stream but verified with the state of the file system. If a location sees a "create" event, it checks to see that
the file exists on the file system and doesn't exist in the database. If so, then it verifies the checksum and adds it
to the db. Similar for any event: verify event matches current+historical state, then update state.

In case of any mismatch, send a notification to manually resolve. In the future, automatic resolution may come along.

## Problems

This isn't perfect event sourcing, as a create followed by update means the system can't verify the create. The file
already exists with the updated content. Hmm... Well, this would only (in normal operation) emit some kind of warning
notification. Turn them off when replaying an event stream to get to current state? But no, how do you know which events
are already accounted for in the db and which are new and might be real problems? 

What about files that already exist before the event stream starts? As in all the files already existing, or if the
stream data is lost somehow? Could have an init cycle that auto-generates a bunch of "create" events from one of the
authoritative locations.

Maybe the idea of a separate db per location is all wrong? Maybe the files are the db? It still doesn't solve the
replaying a create+update problem, either.

Thinking now of a distributed file syncing system like... whatever that was called I used a little bit...
Is there a difference between a system create and a user create? I.e. a file is created in one place because a user put
it there, but then it syncs to another location, and that's a system create. Only user actions should be propagated to
other locations. The syncing is a method of propagation, sort of.1

# On further, further thought...

Each location is independent. There's no central event stream. Each location... is an event stream? Knows where to find
other locations and how to query their streams? Can build a linear sequence of events from all the streams by
reconciling clock differences when comparing event timestamps???

# Random notes

Talking about rebuilding db's from event streams, that implies the events persist forever. Can I do that? What if I have
to TTL events? What happens when I get a "changed" or "deleted" event for a file whose "created" event has TTL'd out?
How about trying an event stream that never expires? git does it!

Can I do event streams in sqlite? Kafka seems like a big task.

I should do Kafka cuz I need to learn it!

If these management scripts overlap at all with the scripts syncing content around, stuff will go sideways.
