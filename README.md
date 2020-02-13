Pghost
======

Postgres online schema/ data migration.

Testing the following:
1. Create a logical replication slot. DONE.
2. Let the slot continue to collect WAL. DONE.
3. Start a keyset page (or naive id range seek) in batches. This allows the vacuum to run. DONE. TODO: Impl keyset.
4. Capture the LSN (using IDENTIFY SYSTEM) after we've copied the last row. DONE.
5. Use a custom apply worker to merge data from rep slot "restart lsn" to "current lsn". KINDA DONE.
6. Either continue to let the custom apply worker run, or switch to walrecv apply.

Logical Replication:
1. Can parse pgoutput. DONE.
2. Can format attribute types into SQL expressions. KINDA DONE.
3. Can upsert into the table up to some LSN. PENDING.

General Flow:
1. Creates a logical replication using pgoutput plugin. DONE.
2. Begins a sync using walking IDs or keyset pagination. This allows vacuums to run and indexes to be cleaned. DONE.
3. After (2) completes, save the current LSN as a checkpoint. DONE.
3. Subscribe to the replication slot upserting SQL into the DB. Slow but good enough for now. Upsert all changes up to the checkpoint LSN. DONE.
4. After (3), switch back to standard pgoutput + postgres replication now that no conflicts will happen.

Why?:
- This tool is currently an experiment to see if it's possible to batch the synchronization part of logical replication to avoid vacuums from cleaning indexes. For large tables, it's been observed that after 12-48 hours of the synchronization starting, performance suffers due to vacuums not running.

Misc TODO:
- Handle replication slot dropped.
- Handle server is going down.

NOTES:

- Current test bed is basically:
```sql
-- create tables for original and replication
create table yolos (id serial primary key);
create table yolos2 (id serial primary key);
-- populate the original with random data
insert into yolos select generate_series(1,10000000);
-- create a publication for testing
create publication pub_on_yolos for table yolos;
```
and then
```
go build && ./pghost
```
