Pghost
======

### Description

Pghost synchronizes large tables between postgresql databases. Pghost will perform a parallel batched COPY of your table and merge in changes using the logical replication pgoutput plugin. Once pghost has finished running, your source and destination tables will be consistent and you can optionally choose to continue replicating changes from the source table by attaching a new subscription to the replication slot made by pghost.

Why bother? Pghost is built to help with moving large tables where doing a serial COPY will hold a transaction for too long, preventing vacuums from cleaning dead tuples and high-churn indexes, causing severe performance problems. If the built-in logical replication works for your table, you don't need to use pghost.

### Compatibility

If your table does not use `id` or can't avoid altering the `id` column while pghost is running, you cannot use this or will end up with an inconsistent data copy.

### Getting Started

You need to create a config to hand to pghost. You can check the `examples` directory of this project for a fully documented config. A minimal example looks like this:

```yaml
---
source_connection: dbname=postgres
source_connection_for_replication: dbname=postgres replication=database
source_table_name: fancy_table
source_schema_name: public

destination_connection: dbname=postgres user=remote_admin password=remote_admin host=localhost port=6432
destination_table_name: new_fancy_table
destination_schema_name: public

publication_name: pub_on_fancy_table
copy_batch_size: 100000
copy_worker_count: 10
copy_use_keyset_pagination: true
```

This assumes you have a publication in the source database. Example:

```
CREATE PUBLICATION pub_on_fancy_tables FOR TABLE fancy_table;
```

And then you can start pghost:

```
$ pghost replicate examples/config.yaml
```

### Replication With Conflict Resolution

Pghost needs to deal with conflict resolution because the COPY step is run in batches and in parallel. Here's how pghost resolves conflicts during the post-COPY replication flow:

| pgoutput message | destination table | action |
-------------------------------------------------
| `INSERT`   | record does not exist  | insert |
| `INSERT`   | record exists          | on conflict do nothing |
| `UPDATE`   | record does not exist  | do nothing |
| `UPDATE`   | record exists          | update |
| `DELETE`   | record does not exist  | do nothing |
| `DELETE`   | record exists          | delete |

The thought here is that:
1. If an insert has a conflict, it means the copy step has already picked up the record. We can ignore.
2. If an update does not hit a record, it means the record was deleted while we were copying. We can ignore.
3. If a delete does not hit a record, it means the record was deleted while we were copying. We can ignore.

In general, the destination database will be inconsistent until the COPY and initial replication to some checkpoint LSN have completed. For example, suppose several UPDATE queries are applied while COPY is running. Then, as the replication step applies change from the WAL, it will overwrite the "most recent" version with each initial revision, but will eventually match what's in master so long as the primary key is not changed.

------------------------

Postgres online schema/ data migration.

Testing the following:
1. Create a logical replication slot. DONE.
2. Let the slot continue to collect WAL. DONE.
3. Start a keyset page (or naive id range seek) in batches. This allows the vacuum to run. DONE. TODO: Impl keyset.
4. Capture the LSN (using IDENTIFY SYSTEM) after we've copied the last row. DONE.
5. Use a custom apply worker to merge data from rep slot "restart lsn" to "current lsn". DONE.
6. Either continue to let the custom apply worker run, or switch to walrecv apply.

Logical Replication:
1. Can parse pgoutput. DONE.
2. Can format attribute types into SQL expressions. KINDA DONE.
3. Can upsert into the table up to some LSN. DONE.

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
- Update the seq on the destination table so autoincr will work.

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
