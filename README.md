Pghost (experimental)
======

Pronounced: pee-gee-ghost. It's meant to be somewhat similar in spirit to gh-ost.

### Description

Pghost synchronizes large tables between postgresql databases. Pghost will perform a parallel batched COPY of your table and merge in changes using the logical replication pgoutput plugin. Once pghost has finished running, your source and destination tables will be consistent and you can optionally choose to continue replicating changes from the source table by attaching a new subscription to the replication slot made by pghost.

Why bother? Pghost is built to help with moving large tables where doing a serial COPY will hold a transaction for too long, preventing vacuums from cleaning dead tuples and high-churn indexes, causing severe performance problems. If the built-in logical replication works for your table, you don't need to use pghost.

### Compatibility

If your table does not use `id` as a primary key or can't avoid altering the `id` (primary key) column while pghost is running, you cannot use this or will end up with an inconsistent data copy.

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

Pghost needs to deal with conflict resolution because the COPY step is run in batches and in parallel. The strategy is essentially "last write wins" but we can drop conflicting inserts. Here's how pghost resolves conflicts during the post-COPY replication flow:

| pgoutput message | destination table | action |
|------------------|-------------------|--------|
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

### Technical Details

Here's the flow pghost follows:

1. Create a logical replication slot to store all pending WAL changes.
2. Start a parallel batched COPY process to move source table contents to destination table.
3. Grab a "checkpoint LSN" (current wal LSN) after COPY is finished.
4. Consume from logical replication slot, merging in changes, until the "checkpoint LSN" has been reached.

After (4) has completed, the source and destination tables will be synchronized. The replication slot can be used with a postgresql subscription to continue replicating changes.

### Future Work and TODOs

- Make the SQL applier much more robust.
- Make pgoutput to SQL much more robust.
- Integration tests to verify conflict resolution.
- Support migration of multiple tables.
- Auto-create subscription to continue replication.
- Better logging.
- Handle message when replication slot is dropped.
- Update the seq on the destination table so auto-increment will work.

### License

MIT
