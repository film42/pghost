Pghost
======

Testing the following:
1. Create a logical replication slot. DONE.
2. Let the slot continue to collect WAL. DONE.
3. Start a keyset page (or naive id range seek) in batches. This allows the vacuum to run. DONE.
4. Capture the LSN after we've copied the last row.
5. Use a custom apply worker to merge data from rep slot "restart lsn" to "current lsn".
6. Either continue to let the custom apply worker run, or switch to walrecv apply.
