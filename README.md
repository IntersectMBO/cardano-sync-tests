# cardano-sync-tests

Sync tests for `cardano-node` and `cardano-db-sync`.

## Main entry point

All sync workflows use a single entry point:

```bash
python3 sync_tests/tests/full_sync_from_clean_state.py
```

### Node-only sync

```bash
python3 sync_tests/tests/full_sync_from_clean_state.py \
  --node-revision master \
  --environment preview
```

### Full sync (node + db-sync)

```bash
python3 sync_tests/tests/full_sync_from_clean_state.py \
  --node-revision master \
  --db-sync-revision master \
  --environment preview
```

### Full sync with era override

```bash
python3 sync_tests/tests/full_sync_from_clean_state.py \
  --node-revision master \
  --db-sync-revision master \
  --environment preview \
  --db-sync-start-era babbage
```

### DB-sync only (attach to an existing node)

```bash
python3 sync_tests/tests/full_sync_from_clean_state.py \
  --db-sync-revision master \
  --environment preview \
  --node-socket-path /path/to/node.socket
```

If `--node-socket-path` is not provided, the test attempts to use:
`CARDANO_NODE_SOCKET_PATH`, then `./db/node.socket`, then
`./test_workdir/db/node.socket`, then `./test_workdir_node_only/db/node.socket`.

## Logs

- Node logs: `test_workdir/logfile.log`
- DB-sync logs: `test_workdir/db_sync_<env>_logfile.log`
