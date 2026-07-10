# cardano-sync-tests

Sync tests for `cardano-node` and `cardano-db-sync`.

## Setup (local, without Nix)

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
# or: pip install -r requirements-dev.txt
```

CI often uses `nix develop --accept-flake-config .#python --command pytest ...` instead.

Example tags below use **10.6.2** for `cardano-node` and **13.6.0.5** for db-sync; substitute current releases as needed.

## Running tests (Pytest)

All sync workflows use Pytest with session-scoped fixtures.

### Node sync only

```bash
pytest sync_tests/tests/test_nodesync_artifacts.py \
  --node-revision 10.6.2 \
  --environment preview
```

### DB-sync only (attach to an existing node)

```bash
pytest sync_tests/tests/test_sync_dbsync.py \
  --db-sync-revision 13.6.0.5 \
  --node-socket-path /path/to/node.socket \
  --environment preview
```

### Node + DB-sync (default shelley era)

```bash
pytest sync_tests/tests/ \
  --node-revision 10.6.2 \
  --db-sync-revision 13.6.0.5 \
  --environment preview
```

### Node + DB-sync with era override

```bash
pytest sync_tests/tests/ \
  --node-revision 10.6.2 \
  --db-sync-revision 13.6.0.5 \
  --db-sync-start-era babbage \
  --environment preview
```

### Node + DB-sync with extra db-sync options

```bash
pytest sync_tests/tests/ \
  --node-revision 10.6.2 \
  --db-sync-revision 13.6.0.5 \
  --db-sync-start-era babbage \
  --db-sync-start-options "consumed-tx-out" \
  --environment preprod
```

### Custom working directory

```bash
pytest sync_tests/tests/ \
  --node-revision 10.6.2 \
  --db-sync-revision 13.6.0.5 \
  --workdir /tmp/sync_output \
  --environment mainnet
```

## CLI options

| Option | Default | Description |
|---|---|---|
| `--environment` | `preview` | `preview` \| `preprod` \| `mainnet` |
| `--node-revision` | `None` | Node tag/branch to build; omit to skip node sync |
| `--db-sync-revision` | `None` | DB-sync tag/branch; omit to skip db-sync |
| `--db-sync-start-era` | `shelley` | Min node era before starting db-sync |
| `--db-sync-start-options` | `""` | Extra db-sync flags |
| `--node-socket-path` | `None` | Existing node socket path |
| `--workdir` | `./test_workdir` | Output directory for logs and artifacts |

## Artifacts

When db-sync tests are included, `test_dbsync_artifacts.py` generates a comprehensive
test results JSON and checks db-sync logs for errors and rollbacks.

GitHub Actions uploads these bundles when CI finishes:

- `sync_logs.zip`
- `sync_results.zip`
- `monitor.zip`

Bundle generation is CI-agnostic and keyed off standard CI variables
(`CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`). The workflow uploads the zips
with `actions/upload-artifact`; local runs keep raw files under `test_workdir/`
for debugging.

Results JSON includes enriched performance samples under `system_metrics`.

Fixtures write sync-completion markers to `sync_markers_<env>.json` so that
post-sync test durations are excluded from measured sync time.

## Logs

- Node log: `test_workdir/node_sync.log`
- DB-sync log: `test_workdir/db_sync.log`

## Graph Generation

Generate performance graphs from sync test results using `sync_static_graphs.py`:

### Node-sync graphs

```bash
python3 sync_tests/scripts/sync_static_graphs.py \
  -i test_workdir/node_sync_results.json \
  -o test_workdir/graphs \
  --mode node
```

Generates (5 graphs):

- `nodesync_cpu_consumption.png` - CPU load over slot
- `nodesync_rss_consumption.png` - RSS memory over slot
- `nodesync_ram_consumption.png` - Heap memory over slot
- `nodesync_duration_per_epoch.png` - Sync duration per epoch
- `nodesync_time_per_era.png` - Sync time per era (stacked bar)

### DB-sync graphs

```bash
python3 sync_tests/scripts/sync_static_graphs.py \
  -i test_workdir/db_sync_results.json \
  -o test_workdir/graphs \
  --mode dbsync
```

Generates (7 graphs):

- `dbsync_cpu_over_time.png` - CPU usage over time
- `dbsync_rss_over_time.png` - RSS memory usage over time
- `dbsync_combined_resources.png` - Combined CPU + RSS (dual Y-axis)
- `dbsync_epoch_duration.png` - Sync duration per epoch
- `dbsync_blocks_per_epoch.png` - Blocks per epoch
- `dbsync_block_throughput.png` - Block throughput (configurable modes)
- `dbsync_era_breakdown.png` - Sync time per era

### Auto-detection

```bash
python3 sync_tests/scripts/sync_static_graphs.py \
  -i test_workdir/*_results.json \
  -o test_workdir/graphs \
  --mode auto
```

The script automatically detects whether JSON files are node-sync or db-sync format
and generates appropriate graphs.

### Graph options

| Option | Default | Description |
|---|---|---|
| `-i, --json-files` | Required | One or more JSON result files |
| `-o, --output-dir` | `.` | Output directory for graphs |
| `--mode` | `auto` | `node`, `dbsync`, or `auto` (detect from JSON) |
| `--throughput-mode` | `both` | `raw` (instantaneous), `rolling` (smoothed), `both` |
| `--throughput-window` | `5` | Rolling average window size in minutes |
| `--dpi` | `150` | Image resolution |
| `--format` | `png` | Output format (`png`, `pdf`, `svg`) |

### Throughput visualization modes

Control how block throughput is rendered:

```bash
# Raw only - shows instantaneous block insertion rate (noisy)
python3 sync_tests/scripts/sync_static_graphs.py \
  -i test_workdir/db_sync_results.json \
  -o test_workdir/graphs \
  --mode dbsync --throughput-mode raw

# Rolling average only - smoothed with configurable window
python3 sync_tests/scripts/sync_static_graphs.py \
  -i test_workdir/db_sync_results.json \
  -o test_workdir/graphs \
  --mode dbsync --throughput-mode rolling --throughput-window 10

# Both (default) - raw + rolling on same chart for correlation analysis
python3 sync_tests/scripts/sync_static_graphs.py \
  -i test_workdir/db_sync_results.json \
  -o test_workdir/graphs \
  --mode dbsync --throughput-mode both
```

### Performance-stats-only files

When using `test_workdir/cardano-db-sync/db_sync_performance_stats.json` (list of system metrics only):

- **Available graphs**: CPU, RSS, Combined Resources
- **Empty graphs**: Epoch Duration, Blocks per Epoch, Block Throughput
- **Recommendation**: Use full `test_workdir/db_sync_results.json` for complete graph set
