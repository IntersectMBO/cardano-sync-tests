# Mainnet sync-result analysis

Use this workflow to compare cardano-node releases or experimental builds.
It covers node sync, not db-sync performance analysis; select the correct
dataset and graph mode before starting.

The aim is to establish what changed in the measurements, whether the runs are
comparable, and how much evidence supports a possible cause. A slower run is
not automatically a software regression.

## 1. Locate and identify the runs

Results and logs are downloaded CI artifacts, not files with a fixed location
in this repository. Use the local path or GitHub Actions run URL supplied in
the prompt. For a run URL, follow the `analyze-sync-results` skill's
[artifact-download instructions](../.agents/skills/analyze-sync-results/references/github-actions.md)
and continue with the downloaded evidence automatically. If neither source is
provided, ask for one before accessing artifacts; do not assume or search a
repository-local results directory.

Typical artifact filenames:

| Artifact | Filename | Purpose |
| --- | --- | --- |
| Result JSON | `node_sync_results.json` or `cardano-node-<label>.json` | Timings, metadata, slot-associated resource samples |
| Node log | `node_sync.log` | Effective configuration, resources, peers, snapshots |
| Completion markers | `sync_markers_mainnet.json` | End of measured sync |
| Monitor log | `monitor.log` | Available host memory, swap, disk capacity, process RSS |
| OOM check | `oom.log` | Archived kernel/journal check, if supplied |
| Build log | `nix_build.log` | Build provenance and references to resolved package plans |
| CI log | `ci_step.log` | Orchestration, startup, and measurement context |

Available artifacts and their layout vary. Inspect the supplied location rather
than assuming every file exists. Ask which logs belong to which run if their
association is unclear. In particular, a missing OOM check is not a clean OOM
check. Progress files normally contain only the latest status, not a time series.

Before calculating:

1. Read the repository instructions and check `git status --short`.
2. Identify the environment, release/build label, revision, and run dates.
3. Check that each JSON and log belongs to the same run. There can be several
   runs of one release with different snapshot settings.
4. Record CPU/RAM capacity, kernel, compiler, CLI version, chain endpoint,
   chain size, and whether the run starts from genesis or resumes a database.
5. Inspect the keys and a few resource rows. Do not print an entire large JSON
   or log just to discover its schema.

`tag_no1`, `node_revision1`, or a filename may contain a git revision or an
informal label. An experimental label such as `11.2.0-fix` is not a release
version. CLI revisions are not necessarily node revisions.

Preserve source artifacts. Any re-extraction, truncation, or diagnostic output
must go to a separate file with its provenance recorded.

## 2. Verify effective configuration

Use each run's startup `Node configuration:` record as evidence of its effective
settings. A currently hosted config or a local checkout is only a reference:
it may differ from the file used in an earlier run.

Compare at least:

- Consensus mode: `PraosMode` versus `GenesisMode`.
- Genesis features: ChainSync jumping, limit-on-patience, limit-on-eagerness,
  density checks, grace periods, and historicity settings.
- LedgerDB backend, query batch size, and snapshot policy.
- All snapshot fields: interval, offset, minimum/maximum delay, rate limit,
  and number of retained disk snapshots.
- Block-fetch concurrency, peer targets, peer-sharing settings, and timeouts.
- Genesis/checkpoint hashes, bootstrap behavior, and peer-snapshot provenance.
- Effective tracing backends, severity filters, resource frequency, and runtime
  options where the artifacts make them available.

Do not confuse a config-schema restructuring or renamed key with a behavior
change. Legacy flat configuration and the version-2 `Configuration` envelope
must be compared semantically.

An omitted value is not necessarily a changed effective value: check the
defaults for the actual build. For example, an explicit concurrency of one
may simply expose a default that was previously implicit.

Matching startup records do not prove identical network conditions, runtime
options, or topology contents. Conversely, identical snapshot settings do not
prove identical runtime snapshot scheduling across implementations. Count
actual completed writes and inspect their timing.

## 3. Establish measurement boundaries

Do this before interpreting elapsed time or cumulative counters.

- Find the first actual chain extension, startup/readiness boundary, sync
  completion timestamp, and completion marker.
- Exclude post-sync artifact tests and shutdown activity from new calculations.
  Use the completion marker when available; otherwise state the timestamp
  cutoff and any uncertainty.
- Result metadata start/end timestamps can surround setup as well as sync.
  Do not assume their difference equals `sync_time_seconds1`.
- Inspect the harness revision used by each run. Clean full-sync measurement
  was changed to include launch/readiness time and count initial Byron from
  epoch zero. Older results can use a first-successful-query boundary.
- A launch timestamp is not the same as the first block timestamp. Use the
  same definition for both runs when making a timing cross-check.

### Startup and epoch-counting pitfalls

Both nodes can start at genesis even when the first successful tip query
observes epoch three or four. The historical era summary calculated its slot
count from that first observed epoch. Four omitted Byron epochs account for
`4 * 21,600 = 86,400` slots, not skipped validation.

Do not invent per-epoch durations for startup epochs that were never observed.
An era summary can correctly start at epoch zero while the observed epoch
series starts later. Initial/final epochs can also be partial.

Era slot counts derived from whole epochs are approximate for a partially
observed era. Current-era slot counts and endpoint metadata are not interchangeable.
Explain any one-second difference between the sum of era durations and the
reported whole-sync duration.

Relevant implementation:

- `sync_tests/utils/sync_entries.py`: run orchestration and launch boundary.
- `sync_tests/utils/node/__init__.py`: `wait_for_node_to_sync`, era/epoch timing,
  and slot-count calculation.
- `sync_tests/utils/logs/metrics_extractor.py`: resource extraction.
- `sync_tests/utils/logs/filtering.py`: log filtering and completion markers.

## 4. Calculate comparable metrics

Use `(new / old - 1) * 100` for percentage changes, calculated from unrounded
values. State when lower is better. Never divide by a zero baseline.

### Resource samples and units

- The node JSON's `log_values` contains timestamped `tip`, `rss_ram`, `heap_ram`,
  and `cpu` values. Process rows in chronological order.
- Memory fields are bytes. Divide by `1024**3` for GiB. Chain size and total
  allocation may instead be reported in decimal GB/TB; label them explicitly.
- For slot-based memory statistics, require a valid recorded tip and a positive
  memory value. Zero-valued RAM can mean a tip update without a resource sample,
  not a genuine zero-memory measurement.
- Retain real `cpu == 0` readings. Filtering them out biases utilization upward.
- Full resource logs can include startup samples without a tip, so their sample
  count can differ from the JSON's usable memory count.
- State sample counts, exclusions, percentiles, and the measured window.

Arithmetic sample means are not automatically time-weighted means. Sampling
gaps, merged timestamps, and carried-forward CPU readings can affect them.
Even over matching slots, a slower run spends more samples in the slow region.
Do not describe a lower whole-run mean as an unconditional improvement.

### Memory

Report peak and mean RSS, peak and mean RTS heap, and a high percentile such as
p99. Inspect the curve as well as the peak:

- Early jumps, staircase growth, long plateaus, or repeated excursions.
- Fractions above a stated threshold, clearly distinguishing RSS from heap.
- Heap decreases, counted between consecutive valid samples.
- Where the curves first diverge, using fixed slot bands.

For retained-data evidence, parse `GC live bytes` from the full resource log.
Also collect major-GC and allocation counters. These are not carried by the
usual result JSON. A higher RSS/heap alone does not establish retained live
data, and higher live data alone does not identify a leak or its owner.

Use `last - first` for cumulative counters inside the chosen window, not the
largest counter value. Detect restarts/resets and split or reject the affected
window. Do not compare process-lifetime maxima with window deltas.

A stable allocated heap that never shrinks is not, by itself, a leak. A full
sync run also does not establish long-term steady-state behavior after syncing.

### CPU

CPU load is percent of one core, not percent of the machine. A mean of 123%
means approximately 1.23 cores busy, regardless of a 32-core host.

Human-readable `Cpu Ticks` and legacy `CentiCpu` are cumulative centiseconds:

- CPU seconds: `delta_counter / 100`.
- CPU utilization percent: `delta_counter / elapsed_wall_seconds`.

Do not multiply the latter by another 100. Older extraction had a scaling
error; verify historical values against the raw log and parser provenance
instead of automatically rescaling every old result.

Compare utilization and aggregate CPU seconds separately. A run can finish
faster at higher utilization while using less total CPU time. Prefer counter
deltas over estimating CPU seconds from the number of JSON rows.

### Control chain growth

Always inspect the endpoints; a newer release usually has more chain to sync.

1. Compare complete fixed historical eras separately.
2. Repeat memory-peak calculations over a shared slot window, ending no later
   than the earlier run's endpoint.
3. Compare resource curves in equal slot bands, not only equal elapsed time.
4. For epoch timings, use genuinely shared complete epochs. Exclude partial
   initial/final epochs and inspect missing epochs before summing.
5. Treat the current era separately. Whole-era duration is not equal work if
   its endpoint moved; derived whole-epoch slots/s is only an approximate rate.

Do not silently bridge missing epochs or claim identical work from the
intersection of observed epoch keys alone. State the actual compared range
and coverage. Restricting peaks to shared slots controls chain range, not
configuration, peers, or host conditions.

## 5. Generate and inspect comparison graphs

Using the project's Python environment, run the existing script from the
user-provided directory containing the result files. For example:

```sh
python -m sync_tests.scripts.sync_static_graphs --mode node \
    -i cardano-node-11.2.1.json cardano-node-11.2.3.json \
    --labels 11.2.1 11.2.3 \
    -o graphs
```

Use a new output directory or confirm that existing output is safe to replace.
Explicit unique labels prevent revision-derived names from obscuring releases.
Do not modify the graph script just to run a comparison.

Inspect RSS, epoch duration, era duration, and smoothed CPU figures. The node
script also produces raw CPU and heap figures. Smoothed CPU defaults to
one-million-slot bands with at least 20 samples; record any overrides.
Overlapping raw CPU traces are often unreadable.

Graph units must agree with the calculations. Historical figures sometimes
say GB while using `1024**3`; explain that mismatch rather than reinterpreting
the numbers. Graphs are not a replacement for numeric matching-range checks.

## 6. Investigate a localized slowdown

Do not stop at an era-wide percentage.

1. Find the epochs/slots contributing most of the difference.
2. Sum the affected window and the rest of the era separately.
3. Map matching slot boundaries to each run's own UTC timestamps.
4. Examine equal-duration intervals immediately before, during, and after the
   slowdown: slots/s, CPU counter deltas, GC, memory, and filesystem counters.
5. Correlate transitions with peer rotation, starvation, timeouts, connection
   changes, forks/rollbacks, snapshots, and errors.

Useful events include:

- `BlockFetch.Decision.PeerStarvedUs`.
- Peer-selection status changes, churn, and promotion/demotion.
- ChainSync timeouts and `Net.Mux.Remote.ExceptionExit`.
- Snapshot start/completion events and their durations.
- Chain-selection starvation and ChainSync-jumping events, if enabled.

Count completed `Took ledger snapshot` messages, not both `Taking` and `Took`.
`Chain extended` traces may be rate-limited: their count is not a block count.
`BlockchainTime.CurrentSlotUnknown` can be expected far from tip; its presence
alone is not a failure explanation. Resource `Net bytes read 0` does not prove
there was no network traffic; verify whether that counter is meaningful.

Monitor logs can support conclusions about swapping or capacity, but ordinary
`free`, `df`, and process-RSS output cannot rule out disk latency, CPU steal,
or contention. Do not describe those as measured when no such telemetry exists.

### Known peer-related example

In the November 20, 2026 11.2.3 run, epochs 318-333 took 15,535 seconds versus
5,497 in 11.2.1. The remaining Alonzo epochs were approximately 2% faster.
During the slow period, utilization was about 53% rather than 122%.

At 22:10:33.884 UTC, `PeerStarvedUs` identified `13.115.54.31:1338`.
Throughput and utilization recovered immediately. That peer's ChainSync
connection timed out at 22:10:44.571, after recovery had begun. Do not reverse
that event order or describe the later disconnect as the demonstrated cause.

This is strong evidence of a peer-dependent block-fetch bottleneck, not proof
of a new release-specific Alonzo validation regression. The logs do not prove
why the peer was slow or exactly how long it was the active supplier.

In the investigated Genesis implementation:

- `fetchDecisionsGenesisM` resets `peersOrderStart` when issuing a request with
  no blocks outstanding.
- `checkLastChainSelStarvation` rotates a peer based on starvation relative to
  that start plus the grace period.
- `selectThePeer` selects one supplier and declines other peers.

These functions are in
`ouroboros-network/lib/Ouroboros/Network/BlockFetch/Decision/Genesis.hs`,
relative to the ouroboros-network repository. Repeated grace-period resets
are a plausible way to retain a slow but responsive peer, not a proven
mechanism in that run. Simply raising configured concurrency is not necessarily
effective in this single-supplier Genesis path.

To establish the mechanism, request targeted debug traces for block-fetch
decisions/client events, chain-selection starvation, and ChainSync jumping.
Pair those with host CPU/disk telemetry. Prefer a repeat from the same database
snapshot with controlled upstream peers before bisecting validation code.

## 7. Check source and dependency changes

Locate the sibling repositories rather than assuming the spelling of their
directory. Existing setups include `../cardano-repos`; repositories may also
be available directly beside this checkout.

Compare the actual tested revisions, not current default branches:

- Node runtime source, configuration defaults, build flags, and RTS options.
- Resolved consensus, networking/diffusion, ledger, TPraos/Praos, and Plutus
  package versions.
- Relevant release tags, patch commits, and changelogs in those repositories.

The build log can reference a Nix `plan-to-nix-pkgs/cabal-files` directory.
If retained, its package definitions identify resolved versions, source hashes,
and flags. These are stronger evidence than Cabal bounds. Plans may include
tools/tests not linked into cardano-node; separate those from runtime changes.

Same-version source hashes and flags can help verify unchanged inputs.
Unavailable archives/plans must be called out, not treated as inspected.
Do not assume same dependency versions alone prove identical binaries.

For the investigated 11.2.1/11.2.3 runs, network, diffusion, Alonzo ledger,
TPraos, and Plutus source hashes matched. Consensus 5.4.0.1 changed LSM
I/O-wait accounting, whereas the runs used V2InMemory. The protocol patch
restored a Praos header decoder. These findings weakened a validation-code
explanation, but do not rule out every software or build interaction.

Recheck these facts for later releases. Do not transfer an old diagnosis to a
new pair of runs just because the symptom looks similar.

## 8. Write the conclusion and report

Separate:

1. **Measured facts:** values, deltas, window, and sample filters.
2. **Correlations:** events aligned with a change.
3. **Hypotheses:** mechanisms supported by code but not demonstrated by traces.
4. **Unknowns and follow-up:** missing artifacts and the next discriminating test.

One run per release does not establish statistical significance or normal
run-to-run variance. Avoid declaring a small difference either a code regression
or harmless noise without support. State historical baselines as historical
when their configuration differs.

For a full report, include:

- Outcome-first summary and key metrics with percentage changes.
- Era/epoch performance, matching-range memory checks, CPU, and retained data.
- Effective configuration and system differences.
- Four main figures: RSS, smoothed CPU, epoch duration, and era duration.
- Methodology, source identities, limitations, and reproduction command.

Existing publication examples are in the sibling cardano-node-tests repository
under `src_docs/source/test_results/sync_reports/`. When asked to publish there,
read that repository's instructions, copy the selected figures with
release-specific filenames, and update its sync-reports index. Do not overwrite
unrelated report work or change earlier measurements without authorization.

Externally shared reports and Slack summaries must omit personal filesystem
paths, internal temporary directories, runner paths, and internal environment
URLs. Use release/revision identifiers, artifact basenames, UTC timestamps, and
public repository references. Keep the fuller internal provenance in analysis
notes, not in the public report.

Validate documentation with the applicable pre-commit hooks. For RST reports,
check figure references and build Sphinx where available; distinguish warnings
in the new report from unrelated autodoc/environment warnings. Do not claim
a warning-free build if it was not warning-free.
