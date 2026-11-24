---
name: analyze-sync-results
description: Analyze cardano-node sync test results, compare releases or experimental builds, investigate sync-time, memory, CPU, or per-era regressions, and prepare comparison graphs or summaries. Use whenever the user asks about node-sync performance, mainnet sync measurements, a cardano-sync-tests GitHub Actions run, downloaded sync artifacts, or where to find their result files and logs. Accepts GitHub Actions run URLs or user-provided local artifact paths. Ask for a location when neither is provided; never assume a local results directory.
---

# Analyze node sync results

Analyze a node-sync run or compare identified runs, using the repository's
analysis guide as the single source of truth.

## 1. Read the analysis guide

Read the repository's `AGENTS.md` and
[agent_docs/mainnet_analysis.md](../../../agent_docs/mainnet_analysis.md)
before inspecting measurements or drawing conclusions. The guide contains
the configuration, timing, filtering, unit, comparison, investigation, and
reporting rules. Follow it rather than substituting a shorter methodology
from this skill.

Resolve repository-relative paths from the cardano-sync-tests checkout, not
from an artifact directory or the current working directory. If this skill
was installed separately and the guide is unavailable, ask for the checkout
or guide location instead of silently proceeding without it.

## 2. Resolve the input

**GitHub Actions run URL:** follow
[Download GitHub Actions artifacts](references/github-actions.md).
Download the relevant artifacts and continue automatically. A run URL is an
artifact source; do not ask the user to download files or provide a local path
as well. Keep each run and attempt separate.

**Local artifacts:** use only the file or directory paths supplied by the user.
Inventory that location and identify each run's result JSON, node log, and
optional monitor/build/CI logs. If a supplied path does not exist, report that
and ask for a correction; do not search another guessed location.

**No location:** ask for a GitHub Actions run URL or local artifact path before
accessing any results or logs. Release numbers and artifact filenames alone
do not establish a location.

For comparisons, resolve both inputs independently. They can be two URLs,
two local paths, or one of each. If the baseline is missing, ask for its
source; do not choose a previous release or unrelated saved run automatically.
Analysis of the supplied run can proceed, but a comparison must not be invented.

## 3. Identify and verify the evidence

Inspect the actual layout. Result JSON can be named `node_sync_results.json`
or renamed to `cardano-node-<label>.json`. Identify the environment and tested
node revision from the contents, not solely the filename or artifact name.
A CI run's head SHA identifies the test harness, not necessarily cardano-node.

Inspect metadata and startup configuration as directed by the guide. If only
JSON is available, report JSON-based findings, but do not claim that effective
configuration, GC live data, or peer events were checked without their logs.
Missing optional files should limit conclusions, not prevent all analysis.

If the artifacts are db-sync rather than node-sync results, say so. This skill
does not turn db-sync measurements into a mainnet node-sync comparison.

## 4. Perform the requested analysis

Follow the guide for the requested depth:

- **Release comparison:** calculate comparable metrics and shared-range checks.
- **Suspected slowdown or memory regression:** investigate the affected interval
  and confounders before attributing a cause to a release.
- **Graphs:** use the existing static graph script with explicit labels and a
  separate output directory. Do not overwrite evidence.
- **Report or Slack summary:** use the requested format and keep measured facts,
  correlations, hypotheses, and unknowns distinct.

Do not start a new sync, build node binaries, change configuration, or modify
the harness merely to inspect existing results. Those are separate tasks.

## 5. Report

Lead with the outcome, then the most useful measurements and limitations.
For a comparison, include both build identities, elapsed time, memory, CPU,
chain coverage, and the main per-era differences where the evidence permits.
Include source run URLs for CI inputs. A successful workflow does not establish
that performance is unchanged.

Do not create a report file unless requested. For externally shared reports
and Slack text, use artifact filenames and public references, not personal,
temporary, or runner filesystem paths. Keep resolved download locations in
the working analysis context and provide them separately if the user asks.

## Answering artifact-location questions

If the question is only where results or logs can be found, inventory the
supplied directory or list the supplied run's artifacts and answer that
question without performing a full performance analysis.

If no source is supplied, ask for it. There is no default artifact directory.
