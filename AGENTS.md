# Agent instructions

## Sync-result analysis

Before comparing release sync results, investigating performance or memory
changes, generating comparison graphs, or writing a sync report or summary,
read [agent_docs/mainnet_analysis.md](agent_docs/mainnet_analysis.md).

Use the `analyze-sync-results` skill in `.agents/skills/analyze-sync-results/`
for this workflow. A supplied GitHub Actions run URL is an artifact source:
download the relevant artifacts rather than asking for a local path as well.
If neither a run URL nor a local artifact path is provided, ask for one before
accessing results or logs. Do not assume a repository-local artifact directory.

Follow its rules for configuration verification, measurement windows, units,
matching chain ranges, and distinguishing observations from possible causes.
Do not attribute a single-run slowdown to a release without investigating
peer behavior and other confounders.

Preserve original result files, archived logs, and unrelated working-tree changes.
Keep internal artifact paths out of externally shared reports.
