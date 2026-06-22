"""Helpers for db-sync performance samples."""

from __future__ import annotations


def enrich_perf_stats_with_era(perf_stats: list[dict], era_activation: list[dict]) -> list[dict]:
    """Attach era activation metadata to each perf stats sample."""
    if not era_activation:
        return perf_stats

    sorted_eras = sorted(era_activation, key=lambda era: era["absolute_slot"])
    slots = [era["absolute_slot"] for era in sorted_eras]
    enriched: list[dict] = []

    for sample in perf_stats:
        slot_no = sample.get("slot_no")
        if slot_no is None:
            enriched.append(sample)
            continue

        idx = 0
        for i, activation_slot in enumerate(slots):
            if activation_slot <= slot_no:
                idx = i
            else:
                break
        era = sorted_eras[idx] if slots[idx] <= slot_no else None
        if era is None:
            enriched.append(sample)
            continue

        enriched.append(
            {
                **sample,
                "protocol_version": era["protocol_version"],
                "era_name": era["era_name"],
                "activation_epoch": era["activation_epoch"],
                "first_block_number": era["first_block_number"],
                "absolute_slot": era["absolute_slot"],
                "activation_time_utc": era["activation_time_utc"],
                "first_block_hash": era["first_block_hash"],
            }
        )
    return enriched
