#!/usr/bin/env python3

import argparse
import ast
import json
import logging
import pathlib as pl
import sys

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib import ticker

sns.set_theme(style="whitegrid")
LOGGER = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sync tests result graphs.")
    parser.add_argument(
        "-i",
        "--json-files",
        required=True,
        type=str,
        nargs="+",
        help="List of JSON file paths to process.",
    )
    parser.add_argument(
        "-o", "--output-dir", required=False, help="Output directory, assume cwd if not provided"
    )
    parser.add_argument("--dpi", type=int, default=150, help="DPI for output images (default: 150)")
    parser.add_argument("--format", type=str, default="png", help="Image format (png, pdf, svg...)")
    return parser.parse_args()


def generate_static_graphs(
    file_list: list[str],
    output_dir: str,
    dpi: int,
    fmt: str,
) -> None:
    LOGGER.info("Starting the sync report generation.")

    eras = ["byron", "shelley", "allegra", "mary", "alonzo", "babbage", "conway"]
    log_data = {}
    sync_duration_data = {}
    sync_time_data = {}

    output_path = pl.Path(output_dir or ".")
    output_path.mkdir(parents=True, exist_ok=True)

    for file_path_str in file_list:
        file_path = pl.Path(file_path_str)
        if not file_path.exists():
            LOGGER.error(f"File not found: {file_path}")
            continue

        LOGGER.info(f"Processing file: {file_path}")

        try:
            with file_path.open(encoding="utf-8") as file:
                sync_results = json.load(file)
                dataset_name = file_path.name

                log_values_dict = ast.literal_eval(str(sync_results["log_values"]))
                log_data[dataset_name] = dict(
                    sorted(log_values_dict.items(), key=lambda item: str(item[0]))
                )

                sync_duration_dict = ast.literal_eval(str(sync_results["sync_duration_per_epoch"]))
                sync_duration_data[dataset_name] = sync_duration_dict

                sync_time_per_era = {
                    era: sync_results.get(f"{era}_sync_duration_secs", 0) for era in eras
                }
                sync_time_data[dataset_name] = sync_time_per_era

        except Exception:
            LOGGER.exception(f"Unexpected error with file {file_path}")

    LOGGER.info("Generating sync results graphs.")
    generate_resource_consumption_graphs(log_data, output_path, dpi=dpi, fmt=fmt)
    generate_duration_per_epoch_graphs(sync_duration_data, output_path, dpi=dpi, fmt=fmt)
    generate_sync_time_per_era_graphs(sync_time_data, eras, output_path, dpi=dpi, fmt=fmt)


def generate_resource_consumption_graphs(
    datasets: dict[str, dict],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    palette = sns.color_palette("tab10")

    def _plot(
        data_type: str, unit_conversion: float, title: str, ylabel: str, basename: str
    ) -> None:
        plt.figure(figsize=(8, 6))
        for idx, (dataset_name, data) in enumerate(datasets.items()):
            x = []
            y = []
            for value in data.values():
                if value.get("tip") and value.get(data_type):
                    raw = float(value[data_type])
                    if raw > 0:
                        x.append(int(value["tip"]))
                        y.append(raw / unit_conversion)
            plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])

        plt.title(title)
        plt.xlabel("Slot Number")
        plt.ylabel(ylabel)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_dir / f"{basename}.{fmt}", dpi=dpi, format=fmt)
        plt.close()

    _plot("rss_ram", 1024**3, "RSS Consumption", "RSS consumed [GB]", "sync_rss_consumption")
    _plot("heap_ram", 1024**3, "RAM Consumption", "RAM consumed [GB]", "sync_ram_consumption")
    _plot("cpu", 1, "CPU Load", "CPU consumed [%]", "sync_cpu_consumption")
    LOGGER.info("Successfully generated sync resource consumption graphs.")


def generate_duration_per_epoch_graphs(
    datasets: dict[str, dict],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    for idx, (dataset_name, data) in enumerate(datasets.items()):
        x = list(map(int, data.keys()))
        y = list(map(float, data.values()))
        plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])

    plt.title("Sync Duration per Epoch")
    plt.xlabel("Epoch Number")
    plt.ylabel("Sync duration [seconds]")
    plt.legend()

    ax = plt.gca()
    ax.xaxis.set_major_locator(ticker.MultipleLocator(50))  # Set x-ticks every 50

    plt.tight_layout()
    plt.savefig(output_dir / f"sync_duration_per_epoch.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Successfully generated duration per epoch graph.")


def generate_sync_time_per_era_graphs(
    datasets: dict[str, dict],
    eras: list[str],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    dataset_names = list(datasets.keys())
    values_per_era = [[datasets[name][era] for name in dataset_names] for era in eras]

    ind = np.arange(len(dataset_names))
    width = 0.6
    bottoms = np.zeros(len(dataset_names))
    palette = sns.color_palette("pastel", n_colors=len(eras))

    plt.figure(figsize=(8, 6))
    for idx, era in enumerate(eras):
        values = values_per_era[idx]
        plt.bar(ind, values, width, bottom=bottoms, label=era, color=palette[idx])
        bottoms += values

    for i, total in enumerate(bottoms):
        plt.text(ind[i], total + 5, f"{total / 1000:.3f}K", ha="center", va="bottom", fontsize=9)

    plt.xticks(ind, dataset_names, rotation=45, ha="right")
    plt.title("Sync Duration per Era")
    plt.ylabel("Sync duration [seconds]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / f"sync_time_per_era.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Successfully generated sync time per era graph.")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = get_args()
    generate_static_graphs(
        file_list=args.json_files,
        output_dir=args.output_dir or "",
        dpi=args.dpi,
        fmt=args.format,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
