import argparse
import ast
import json
import logging
import os
import sys

import plotly.express as px
import plotly.graph_objects as go

COLORS = px.colors.qualitative.Plotly
LOGGER = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    """Get command line arguments."""
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
        "-o",
        "--output-dir",
        required=False,
        help="Output directory, assume cwd if not provided",
    )

    return parser.parse_args()


def generate_static_graphs(file_list: list, output_dir: str = "") -> None:
    """Process a list of JSON files, loading their content and generate sync graphs."""
    LOGGER.info("Starting the sync report generation.")

    eras = ["byron", "shelley", "allegra", "mary", "alonzo", "babbage", "conway"]

    log_data = {}
    sync_duration_data = {}
    sync_time_data = {}

    for file_path in file_list:
        if not os.path.exists(file_path):
            LOGGER.error(f"File not found: {file_path}")
            continue

        LOGGER.info(f"Processing file: {file_path}")

        try:
            with open(file_path, encoding="utf-8") as file:
                sync_results = json.load(file)
                dataset_name = os.path.basename(file_path)

                # Resources consumption
                log_values_dict = ast.literal_eval(str(sync_results["log_values"]))
                log_data[dataset_name] = dict(
                    sorted(log_values_dict.items(), key=lambda item: str(item[0]))
                )

                # Sync duration per epoch
                sync_duration_dict = ast.literal_eval(str(sync_results["sync_duration_per_epoch"]))
                sync_duration_data[dataset_name] = sync_duration_dict

                # Sync time per Era
                sync_time_per_era = {}
                for era in eras:
                    sync_time_per_era[era] = sync_results.get(f"{era}_sync_duration_secs", 0)
                sync_time_data[dataset_name] = sync_time_per_era

                LOGGER.info(f"Successfully loaded JSON data from {file_path}.")
        except Exception:
            LOGGER.exception(f"Unexpected error with file {file_path}")

    LOGGER.info("Generating sync results graphs.")
    generate_resource_consumption_graphs(datasets=log_data, output_dir=output_dir)
    generate_duration_per_epoch_graphs(datasets=sync_duration_data, output_dir=output_dir)
    generate_sync_time_per_era_graphs(datasets=sync_time_data, eras=eras, output_dir=output_dir)


def generate_resource_consumption_graphs(datasets: dict, output_dir: str) -> None:
    """Generate graphs related to resource consumption during sync."""
    # Figures for RSS, RAM, and CPU
    fig_rss = go.Figure()
    fig_ram = go.Figure()
    fig_cpu = go.Figure()

    # Helper function to process data and add traces
    def _add_traces(fig: go.Figure, data_type: str, unit_conversion: int = 1) -> None:
        for idx, (dataset_name, data) in enumerate(datasets.items()):
            resource_data = {}

            for value in data.values():
                if value["tip"] and value[data_type] and float(value[data_type]) > 0:
                    resource_data[int(value["tip"])] = float(value[data_type]) / unit_conversion

            fig.add_trace(
                go.Scatter(
                    x=list(resource_data.keys()),
                    y=list(resource_data.values()),
                    mode="lines",
                    name=dataset_name,
                    line={"color": COLORS[idx]},
                )
            )

    # Add traces for each graph
    _add_traces(fig=fig_rss, data_type="rss_ram", unit_conversion=1024**3)
    _add_traces(fig=fig_ram, data_type="heap_ram", unit_conversion=1024**3)
    _add_traces(fig=fig_cpu, data_type="cpu")

    # Update layouts
    fig_rss.update_layout(
        title="RSS Consumption",
        xaxis_title="Slot Number",
        yaxis_title="RSS consumed [GB]",
        width=800,
        height=600,
    )

    fig_ram.update_layout(
        title="RAM Consumption",
        xaxis_title="Slot Number",
        yaxis_title="RAM consumed [GB]",
        width=800,
        height=600,
    )

    fig_cpu.update_layout(
        title="CPU load",
        xaxis_title="Slot Number",
        yaxis_title="CPU consumed [%]",
        width=800,
        height=600,
    )

    # Save the graphs
    fig_rss.write_image(os.path.join(output_dir, "sync_rss_consumption.png"))
    fig_ram.write_image(os.path.join(output_dir, "sync_ram_consumption.png"))
    fig_cpu.write_image(os.path.join(output_dir, "sync_cpu_consumption.png"))

    LOGGER.info("Successfully generated sync resource consumption graphs.")


def generate_duration_per_epoch_graphs(datasets: dict, output_dir: str) -> None:
    """Generate graphs related to sync duration per epoch."""
    fig_duration_per_epoch = go.Figure()

    # Add traces for each dataset
    for idx, (dataset_name, data) in enumerate(datasets.items()):
        fig_duration_per_epoch.add_trace(
            go.Scatter(
                x=list(data.keys()),  # Epoch numbers
                y=list(data.values()),  # Sync durations
                mode="lines",
                name=dataset_name,
                line={"color": COLORS[idx]},
            )
        )

    # Update layout for better readability
    fig_duration_per_epoch.update_layout(
        title="Sync Duration per Epoch",
        xaxis_title="Epoch Number",
        yaxis_title="Sync duration [seconds]",
        width=800,
        height=600,
    )

    # Save the graph
    fig_duration_per_epoch.write_image(os.path.join(output_dir, "sync_duration_per_epoch.png"))

    LOGGER.info("Successfully generated duration per epoch graph.")


def generate_sync_time_per_era_graphs(datasets: dict, eras: list, output_dir: str) -> None:
    """Generate a bar graph showing sync duration per era for different datasets."""
    datasets_names = list(datasets.keys())
    fig_time_per_era = go.Figure()

    # Add a bar for each era
    for era in eras:
        era_values = [data[era] for data in datasets.values()]
        fig_time_per_era.add_trace(
            go.Bar(
                x=datasets_names,
                y=era_values,
                name=era,
            )
        )

    # Calculate totals for each dataset
    totals = [sum(data.values()) for data in datasets.values()]

    # Add annotations for totals above each stacked bar
    for i, total in enumerate(totals):
        fig_time_per_era.add_annotation(
            x=datasets_names[i],
            y=total,
            text=f"{total / 1000:.3f}K",
            showarrow=False,
            font={"size": 12, "color": "black"},
            yshift=10,
        )

    # Update layout for better readability
    fig_time_per_era.update_layout(
        title="Sync duration per era",
        yaxis_title="Sync duration [seconds]",
        barmode="stack",
        width=800,
        height=600,
    )

    # Save the graph
    fig_time_per_era.write_image(os.path.join(output_dir, "sync_time_per_era.png"))

    LOGGER.debug("Successfully generated sync time per era graph.")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = get_args()

    generate_static_graphs(file_list=args.json_files, output_dir=args.output_dir or "")

    return 0


if __name__ == "__main__":
    sys.exit(main())
