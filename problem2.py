#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Problem 2: Cluster Usage Analysis

Outputs (to --outdir, default data/output):
1) problem2_timeline.csv
2) problem2_cluster_summary.csv
3) problem2_stats.txt
4) problem2_bar_chart.png
5) problem2_density_plot.png

Usage:
# Local quick run on sample folder
uv run python problem2.py local[*] \
  --net-id YOUR-NET-ID \
  --input "file://$(pwd)/data/sample" \
  --outdir data/output

# Full Spark cluster
uv run python problem2.py spark://$MASTER_PRIVATE_IP:7077 \
  --net-id YOUR-NET-ID \
  --input "hdfs:///path/to/SparkLogsRootOrLocalMount" \
  --outdir data/output

# Regenerate visuals from existing CSVs (no Spark)
uv run python problem2.py --skip-spark --outdir data/output
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

try:
    import seaborn as sns
    HAS_SNS = True
except Exception:
    HAS_SNS = False

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------- utils ----------

def ensure_outdir(outdir: str):
    Path(outdir).mkdir(parents=True, exist_ok=True)


def build_spark(master: str, app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        # Recursively read the application * subdirectory
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
        # Disable ANSI strict validation to avoid minor differences in individual functions between 3.x and 2.x
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


# ---------- core logic ----------

def parse_logs_to_timeline(spark: SparkSession, base_dir: str):
    """
    读取 {base_dir}/application_*/container_*.log
    抽取：
      - timestamp_str -> to_timestamp
      - application_id (application_<cluster_id>_<app_number>)
      - cluster_id (<cluster_id>)
      - app_number (<app_number>)
    聚合得到每个 application 的 start_time / end_time
    """
    # Precise matching to avoid mistaking directories for files
    glob_path = f"{base_dir}/application_*/container_*.log"

    # Read the text line by line while retaining the file path
    df = spark.read.text(glob_path).withColumn("file_path", F.input_file_name())

    # Sample of the timestamp format at the beginning of a log line: 17/03/29 10:04:41
    parsed = (
        df.select(
            # Line header timestamp
            F.regexp_extract(F.col("value"),
                             r"^(\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})",
                             1).alias("timestamp_str"),
            # Extract application_id/cluster_id/app_number from the file path
            F.regexp_extract(F.col("file_path"),
                             r"(application_\d+_\d+)",
                             1).alias("application_id"),
            F.regexp_extract(F.col("file_path"),
                             r"application_(\d+)_\d+",
                             1).alias("cluster_id"),
            F.regexp_extract(F.col("file_path"),
                             r"application_\d+_(\d+)",
                             1).alias("app_number"),
        )
        .filter(F.col("timestamp_str") != "")
        
        # Analysis of the Best Compatible Time
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp_str"), "yy/MM/dd HH:mm:ss"))
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("application_id") != "")
    )

    # The start and end times of each application
    timeline = (
        parsed.groupBy("cluster_id", "application_id", "app_number")
        .agg(
            F.min("timestamp").alias("start_time"),
            F.max("timestamp").alias("end_time"),
        )
        .orderBy("cluster_id", "app_number")
    )
    return timeline


def write_outputs_and_plots(timeline_pdf: pd.DataFrame, outdir: str):
    ensure_outdir(outdir)

    # --- CSV 1: timeline ---
    tl = timeline_pdf.copy()
    if tl.empty:
        timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
        tl.to_csv(timeline_csv, index=False)
        # Write the empty cluster summary/stats and skip the chart
        cluster_summary = pd.DataFrame(columns=["cluster_id", "num_applications",
                                                "cluster_first_app", "cluster_last_app"])
        cluster_summary.to_csv(os.path.join(outdir, "problem2_cluster_summary.csv"), index=False)
        with open(os.path.join(outdir, "problem2_stats.txt"), "w", encoding="utf-8") as f:
            f.write("Total unique clusters: 0\nTotal applications: 0\nAverage applications per cluster: 0.00\n")
        return

    for c in ["start_time", "end_time"]:
        tl[c] = pd.to_datetime(tl[c])

    tl = tl[["cluster_id", "application_id", "app_number", "start_time", "end_time"]]
    timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
    tl.to_csv(timeline_csv, index=False)

    # --- CSV 2: cluster summary ---
    cluster_summary = (
        tl.groupby("cluster_id", as_index=False)
          .agg(num_applications=("application_id", "nunique"),
               cluster_first_app=("start_time", "min"),
               cluster_last_app=("end_time", "max"))
          .sort_values(["num_applications", "cluster_id"], ascending=[False, True])
    )
    cluster_summary_csv = os.path.join(outdir, "problem2_cluster_summary.csv")
    cluster_summary.to_csv(cluster_summary_csv, index=False)

    # --- TXT 3: stats ---
    stats_txt = os.path.join(outdir, "problem2_stats.txt")
    total_clusters = cluster_summary.shape[0]
    total_apps = tl["application_id"].nunique()
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0.0

    lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}",
        "",
        "Most heavily used clusters:",
    ]
    for _, row in cluster_summary.head(10).iterrows():
        lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications")

    with open(stats_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # --- PNG 4: bar chart (apps per cluster) ---
    bar_png = os.path.join(outdir, "problem2_bar_chart.png")
    plt.figure()
    if HAS_SNS:
        sns.barplot(data=cluster_summary, x="cluster_id", y="num_applications")
    else:
        plt.bar(cluster_summary["cluster_id"].astype(str), cluster_summary["num_applications"])
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")

    ax = plt.gca()
    # Number marked at the top
    for p in getattr(ax, "patches", []):
        try:
            h = p.get_height()
            ax.annotate(f"{int(h)}",
                        (p.get_x() + p.get_width()/2, h),
                        ha='center', va='bottom', fontsize=9,
                        xytext=(0, 3), textcoords='offset points')
        except Exception:
            pass

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(bar_png, dpi=150)
    plt.close()

    # --- PNG 5: density (largest cluster durations) ---
    dens_png = os.path.join(outdir, "problem2_density_plot.png")
    if not cluster_summary.empty:
        top_cluster = cluster_summary.iloc[0]["cluster_id"]
        tl_top = tl[tl["cluster_id"] == top_cluster].copy()
        if not tl_top.empty:
            tl_top["duration_sec"] = (tl_top["end_time"] - tl_top["start_time"]).dt.total_seconds().clip(lower=1)

            plt.figure()
            if HAS_SNS:
                # Histogram + KDE; When the quantity is small, the KDE may be unstable, but it is still visible
                sns.histplot(tl_top["duration_sec"], kde=True)
            else:
                plt.hist(tl_top["duration_sec"], bins=30, alpha=0.75)
            #The duration distribution is usually skewed and logarithmic coordinates are used
            
            plt.xscale("log")
            plt.title(f"Job Duration Distribution (cluster {top_cluster}, n={len(tl_top)})")
            plt.xlabel("Duration (seconds, log scale)")
            plt.ylabel("Count")
            plt.tight_layout()
            plt.savefig(dens_png, dpi=150)
            plt.close()
        else:
            # If there is no data in the top cluster, generate an empty image to occupy the position
            plt.figure()
            plt.title("No durations available")
            plt.savefig(dens_png, dpi=150)
            plt.close()


def run_spark(master: str, net_id: str, base: str, outdir: str):
    spark = build_spark(master, f"dsan6000-problem2-{net_id}")
    timeline_df = parse_logs_to_timeline(spark, base)
    timeline_pdf = timeline_df.toPandas()
    write_outputs_and_plots(timeline_pdf, outdir)
    spark.stop()


def regenerate_from_csv(outdir: str):
    timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
    if not os.path.exists(timeline_csv):
        raise FileNotFoundError(
            f"--skip-spark should exist {timeline_csv}. please run Spark first, then use --skip-spark。"
        )
    tl = pd.read_csv(timeline_csv)
    if not tl.empty:
        tl["start_time"] = pd.to_datetime(tl["start_time"], errors="coerce")
        tl["end_time"] = pd.to_datetime(tl["end_time"], errors="coerce")
    write_outputs_and_plots(tl, outdir)


def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master", nargs="?", default="local[*]",
                        help="Spark master URL, e.g., local[*] or spark://HOST:7077")
    parser.add_argument("--net-id", default="NETID", help="Your NetID for Spark app naming")
    parser.add_argument("--input", default=None,
                        help="Base directory containing application_* subfolders (local or file:// or hdfs://). "
                             "If omitted, defaults to file://$PWD/data/sample")
    parser.add_argument("--outdir", default="data/output", help="Output directory")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Regenerate plots & stats from existing CSVs without Spark")
    args = parser.parse_args()

    # The default input points to sample
    if args.input is None:
        args.input = f"file://{os.getcwd()}/data/sample"

    ensure_outdir(args.outdir)

    if args.skip_spark:
        regenerate_from_csv(args.outdir)
    else:
        run_spark(args.master, args.net_id, args.input, args.outdir)


if __name__ == "__main__":
    main()
