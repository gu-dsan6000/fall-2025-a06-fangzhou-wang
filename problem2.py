#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# seaborn 可选（若环境没有会自动降级到 matplotlib）
try:
    import seaborn as sns
    HAS_SNS = True
except Exception:
    HAS_SNS = False

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, try_to_timestamp,
    min as smin, max as smax, countDistinct, count, lit
)


def ensure_outdir(outdir: str):
    Path(outdir).mkdir(parents=True, exist_ok=True)


def build_spark(master: str, app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    return spark


def parse_logs_to_timeline(spark: SparkSession, base: str):
    """
    读取 {base}/application_*/*.log，解析时间戳、application_id、cluster_id、app_number，
    生成每个 application 的 start_time / end_time（按日志时间最小/最大值）
    """
    glob_path = f"{base}/application_*/*.log"
    df = spark.read.text(glob_path)

    # 从文件路径提取 application_id、cluster_id、app_number
    # 路径形如：.../application_1485248649253_0052/container_...log
    df = df.withColumn("file_path", col("value")*0 + lit(""))  # 占位，防 Analyzer 合并；稍后替换
    # 重新读取并附加 input_file_name (避免上面 trick)
    from pyspark.sql.functions import input_file_name
    df = spark.read.text(glob_path).withColumn("file_path", input_file_name())

    parsed = (
        df.select(
            # 解析行首时间戳： 17/03/29 10:04:41
            regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp_str"),
            # 应用与集群信息
            regexp_extract("file_path", r"(application_\d+_\d+)", 1).alias("application_id"),
            regexp_extract("file_path", r"application_(\d+)_\d+", 1).alias("cluster_id"),
            regexp_extract("file_path", r"application_\d+_(\d+)", 1).alias("app_number"),
        )
        .withColumn("timestamp", try_to_timestamp(col("timestamp_str"), "yy/MM/dd HH:mm:ss"))
        .filter((col("application_id") != "") & col("timestamp").isNotNull())
    )

    # 对每个 application 聚合得到 start_time / end_time
    timeline = (
        parsed.groupBy("cluster_id", "application_id", "app_number")
        .agg(
            smin("timestamp").alias("start_time"),
            smax("timestamp").alias("end_time"),
        )
        .orderBy("cluster_id", "app_number")
    )

    return timeline


def write_outputs_and_plots(timeline_pdf: pd.DataFrame, outdir: str):
    """
    根据时间线 DataFrame 生成：
      1) problem2_timeline.csv
      2) problem2_cluster_summary.csv
      3) problem2_stats.txt
      4) problem2_bar_chart.png
      5) problem2_density_plot.png
    """
    ensure_outdir(outdir)

    # 1) timeline.csv
    timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
    # 规范列顺序/类型
    tl = timeline_pdf.copy()
    for c in ["start_time", "end_time"]:
        tl[c] = pd.to_datetime(tl[c])
    tl = tl[["cluster_id", "application_id", "app_number", "start_time", "end_time"]]
    tl.to_csv(timeline_csv, index=False)

    # 2) cluster_summary.csv
    cluster_summary = (
        tl.groupby("cluster_id", as_index=False)
          .agg(num_applications=("application_id", "nunique"),
               cluster_first_app=("start_time", "min"),
               cluster_last_app=("end_time", "max"))
          .sort_values("num_applications", ascending=False)
    )
    cluster_summary_csv = os.path.join(outdir, "problem2_cluster_summary.csv")
    cluster_summary.to_csv(cluster_summary_csv, index=False)

    # 3) stats.txt
    stats_txt = os.path.join(outdir, "problem2_stats.txt")
    total_clusters = cluster_summary.shape[0]
    total_apps = tl["application_id"].nunique()
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0.0

    lines = []
    lines.append(f"Total unique clusters: {total_clusters}")
    lines.append(f"Total applications: {total_apps}")
    lines.append(f"Average applications per cluster: {avg_apps:.2f}")
    lines.append("")
    lines.append("Most heavily used clusters:")
    for _, row in cluster_summary.head(10).iterrows():
        lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications")

    with open(stats_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # 4) Bar chart: apps per cluster
    bar_png = os.path.join(outdir, "problem2_bar_chart.png")
    plt.figure()
    if HAS_SNS:
        sns.barplot(data=cluster_summary, x="cluster_id", y="num_applications")
    else:
        plt.bar(cluster_summary["cluster_id"].astype(str), cluster_summary["num_applications"])
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")

    # 在柱子上方标注数值
    ax = plt.gca()
    for p in ax.patches:
        try:
            height = p.get_height()
            ax.annotate(f"{int(height)}", (p.get_x() + p.get_width()/2, height),
                        ha='center', va='bottom', fontsize=9, rotation=0, xytext=(0,3), textcoords='offset points')
        except Exception:
            pass
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(bar_png, dpi=150)
    plt.close()

    # 5) Density plot（最大应用数的集群）: 作业时长分布（对数x轴）
    dens_png = os.path.join(outdir, "problem2_density_plot.png")
    if total_apps > 0:
        # 找 app 最多的集群
        top_cluster = cluster_summary.iloc[0]["cluster_id"]
        tl_top = tl[tl["cluster_id"] == top_cluster].copy()
        # 计算持续时间（秒）
        tl_top["duration_sec"] = (tl_top["end_time"] - tl_top["start_time"]).dt.total_seconds().clip(lower=1)

        plt.figure()
        if HAS_SNS:
            sns.histplot(tl_top["duration_sec"], kde=True)
        else:
            # matplotlib 直方图（无 KDE）
            plt.hist(tl_top["duration_sec"], bins=30, alpha=0.7)
        plt.xscale("log")
        plt.title(f"Job Duration Distribution (cluster {top_cluster}, n={len(tl_top)})")
        plt.xlabel("Duration (seconds, log scale)")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(dens_png, dpi=150)
        plt.close()


def run_spark(master: str, net_id: str, base: str, outdir: str):
    spark = build_spark(master, f"dsan6000-problem2-{net_id}")

    timeline_df = parse_logs_to_timeline(spark, base)
    # 拉到 pandas 做统计与绘图
    timeline_pdf = timeline_df.toPandas()

    write_outputs_and_plots(timeline_pdf, outdir)

    spark.stop()


def regenerate_from_csv(outdir: str):
    """
    --skip-spark 模式：在已有 CSV 的基础上快速重绘图和 stats。
    需要：problem2_timeline.csv 存在。
    """
    timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
    if not os.path.exists(timeline_csv):
        raise FileNotFoundError(
            f"--skip-spark 需要先存在 {timeline_csv}。请先跑一次 Spark 再使用 --skip-spark。"
        )
    tl = pd.read_csv(timeline_csv)
    tl["start_time"] = pd.to_datetime(tl["start_time"])
    tl["end_time"] = pd.to_datetime(tl["end_time"])

    write_outputs_and_plots(tl, outdir)


def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master", nargs="?", default="local[*]",
                        help="Spark master URL, e.g., local[*] or spark://HOST:7077")
    parser.add_argument("--net-id", required=False, default="NETID",
                        help="Your NetID for Spark app naming")
    parser.add_argument("--input", required=False, default="file://$(pwd)/data/sample",
                        help="Base input directory (e.g., file://$(pwd)/data/sample or s3a://bucket/path)")
    parser.add_argument("--outdir", required=False, default="data/output",
                        help="Output directory")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark: regenerate plots & stats from CSVs")

    args = parser.parse_args()

    ensure_outdir(args.outdir)

    if args.skip_spark:
        regenerate_from_csv(args.outdir)
    else:
        # 注意：不要在 Python 中让 shell 展开 $(pwd)，直接把原样字符串传给 Spark 即可。
        # 在 Bash 中使用时用引号包裹： "file://$(pwd)/data/sample"
        run_spark(args.master, args.net_id, args.input, args.outdir)


if __name__ == "__main__":
    main()
