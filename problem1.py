#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, argparse, csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp, rand
from pyspark.sql import functions as F

def write_counts_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["log_level", "count"])
        for lvl, cnt in rows:
            w.writerow([lvl, cnt])

def write_sample_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["log_entry", "log_level"])
        for log_entry, log_level in rows:
            # 让 CSV 自带引号处理（避免逗号/引号问题）
            w.writerow([log_entry, log_level])

def write_summary_txt(total_lines, lines_with_level, counts_rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    uniq = len(counts_rows)
    lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {lines_with_level:,}",
        f"Unique log levels found: {uniq}",
        "",
        "Log level distribution:"
    ]
    if lines_with_level > 0:
        for lvl, cnt in counts_rows:
            pct = cnt / lines_with_level
            lines.append(f"  {lvl:<5}: {cnt:>10,} ({pct:6.2%})")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    ap.add_argument("master", help="Spark master (e.g., local[*] or spark://<IP>:7077)")
    ap.add_argument("--net-id", required=True, help="Your net id (for app name tagging)")
    ap.add_argument("--input", required=True,
                    help="Input base path (e.g., file:///.../data/sample or s3a://<bucket>/data/)")
    ap.add_argument("--outdir", default="data/output", help="Output directory (default: data/output)")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .master(args.master)
             .appName(f"Problem1-LogLevel-{args.net_id}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 兼容用户传入 base 目录：我们在后面追加 application_*/*.log
    base = args.input.rstrip("/")
    input_glob = f"{base}/application_*/*.log"

    df = spark.read.text(input_glob)

    parsed = df.select(
        regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp_str"),
        regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("log_level"),
        regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)\s+([^:]+):", 2).alias("component"),
        F.col("value").alias("log_entry")
    ).withColumn(
        "timestamp", to_timestamp("timestamp_str", "yy/MM/dd HH:mm:ss")
    )

    total_lines = df.count()
    has_level = parsed.filter(col("log_level") != "").cache()
    lines_with_level = has_level.count()

    # 级别统计（按数量降序）
    level_counts_df = has_level.groupBy("log_level").count().orderBy(F.desc("count"))
    counts_rows = [(r["log_level"], int(r["count"])) for r in level_counts_df.collect()]

    # 随机样本 10 行
    sample_df = has_level.orderBy(rand()).limit(10).select("log_entry", "log_level")
    sample_rows = [(r["log_entry"], r["log_level"]) for r in sample_df.collect()]

    # 写出三份产物为“单文件”
    counts_path  = os.path.join(args.outdir, "problem1_counts.csv")
    sample_path  = os.path.join(args.outdir, "problem1_sample.csv")
    summary_path = os.path.join(args.outdir, "problem1_summary.txt")

    write_counts_csv(counts_rows, counts_path)
    write_sample_csv(sample_rows, sample_path)
    write_summary_txt(total_lines, lines_with_level, counts_rows, summary_path)

    print("✅ Done.")
    print(f" - {counts_path}")
    print(f" - {sample_path}")
    print(f" - {summary_path}")

    spark.stop()

if __name__ == "__main__":
    main()
