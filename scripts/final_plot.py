import pandas as pd
import matplotlib.pyplot as plt

# Load benchmark data
seq_df = pd.read_csv("benchmark_sequential.csv")
par_df = pd.read_csv("benchmark_parallel.csv")

# Add Batch numbers (if not already present)
if "Batch" not in seq_df.columns:
    seq_df["Batch"] = range(1, len(seq_df) + 1)
if "Batch" not in par_df.columns:
    par_df["Batch"] = range(1, len(par_df) + 1)

# --- 1. Processing Time per Batch ---
plt.figure(figsize=(10, 5))
plt.plot(seq_df["Batch"], seq_df["TimeTaken(s)"], marker='o', label="Sequential")
plt.plot(par_df["Batch"], par_df["TimeTaken(s)"], marker='x', label="Parallel")
plt.title("⏱️ Processing Time per Batch")
plt.xlabel("Batch Number")
plt.ylabel("Time Taken (s)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("processing_time_comparison.png")
plt.show()

# --- 2. Throughput per Batch ---
plt.figure(figsize=(10, 5))
plt.plot(seq_df["Batch"], seq_df["Throughput(rec/sec)"], marker='o', label="Sequential")
plt.plot(par_df["Batch"], par_df["Throughput(rec/sec)"], marker='x', label="Parallel")
plt.title("📈 Throughput per Batch")
plt.xlabel("Batch Number")
plt.ylabel("Throughput (records/sec)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("throughput_comparison.png")
plt.show()

# --- 3. Records Processed per Batch ---
plt.figure(figsize=(10, 5))
plt.plot(seq_df["Batch"], seq_df["Records"], marker='o', label="Sequential")
plt.plot(par_df["Batch"], par_df["Records"], marker='x', label="Parallel")
plt.title("📊 Records Processed per Batch")
plt.xlabel("Batch Number")
plt.ylabel("Number of Records")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("record_count_comparison.png")
plt.show()
