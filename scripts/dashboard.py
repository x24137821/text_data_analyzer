import streamlit as st
import pandas as pd
import json
import altair as alt
from streamlit_autorefresh import st_autorefresh

# Refresh every 5 seconds
st_autorefresh(interval=5000, key="refresh_key")

# Set Streamlit page config
st.set_page_config(page_title="Kinesis Dashboard", layout="wide")
st.title("📊 Real-Time Kinesis Monitoring Dashboard")
st.caption("Auto-updated every 5 seconds")

# ========== Load Word Frequency ==========
st.subheader("📌 Top 5 Trending Words (Last 5 Minutes)")
try:
    with open("word_freq.json", "r") as f:
        word_freq = json.load(f)
    top_words_df = pd.DataFrame(list(word_freq.items()), columns=["Word", "Frequency"]).nlargest(5, "Frequency")

    bar_chart = alt.Chart(top_words_df).mark_bar().encode(
        x=alt.X("Word", sort="-y"),
        y="Frequency",
        tooltip=["Word", "Frequency"]
    ).properties(width=600)
    st.altair_chart(bar_chart, use_container_width=True)
except Exception as e:
    st.error("⚠️ Could not load or parse 'word_freq.json'")
    st.exception(e)

# ========== Load Sentiment ==========
st.subheader("❤️ Real-Time Sentiment Score")
try:
    with open("sentiment.json", "r") as f:
        sentiment_data = json.load(f)
        sentiment_score = sentiment_data.get("score", 0.0)
        sentiment_label = (
            "😊 Positive" if sentiment_score > 0.1 else
            "😐 Neutral" if sentiment_score > -0.1 else
            "😠 Negative"
        )
        st.metric(label="Average Sentiment", value=f"{sentiment_score:.3f}", delta=sentiment_label)
except Exception as e:
    st.error("⚠️ Could not load or parse 'sentiment.json'")
    st.exception(e)

# ========== Benchmark Charts ==========
st.subheader("🚀 Benchmark Comparison (Sequential vs Parallel)")
try:
    seq_df = pd.read_csv("benchmark_sequential.csv")
    par_df = pd.read_csv("benchmark_parallel.csv")

    seq_df["Type"] = "Sequential"
    par_df["Type"] = "Parallel"
    combined_df = pd.concat([seq_df, par_df])

    chart_col1, chart_col2 = st.columns(2)

    # Throughput Comparison
    throughput_chart = alt.Chart(combined_df).mark_line(point=True).encode(
        x="Batch",
        y="Throughput(rec/sec)",
        color="Type",
        tooltip=["Batch", "Throughput(rec/sec)", "Type"]
    ).properties(title="📈 Throughput Comparison")
    chart_col1.altair_chart(throughput_chart, use_container_width=True)

    # Processing Time Comparison
    time_chart = alt.Chart(combined_df).mark_line(point=True).encode(
        x="Batch",
        y="TimeTaken(s)",
        color="Type",
        tooltip=["Batch", "TimeTaken(s)", "Type"]
    ).properties(title="⏱️ Processing Time Comparison")
    chart_col2.altair_chart(time_chart, use_container_width=True)

except Exception as e:
    st.error("⚠️ Could not load benchmark CSVs")
    st.exception(e)
