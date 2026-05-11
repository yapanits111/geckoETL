import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from config import Config


st.set_page_config(page_title="Crypto Dashboard", layout="wide")


def apply_custom_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

        .stApp {
            background:
                radial-gradient(circle at 0% 0%, rgba(14, 165, 233, 0.16), transparent 34%),
                radial-gradient(circle at 100% 20%, rgba(34, 197, 94, 0.12), transparent 28%),
                linear-gradient(180deg, #f6fbff 0%, #f7faf9 100%);
        }

        .main .block-container {
            padding-top: 2.2rem;
            padding-bottom: 2rem;
            max-width: 1200px;
        }

        h1, h2, h3 {
            font-family: "Space Grotesk", sans-serif !important;
            letter-spacing: -0.02em;
            color: #0f172a;
        }

        p, span, label, div {
            font-family: "Space Grotesk", sans-serif;
        }

        .hero {
            background: linear-gradient(120deg, #0f172a 0%, #1d4ed8 45%, #0891b2 100%);
            border-radius: 18px;
            padding: 1.15rem 1.3rem;
            color: #f8fafc;
            margin-bottom: 1.1rem;
            box-shadow: 0 16px 36px rgba(15, 23, 42, 0.28);
        }

        .hero h1 {
            margin: 0;
            color: #f8fafc;
            font-size: 2.25rem;
            line-height: 1.1;
            font-weight: 700;
        }

        .hero p {
            margin: 0.38rem 0 0 0;
            color: #dbeafe;
            font-size: 0.98rem;
        }

        [data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.95);
            border: 1px solid #dbeafe;
            border-radius: 14px;
            padding: 0.65rem 0.8rem;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
        }

        [data-testid="stMetricLabel"] {
            font-family: "IBM Plex Mono", monospace;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #334155;
            font-size: 0.74rem;
        }

        [data-testid="stMetricValue"] {
            color: #0f172a;
        }

        div[data-testid="stSelectbox"] > label {
            font-family: "IBM Plex Mono", monospace;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.76rem;
            color: #334155;
        }

        [data-testid="stDataFrame"] {
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_db_url() -> str:
    return Config.get_db_url()


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    engine = create_engine(build_db_url())
    query = "SELECT symbol, date, price, daily_return, \"7day_avg\", volatility, is_bullish FROM crypto_prices ORDER BY date"
    df = pd.read_sql(query, engine)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    return df


def fmt_price(value: float) -> str:
    return f"${value:,.2f}"


def main() -> None:
    apply_custom_theme()

    df = load_data()

    if df.empty:
        st.warning("No data found. Run the ETL pipeline first with python main.py")
        return

    last_ts = df["date"].max().strftime("%Y-%m-%d")
    st.markdown(
        f"""
        <div class="hero">
            <h1>Crypto Market Dashboard</h1>
            <p>Live market indicators from your ETL pipeline. Last updated: {last_ts}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    latest = df[df["date"] == df["date"].max()]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Records", f"{len(df):,}")
    col2.metric("Coins Tracked", f"{df['symbol'].nunique()}")
    col3.metric("Avg 7-Day Price", fmt_price(latest["7day_avg"].mean()))
    col4.metric("Avg Volatility", f"{latest['volatility'].mean():.2f}")

    st.subheader("Latest Price By Symbol")
    metric_cols = st.columns(min(4, latest["symbol"].nunique() or 1))
    for idx, (_, row) in enumerate(latest.sort_values("symbol").iterrows()):
        metric_cols[idx % len(metric_cols)].metric(row["symbol"].upper(), fmt_price(float(row["price"])))

    symbol = st.selectbox("Select coin", sorted(df["symbol"].unique().tolist()))
    filtered = df[df["symbol"] == symbol].sort_values("date")

    chart_col_left, chart_col_right = st.columns(2)

    with chart_col_left:
        st.subheader("Price History")
        fig_price = px.line(
            filtered,
            x="date",
            y="price",
            title=f"{symbol.upper()} Price History",
            template="plotly_white",
        )
        fig_price.update_traces(line={"width": 3, "color": "#2563eb"})
        fig_price.update_layout(
            title={"font": {"size": 22}},
            font={"color": "#0f172a"},
            legend={"font": {"color": "#0f172a"}},
            xaxis={"titlefont": {"color": "#0f172a"}, "tickfont": {"color": "#0f172a"}},
            yaxis={"titlefont": {"color": "#0f172a"}, "tickfont": {"color": "#0f172a"}},
            paper_bgcolor="rgba(255,255,255,0)",
            plot_bgcolor="rgba(255,255,255,0)",
            margin={"l": 10, "r": 10, "t": 44, "b": 10},
        )
        st.plotly_chart(fig_price, use_container_width=True)

    with chart_col_right:
        st.subheader("Daily Returns")
        fig_returns = px.bar(
            filtered,
            x="date",
            y="daily_return",
            title=f"{symbol.upper()} Daily Returns",
            template="plotly_white",
        )
        fig_returns.update_traces(marker={"color": "#0ea5e9"})
        fig_returns.update_layout(
            title={"font": {"size": 22}},
            font={"color": "#0f172a"},
            legend={"font": {"color": "#0f172a"}},
            xaxis={"titlefont": {"color": "#0f172a"}, "tickfont": {"color": "#0f172a"}},
            yaxis={"titlefont": {"color": "#0f172a"}, "tickfont": {"color": "#0f172a"}},
            paper_bgcolor="rgba(255,255,255,0)",
            plot_bgcolor="rgba(255,255,255,0)",
            margin={"l": 10, "r": 10, "t": 44, "b": 10},
        )
        st.plotly_chart(fig_returns, use_container_width=True)

    st.subheader("Raw Data")
    st.dataframe(df.sort_values(["date", "symbol"]).tail(100), use_container_width=True)


if __name__ == "__main__":
    main()
