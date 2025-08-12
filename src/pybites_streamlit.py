"""Streamlit dashboard for Pybites blog"""
import os
from db.duckdb_client import DuckDBConnector, enable_aws_for_duckdb
import streamlit as st
from loguru import logger
from typing import Any, List, Tuple
from datetime import date, datetime, timedelta
import altair as alt
import pandas as pd

db = DuckDBConnector('pybites.db')
try:
    enable_aws_for_duckdb(db, region='us-west-2', logger=logger)
except Exception as e:
    logger.error(f"Error enabling AWS for DuckDB: {e}")
    raise

st.set_page_config(
    page_title="🐍 Pybites Blog Analytics Dashboard", 
    page_icon="🐍",
    layout="wide"
)

def get_overview_metrics():
    today = date.today()
    last_six_month_start = date(today.year, today.month, 1) - timedelta(days=180)

    total_query = "select count(*) as total from silver_pybites_blogs"
    total_articles = db.fetchall(total_query)[0][0]

    last_six_month_query = f"""
        select count(*) as last_6_monthly_count 
        from silver_pybites_blogs 
        where date_published >= '{last_six_month_start}'
    """
    last_six_month_articles = db.fetchall(last_six_month_query)[0][0]

    top_author_query = """
        select author, count(*) as article_count
        from silver_pybites_blogs 
        group by author 
        order by count(*) desc
        limit 1
    """
    top_author_result = db.fetchall(top_author_query)[0][0]
    top_author = top_author_result or "N/A"

    top_three_tags = """
        with base as (
            select
                unnest(tags) t
            from silver_pybites_blogs
            )
            select
                t,
                count(*)
            from base
            group by t
            order by count(*) desc
            limit 1
    """
    top_tag = db.fetchall(top_three_tags)[0][0]

    return total_articles, last_six_month_articles, top_author, top_tag


def fetch_authors(table_name: str = "author_list"):
    result = db.fetchall(f"select author from {table_name}")


def line_chart(date_range: Tuple[datetime, datetime]) -> None:
    """Show the article count trend for the chosen date range"""
    start_date, end_date = date_range
    start_date = f"{start_date} 00:00:00"
    end_date = f"{end_date} 23:59:59"
    qry = f"""
            select
                extract(year from date_published) as year,
                extract(month from date_published) as month,
                count(*) as n_articles
            from
                silver_pybites_blogs
            where
                date_published between '{start_date}' and '{end_date}'
            group by
                extract(year from date_published),
                extract(month from date_published)
            order by
                extract(year from date_published),
                extract(month from date_published)
        """
    result = db.fetchall(qry)
    if not result:
        st.write("No data for the chosen range")
        return
    df = pd.DataFrame(result, columns=["year", "month", "n_articles"])
    df["date"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-" + "01"
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    # fill the gaps where no blogs were published
    full_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='MS')
    df_full = df.reindex(full_index)
    df_full["year"] = df_full.index.year
    df_full["month"] = df_full.index.month
    df_full["year_month"] = df_full.index.strftime("%Y-%m")

    chart = alt.Chart(df_full.reset_index()).mark_line(point=True).encode(
        x=alt.X("year_month", title="Year-Month"),
        y=alt.Y("n_articles", title="n_articles"),
        tooltip=["year_month", "n_articles"]
    ).properties(
        title="Articles per month"
    )
    
    st.altair_chart(chart, use_container_width=True)

def author_chart():
    """Create bar chart for articles by author"""
    qry = """
        select author, count(*) as article_count
        from silver_pybites_blogs 
        group by author 
        order by count(*) desc 
        limit 10
    """
    
    result = db.fetchall(qry)
    if not result:
        st.warning("No author data found")
        return
        
    df = pd.DataFrame(result, columns=["author", "article_count"])
    
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("article_count", title="Number of Articles"),
        y=alt.Y("author", sort="-x", title="Author"),
        tooltip=["author", "article_count"]
    ).properties(
        title="Articles by Author",
        height=400
    )
    
    st.altair_chart(chart, use_container_width=True)

def get_recent_articles(limit=10):
    """Get recent articles for the data tab"""
    qry = f"""
        select title, author, tags, date_published
        from silver_pybites_blogs 
        order by date_published desc, author
        limit {limit}
    """
    
    result = db.fetchall(qry)
    if result:
        df = pd.DataFrame(result, columns=["Title", "Author", "Tags", "Published"])
        return df
    return pd.DataFrame()

def main():
    st.title("🐍 Pybites Blog Analytics Dashboard")

    # author = st.selectbox("Author", ["All"] + authors)
    with st.sidebar:
        today = date.today()
        default_start = date(today.year - 1, 1, 1)
        
        st.sidebar.write("**Select Date Range**")
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            start_date = st.date_input(
                "From",
                value=default_start,
                min_value=date(2016, 1, 1),
                max_value=today,
                key="start_date"
            )
        
        with col2:
            end_date = st.date_input(
                "To", 
                value=today,
                min_value=start_date,
                max_value=today,
                key="end_date"
            )
        
        date_range = (start_date, end_date)
        st.sidebar.info(f"Range: {start_date} to {end_date}")
    
    overview_tab, trends_tab, authors_tab, data_tab = st.tabs([
        "📊 Overview", 
        "📈 Trends", 
        "👥 Authors", 
        "📋 Data"
    ])

    with overview_tab:
        st.header("Key Metrics")
        
        # Get overview metrics
        total_articles, current_month, top_author, top_tag = get_overview_metrics()
        
        # Create metric cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="🗂️ Total Articles", 
                value=f"{total_articles:,}",
                help="All time total articles published"
            )
        
        with col2:
            st.metric(
                label="📅 Last Six Months", 
                value=f"{current_month}",
                help="Articles published this month"
            )
        
        with col3:
            st.metric(
                label="✍️ Top Author", 
                value=top_author,
                help="Author with most articles"
            )
        
        with col4:
            st.metric(
                label="🏷️ Top Category", 
                value=top_tag,
                help="Most popular tag or most active year"
            )
    
        st.divider()
        
        # Quick overview chart
        st.subheader("📈 Recent Activity")
        # Show last 6 months by default
        recent_start = max(start_date, today - timedelta(days=180))
        line_chart((recent_start, end_date))

    with authors_tab:
        st.header("👥 Author Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            author_chart()
        
        with col2:
            st.subheader("Author Stats")
            # You could add more author-specific metrics here
            st.info("📊 Detailed author statistics and collaboration patterns")
    
    with trends_tab:
        st.header("📈 Publishing Trends")
        st.write(f"Detailed analysis for period: **{start_date}** to **{end_date}**")
        
        line_chart(date_range)
        
        # Add some insights
        st.subheader("💡 Insights")
        with st.expander("View Analysis Notes"):
            st.write("""
            - The month of August 2023 saw the highest number of articles ever published - 15
            - The period from 2022-03 to 2024-04 witnessed at least one article being published each month
            - Article publishing after 2024-04 has been sporadic at best
            """)
    
    with data_tab:
        st.header("📋 Recent Articles")

        num_articles = st.number_input(
            "Number of articles to display:",
            min_value=1,
            max_value=total_articles,
            value=20,
            step=1,
            help="Choose how many recent articles to show (1-100)"
        )

        recent_df = get_recent_articles(num_articles)
        if not recent_df.empty:
            st.dataframe(recent_df, use_container_width=True)
        else:
            st.warning("No recent articles found")
        
        # Add download option
        if not recent_df.empty:
            csv = recent_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="pybites_articles.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()