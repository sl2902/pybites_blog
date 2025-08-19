"""Streamlit dashboard for Pybites blog"""
import os
# from db.duckdb_client import DuckDBConnector, enable_aws_for_database
from db.supabase_client import SupabaseConnector
import streamlit as st
from loguru import logger
from typing import Any, List, Dict, Tuple
from datetime import date, datetime, timedelta
import altair as alt
import pandas as pd
from openai_services.openai_client import openai_service
from rag_system.rag_client import milvus_hybrid_service
import tiktoken
import asyncio

# duckdb_db = DuckDBConnector('pybites.db')
# try:
#     enable_aws_for_database(duckdb_db, region='us-west-2', logger=logger)
# except Exception as e:
#     logger.error(f"Error enabling AWS for DuckDB: {e}")
#     raise

params = {
     "host": os.getenv("SUPABASE_HOST"),
     "database": os.getenv("SUPABASE_DB"),
     "user": os.getenv("SUPABASE_USER"),
     "password": os.getenv("SUPABASE_PWD"),
     "port": os.getenv("SUPABASE_PORT"),
    #  "pool_mode": os.getenv("SUPABASE_POOLMODE")
}

try:
    db = SupabaseConnector()
    logger.info("Supabase is using Streamlit secrets")
except st.errors.StreamlitSecretNotFoundError:
    db = SupabaseConnector(params)
    logger.info("Supabase is using locally stored secrets")


gold_table = "gold_pybites_blogs"

st.set_page_config(
    page_title="🐍 Pybites Blog Analytics Dashboard", 
    page_icon="🐍",
    layout="wide"
)

def get_overview_metrics():
    today = date.today()
    last_six_month_start = date(today.year, today.month, 1) - timedelta(days=180)

    total_query = f"select count(*) as total from {gold_table}"
    total_articles = db.fetchall(total_query)[0][0]

    last_six_month_query = f"""
        select count(*) as last_6_monthly_count 
        from {gold_table} 
        where date_published >= '{last_six_month_start}'
    """
    last_six_month_articles = db.fetchall(last_six_month_query)[0][0]

    top_author_query = f"""
        select author, count(*) as article_count
        from {gold_table} 
        group by author 
        order by count(*) desc
        limit 1
    """
    top_author_result = db.fetchall(top_author_query)[0][0]
    top_author = top_author_result or "N/A"

    top_tag = f"""
        with base as (
            select
                unnest(tags) t
            from {gold_table}
            )
            select
                t,
                count(*)
            from base
            group by t
            order by count(*) desc
            limit 1
    """
    top_tag = db.fetchall(top_tag)[0][0]

    return total_articles, last_six_month_articles, top_author, top_tag


def fetch_authors(table_name: str) -> List[Tuple]:
    result = db.fetchall(f"select author from {table_name} group by author order by author")
    return result

def fetch_tags(table_name: str) -> List[Tuple]:
    result = db.fetchall(f"""
        with base as (
            select
                unnest(tags) as t
            from
                {table_name}
        )
        select t as tags from base
    """
    )
    return result


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
                {gold_table}
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
    qry = f"""
        select author, count(*) as article_count
        from {gold_table} 
        group by author 
        order by count(*) desc, author
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

def get_recent_articles(query_selection: List, limit: int = 10, choice: str = "And") -> pd.DataFrame:
    """Get recent articles for the data tab"""
    qry = f"""
        select title, author, tags, date_published, date_modified
        from {gold_table}
        where 1=1
    """
    and_author, and_tag, or_tag = "", "", ""
    if not query_selection:
        result = db.fetchall(qry)
    if "author" in query_selection:
        if "All" in query_selection.get("author"):
            result = db.fetchall(qry)
        else:
            and_author = f"""
            and author in ({", ".join([f"'{author}'" for author in query_selection.get("author")])})
        """
    if "tag" in query_selection:
        if "All" in query_selection.get("tag"):
            result = db.fetchall(qry)
        elif len(and_author) > 0:
            and_tag = f"""
            and tags @> array[{", ".join([f"'{tag}'" for tag in query_selection.get("tag")])}]
        """
            or_tag = f"""
            or tags @> array[{", ".join([f"'{tag}'" for tag in query_selection.get("tag")])}]
        """
        else:
            and_tag = f"""
            and tags @> array[{", ".join([f"'{tag}'" for tag in query_selection.get("tag")])}]
        """

    order_clause = f"""
        order by date_published desc
        limit {limit}
    """
    if choice == "And":
        if and_author:
            qry += and_author.lstrip()
        if and_tag:
            qry += and_tag.lstrip()
    else:
        if and_author:
            qry += and_author.lstrip()
        if and_author and and_tag:
            qry += or_tag.lstrip()
        else:
            qry += and_tag.lstrip()
    
    qry +=  order_clause.lstrip()
    # logger.info(f"{qry}")
    result = db.fetchall(qry)
    
    if result:
        df = pd.DataFrame(result, columns=["Title", "Author", "Tags", "Published", "Modified"])
        return df

def preprocess(text: str) -> str:
    """Preprocess the user query"""
    encoder = tiktoken.get_encoding("cl100k_base")
    tokens = encoder.encode(text)
    decoded = encoder.decode(tokens)
    return decoded

def format_metadata(milvus_output: List[Dict[str, Any]]):
    """Format the metadata from Milvus db output"""
    metadata_collection = {}
    for result in milvus_output:
        metadata = result.get("metadata")
        if not "url" in metadata_collection:
            metadata_collection["url"] = []
            metadata_collection["author"] = []
            metadata_collection["date_published"] = []
            metadata_collection["tags"] = []
        if metadata.get("url") not in metadata_collection["url"]:
            metadata_collection["url"].append(metadata.get("url"))
            metadata_collection["author"].append(metadata.get("author"))
            metadata_collection["date_published"].append(metadata.get("date_published"))
            metadata_collection["tags"].append(metadata.get("tags"))
    
    def make_clickable(url):
        return f'<a href="{url}" target="_blank">{url}</a>'

    if metadata_collection:
        df = pd.DataFrame(metadata_collection)
        # df["url"] = df["url"].apply(make_clickable)
        # st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
        st.dataframe(
            df,
            column_config={"url": st.column_config.LinkColumn("Website link")},
        )
    
    # st.dataframe(df)

async def main():
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
        authors = [author[0] for author in fetch_authors(gold_table)]
        author_selection = st.multiselect("Authors", ["All"] + authors)

        choice = st.radio("Choice", ["And", "Or"])

        tags = [tag[0] for tag in fetch_tags(gold_table)]
        tag_selection = st.multiselect("Tags", ["All"] + tags)
    
    info, overview_tab, trends_tab, authors_tab, data_tab, search_tab = st.tabs([
        "ℹ️ Information",
        "📊 Overview", 
        "📈 Trends", 
        "👥 Authors", 
        "📋 Data",
        "🔍 Search",
    ])

    with info:
        st.title("Dashboard Information & Usage Guide")

        st.header("About the Data Source")
        st.markdown("""
        - Blog post data comes from [pybit.es](https://pybit.es).
        - Only one sitemap index was parsed [sitemap1](https://pybit.es/post-sitemap1.xml)
        - Data is scraped using custom tools and stored in DuckDB/S3/Supabase.
        """)

        st.header("How to Use the Dashboard Filters")
        st.markdown("""
        - Use the **date**, **author**, and **tags** filters at the top.
        - Combine them to narrow results.
        - The date range filter is used to query the **Trends** tab
        - The author and tag multiselect filter is used to query the **Data** tab
        - By default, when no filter selection is made 20 results are returned in the **Data** tab.
        - When "Or" is selected and selections are made for both Author and Tag, results will include articles that match either the selected authors, the selected tags, or both.
        - When "And" is selected and selections include both Author and Tag, only articles that match both the selected author(s) and tag(s) are shown.
        - Since `tags` is an array, any selection returns results if available.
        """)

        st.header("Interpreting Results")
        st.markdown("""
        - All visualizations and tables update to reflect active filters.
        """)

        st.info("Data is historical. Contact admin for questions.")

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
                help="Most popular tag"
            )
    
        st.divider()
        
        # Quick overview chart
        st.subheader("📈 Recent Activity")
        # Show last 6 months by default
        recent_start = max(start_date, date(today.year, today.month, 1) - timedelta(days=180))
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

        query_selection = {}
        if author_selection:
            query_selection = {
                "author": author_selection,
            }
        if tag_selection:
            query_selection.update({
                "tag": tag_selection
            })
        recent_df = get_recent_articles(query_selection, num_articles, choice)
        try:
            if not recent_df.empty:
                st.dataframe(recent_df, use_container_width=True)
            else:
                st.warning("No recent articles found")
        except Exception as e:
            st.warning("No recent articles found")
        try:
            # Add download option
            if not recent_df.empty:
                csv = recent_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name="pybites_articles.csv",
                    mime="text/csv"
                )
            # else:
            #     st.warning("No recent articles to download")
        except Exception as e:
            pass
    
    with search_tab:
        password = st.text_input("Enter search password:", type="password")
        if password != st.secrets.get("search_password"):
            st.warning("Please enter the correct password to use the search feature.")
            st.stop()
        choice = st.radio("Choice", ["Keyword", "Contextual Search", "Hybrid Search"])
        query = st.text_input("Enter text")
        preprocessed_query = preprocess(query)
        embedding = await openai_service.get_embedding(preprocessed_query)

        if choice == "Hybrid Search":
            results = milvus_hybrid_service.search_similarity(query, embedding, 5)
        elif choice == "Keyword":
            results = milvus_hybrid_service.search_sparse_only(query, 5)
        else:
            results = milvus_hybrid_service.search_dense_only(embedding, 5)
        # st.write(results)
        format_metadata(results)

if __name__ == "__main__":
    asyncio.run(main())