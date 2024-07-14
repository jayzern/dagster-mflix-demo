from dagster_snowflake import SnowflakeResource
from dagster import asset, AutoMaterializePolicy
from ..partitions import monthly_partition

import os
import pandas as pd
import matplotlib.pyplot as plt


@asset(
    deps=["dlt_mongodb_comments", "dlt_mongodb_embedded_movies"]
)
def user_engagement(snowflake: SnowflakeResource) -> None:
    """
    Movie titles and the number of user engagement (i.e. comments)
    """
    query = """
        select
            movies.title,
            movies.year AS year_released,
            count(*) as number_of_comments
        from comments comments
        join embedded_movies movies on comments.movie_id = movies._id
        group by movies.title, movies.year
        order by number_of_comments desc
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        engagement_df = cursor.fetch_pandas_all()

    engagement_df.to_csv('data/movie_engagement.csv', index=False)


@asset(
    deps=["dlt_mongodb_embedded_movies"],
    partitions_def=monthly_partition
)
def top_movies_by_month(context, snowflake: SnowflakeResource) -> None:
    """
    Top movie genres based on IMBD ratings, partitioned by month
    """
    partition_date = context.partition_key

    query = f"""
        select
            movies.title,
            movies.released,
            movies.imdb__rating,
            movies.imdb__votes,
            genres.value as genres
        from embedded_movies movies
        join embedded_movies__genres genres
            on movies._dlt_id = genres._dlt_parent_id
        where released >= '{partition_date}'::date
        and released < '{partition_date}'::date + interval '1 month'
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        movies_df = cursor.fetch_pandas_all()

    # Find top films per genre
    movies_df['partition_date'] = partition_date
    # Get the index/rows of top ratings per genre
    _selected = movies_df.groupby('GENRES')['IMDB__RATING'].idxmax()
    # Drop the NA values
    _selected = _selected.dropna()
    movies_df = movies_df.loc[_selected]

    try:
        existing = pd.read_csv('data/top_movies_by_month.csv')
        existing = existing[existing['partition_date'] != partition_date]
        existing = pd.concat([existing, movies_df]).sort_values(by="partition_date")
        existing.to_csv('data/top_movies_by_month.csv', index=False)
    except FileNotFoundError:
        movies_df.to_csv('data/top_movies_by_month.csv', index=False)


@asset(
    deps=["user_engagement"],
    auto_materialize_policy=AutoMaterializePolicy.eager()
)
def top_movies_by_engagement():
    """
    Generate a bar chart based on top 10 movies by engagement
    """
    movie_engagement = pd.read_csv('data/movie_engagement.csv')
    top_10_movies = movie_engagement.sort_values(by='NUMBER_OF_COMMENTS', ascending=False).head(10)

    plt.figure(figsize=(10, 8))
    bars = plt.barh(top_10_movies['TITLE'], top_10_movies['NUMBER_OF_COMMENTS'], color='skyblue')

    # Add year_released as text labels
    for bar, year in zip(bars, top_10_movies['YEAR_RELEASED'].astype(int)):
        plt.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2, f'{year}',
                 va='center', ha='left', color='black')

    plt.xlabel('Engagement (comments)')
    plt.ylabel('Movie Title')
    plt.title('Top Movie Engagement with Year Released')
    plt.gca().invert_yaxis()
    plt.savefig('data/top_movie_engagement.png')
