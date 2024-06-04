from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

import pandas as pd
from matplotlib import pyplot as plt
from sklearn.manifold import TSNE


class AdhocConfig(Config):
    filename: str
    ratings: str


def _parse_embedding(embedding_str):
    cleaned_str = embedding_str.replace(' ', '').replace('\n', '').replace('[', '').replace(']', '')
    return list(eval(cleaned_str))


@asset(
    deps=["dlt_mongodb_embedded_movies"]
)
def movie_embeddings(config: AdhocConfig, snowflake: SnowflakeResource):
    """
    Generate movie embedding plots using t-NSE algorithm, based on movie ratings
    """
    query = f"""
        select
            movies.title,
            ARRAY_AGG(embeddings.value) AS plot_embeddings
        from embedded_movies movies
        join embedded_movies__plot_embedding embeddings
            on movies._dlt_id = embeddings._dlt_parent_id
        where
            movies.imdb__rating >= {config.ratings}
        group by movies.title
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()

    # Unwrap movie embeddings, fill NA values with zero
    X = df['PLOT_EMBEDDINGS'].apply(_parse_embedding).apply(pd.Series).fillna(0)

    # Generate embeddings
    embs = TSNE(
        n_components=2,
        learning_rate='auto',
        init='random',
        perplexity=3
    ).fit_transform(X)

    # Scatter plot
    df['x'] = embs[:, 0]
    df['y'] = embs[:, 1]

    _, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(df['x'], df['y'], alpha=.1)
    for idx, title in enumerate(df['TITLE']):
        ax.annotate(title, (df['x'][idx], df['y'][idx]))
    plt.savefig('data/tnse_visualizations.png')
