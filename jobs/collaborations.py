from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import count


def actor_director_collab(relation: DataFrame, movies: DataFrame, artists: DataFrame,
                          top_n_collaborations: int = 10) -> DataFrame:
    """
    Finds the frequency of movies in which actors/directors have worked in the pair.
    :param relation: DataFrame
        Relation Dataframe
    :param movies: DataFrame
        Movies Dataframe
    :param artists: DataFrame
        Artists Dataframe
    :return: DataFrame
        Actor/Director collaboration Dataframe
    """
    # TODO/Assumption:
    #    - Actresses are not included, it is explicity mentioned "actor-director pairs"
    #    - To include actresses, add 'actresses' filter with an OR condition

    movies_act_dir = relation.select(relation['tconst'],
                                     relation['nconst'],
                                     relation['category']) \
        .filter((relation['category'] == 'actor') | (relation['category'] == 'director')) \
        .join(movies.select(movies['tconst'].alias('movie_tconst'),
                            col('primaryTitle')), on=[col('movie_tconst') == relation['tconst']], how='left_outer') \
        .join(artists.select(col('nconst').alias('artist_nconst'), col('primaryName')),
              col('artist_nconst') == relation['nconst'], how='left_outer')

    movies_act_dir.explain()
    df_actor = movies_act_dir.alias('df_actor')
    df_director = movies_act_dir.alias('df_director')

    # Switched from SMJ to SHJ because of data spillage to disk, but that shouldn't be the case with a dedicated
    # 16 GB memory resource; I was running at 16 GB, with about 55% used by other processes.
    return df_actor.join(df_director.hint('shuffle_hash')
                         .select(col('movie_tconst').alias('df_director_tconst'),
                                 col('category').alias('df_director_category'),
                                 col('primaryName').alias('df_director_primaryName')),
                         on=[(df_actor['category'] != col('df_director_category')) & (
                                 df_actor['movie_tconst'] == col('df_director_tconst'))], how='inner') \
        .filter(df_actor['category'] == 'actor') \
        .groupBy(col('df_director_primaryName').alias('Director'), col('primaryName').alias('Actor')) \
        .agg(count(col('df_director_tconst')).alias('collaboration_count')) \
        .orderBy(col('collaboration_count'), ascending=False) \
        .limit(top_n_collaborations)
