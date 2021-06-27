from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType


def most_genre_actor_worked_in(actor_name: str,
                               relation: DataFrame,
                               movies: DataFrame,
                               artists: DataFrame,
                               top_n_genre: int = 3) -> DataFrame:
    """
    Finds the genre(s) that an actor has mostly worked in, i.e. the number of movies and tv shows per genre.
    :param actor: str
        Name of the actor whose genre(s) are to be determined
    :param relation: DataFrame
        Relation Dataframe
    :param movies: DataFrame
        Movies Dataframe
    :param artists: DataFrame
        Artists Dataframe
    :param top_n_genre: int
        Top N genre(s)
    :return: DataFrame
        Actor/Director collaboration Dataframe
    """
    actor = artists.select(artists['nconst'], artists['primaryName']) \
        .filter(artists['primaryName'] == actor_name)

    actor_movies_gener = relation.filter((relation['category'] == 'actor') |
                                         (relation['category'] == 'actress')) \
        .join(actor, relation['nconst'] == actor['nconst'], how='inner') \
        .join(movies, relation['tconst'] == movies['tconst'], how='left_outer') \
        .select(relation['tconst'],
                relation['nconst'],
                movies["genres"],
                movies["primaryTitle"])

    actors_earch_genre_per_movie = actor_movies_gener.filter(actor_movies_gener['genres'] != "\\N") \
        .withColumn('genres', split(col('genres'), ',\\s*').cast(ArrayType(StringType()))) \
        .withColumn('genres', explode(col('genres')))

    return actors_earch_genre_per_movie.groupBy(col('genres')).agg(count(col('genres'))) \
        .orderBy(col('count(genres)'), ascending=False) \
        .limit(top_n_genre)
