import os

from jobs.frequent_genres import most_genre_actor_worked_in
from parameterized import parameterized
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from test.session import PySparkTestCase


class TestActorsMostGenres(PySparkTestCase):

    @parameterized.expand(['Saoirse Ronan'])
    def test_most_genre_actor_worked_in(self, actor_name):

        movies = self.sqlContext.read.options(header=True, sep=r'\t').csv(
            f"{os.getcwd()}{self.config['path']['movies']}")
        relation = self.sqlContext.read.options(header=True, sep=r'\t').csv(
            f"{os.getcwd()}{self.config['path']['relation']}")
        artists = self.sqlContext.read.options(header=True, sep=r'\t').csv(
            f"{os.getcwd()}{self.config['path']['artists']}")

        movies_titles = artists.select(artists['knownForTitles']).filter(
            ((artists['knownForTitles'] != '\\N') & (artists['primaryName'] == actor_name))).limit(1).withColumn(
            'knownForTitles', split(col('knownForTitles'), ',\\s*').cast(ArrayType(StringType()))) \
            .withColumn('knownForTitles', explode(col('knownForTitles')))

        actor_genres = movies.join(broadcast(movies_titles),
                                   movies_titles['knownForTitles'] == movies['tconst'],
                                   how='inner').select(col('genres')).withColumn('genres',
                                                                                 split(col('genres'), ',\\s*').cast(
                                                                                     ArrayType(StringType()))) \
            .withColumn('genres', explode(col('genres'))).distinct()

        df = most_genre_actor_worked_in(actor_name, relation, movies, artists, top_n_genre=4)
        assert df.count() > 0
        assert len(df.columns) == 2
        assert df.select(['genres']).distinct().take(1)

        df.join(actor_genres, df['genres'] == actor_genres['genres'], 'inner')

        if df.count() == 1:
            self.logger.warning(
                f'Contradictions in genres, {actor_name} worked mainly in different genres but was famous for'
                f'different genres')

        if df.count() == 0:
            self.logger.error(f'Make sure both actors have the same, Omar mainly works in a different genre than the'
                              f'one he/she is famous for...')
            assert False
