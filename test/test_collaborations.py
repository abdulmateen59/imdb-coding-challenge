import os
from random import randrange

from jobs.collaborations import actor_director_collab
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from test.session import PySparkTestCase


class TestCollaborations(PySparkTestCase):

    def test_actor_director_collaborations(self):
        movies = self.sqlContext.read.options(header=True, sep=r'\t').csv(
            f"{os.getcwd()}{self.config['path']['movies']}")
        relation = self.sqlContext.read.options(header=True, sep=r'\t').csv(
            f"{os.getcwd()}{self.config['path']['relation']}")
        artists = self.sqlContext.read.options(header=True, sep=r'\t').csv(
            f"{os.getcwd()}{self.config['path']['artists']}")

        assert relation.count() > 0
        assert artists.count() > 0

        collaborations = actor_director_collab(relation, movies, artists).toPandas()
        assert len(collaborations) > 0
        assert len(collaborations.columns) == 3

        irand = randrange(0, 10)
        director_name, actor_name, collab_count = collaborations.iloc[irand]

        actor_director_pair = artists.select(artists['nconst'], artists['primaryName']) \
            .filter(((artists['primaryName'] == actor_name) | (artists['primaryName'] == director_name)))

        relation = relation.filter(((relation['category'] == 'actor') | (relation['category'] == 'director')))
        df = relation.join(broadcast(actor_director_pair), relation['nconst'] == actor_director_pair['nconst'],
                           how='inner')
        actor = df.alias('actor')
        director = df.alias('director')

        assert actor.join(director.select(col('tconst').alias('director_tconst'),
                                          col('category').alias('director_category'),
                                          col('primaryName').alias('director_primaryName')),
                          on=[(actor['category'] != col('director_category')) & (
                                  actor['tconst'] == col('director_tconst'))], how='inner') \
                    .filter(actor['category'] == 'actor').count() == collab_count
