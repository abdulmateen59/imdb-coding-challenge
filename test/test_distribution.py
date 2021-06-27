import os
from random import randrange

from jobs.distribution import movies_per_year
from test.session import PySparkTestCase
from pyspark.sql.functions import col


class TestMoviesPerYear(PySparkTestCase):

    def test_get_movies_per_annum(self):

        df = self.sqlContext.read.options(header=True, sep=r'\t').csv(f"{os.getcwd()}{self.config['path']['movies']}")
        movies_per_annum = movies_per_year(df)

        assert df.count() > 0
        assert len(movies_per_annum) > 0
        assert len(movies_per_annum.columns) == 2
        assert movies_per_annum['startYear'].is_unique

        irand = randrange(0, self.config['jobs-param']['distribution_past_years'])
        year, movies_count = movies_per_annum.iloc[irand]
        assert df.filter((df['startYear'] == int(year)) & (
                    (df['titleType'] == 'movie') | (df['titleType'] == 'TvMovie'))).count() == movies_count
