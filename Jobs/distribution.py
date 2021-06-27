import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import count
from pyspark.sql.types import IntegerType


def movies_per_year(movies: DataFrame) -> pd.Series:
    """
    Determines the number of movies released per year.
    :param movies: Dataframe
        Movies Dataframe
    :return: pd.Series
        Actor/Director collaboration Dataframe
    """
    # TODO/Assumption:
    # - Start year is also the release year

    movies = movies.withColumn("startYear", movies["startYear"].cast(IntegerType()))

    # movies = movies.withColumn("startYear", movies["endYear"].cast(IntegerType()))
    # movies = movies.withColumn("startYear",coalesce(movies.startYear, movies.endYear))

    return movies.filter(((movies['titleType'] == 'movie') | (movies['titleType'] == 'TvMovie'))) \
        .groupBy(movies.startYear) \
        .agg(count('titleType').alias("moviesCount")) \
        .orderBy('startYear', ascending=False) \
        .toPandas()


def plot_distribution(df: pd.Series, past_years: int = 100, current_year: int = 2021) -> None:
    """
    Determines the number of movies released per year.
    :param df: Series
        Movies pandas dataframe
    :param past_years: int
        Past years for graph distribution
    :param current_year: int
        Present year
    :return: Non
        Saves graph in /resources/graphs/*.png
    """

    current_year_idx = df.startYear[df.startYear == current_year].index.tolist()
    df[current_year_idx[0]: past_years + 1].plot.area(x="startYear", y='moviesCount')
    plt.ylabel('Count')
    plt.savefig(f'{os.getcwd()}/resources/graphs/past_{past_years}_years_distribution.png')
