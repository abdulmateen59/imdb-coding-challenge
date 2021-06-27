"""
Author : Abdul Mateen
GitHub: https://github.com/abdulmateen59
Email: abdul.mateen59@yahoo.com

*** Coding Challenge ***
IMDB Movies Dataset
- https://datasets.imdbws.com/
"""
import argparse
import logging
import os

import yaml
from jobs.collaborations import actor_director_collab
from jobs.distribution import movies_per_year
from jobs.distribution import plot_distribution
from jobs.frequent_genres import most_genre_actor_worked_in
from pyspark.sql import SparkSession

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    parser = argparse.ArgumentParser()
    parser.add_argument("--Remote",
                        type=bool,
                        nargs='?',
                        default=False, help=f'Remote host needs to be configured from configuration file')
    args = parser.parse_args()

    spark_session = SparkSession.builder \
        .master(config['spark-config']['host'] if args.Remote else 'local[*]') \
        .appName(config['spark-config']['appName']) \
        .config('spark.sql.shuffle.partitions', config['spark-config']['partitions']) \
        .config('spark.sql.orc.filterPushdown', config['spark-config']['filterPushdown']) \
        .getOrCreate()
    spark_session.sparkContext.setLogLevel('WARN')

    logger.info('Loading files...')
    movies = spark_session.read.options(header=True, sep=r'\t').csv(f"{os.getcwd()}{config['path']['movies']}")
    relation = spark_session.read.options(header=True, sep=r'\t').csv(f'{os.getcwd()}'
                                                                      f"{config['path']['relation']}")
    artists = spark_session.read.options(header=True, sep=r'\t').csv(f"{os.getcwd()}{config['path']['artists']}")

    logger.info('Starting job (films produced annually and their distribution over the past 100 years)...')
    plot_distribution(movies_per_year(movies),
                      config['jobs-param']['distribution_past_years'],
                      config['jobs-param']['year'])
    logger.info('Job successfully executed')

    logger.info('Starting job (Actors directors most collaborations)...')
    actor_director_collab(relation, movies, artists, config['jobs-param']['top_n_collaborations']).show()
    logger.info('Job successfully executed')

    logger.info('Searching for Omar Sy top 3 genres he has mainly worked...')
    most_genre_actor_worked_in('Omar Sy', relation, movies, artists).show()
    logger.info('Job successfully executed')

    logger.info('Searching for Frances McDormand top 3 genres she has mainly worked...')
    most_genre_actor_worked_in('Frances McDormand', relation, movies, artists).show()
    logger.info('Job successfully executed')

    logger.info('Searching for Saoirse Ronan top 3 genres she has mainly worked...')
    most_genre_actor_worked_in('Saoirse Ronan', relation, movies, artists).show()
    logger.info('Job successfully executed')

    spark_session.stop()
