import logging
import unittest

import yaml
from pyspark.sql import SparkSession


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Creates Spark session for test cases
        """
        with open('config/config.yaml', 'r') as f:
            cls.config = yaml.safe_load(f)

        cls.sqlContext = SparkSession.builder \
            .master(cls.config['spark-config']['host']) \
            .appName(cls.config['spark-config']['appName']) \
            .config('spark.sql.shuffle.partitions', cls.config['spark-config']['partitions']) \
            .config('spark.sql.orc.filterPushdown', cls.config['spark-config']['filterPushdown']) \
            .getOrCreate()
        cls.sqlContext.sparkContext.setLogLevel('WARN')

        cls.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    @classmethod
    def tearDownClass(cls):
        """
        Stops Spark session
        """
        cls.sqlContext.stop()
