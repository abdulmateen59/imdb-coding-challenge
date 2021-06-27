import logging
import unittest

import yaml
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import sql


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Creates Spark session for test cases
        """
        with open('config/config.yaml', 'r') as f:
            cls.config = yaml.safe_load(f)
        conf = SparkConf().setAppName(
            cls.config['spark-config']['testAppName']).setMaster(cls.config['spark-config']['host'])
        cls.sc = SparkContext(conf=conf)
        cls.sc.setLogLevel('ERROR')
        sqlContext = sql.SQLContext(cls.sc)
        sqlContext.setConf('spark.sql.shuffle.partitions', cls.config['spark-config']['partitions'])
        sqlContext.setConf('spark.sql.orc.filterPushdown', cls.config['spark-config']['filterPushdown'])
        cls.sqlContext = sqlContext

        cls.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    @classmethod
    def tearDownClass(cls):
        """
        Stops Spark session
        """
        cls.sc.stop()
