import pandas as pd
import numpy as np
import pymysql
from pyhive import hive
import cx_Oracle as cx
from pyspark import SparkContext, SparkConf
import warnings
warnings.filterwarnings('ignore')


class ConfData:
    config = {'zhijunfund': {'host': '10.56.36.145', 'port': 3306, 'user': 'zhijunfund', 'passwd': 'zsfdcd82sf2dmd6a', 'database': 'zhijunfund'},
              'funddata': {'host': 'localdev.zhijuninvest.com', 'port': 3306, 'user': 'devuser', 'passwd':'hcy6YJF123', 'database': 'funddata'},
              'zdj': ('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate'),
              'jydb': ('jydb', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate'),
              'edw': {'host': '10.52.40.222', 'port': 21050, 'username': 'fundrating', 'password': '6B2O02sP1OhYoLlX12OR',
                      'database': 'edw', 'auth': 'LDAP'},
              'bizdm': {'host': '10.52.40.222', 'port': 21050, 'username': 'fundrating', 'password': '6B2O02sP1OhYoLlX12OR',
                        'database': 'bizdm', 'auth': 'LDAP'},
              'simuwang': {'host': '120.24.90.158', 'port': 3306, 'user': 'data_user_zheshangzq', 'passwd': 'zszq@2022', 'database': 'rz_hfdb_core'}}

    @classmethod
    def get_conn(cls, schema):
        if schema == 'zhijunfund':
            conn = pymysql.connect(**(cls.config['zhijunfund']))
        elif schema == 'simuwang':
            conn = pymysql.connect(**(cls.config['simuwang']))
        elif schema == 'funddata':
            conn = pymysql.connect(**(cls.config['funddata']))
        elif schema == 'zdj':
            conn = cx.connect(*(cls.config['zdj']))
        elif schema == 'bizdm':
            conn = hive.Connection(**(cls.config['bizdm']))
        elif schema == 'edw':
            conn = hive.Connection(**(cls.config['edw']))
        elif schema == 'spark':
            sparkconf = SparkConf().setAppName('test1').setMaster('local[*]').set('spark.ui.showConsoleProgress', 'false')
            spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()
            spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')
            return spark
        else:
            conn = pymysql.connect(**(cls.config['zhijunfund']))
        return conn

    @classmethod
    def save(cls, df, table_name, cols=[]):
        if len(cols) == 0:
            cols = list(df.columns)
        schema, table = table_name.split('.')
        df = df[cols].where(pd.notnull(df), None)
        values = list(zip(*(df[i] for i in df.columns)))
        cols_ = ('%s, ' * len(cols))[: -2]
        values_new = values
        while True:
            print(len(values_new))
            if len(values_new) > 50000:
                values_in = values_new[:50000]
                values_new = values_new[50000:]
            elif len(values_new) > 0:
                values_in = values_new
                values_new = []
            else:
                break
            conn = cls.get_conn(schema)
            cursor = conn.cursor()
            cursor.executemany(f"replace into {table_name} ({','.join(cols)}) values ({cols_})", values_in)
            conn.commit()
