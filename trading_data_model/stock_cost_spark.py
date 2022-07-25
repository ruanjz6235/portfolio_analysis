# -*- conding: UTF-8 -*-
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Row, Window
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lag, isnan, sum, avg, lit, max, when, col, exp, udf, concat, pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType, DoubleType
from pyspark.sql import DataFrame
from functools import reduce
from time import time
import numpy as np
import pandas as pd
import warnings
from glob import glob
import feather

warnings.filterwarnings('ignore')
sparkconf = SparkConf().setAppName('test1').setMaster('local[*]').set('spark.ui.showConsoleProgress', 'false')
sc = SparkContext(conf=sparkconf)
spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')


class NameConst:
    fund_name = 'fund'
    code_name = 'code'
    flag_name = 'flag'
    type_name = 'type'
    hold_type_name = 'hold_type'

    date_name = 'date'
    time_name = 'time'
    price_name = 'price'
    close_name = 'close'
    preclose_name = 'preclose'
    volume_name = 'volume'

    hold_name = 'holding'
    days_name = 'days'
    dur_name = 'duration'
    hold_mv_name = 'holding_mv'
    count_name = 'count'
    codes_nm = ['fund', 'code']
    realized_nm = ['gx', 'rn', 'dx', 'hg', 'pg', 'qz', 'pt']
    realize_nm = ['pt_realize', 'hg_realize', 'dx_realize', 'qz_realize', 'pg_realize']
    hold_nm = ['holding', 'duration']
    turn_nm = ['buy_turn', 'sell_turn', 'turnover']


nc = NameConst()
columns = [x for x in NameConst.__dict__.keys() if x.find('name') >= 0]
columns_string = ['fund', 'code', 'flag', 'hold_type']
columns_double1 = ['date', 'time', 'price', 'volume', 'close', 'preclose', 'type', 'count']
columns_double2 = nc.realize_nm + nc.realized_nm + nc.hold_nm + nc.turn_nm
SCHEMA = []
for i, list_ in enumerate([columns_string, columns_double1, columns_double2]):
    for name in list_:
        if i == 0:
            name = [x for x in NameConst.__dict__.keys() if x.find(name) >= 0][0]
            type_ = StringType()
            field = StructField(getattr(nc, name), type_, True)
        elif i == 1:
            name = [x for x in NameConst.__dict__.keys() if x.find(name) >= 0][0]
            type_ = DoubleType()
            field = StructField(getattr(nc, name), type_, True)
        else:
            type_ = DoubleType()
            field = StructField(name, type_, True)
        SCHEMA.append(field)
SCHEMA = StructType(SCHEMA)


class ConfData:
    config = {'zhijunfund': {'host': '10.56.36.145', 'port': 3306, 'user': 'zhijunfund', 'passwd': 'zsfdcd82sf2dmd6a',
                             'database': 'zhijunfund'},
              'zdj': ('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate'),
              'jydb': ('jydb', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate'),
              'edw': {'host': '10.52.40.222', 'port': 21050, 'username': 'fundrating',
                      'password': '6B2O02sP1OhYoLlX12OR',
                      'database': 'edw', 'auth': 'LDAP'},
              'bizdm': {'host': '10.52.40.222', 'port': 21050, 'username': 'fundrating',
                        'password': '6B2O02sP1OhYoLlX12OR',
                        'database': 'bizdm', 'auth': 'LDAP'}}
    data_dict = {'holding': 'hld_qty', 'holding_mv': 'hld_mktval', 'pre_holding_mv': 'pre_hld_mktval',
                 'pt': 'bs_trd_prft', 'dx': 'newsec_trd_prft', 'rn': 'dayin_trd_prft', 'pg': 'quota_trd_prft',
                 'hg': 'bonus_share_trd_prft', 'gx': 'dividend_trd_prft', 'pt_realize': 'bs_hld_prft',
                 'dx_realize': 'newsec_hld_prft', 'pg_realize': 'quota_hld_prft', 'hg_realize': 'bonus_share_hld_prft',
                 'duration': 'hld_term', 'turnover': 'mtch_amt', 'buy_turn': 'buy_mtch_amt',
                 'sell_turn': 'sell_mtch_amt',
                 'rn_turnover': 'dayin_mtch_amt', 'frn_turnover': 'undayin_mtch_amt'}

    def get_days_data(self, dates, query, schema, **kwargs):
        days_data = []
        for date in dates:
            day_data = spark.sql(query.format(date=date, **kwargs))
            days_data.append(day_data)
        days_data = reduce(DataFrame.unionAll, days_data)
        return days_data

    def get_stock_close(self, dates):
        stock_close_query = """select biz_dt, sec_code, cls_price, last_price
        from edw.T_PRD_EXCH_SEC_QUOTE where biz_dt = {date} and trd_cgy_cd in ('1', '2')"""
        stock_close = self.get_days_data(dates, stock_close_query, 'edw')
        columns_old = [x.split(' ')[-1] for x in stock_close_query.split('\n')[0].split(', ')]
        columns_new = ['date', 'code', 'close', 'preclose']
        map_col = dict(zip(columns_old, columns_new))
        return stock_close.select([col(c).alias(map_col.get(c, c)) for c in stock_close.columns])

    def get_day_tradings(self, date):
        query = """select src_cust_no, evt_dt, mtch_tm, biz_flg, sec_code, mtch_price, mtch_qty
        from edw.T_EVT_SEC_DLV_JOUR where evt_dt = {}""".format(str(int(date)))
        day_trading = spark.sql(query)
        columns_old = [x.split(' ')[-1] for x in query.split('\n')[0].split(', ')]
        columns_new = ['fund', 'date', 'time', 'flag', 'code', 'price', 'volume']
        map_col = dict(zip(columns_old, columns_new))
        day_trading = day_trading.select([col(c).alias(map_col.get(c, c)) for c in day_trading.columns])
        day_trading = day_trading.withColumn('flag', day_trading['flag'].cast('int').cast('string'))
        return day_trading

    def get_trd_idx(self, zs_code, dates):
        trading_query = """select * from T_CUST_D_STK_TRD_IDX where biz_dt = {date} and src_cust_no = '{zs_code}'
        and idx_code in ('hld_mktval', 'buy_mtch_amt', 'sell_mtch_amt', 'mtch_amt')"""
        trading = self.get_days_data(dates, trading_query, 'zhijunfund', zs_code=zs_code)
        del trading['id'], trading['updatetime']
        return trading

    def get_dates(self, **kwargs):
        last_date = kwargs.get('last_date')
        target, source = kwargs.get('target', 'edw.T_CUST_D_STK_TRD_IDX'), kwargs.get('source',
                                                                                      'edw.T_EVT_SEC_DLV_JOUR')
        date_name = kwargs.get('dt', 'biz_dt')
        tar_dt, sou_dt = kwargs.get('tar_dt', date_name), kwargs.get('sou_dt', date_name)
        if not last_date:
            query = """select max({}) from {}""".format(tar_dt, target)
            last_date = spark.sql(query).toPandas()
            last_date = last_date.iloc[0].iloc[0]
            if last_date is None:
                last_date = '20170101'
            else:
                last_date = str(last_date)
        if kwargs.get('if_source', None):
            query = """select distinct {} from {} where {} >= {} order by {} asc""".format(
                sou_dt, source, sou_dt, last_date, sou_dt)
            print(query)
            dates = spark.sql(query).toPandas()[sou_dt].astype('int').tolist()
        else:
            dates = [int(x.strftime('%Y%m%d')) for x in pd.date_range(last_date, pd.to_datetime('today').normalize())]
        return dates


def get_column_idx(df, column_name, func):
    df_ = df.select(func(*(df[cn] for cn in column_name)))
    num = df_.collect()[0][df_.columns[0]]
    return num


class BaseStockCost(object):
    """
    basic func
    """
    def __init__(self):
        dict_ = {k: v for k, v in NameConst.__dict__.items() if k.find('name') >= 0 or k.find('nm') >= 0}
        self.__dict__.update(dict_)

    @staticmethod
    def assert_columns(df, name):
        df_name = [x for x in df.columns if (x.lower().find(name) + 1)]
        if len(df_name) == 0:
            return df
        else:
            return df.withColumnRenamed(df_name[0], name)

    def agg_func(self, df):
        volume = sum(df[self.volume_name])
        amount = sum(df[self.volume_name] * df[self.price_name])
        price = amount / volume
        return pd.Series([volume, price, amount], index=['volume', 'amount', 'price'])

    def volume_plus(self, df):
        return df[self.volume_name] > 0

    def volume_minus(self, df):
        return df[self.volume_name] < 0

    def volume_zero(self, df):
        return df[self.volume_name] == 0

    def volume_all(self, df):
        return df[self.volume_name] > - 1e12

    def price_no_zero(self, df):
        return df[self.price_name] != 0

    @staticmethod
    def hold_type(df, deal):
        return df['hold_type'] == deal

    @staticmethod
    def deal_type(df):
        return df['hold_type'].isNull()

    @staticmethod
    def type_one(df):
        return df['type'] == 1

    @staticmethod
    def type_two(df):
        return df['type'] == 2

    cols = ['fund', 'code', 'flag', 'hold_type', 'date', 'time', 'price', 'volume', 'close', 'preclose', 'type']
    cols = [getattr(nc, x+'_name') for x in cols]
    schema_out = StructType([SCHEMA[i] for i in cols])

    def deal_factor(self, day_amount, day_trading, cond1, cond2, cond3):
        deals = day_amount.filter(cond1 & cond2)
        if deals.count() == 0:
            schema = day_trading.schema.add(StructField('type', IntegerType(), True))
            deals_new = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
            deals_new = deals_new.drop('count').withColumn('hold_type', lit('')).withColumn(
                'preclose', lit(np.nan))[self.schema_out.names]
        else:
            deals = deals.toPandas()
            deal_codes = (deals[self.fund_name] + '-' + deals[self.code_name]).tolist()
            day_trading = day_trading.withColumn('fc', concat(col(self.fund_name), lit('-'), col(self.code_name)))
            deals_new = day_trading.filter(day_trading['fc'].isin(deal_codes))
            deals_new = deals_new.drop('count', 'fc')
            if cond3 == 'one':
                deals_new = deals_new.withColumn('type', lit(2)).withColumn('hold_type', lit('')).withColumn(
                    'preclose', lit(np.nan))[self.schema_out.names]
            elif cond3 in ['rn', 'dx', 'pt', 'pg', 'qz', 'hg']:
                deals_new = deals_new.withColumn('type', lit(1)).withColumn('hold_type', lit('')).withColumn(
                    'preclose', lit(np.nan))[self.schema_out.names]
            else:
                if cond3 == 'buy':
                    deals_new = deals_new.groupBy(self.codes_nm).apply(self.get_buy_deal)
                else:
                    deals_new = deals_new.groupBy(self.codes_nm).apply(self.get_sell_deal)
        return deals_new

    @staticmethod
    @pandas_udf(schema_out, PandasUDFType.GROUPED_MAP)
    def get_buy_deal(deal_buy):
        # columns=['time', 'volume', 'price']
        b_i = 1
        deal_buy['type'] = np.nan
        deal_buy['cum'] = np.nan
        dt = np.dtype({'names': deal_buy.columns, 'formats': ['O'] * len(deal_buy.columns)})
        deal_buy = np.array(list(zip(*(deal_buy[i] for i in deal_buy.columns))), dtype=dt)
        # 划分日内回转交易和留底仓交易
        sell_deal = deal_buy[deal_buy['volume'] * b_i < 0]
        if len(sell_deal) == 0:
            deal_buy['type'] = 2
        else:
            s_num = - b_i * sell_deal['volume'].sum()
            buy_deal = deal_buy[deal_buy['volume'] * b_i > 0]
            buy_deal['cum'] = buy_deal['volume'].cumsum()
            one = buy_deal[buy_deal['cum'] * b_i <= s_num]
            if len(one) == 0:
                b_num = 0
            else:
                b_num = (b_i * one['cum']).max()
            two = buy_deal[buy_deal['cum'] * b_i > s_num]
            if s_num > b_num:
                deal = two[:1]
                one = np.hstack([one, deal])
                one['volume'][-1:] = b_i * (s_num - b_num)
                two['volume'][:1] = two['volume'][:1] - b_i * (s_num - b_num)
            # 通过type字段标记
            sell_deal['type'] = 1
            one['type'] = 1
            two['type'] = 2
            deal_buy = np.hstack([sell_deal, one, two])
            deal_buy.sort(order=['time'])
        return pd.DataFrame(deal_buy).drop(columns=['cum'])

    @staticmethod
    @pandas_udf(schema_out, PandasUDFType.GROUPED_MAP)
    def get_sell_deal(deal_buy):
        # columns=['time', 'volume', 'price']
        b_i = - 1
        deal_buy['type'] = np.nan
        deal_buy['cum'] = np.nan
        dt = np.dtype({'names': deal_buy.columns, 'formats': ['O'] * len(deal_buy.columns)})
        deal_buy = np.array(list(zip(*(deal_buy[i] for i in deal_buy.columns))), dtype=dt)
        # 划分日内回转交易和留底仓交易
        sell_deal = deal_buy[deal_buy['volume'] * b_i < 0]
        if len(sell_deal) == 0:
            deal_buy['type'] = 2
        else:
            s_num = - b_i * sell_deal['volume'].sum()
            buy_deal = deal_buy[deal_buy['volume'] * b_i > 0]
            buy_deal['cum'] = buy_deal['volume'].cumsum()
            one = buy_deal[buy_deal['cum'] * b_i <= s_num]
            if len(one) == 0:
                b_num = 0
            else:
                b_num = (b_i * one['cum']).max()
            two = buy_deal[buy_deal['cum'] * b_i > s_num]
            if s_num > b_num:
                deal = two[:1]
                one = np.hstack([one, deal])
                one['volume'][-1:] = b_i * (s_num - b_num)
                two['volume'][:1] = two['volume'][:1] - b_i * (s_num - b_num)
            # 通过type字段标记
            sell_deal['type'] = 1
            one['type'] = 1
            two['type'] = 2
            deal_buy = np.hstack([sell_deal, one, two])
            deal_buy.sort(order=['time'])
        return pd.DataFrame(deal_buy).drop(columns=['cum'])

    @staticmethod
    @pandas_udf('double', PandasUDFType.GROUPED_AGG)
    def prod_agg(*args):
        args = [x.values for x in args]
        return np.sum(np.prod(args, axis=0))

    @staticmethod
    @pandas_udf('double', PandasUDFType.GROUPED_AGG)
    def weight_mean(x, weight):
        try:
            return sum(x.values * weight.values) / sum(x.values)
        except ZeroDivisionError:
            return 0


class StockCost(BaseStockCost):
    def __init__(self, stock_close, trading, trading_assert):
        super(StockCost, self).__init__()
        self.trading = trading
        self.stock_close = stock_close
        self.trading_assert = trading_assert
        names = [x for x in self.__dict__.keys() if x.find('name') >= 0]
        for data_name in ['trading', 'trading_assert', 'stock_close']:
            for name in names:
                self.__dict__[data_name] = self.assert_columns(self.__dict__[data_name], self.__dict__[name])

    def update_trading_data(self, day_trading):
        pass

    def update_gx_data(self, day_trading):
        gx = day_trading.filter(day_trading[self.flag_name].isin(['4018']))
        gx_ret = gx.groupBy(self.codes_nm).agg({self.price_name: 'sum'})
        new = day_trading.filter(~day_trading[self.flag_name].isin(['4018']))
        return gx_ret.withColumnRenamed(gx_ret.columns[-1], 'gx'), new

    def update_rn_data(self, day_trading):
        bs_data = day_trading.filter(day_trading[self.flag_name].isin(['4001', '4002']))
        bs_data = bs_data.orderBy([self.date_name, self.time_name], ascending=[0, 1])
        bs_data = self.extract_rn(bs_data)
        type_one = self.type_one(bs_data)
        type_two = self.type_two(bs_data)
        rn_data = bs_data.filter(type_one)
        frn_data = bs_data.filter(type_two)
        schema = StructType([x for x in SCHEMA if x.name in self.codes_nm + ['rn']])
        if rn_data.count() == 0:
            rn_ret = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        else:
            rn_ret = rn_data.groupby(self.codes_nm).agg(- self.prod_agg(col(self.volume_name), col(self.price_name)))
            rn_ret = rn_ret.withColumnRenamed(rn_ret.columns[-1], 'rn')
        frn_data = frn_data.drop('type')
        return rn_ret, frn_data

    def extract_rn(self, day_trading):
        # columns=['time', 'code', 'volume', 'price']
        day_trading = day_trading.withColumn('count', lit(1))
        day_amount = day_trading[self.codes_nm + [self.volume_name, 'count']].groupBy(self.codes_nm).agg(
            {self.volume_name: 'sum', 'count': 'count'})
        columns_old = day_amount.columns
        columns_new = self.codes_nm + [self.volume_name, 'count']
        map_col = dict(zip(columns_old, columns_new))
        day_amount = day_amount.select([col(c).alias(map_col.get(c, c)) for c in day_amount.columns])
        count1 = day_amount['count'] == 1
        volume_plus = self.volume_plus(day_amount)
        volume_minus = self.volume_minus(day_amount)
        volume_zero = self.volume_zero(day_amount)
        volume_all = self.volume_all(day_amount)
        print('extract_rn: ', day_trading.columns)
        one_deal = self.deal_factor(day_amount, day_trading, count1, volume_all, 'one')
        two_deal_buy = self.deal_factor(day_amount, day_trading, ~count1, volume_plus, 'buy')
        two_deal_sell = self.deal_factor(day_amount, day_trading, ~count1, volume_minus, 'sell')
        two_deal_rn = self.deal_factor(day_amount, day_trading, ~count1, volume_zero, 'rn')
        day_data = one_deal.union(two_deal_buy).union(two_deal_sell).union(two_deal_rn)
        return day_data

    def update_dx_data(self, day_trading, deal):
        day_trading = day_trading.orderBy([self.date_name, self.time_name], ascending=[0, 1])
        dx_data, fdx_data1 = self.extract_dx(day_trading, deal)

        type_one = self.type_one(dx_data)
        type_two = self.type_two(dx_data)
        hold_type = self.hold_type(dx_data, deal)
        deal_type = self.deal_type(dx_data)
        dx_data1 = dx_data.filter(type_one)
        dx_holding = dx_data.filter(type_two & hold_type)
        fdx_data2 = dx_data.filter(type_two & deal_type)

        if dx_data1.count() == 0:
            schema = StructType([x for x in SCHEMA if x.name in self.codes_nm + [deal]])
            dx_ret = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        else:
            dx_ret = dx_data1.groupBy(self.codes_nm).agg(- self.prod_agg(col(self.volume_name), col(self.price_name)))
            dx_ret = dx_ret.withColumnRenamed(dx_ret.columns[-1], deal)
        self.trading_assert = self.trading_assert.filter(~self.hold_type(self.trading_assert, deal)).union(
            dx_holding[self.trading_assert.columns])
        fdx_data = fdx_data1[day_trading.columns].union(fdx_data2[day_trading.columns])
        return dx_ret, fdx_data

    def extract_dx(self, day_trading, deal):
        dx_cond1 = self.hold_type(self.trading_assert, deal)
        last_dx = self.trading_assert.filter(dx_cond1)

        last_dx1 = last_dx.toPandas()
        dx_codes = (last_dx1[self.code_name] + '-' + last_dx1[self.fund_name]).tolist()
        day_trading = day_trading.withColumn('fc', concat(col(self.code_name), lit('-'), col(self.fund_name)))
        flag_cond = day_trading[self.flag_name] == '4001'
        dx_cond2 = day_trading['fc'].isin(dx_codes)
        today_dx = day_trading.filter(flag_cond & dx_cond2)

        last_dx = last_dx.withColumn('flag', lit(np.nan)).withColumn('preclose', lit(np.nan))
        today_dx = today_dx.withColumn('hold_type', lit(np.nan)).drop('fc', 'count')
        dx_data = last_dx.union(today_dx[last_dx.columns])
        dx_data = dx_data.withColumn('count', lit(1))
        dx_agg = dx_data.select(*self.codes_nm, self.volume_name, 'count').groupBy(self.codes_nm).agg(
            {self.volume_name: 'sum', 'count': 'count'})

        columns_old = dx_agg.columns
        columns_new = self.codes_nm + [self.volume_name, 'count']
        map_col = dict(zip(columns_old, columns_new))
        dx_agg = dx_agg.select([col(c).alias(map_col.get(c, c)) for c in dx_agg.columns])

        count1 = dx_agg['count'] == 1
        volume_plus = self.volume_plus(dx_agg)
        volume_minus = self.volume_minus(dx_agg)
        volume_zero = self.volume_zero(dx_agg)
        volume_all = self.volume_all(dx_agg)

        print(f'extract_{deal}: ', dx_data.columns)
        one_deal = self.deal_factor(dx_agg, dx_data, count1, volume_all, 'one')
        two_deal_buy = self.deal_factor(dx_agg, dx_data.drop('type'), ~count1, volume_plus, 'buy')
        two_deal_sell = self.deal_factor(dx_agg, dx_data.drop('type'), ~count1, volume_minus, 'sell')
        two_deal_dx = self.deal_factor(dx_agg, dx_data.drop('type'), ~count1, volume_zero, deal)
        dx_data = one_deal.union(two_deal_buy).union(two_deal_sell).union(two_deal_dx)

        flag_cond2 = day_trading[self.flag_name] == '4002'
        dx_cond3 = ~dx_cond2
        fdx_data = day_trading.filter(flag_cond2 | dx_cond3)
        return dx_data, fdx_data

    def update_trading_assert1(self, day_trading):
        dx = day_trading.filter(day_trading[self.flag_name].isin(['4016']))
        dx = dx.withColumn('hold_type', lit('dx')).withColumn(self.close_name, dx[self.price_name])
        pg = day_trading.filter(day_trading[self.flag_name].isin(['4013']))
        pg = pg.withColumn('hold_type', lit('pg')).withColumn(self.close_name, pg[self.price_name])
        hg = day_trading.filter(day_trading[self.flag_name].isin(['4015']))
        hg = hg.withColumn('hold_type', lit('hg')).withColumn(self.close_name, lit(0))
        qz = day_trading.filter(day_trading[self.flag_name].isin(['4017']))
        qz = qz.withColumn('hold_type', lit('qz')).withColumn(self.close_name, lit(0))
        trading_assert = dx.union(pg).union(hg).union(qz)[self.trading_assert.columns]
        trading_assert1 = trading_assert[trading_assert[self.close_name].isNull()]
        trading_assert2 = trading_assert[~trading_assert[self.close_name].isNull()]
        return trading_assert1.withColumn(self.close_name, col(self.price_name)).union(trading_assert2)

    def update_trading_assert2(self, day_trading, date):
        pt_cond = self.trading_assert['hold_type'] == 'pt'
        last_holding = self.trading_assert[pt_cond].withColumn('preclose', lit(np.nan))
        last_holding = last_holding.withColumn('flag', lit(np.nan))
        today_holding = last_holding.union(day_trading.withColumn('hold_type', lit('pt'))[last_holding.columns])

        columns_new = self.codes_nm + [self.volume_name, 'count']
        today_holding = today_holding.withColumn('count', lit(1))
        hold_agg = today_holding[columns_new].groupBy(self.codes_nm).agg({self.volume_name: 'sum', 'count': 'count'})
        columns_old = hold_agg.columns
        map_col = dict(zip(columns_old, columns_new))
        hold_agg = hold_agg.select([col(c).alias(map_col.get(c, c)) for c in hold_agg.columns])
        count1 = hold_agg['count'] == 1
        volume_plus = self.volume_plus(hold_agg)
        volume_minus = self.volume_minus(hold_agg)
        volume_zero = self.volume_zero(hold_agg)
        volume_all = self.volume_all(hold_agg)

        print('extract_pt: ', today_holding.columns)
        one_deal = self.deal_factor(hold_agg, today_holding, count1, volume_all, 'one')
        two_deal_buy = self.deal_factor(hold_agg, today_holding, ~count1, volume_plus, 'buy')
        two_deal_sell = self.deal_factor(hold_agg, today_holding, ~count1, volume_minus, 'sell')
        two_deal_pt = self.deal_factor(hold_agg, today_holding, ~count1, volume_zero, 'pt')
        trading_assert = one_deal.union(two_deal_buy).union(two_deal_sell).union(two_deal_pt)
        type_one = self.type_one(trading_assert)
        type_two = self.type_two(trading_assert)
        pt_holding = trading_assert.filter(type_two)
        self.trading_assert = self.trading_assert.filter(~self.hold_type(self.trading_assert, 'pt')).union(
            pt_holding[self.trading_assert.columns])
        self.trading_assert = self.trading_assert.na.fill({'hold_type': 'pt'})
        pt = trading_assert.filter(type_one)
        if pt.count() == 0:
            schema = StructType([x for x in SCHEMA if x.name in self.codes_nm + ['pt']])
            pt_ret = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        else:
            pt_ret = pt.groupBy(self.codes_nm).agg(- self.prod_agg(pt[self.volume_name], pt[self.price_name]))
            pt_ret = pt_ret.withColumnRenamed('pt')
        pt_ret = pt_ret.withColumn(self.date_name, lit(date))
        return pt_ret

    def update_realize_return(self, trading_assert, day_close, date):
        self.trading_assert = self.trading_assert.drop(self.close_name)
        self.trading_assert = self.trading_assert.join(day_close[[self.code_name, self.close_name]],
                                                       on=self.code_name, how='left')
        na_ = self.trading_assert[self.trading_assert[self.close_name].isNull()]
        na_ = na_.withColumn(self.close_name, col(self.price_name))
        non_na_ = self.trading_assert[~self.trading_assert[self.close_name].isNull()]
        self.trading_assert = na_.union(non_na_).union(trading_assert)
        if self.trading_assert.empty:
            schema = StructType([x for x in SCHEMA if x.name in self.codes_nm + [self.hold_type_name, 'pt']])
            realize_return = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
            schema = StructType([x for x in SCHEMA if x.name in self.codes_nm + [self.hold_name, self.dur_name]])
            holding = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        else:
            @pandas_udf('double', PandasUDFType.GROUPED_AGG)
            def realize_agg(x, y, z):
                return np.sum(x.values * (y.values - z.values))

            realize_return = self.trading_assert.groupBy(self.codes_nm + ['hold_type']).agg(
                realize_agg(col(self.volume_name), col(self.close_name), col(self.price_name)))
            realize_return = realize_return.withColumnRenamed(realize_return.columns[-1], 'ret')
            holding = self.get_hold_days(day_close, date)
        realize_return = realize_return.groupby(self.code_name).pivot('hold_type').avg('ret')
        realize_return.columns = realize_return.columns + '_realize'
        return realize_return, holding

    def get_hold_days(self, day_close, date):
        @pandas_udf('double', PandasUDFType.SCALAR)
        def days_change(x):
            return (date - x).days

        self.trading_assert = self.trading_assert.withColumn(self.days_name, days_change(col(self.date_name)))
        holding = self.trading_assert.groupBy(self.codes_nm).agg({self.volume_name: 'sum'})
        holding = holding.withColumnRenamed(holding.columns[-1], self.hold_name)
        duration = self.trading_assert.groupBy(self.codes_nm).agg(self.weight_mean(
            col(self.volume_name), col(self.days_name)))
        duration = duration.withColumnRenamed(duration.columns[-1], 'duration')
        holding = holding.join(duration, on=self.codes_nm, how='outer')
        close = day_close[[self.code_name, self.close_name]]
        holding = holding.join(close, on=self.code_name, how='left')
        holding = holding.withColumn(self.hold_mv_name, col(self.hold_name) * col(self.close_name))
        self.trading_assert = self.trading_assert.drop(self.days_name)
        return holding

    def get_turnover(self, gx_ret, day_deal, other_deal):
        day_deal_new = day_deal.filter(day_deal[self.flag_name].isin(['4001', '4002']))
        trading_assert = other_deal.filter(self.hold_type(other_deal, 'hg') & self.price_no_zero(other_deal))
        day_turnover = day_deal_new.groupBy(self.codes_nm).agg(
            lambda x: sum(x[self.volume_name].abs() * x[self.price_name])).withColumnRenamed('day')
        other_turnover = trading_assert.groupBy(self.codes_nm).apply(
            lambda x: sum(x[self.volume_name].abs() * x[self.price_name])).withColumnRenamed('other')
        turnover = reduce(DataFrame.unionAll, [gx_ret, day_turnover, other_turnover]).sum(axis=1).withColumnRenamed(
            'turnover')
        return turnover

    def deal_decompose(self, date):
        print('calculate')
        day_deal = self.trading.join(self.stock_close, on=[self.date_name, self.code_name], how='outer')
        gx_ret, day_deal = self.update_gx_data(day_deal)
        rn_ret, day_trading = self.update_rn_data(day_deal)
        dx_ret, day_trading = self.update_dx_data(day_trading, 'dx')
        hg_ret, day_trading = self.update_dx_data(day_trading, 'hg')
        pg_ret, day_trading = self.update_dx_data(day_trading, 'pg')
        qz_ret, day_trading = self.update_dx_data(day_trading, 'qz')
        trading_assert1 = self.update_trading_assert1(day_deal)
        pt_ret = self.update_trading_assert2(day_trading, date)
        realize_return, holding = self.update_realize_return(trading_assert1, day_close, date)
        realized_return = reduce(DataFrame.unionAll, [gx_ret, rn_ret, dx_ret, pg_ret, qz_ret, pt_ret, hg_ret])
        turnover = self.get_turnover(gx_ret, day_deal, trading_assert1)
        ret = reduce(DataFrame.unionAll, [realize_return, realized_return, holding, turnover])
        ret[self.date_name] = date
        ret = ret.drop('index')
        return ret


if __name__ == '__main__':
    conf = ConfData()
    dates = conf.get_dates(last_date='20170101')
    assert_cols = ['fund', 'code', 'hold_type', 'date', 'time', 'price', 'volume', 'close']
    assert_schema = StructType([SCHEMA[i] for i in assert_cols])
    trading_assert = spark.createDataFrame(spark.sparkContext.emptyRDD(), assert_schema)
    k = 0
    time1 = time()
    funds0 = []
    close_all = feather.read_dataframe('../data/stock_close.feather')
    close_all = spark.createDataFrame(close_all)
    trading_columns = ['fund', 'code', 'flag', 'date', 'time', 'price', 'volume', 'close']
    for date in dates[1002:]:
        print(date)
        stock_close = close_all.filter(close_all['date'] == date).drop_duplicates(subset=['code'])
        trading = []
        for path in glob('../data/*.feather'):
            if path.split('/')[-1].split('.')[0] == 'stock_close':
                continue
            trading_ = feather.read_dataframe(path)
            trading_['fund'] = path.split('/')[-1].split('.')[0]
            trading.append(trading_[trading_['date'] == date])
        trading = pd.concat(trading)
        if len(trading) == 0:
            continue
        trading = spark.createDataFrame(trading)
        stc = StockCost(stock_close, trading, trading_assert)
        day_deal = stc.trading.join(stc.stock_close, on=[stc.date_name, stc.code_name], how='outer')[trading_columns]
        print('gx: ', day_deal)
        gx_ret, day_deal = stc.update_gx_data(day_deal)
        print('rn: ', day_deal)
        rn_ret, day_trading = stc.update_rn_data(day_deal)
    #     ret_old = stc.deal_decompose(date)
    #
    #
    #
    # for date in dates:
    #     time2 = time()
    #     stock_close = conf.get_stock_close([date]).drop_duplicates(subset=['date', 'code'])
    #     trading = conf.get_day_tradings(date)
    #     sc = StockCost(stock_close, trading, trading_assert)
    #     ret_old = sc.deal_decompose(date)
    #     columns = sc.isin_list(ret_old.columns, sc.realized_nm + sc.realize_nm + sc.hold_nm + sc.turn_nm)
    #     ret_old[columns] = ret_old[columns].fillna(0)
    #     trading_assert = sc.trading_assert.copy()
    #     ret_old['ind'] = None
    #     ret_old.to_parquet('./trd_idx/{}.parquet'.format(date))
    #     funds1 = ret_old[ret_old.columns[0]].unique()
    #     funds = list(funds1[~np.isin(funds1, funds0)])
    #     funds0 = funds1
    #     print('date:',date,',time:',round(time2-time1, 4),',close:',len(stock_close),',trd:',len(trading),',hld:',len(trading_assert),',trd_idx:',len(ret_old),',k:',k,',funds:',str(funds))
    #     time1 = time2
    #     k += 1
