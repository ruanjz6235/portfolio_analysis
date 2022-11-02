#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/26 8:56 上午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : test20220926.py
# @IDE     : PyCharm
# @REMARKS : 说明文字


import pandas as pd
import numpy as np
from functools import reduce
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm
import pymysql
import cx_Oracle as cx
import warnings

warnings.filterwarnings('ignore')


class ConfData:
    config = {'zhijunfund': {'host': '10.56.36.145', 'port': 3306, 'user': 'zhijunfund', 'passwd': 'zsfdcd82sf2dmd6a',
                             'database': 'zhijunfund'},
              'funddata': {'host': 'localdev.zhijuninvest.com', 'port': 3306, 'user': 'devuser', 'passwd': 'hcy6YJF123',
                           'database': 'funddata'},
              'zdj': ('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate'),
              'jydb': ('jydb', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate'),
              'edw': {'host': '10.52.40.222', 'port': 21050, 'username': 'fundrating',
                      'password': '6B2O02sP1OhYoLlX12OR',
                      'database': 'edw', 'auth': 'LDAP'},
              'bizdm': {'host': '10.52.40.222', 'port': 21050, 'username': 'fundrating',
                        'password': '6B2O02sP1OhYoLlX12OR',
                        'database': 'bizdm', 'auth': 'LDAP'},
              'simuwang': {'host': '120.24.90.158', 'port': 3306, 'user': 'data_user_zheshangzq', 'passwd': 'zszq@2022',
                           'database': 'rz_hfdb_core'}}

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
            sparkconf = SparkConf().setAppName('test1').setMaster('local[*]').set('spark.ui.showConsoleProgress',
                                                                                  'false')
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


class BaseSelect(ConfData):
    # cronjob
    @classmethod
    def get_dates(cls, **kwargs):
        last_date = kwargs.get('last_date')
        today = pd.to_datetime('today').normalize()
        if not kwargs.get('if_source'):
            return np.array([int(x.strftime('%Y%m%d')) for x in pd.date_range(last_date, today)])
        zjfund = cls.get_conn('zhijunfund')
        tar_con, sou_con = kwargs.get('tar_con', zjfund), kwargs.get('sou_con', zjfund)
        target, source = kwargs.get('target', 'T_CUST_D_STK_TRD_IDX'), kwargs.get('source', 'T_EVT_SEC_DLV_JOUR')
        date_name = kwargs.get('dt', 'etl_date')
        tar_dt, sou_dt = kwargs.get('tar_dt', date_name), kwargs.get('sou_dt', date_name)
        if not last_date:
            query = f"""select max({tar_dt}) from {target}"""
            last_date = pd.read_sql(query, tar_con)
            last_date = last_date.iloc[0].iloc[0]
            if last_date is None:
                last_date = '20150101'
            else:
                last_date = str(int(last_date))
        query = f"""select distinct {sou_dt} from {source} where {sou_dt} >= {last_date} order by {sou_dt} asc"""
        dates = pd.read_sql(query, sou_con)[sou_dt].astype('int').values
        return dates

    @classmethod
    # select data from table partitioned by date
    def get_days_data(cls, dates, query, schema, **kwargs):
        days_data = []
        for date in dates:
            print(date)
            day_data = pd.read_sql(query.format(date=date, **kwargs), cls.get_conn(schema))
            days_data.append(day_data)
        days_data = pd.concat(days_data)
        return days_data

    @classmethod
    def get_data(cls, query_name, schema, **kwargs):
        """
        if select portfolio data by date，kwargs = {'query': 'fund_port', 'schema': 'edw', 'dates': dates,...}
        the key 'query' and 'schema' is required, but 'dates' is not necessary
        """
        dates = kwargs.get('dates')
        query = getattr(cls, query_name)
        # we need keep the return form consistent
        if not dates:
            return pd.read_sql(query.format(**kwargs), ConfData.get_conn(schema))
        else:
            kwargs.pop('dates')
            return cls.get_days_data(dates, query, schema, **kwargs)

    @classmethod
    def complete_df(cls, df, **kwargs):
        # qs是一个对象，columns=['query', 'conn', 'merge_on', 'merge_how']
        qs = kwargs.get('query')
        for query in qs:
            conn = cls.get_conn(query.get('conn', 'zhijunfund'))
            merge_on = query.get('merge_on', ['src_cust_no'])
            merge_how = query.get('merge_how', 'outer')
            cols = query.get('cols', {})
            data = pd.read_sql(query['query'], conn).rename(columns=cols)
            columns = data.columns
            df = df.merge(data, on=merge_on, how=merge_how)
            df[columns] = df[columns].ffill().bfill()
        return df


class PortSelect(BaseSelect):
    asset_port_jy_pub = """select secucode fund, report_date date, stock_code code, ratio_nv weight from fund_stockdetail
    where report_date = '{date}'"""


class RetSelect(BaseSelect):
    index_price = """
    """
    a_ret = """select code, date, simple_return ret from stock_daily_quote where date = '{date}'"""
    h_ret = """select secucode code, date, simple_return ret from stock_hk_daily_quote where date = '{date}'"""
    stock_ret = a_ret + ' union ' + h_ret


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
    fund_date_nm = ['fund', 'date']
    realized_nm = ['gx', 'rn', 'dx', 'hg', 'pg', 'qz', 'pt']
    realize_nm = ['pt_realize', 'hg_realize', 'dx_realize', 'qz_realize', 'pg_realize']
    hold_nm = ['holding', 'duration']
    turn_nm = ['buy_turn', 'sell_turn', 'turnover']

    style_name = 'style'
    style_type_name = 'style_type'
    weight_name = 'weight'
    ret_name = 'ret'
    va_name = 'va'
    nv_name = 'nv'
    cum_nv_name = 'cum_nv'
    complex_nv_name = 'complex_nv'
    label_name = 'label'
    mul_name = 'mul'

    today = pd.to_datetime('today').normalize()
    date_dict = {'since_found': '1970-01-01',
                 'five_years': (today - pd.DateOffset(years=5)).strftime('%Y-%m-%d'),
                 'three_years': (today - pd.DateOffset(years=3)).strftime('%Y-%m-%d'),
                 'two_years': (today - pd.DateOffset(years=2)).strftime('%Y-%m-%d'),
                 'one_year': (today - pd.DateOffset(years=1)).strftime('%Y-%m-%d'),
                 'year_start': str(today.year - 1) + '-12-31'}
    date_label = list(date_dict.keys())


nc = NameConst()


class StyleSelect(BaseSelect):
    barra_style = """select * from FM_FactorExposure where date = {date}"""
    sw_style = """select secucode code, first_industry style from stock_industry where standard in ({standard}) and if_executed = 1"""
    ms_style = """select * from stock_industry where date = {date} and standard in ({standard})"""


class RetAttr:
    """
    a class to calculate position and attribution
    portfolio.columns = ['fund', 'date', 'code', 'weight', 'style']
    'style' here means asset type
    """
    def __init__(self, portfolio: pd.DataFrame, start: str = '', end: str = ''):
        self.portfolio = portfolio
        self.codes = portfolio[nc.code_name].drop_duplicates().tolist()

        start = portfolio[nc.date_name].min() if len(start) == 0 else start
        end = portfolio[nc.date_name].max() if len(end) == 0 else end
        self.tradingdays = get_tradingdays(start, end)
        self.style_list = None

    def get_style(self, style, **kwargs):
        """
        Default variable cols exist in kwargs when style == 'asset'
        """
        cols = kwargs.get('cols')
        if cols:
            kwargs.pop('cols')
        else:
            if style == 'asset':
                raise KeyError("Default variable cols exist in kwargs when style == 'asset'")
            else:
                cols = self.portfolio.columns[5:]
                cols = dict(zip(cols, cols))

        if style != 'asset':

            if f'{style}_style_data' not in self.__dict__.keys():

                nm = [x for x in kwargs.keys() if len(kwargs[x].split(', ')) > 1 and x != 'dates']
                # one = {k: v for k, v in kwargs.items() if k in nm}
                # other = {k: v for k, v in kwargs.items() if k not in nm}
                # new_object = pd.MultiIndex.from_product(one.values()).to_frame(index=False, name=one.keys())
                # new_object = list(new_object.T.to_dict().values())

                style_data = StyleSelect.get_data(query_name=f'{style}_style',
                                                  schema='funddata', **kwargs)
                style_data[nc.style_name] = reduce(lambda x, y: x + '--' + y,
                                                   [style_data[i] for i in nm+[nc.style_name]])

                setattr(self, f'{style}_style_data', style_data.drop(nm, axis=1))

            style_data = getattr(self, f'{style}_style_data')
            merge_on = [nc.code_name] + ([nc.date_name] if nc.date_name in style_data.columns else [])

            self.portfolio = self.portfolio.drop(self.portfolio.columns[5:], axis=1).merge(style_data, on=merge_on, how='left')
            self.portfolio[nc.style_name] = self.portfolio[nc.style_name].fillna('其他')

        if style not in ['barra']:
            print(cols)
            self.portfolio = DataTransform(self.portfolio).get_dummy(cols).get_df()
        self.style_list = self.portfolio.columns[5:]

    def fill_portfolio(self):
        dates = self.portfolio[nc.date_name].unique()
        miss_dates = self.tradingdays[~np.isin(self.tradingdays, dates)]
        port = self.portfolio.set_index([nc.date_name, nc.code_name]).unstack().reset_index()
        port = port.append(pd.DataFrame(dates, columns=[nc.date_name])).sort_values(nc.date_name)
        port.loc[port.index.isin(miss_dates), 'miss'] = 1
        first_dates = port.loc[(~port['miss'].isna()) & (port['miss'].shift(1).isna())][nc.date_name].tolist()
        port['miss'] = pd.cut(port[nc.date_name], first_dates)
        for i in port['miss'].drop_duplicates().tolist():
            port.loc[port['miss'] == i, port.columns[:-1]] = port[port.columns[:-1]].ffill()
        del port['miss']
        self.portfolio = port.copy()
        return port.stack().reset_index()

    def get_stock_ret(self, asset, asset_ret=None, **kwargs):
        if asset_ret is not None:
            setattr(self, f'{asset}_ret', asset_ret)
        else:
            if asset == 'stock':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='stock_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)
            elif asset == 'asset':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='asset_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)
            elif asset == 'bond':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='bond_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)
            elif asset == 'future':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='future_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)

        self.portfolio = self.portfolio.merge(getattr(self, f'{asset}_ret'),
                                              on=[nc.code_name, nc.date_name],
                                              how='inner')

    def get_daily_attr(self, port, lst=[]):
        if len(lst) == 0:
            lst = self.style_list
        port1, port2 = port.copy(), port.copy()
        port1[lst] = port1[lst].mul(port1[nc.weight_name], axis=0).mul(port1[nc.ret_name], axis=0)
        port1 = port1.groupby([nc.fund_name, nc.date_name])[lst].sum()
        port1_new = port1.stack().reset_index()
        port1_new.columns = [nc.fund_name, nc.date_name, nc.style_name, nc.va_name]

        port2[lst] = port2[lst].mul(port2[nc.weight_name], axis=0)
        port2 = port2.groupby([nc.fund_name, nc.date_name])[lst].sum()
        port2_new = port2.stack().reset_index()
        port2_new.columns = [nc.fund_name, nc.date_name, nc.style_name, nc.weight_name]

        port_new = port1_new.merge(port2_new, on=port1_new.columns[:3].to_list(), how='outer')
        return port1, port2, port_new

    def get_daily_style(self, port, lst=[]):
        if len(lst) == 0:
            lst = self.style_list
        port[lst] = port[lst].mul(port[nc.weight_name], axis=0)
        return port[lst].sum()

    def get_cum_attr(self, port, lst=[]):
        """this is just one method of multi-period of decomposition of portfolio return"""
        if len(lst) == 0:
            lst = self.style_list
        port['va_all'] = port[lst].sum(axis=1)
        port = port.groupby(nc.fund_name).apply(lambda x: x[lst] * (1 + x['va_all']))
        return port


class DataTransform:
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df

    def __getattr__(self, item):
        if item not in self.__dict__.keys():
            return getattr(self.df, item)
        else:
            return self.__dict__[item]

    def __getitem__(self, item):
        try:
            return self.__dict__[item]
        except:
            return self.df[item]

    def __repr__(self):
        return self.df.__repr__()

    def get_dummy(self, columns=None):
        if not columns:
            columns = self.df[nc.style_name].drop_duplicates().tolist()
            columns = dict(zip(columns, columns))
        if self.df.empty:
            return pd.DataFrame(columns=[nc.code_name] + list(dict(columns).values()))
        self.df[nc.style_name] = self.df[nc.style_name].apply(lambda x: dict(columns)[x])
        df_ = pd.get_dummies(self.df[nc.style_name])
        self.df = pd.concat([self.df.drop(nc.style_name, axis=1), df_], axis=1)
        return self

    def rename(self, columns):
        self.df = self.df.rename(columns=columns)
        return self

    def clear_data(self, *conds):
        for cond in conds:
            self.df = self.df.query(cond)
        return self

    def get_df(self):
        return self.df

    def align(self, *dfs):
        dfs = (self.df, ) + dfs
        if any(len(df.shape) == 1 or 1 in df.shape for df in dfs):
            dims = 1
        else:
            dims = 2
        mut_index = sorted(reduce(lambda x, y: x.intersection(y), (df.index for df in dfs)))
        mut_columns = sorted(reduce(lambda x, y: x.intersection(y), (df.columns for df in dfs)))
        if dims == 2:
            dfs = [df.loc[mut_index, mut_columns] for df in dfs]
        else:
            dfs = [df.loc[mut_index, :] for df in dfs]
        return dfs


portfolio = PortSelect.get_data(query_name='asset_port_jy_pub', schema='funddata', dates=['2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31'])
dates = ['2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']
asset = 'stock'
asset_ret = None
ret_attr = RetAttr(portfolio)
ret_attr.portfolio['date'] = pd.to_datetime(ret_attr.portfolio['date'])
style = 'ind'
ret_attr.get_stock_ret(dates=dates, asset=asset, asset_ret=asset_ret)
ret_attr.get_style('ind', standard='38')









