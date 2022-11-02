import pandas as pd
import numpy as np
from functools import reduce

from ..const import nc
from ..data_transform import DataTransform

from ..query import (get_tradingdays,
                     StyleSelect,
                     RetSelect)


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


class RetAttr2(RetAttr):
    """calculate overweight holding"""

    def __init__(self, portfolio: pd.DataFrame, start: str = '', end: str = ''):
        super(RetAttr2, self).__init__(portfolio, start, end)
        self.portfolio_new = None
        self.style_portfolio = pd.DataFrame()

    def get_daily_style2(self, style, **kwargs):

        def get_daily_top(port, n, lst):
            port['count'] = range(len(port))
            port['count'] = port.groupby([nc.fund_name, nc.date_name])['count'].apply(lambda x: x - x.iloc[0])
            port.loc[port['count'] < n - 1, lst] = 1
            port.loc[port['count'] >= n - 1, lst] = 0
            return port

        def get_daily_sub_style_label(port, style_label):
            if style_label.endswith('daily_weight'):
                port[[style_label]] = 1
            elif style_label.endswith('daily_top3'):
                port = get_daily_top(port, 3, [style_label])
            elif style_label.endswith('daily_top5'):
                port = get_daily_top(port, 5, [style_label])
            elif style_label.endswith('daily_num'):
                port['weight'] = 1
                port[[style_label]] = 1
            return port

        if style.startswith('stock'):
            portfolio = self.portfolio[self.portfolio[nc.style_name] == '股票']
            portfolio = get_daily_sub_style_label(portfolio, style)
        elif style.startswith('bond'):
            portfolio = self.portfolio[self.portfolio[nc.style_name] == '债券']
            portfolio = get_daily_sub_style_label(portfolio, style)
        elif style.startswith('future'):
            portfolio = self.portfolio[self.portfolio[nc.style_name] == '期货']
            portfolio = get_daily_sub_style_label(portfolio, style)
        elif style.startswith('ind') and len(style.split('_')) > 1:
            portfolio = self.portfolio.copy()
            portfolio = get_daily_sub_style_label(portfolio, style)
        else:
            self.get_style(style, **kwargs)
            portfolio = self.portfolio.copy()
        return portfolio

    # 风格时间序列: 包括前三大/前五大/全部的行业/个股权重序列，持股数量序列，前三大/前五大/全部的个券/期货权重序列均由这个方法给出。
    def get_daily_style_port(self, style, **kwargs):
        self.fill_portfolio()
        self.style_list = [style]
        portfolio = self.get_daily_style2(style, **kwargs)
        style_portfolio = portfolio.groupby(nc.fund_date_nm).apply(self.get_daily_style)
        style_portfolio = style_portfolio.stack().rename(nc.style_name).reset_index()
        return style_portfolio

    def get_stock_ret2(self, asset, port=None, asset_ret=None, **kwargs):
        port_old = self.portfolio.copy()
        self.portfolio = port.copy()
        super(RetAttr2, self).get_stock_ret(asset, asset_ret, **kwargs)
        port = self.portfolio.copy()
        self.portfolio = port_old.copy()
        return port

    def get_all_date_label(self, i, style, job_type=None, **kwargs):
        if job_type != 'cronjob':
            return self.get_port_interval_style(style, **kwargs)
        else:
            try:
                start = nc.date_dict[nc.date_label[i]]
            except StopIteration:
                return None
            self.portfolio_new = self.portfolio[self.portfolio[nc.date_name] > start]
            style_portfolio = self.get_port_interval_style(style, job_type, **kwargs)
            self.style_portfolio = self.style_portfolio.append(style_portfolio)
            self.get_all_date_label(i+1, style, job_type, **kwargs)

    def get_port_interval_style(self, style, job_type=None, **kwargs):

        def get_port_style(port, style_label):
            k = 1
            if style_label.endswith('weight'):
                port[style_label] = port[nc.weight_name]
                k = 0
            elif style_label.endswith('profit'):
                port[style_label] = np.log(port[nc.weight_name] * port[nc.ret_name])
                k = 1
            elif style_label.endswith('loss'):
                port[style_label] = - np.log(port[nc.weight_name] * port[nc.ret_name])
                k = 1
            elif style_label.endswith('days'):
                port[style_label] = 1
                k = 1
            return port, k

        def get_num(style_label):
            if 'top3' in style_label.split('_'):
                return 3
            elif 'top5' in style_label.split('_'):
                return 5
            elif 'top10' in style_label.split('_'):
                return 10
            elif 'all' in style_label.split('_'):
                return 10000

        def get_port_interval_sub_style_label(port, style_label):
            port, k = get_port_style(port, style_label)
            num = get_num(style_label)
            if k == 0:
                port = port.groupby(nc.fund_date_nm)[style_label].mean().sort_values()[:num].reset_index()
            else:
                port = port.groupby(nc.fund_date_nm)[style_label].sum().sort_values()[:num].reset_index()
            return port

        if job_type == 'cronjob':
            port_old = self.portfolio_new.copy()
        else:
            port_old = self.portfolio.copy()
        map_dict = {'stock': '股票', 'bond': '债券', 'future': '期货'}
        asset = style.split('_')[0]
        if asset in map_dict.keys():
            assets = map_dict.get(asset, port_old[nc.style_name].unique())
            portfolio = port_old[port_old[nc.style_name].isin(assets)]
            portfolio = self.get_stock_ret2(asset, portfolio)
            portfolio = get_port_interval_sub_style_label(portfolio, style)
        else:
            self.get_style(style, **kwargs)
            portfolio = self.portfolio.copy()
        return portfolio

    # 前十大个股(权重/盈利/亏损)，前五大个券(权重/盈利/亏损)，前五大期货(权重/盈利/亏损)均由这个方法给出。
    def get_interval_style_port(self, style, job_type=None, **kwargs):
        self.fill_portfolio()
        self.style_list = [style]
        self.get_all_date_label(0, style, job_type, **kwargs)
        style_portfolio = self.style_portfolio.copy()
        style_portfolio[nc.style_name] = style
        return style_portfolio
