import pandas as pd
import numpy as np
from functools import reduce
from ..data_transform import DataTransform

from ..query import (get_tradingdays,
                     StyleSelect,
                     RetSelect,
                     PortSelect)


class RetAttr:
    """
    a class to calculate position and attribution
    portfolio.columns = ['fund', 'date', 'code', 'weight', 'style']
    'style' here means asset type
    """
    def __init__(self, portfolio: pd.DataFrame, start: str = '', end: str = ''):
        self.portfolio = portfolio
        self.codes = portfolio['code'].drop_duplicates().tolist()

        start = portfolio['date'].min() if len(start) == 0 else start
        end = portfolio['date'].max() if len(end) == 0 else end
        self.tradingdays = get_tradingdays(start, end)

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

                nm = [x for x in kwargs.keys() if type(kwargs[x]) == list and x != 'dates']
                # one = {k: v for k, v in kwargs.items() if k in nm}
                # other = {k: v for k, v in kwargs.items() if k not in nm}
                # new_object = pd.MultiIndex.from_product(one.values()).to_frame(index=False, name=one.keys())
                # new_object = list(new_object.T.to_dict().values())

                style_data = StyleSelect.get_data(query_name=getattr(StyleSelect, f'{style}_style'),
                                                  schema='zhijunfund', **kwargs)
                style_data['style'] = reduce(lambda x, y: x + '--' + y, [style_data[i] for i in nm+['style']])
                self.__dict__[f'{style}_style_data'] = style_data

            self.portfolio = self.portfolio.drop(self.portfolio.columns[5:], axis=1).merge(
                getattr(self, f'{style}_style_data'), on=['code', 'date'], how='left')
            self.portfolio['style'] = self.portfolio['style'].fillna('其他')

        if style not in ['barra']:
            self.portfolio = DataTransform(self.portfolio).get_dummy(cols).get_df()
        self.style_list = self.portfolio.columns[5:]

    def fill_portfolio(self):
        dates = self.portfolio['date'].unique()
        miss_dates = self.tradingdays[~np.isin(self.tradingdays, dates)]
        port = self.portfolio.set_index(['date', 'code']).unstack().reset_index()
        port = port.append(pd.DataFrame(dates, columns=['date'])).sort_values('date')
        port.loc[port.index.isin(miss_dates), 'miss'] = 1
        first_dates = port.loc[(~port['miss'].isna()) & (port['miss'].shift(1).isna())]['date'].tolist()
        port['miss'] = pd.cut(port['dates'], first_dates)
        for i in port['miss'].drop_duplicates().tolist():
            port.loc[port['miss'] == i, port.columns[:-1]] = port[port.columns[:-1]].ffill()
        del port['miss']
        self.portfolio = port.copy()
        return port.stack().reset_index()

    def get_stock_ret(self, **kwargs):
        if 'stock_ret' not in self.__dict__.keys():
            a_ret = RetSelect.get_data(query_name=RetSelect.a_ret, schema='zhijunfund', **kwargs)
            kc_ret = RetSelect.get_data(query_name=RetSelect.kc_ret, schema='zhijunfund', **kwargs)
            h_ret = RetSelect.get_data(query_name=RetSelect.h_ret, schema='zhijunfund', **kwargs)
            self.stock_ret = pd.concat([a_ret, kc_ret, h_ret])

        return self.portfolio.merge(self.stock_ret, on=['code', 'date'], how='inner')

    def get_daily_attr(self, port, lst=[]):
        if len(lst) == 0:
            lst = self.style_list
        port[lst] = port[lst].mul(port['weight'], axis=0).mul(port['ret'], axis=0)
        return port[lst].sum()

    def get_daily_style(self, port, lst=[]):
        if len(lst) == 0:
            lst = self.style_list
        port[lst] = port[lst].mul(port['weight'], axis=0)
        return port[lst].sum()

    def get_cum_attr(self, port, lst=[]):
        """this is just one method of multi-period of decomposition of portfolio return"""
        if len(lst) == 0:
            lst = self.style_list
        port['va_all'] = port[lst].sum(axis=1)
        port = port.groupby('fund').apply(lambda x: x[lst] * (1 + x['va_all']))
        return port
