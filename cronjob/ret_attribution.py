import pandas as pd
import numpy as np
from functools import reduce

from ..util import BaseSelect
from ..query import RetSelect

from ..engine.ret_attribution import (cal_ret_attr_data,
                                      cal_barra_attr_data,
                                      cal_brinson_attr_data,
                                      brinson_use)


# %%
# other requirements--the first layer of brinson_position
def get_fund_base_data(ret_attr_fund, style_fund, save_brinson=True):
    ret_fund = ret_attr_fund / style_fund
    dates_new = ret_fund.levels[1]

    style_base = style_fund.copy()
    style_base.iloc[:, 0] = 1
    style_base.iloc[:, 1:] = 0

    ret_base = RetSelect.get_data(query_name=RetSelect.index_ret,
                                  schema='zhijunfund',
                                  codes=['000300', 'H11001'],
                                  dates=sub_dates).set_index(['date', 'fund']).unstack().loc[dates_new]
    ret_base.index = ret_fund.index.copy()
    if save_brinson:
        brinson_use(ret_fund, style_fund, ret_base, style_base, if_cross=False)
    else:
        return ret_fund, style_fund, ret_base, style_base


# %%
dates = BaseSelect.get_dates(last_date='20190101')
x, y = len(dates) // 30, len(dates) % 30
dates = np.vstack([dates[: len(dates) - y].reshape(x, 30), dates[len(dates) - y:]])


# %%
# cronjob
# %%
# CALCULATE ATTRIBUTION AND HOLDING
# 1. asset_attribution
def get_asset_attr():
    style = 'asset'
    cols = {'股票': 'stock_', '债券': 'bond_', '基金': 'fund_', '理财': 'fipro_', '衍生品': 'derive_'}
    for sub_dates in dates:
        ret_attr_fund, style_fund, _ = cal_ret_attr_data(style=style,
                                                         cols=cols,
                                                         dates=sub_dates,
                                                         query_name='daily_port')
        get_fund_base_data(ret_attr_fund, style_fund, save_brinson=True)


# 2. stock_ind_attribution
def get_stock_attr():
    style = 'ind'
    cols = {}
    for sub_dates in dates:
        cal_ret_attr_data(style=style,
                          cols=cols,
                          dates=sub_dates,
                          query_name='daily_stock_port',
                          standard=[38, 22, 28])


# 3. bond_attribution
def get_bond_attr():
    style = 'bond'
    for sub_dates in dates:
        cal_ret_attr_data(style=style,
                          dates=sub_dates,
                          query_name='daily_bond_port')


# 4. future_attribution
def get_future_attr():
    style = 'future'
    for sub_dates in dates:
        cal_ret_attr_data(style=style,
                          dates=sub_dates,
                          query_name='daily_future_port')


# 5. barra_attribution
# seven levels:
# (1) total return attributed by active and benchmark
# (2) total return attributed by factors (contributed by country, style, ind, individual)
# (3) total return attributed by style factors
# (4) total return attributed by industry factors
# (5) active return attributed by factors (contributed by country, style, ind, individual)
# (6) active return attributed by style factors
# (7) active return attributed by industry factors
def get_barra_attr():
    for sub_dates in dates:
        cal_barra_attr_data(dates=sub_dates)


# 6. brinson_attribution
# two methods:
# (1) based on allocation (one level)
# (2) based on position (two level)
def get_brinson_attr():
    for sub_dates in dates:
        cal_brinson_attr_data(dates=sub_dates)
