import pandas as pd
import numpy as np
from functools import reduce

from ..util import BaseSelect
from ..query import RetSelect, PortSelect

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
        exces, alloc, selct = brinson_use(ret_fund,
                                          style_fund,
                                          ret_base,
                                          style_base,
                                          if_cross=False)
        ConfData.save(exces, 'zhijunfund.brinson_attr')
        ConfData.save(alloc, 'zhijunfund.brinson_attr')
        ConfData.save(selct, 'zhijunfund.brinson_attr')
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
# 1. asset_attribution, first_layer of method of brinson based on position (two layers)
def get_asset_attr():
    query_name = 'daily_port'
    style = 'asset'
    cols = {'股票': 'stock_', '债券': 'bond_', '基金': 'fund_', '理财': 'fipro_', '衍生品': 'derive_'}
    for sub_dates in dates:
        portfolio = BaseSelect.get_data(query_name=getattr(PortSelect, query_name), schema='edw', dates=dates)
        ret_attr_fund, style_fund, ret_attr_data_new, style_data_new, _ = cal_ret_attr_data(style=style,
                                                                                            cols=cols,
                                                                                            dates=sub_dates,
                                                                                            portfolio=portfolio)
        ConfData.save(ret_attr_data_new, 'zhijunfund.fund_ret_attr')
        ConfData.save(style_data_new, 'zhijunfund.fund_style')
        get_fund_base_data(ret_attr_fund, style_fund, save_brinson=True)


# 2. stock_ind_attribution
def get_stock_attr():
    query_name = 'daily_stock_port'
    style = 'ind'
    cols = {}
    for sub_dates in dates:
        portfolio = BaseSelect.get_data(query_name=getattr(PortSelect, query_name), schema='edw', dates=dates)
        _, _, ret_attr_data_new, style_data_new, _ = cal_ret_attr_data(style=style,
                                                                       cols=cols,
                                                                       dates=sub_dates,
                                                                       portfolio=portfolio,
                                                                       standard=[38, 22, 28])
        ConfData.save(ret_attr_data_new, 'zhijunfund.fund_ret_attr')
        ConfData.save(style_data_new, 'zhijunfund.fund_style')


# 3. bond_attribution
def get_bond_attr():
    query_name = 'daily_bond_port'
    style = 'bond'
    for sub_dates in dates:
        portfolio = BaseSelect.get_data(query_name=getattr(PortSelect, query_name), schema='edw', dates=dates)
        _, _, ret_attr_data_new, style_data_new, _ = cal_ret_attr_data(style=style,
                                                                       dates=sub_dates,
                                                                       portfolio=portfolio)
        ConfData.save(ret_attr_data_new, 'zhijunfund.fund_ret_attr')
        ConfData.save(style_data_new, 'zhijunfund.fund_style')


# 4. future_attribution
def get_future_attr():
    query_name = 'daily_future_port'
    style = 'future'
    for sub_dates in dates:
        portfolio = BaseSelect.get_data(query_name=getattr(PortSelect, query_name), schema='edw', dates=dates)
        _, _, ret_attr_data_new, style_data_new, _ = cal_ret_attr_data(style=style,
                                                                       dates=sub_dates,
                                                                       portfolio=portfolio)
        ConfData.save(ret_attr_data_new, 'zhijunfund.fund_ret_attr')
        ConfData.save(style_data_new, 'zhijunfund.fund_style')


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
        fund_port = BaseSelect.get_data(query_name=PortSelect.daily_port, schema='edw', dates=dates)
        base_port = BaseSelect.get_data(query_name=PortSelect.daily_base, schema='edw', dates=dates, base='000300')
        exces, alloc, selct, cross = cal_brinson_attr_data(fund_port, base_port, dates=sub_dates)
        ConfData.save(exces, 'zhijunfund.brinson_attr')
        ConfData.save(alloc, 'zhijunfund.brinson_attr')
        ConfData.save(selct, 'zhijunfund.brinson_attr')
        ConfData.save(cross, 'zhijunfund.brinson_attr')
