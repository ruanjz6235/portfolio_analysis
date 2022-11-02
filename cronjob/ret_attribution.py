import pandas as pd
import numpy as np
from functools import reduce
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm

from ..config import ConfData
from ..util import BaseSelect
from ..query import RetSelect, PortSelect
from ..const import nc

from ..engine.ret_attribution import (cal_ret_attr_data,
                                      cal_barra_attr_data,
                                      cal_brinson_attr_data,
                                      brinson_use,
                                      cal_ts_style,
                                      cal_interval_port_style)


# %%
# other requirements.txt--the first layer of brinson_position
def get_fund_base_data(ret_attr_fund, style_fund, save_brinson=True):
    ret_fund = ret_attr_fund / style_fund
    dates_new = ret_fund.levels[1]

    style_base = style_fund.copy()
    style_base.iloc[:, 0] = 1
    style_base.iloc[:, 1:] = 0

    ret_base = RetSelect.get_data(query_name=RetSelect.index_ret,
                                  schema='funddata',
                                  codes=['000300', 'H11001'],
                                  dates=sub_dates).set_index(['date', 'fund']).unstack().loc[dates_new]
    ret_base.index = ret_fund.index.copy()
    if save_brinson:
        exces, alloc, selct = brinson_use(ret_fund,
                                          style_fund,
                                          ret_base,
                                          style_base,
                                          if_cross=False)
        ConfData.save(exces, 'zdj.private_fund_style_allocation')
        ConfData.save(alloc, 'zdj.private_fund_style_allocation')
        ConfData.save(selct, 'zdj.private_fund_style_allocation')
    else:
        return ret_fund, style_fund, ret_base, style_base


# %%
dates = BaseSelect.get_dates(last_date='20190101')
x, y = len(dates) // 30, len(dates) % 30
dates = dates[: len(dates) - y].reshape(x, 30).tolist() + [dates[len(dates) - y:].tolist()]


# %%
# cronjob
# %%
# CALCULATE ATTRIBUTION AND HOLDING
# 1. asset_attribution, first_layer of method of brinson based on position (two layers)
# 2. stock_ind_attribution
# 3. bond_attribution
# 4. future_attribution
def get_asset_attr():
    query_name = 'asset_port'
    for sub_dates in dates:
        portfolio = BaseSelect.get_data(query_name=getattr(PortSelect, query_name), schema='edw', dates=sub_dates)
        style = 'asset'
        cols = {'股票': 'stock_', '债券': 'bond_', '基金': 'fund_', '理财': 'fipro_', '衍生品': 'derive_'}
        ret_attr_fund, style_fund, style_allocation, _ = cal_ret_attr_data(
            style=style, cols=cols, dates=sub_dates, portfolio=portfolio)
        style_allocation['style_type'] = style
        ConfData.save(style_allocation, 'zdj.private_fund_style_allocation')
        get_fund_base_data(ret_attr_fund, style_fund, save_brinson=True)

        style = 'ind'
        cols = {}
        stock_portfolio = portfolio[portfolio['style'] == '股票']
        asset_ret = None
        ind_dict = {'38': 'sw', '22': 'zjh', '28': 'zz'}
        for standard, style_name in ind_dict:
            kwargs = {} if asset_ret is None else {'asset_ret': asset_ret}
            _, _, style_allocation, asset_ret = cal_ret_attr_data(
                style=style, cols=cols, dates=sub_dates, portfolio=stock_portfolio, standard=standard, **kwargs)
            style_allocation['style_type'] = style_name
            ConfData.save(style_allocation, 'zdj.private_fund_style_allocation')

        style = 'bk'
        cols = {}
        _, _, style_allocation, _ = cal_ret_attr_data(
            style=style, cols=cols, dates=sub_dates, portfolio=stock_portfolio, asset_ret=asset_ret)
        style_allocation['style_type'] = style
        ConfData.save(style_allocation, 'zdj.private_fund_style_allocation')

        style = 'ms'
        cols = {}
        _, _, style_allocation, _ = cal_ret_attr_data(
            style=style, cols=cols, dates=sub_dates, portfolio=stock_portfolio, asset_ret=asset_ret)
        style_allocation['style_type'] = style
        ConfData.save(style_allocation, 'zdj.private_fund_style_allocation')

        sub_style = 'bond'
        bond_portfolio = portfolio[portfolio['style'] == '债券']
        asset_ret = None
        for sub_type in ['type', 'rating']:
            style = sub_style + sub_type
            kwargs = {} if asset_ret is None else {'asset_ret': asset_ret}
            _, _, style_allocation, _ = cal_ret_attr_data(
                style=style, dates=sub_dates, portfolio=bond_portfolio, **kwargs)
            style_allocation['style_type'] = style
            ConfData.save(style_allocation, 'zdj.private_fund_style_allocation')

        sub_style = 'future'
        future_portfolio = portfolio[portfolio['style'] == '期货']
        asset_ret=None
        for sub_type in ['type', 'target', 'direct']:
            style = sub_style + sub_type
            kwargs = {} if asset_ret is None else {'asset_ret': asset_ret}
            _, _, style_allocation, _ = cal_ret_attr_data(
                style=style, dates=sub_dates, portfolio=future_portfolio, **kwargs)
            style_allocation['style_type'] = style
            ConfData.save(style_allocation, 'zdj.private_fund_style_allocation')


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


# %%
# 7. ts_style
def get_ts_style():
    query_name = 'asset_port'

    style_list = ['stock_daily_weight', 'stock_daily_top3', 'stock_daily_top5', 'stock_daily_num',
                  'bond_daily_weight', 'bond_daily_top3', 'bond_daily_top5', 'bond_daily_num',
                  'future_daily_weight', 'future_daily_top3', 'future_daily_top5', 'future_daily_num',
                  'ind_daily_top3', 'ind_daily_top5', 'ind_daily_num']
    def save_ts_style(ds, q_nm, s_lst, **kwargs):
        port = PortSelect.get_data(query_name=getattr(PortSelect, q_nm), schema='edw', dates=ds)
        ports = cal_ts_style(port, s_lst, **kwargs)
        ports[nc.style_type_name] = 'other'
        ConfData.save(ports, 'zdj.private_fund_style_allocation')

    Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(save_ts_style)(sub_dates, query_name, style_list, standard='38, 22, 24')
        for sub_dates in dates
    )


# %%
# 8. interval_port_style
def get_interval_port_style():
    query_name = 'funds_asset_port'

    asset_type = ['stock', 'bond', 'future']
    name_type = ['top3', 'top5', 'top10', 'all']
    data_type = ['weight', 'profit', 'loss', 'days']
    style_list = ['_'.join(xs) for xs in pd.MultiIndex.from_product([asset_type, name_type, data_type])]
    codes = []

    def save_interval_port_style(ds, cs, q_nm, s_lst, **kwargs):
        cs = ','.join(cs)
        port = []
        for sub_ds in ds:
            sub_port = PortSelect.get_data(query_name=getattr(PortSelect, q_nm), schema='edw', dates=sub_ds, code=cs)
            port.append(sub_port)
        port = pd.concat(port).reset_index(drop=True)
        ports = cal_interval_port_style(port, s_lst, **kwargs)
        ports[nc.style_type_name] = 'hold'
        ConfData.save(ports, 'zdj.private_fund_interval_port_style')

    Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(save_interval_port_style)(dates, sub_codes, query_name, style_list)
        for sub_codes in codes
    )
