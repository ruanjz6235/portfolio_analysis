#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/17 7:20 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : ret_risk.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
import pandas as pd
import numpy as np
from functools import lru_cache, reduce, wraps
from copy import deepcopy
import statsmodels.api as sm


def rf_multiplier(freq):
    multiplier = 252
    if freq == 'weekly':
        multiplier = 52
    elif freq == 'monthly':
        multiplier = 12
    return 0.03 / multiplier, multiplier


def cal_decorator(func):
    @wraps(func)
    def cal_func(r, *args, **kwargs):
        if len(r) > 0:
            data = func(r, *args, **kwargs)
        else:
            if 'and' in func.__name__:
                data = [np.nan, np.nan]
            else:
                data = np.nan
        return data

    return cal_func


@cal_decorator
def cal_annual_data(ret, multiplier, data_type):
    if data_type == 'ret':
        annual_data = multiplier * ret.mean()
        annual_data = np.exp(annual_data) - 1
    else:
        annual_data = ret.std() * np.sqrt(multiplier)
    return annual_data


@cal_decorator
def cal_annual_ret_daily(ret):
    start, end = ret.index.min(), ret.index.max()
    day_num = (end - start).days
    annual_ret = np.exp(ret.sum()) ** (365/day_num) - 1
    return annual_ret


@cal_decorator
def cal_alpha_and_beta(ret, rf, multiplier):
    y = ret - rf
    x = ret[ret.columns[-1]] - rf
    x = sm.add_constant(x)
    model = sm.WLS(y, x, missing='drop').fit()
    alpha = model.params[0]
    alpha = multiplier * alpha
    if len(y) > 1:
        beta = model.params[1]
    else:
        beta = np.nan
    return alpha, beta


@cal_decorator
def cal_sharpe(ret, multiplier):
    annual_ret = np.log(1 + cal_annual_data(ret, multiplier, 'ret'))
    annual_vol = cal_annual_data(ret, multiplier, 'std')
    sharpe = (annual_ret - 0.03) / annual_vol
    return sharpe


@cal_decorator
def cal_ir(ret, multiplier):
    tracking_error = cal_tracking_error(ret, multiplier)
    asset_ret = np.log(1 + cal_annual_data(ret['asset'], multiplier, 'ret'))
    index_ret = np.log(1 + cal_annual_data(ret['index'], multiplier, 'ret'))
    ir = (asset_ret - index_ret) / tracking_error
    return ir


@cal_decorator
def cal_tracking_error(ret, multiplier):
    active_ret = ret['asset'] - ret['index']
    tracking_error = (active_ret.std(ddof=1)) * np.sqrt(multiplier)
    return tracking_error


@cal_decorator
def cal_sortino(ret, rf, multiplier):
    downside_vol = cal_downside_vol(ret, rf, multiplier)
    annual_ret = np.log(1 + cal_annual_data(ret, multiplier, 'ret'))
    sortino = (annual_ret - 0.03) / downside_vol
    return sortino


@cal_decorator
def cal_treynor(ret, rf, multiplier):
    _, beta = cal_alpha_and_beta(ret, rf, multiplier)
    annual_ret = np.log(1 + cal_annual_data(ret, multiplier, 'ret'))
    treynor = (annual_ret - 0.03) / beta
    return treynor


@cal_decorator
def cal_jensens(ret, rf, multiplier):
    _, beta = cal_alpha_and_beta(ret, rf, multiplier)
    asset_ret = np.log(1 + cal_annual_data(ret['asset'], multiplier, 'ret'))
    index_ret = np.log(1 + cal_annual_data(ret['index'], multiplier, 'ret'))
    jensens = asset_ret - (0.03 + beta * (index_ret - 0.03))
    return jensens


@cal_decorator
def cal_m2(ret, multiplier):
    asset_ret = np.log(1 + cal_annual_data(ret['asset'], multiplier, 'ret'))
    asset_vol = cal_annual_data(ret['asset'], multiplier, 'vol')
    index_vol = cal_annual_data(ret['index'], multiplier, 'vol')
    m2 = 0.03 + (asset_ret - 0.03) * index_vol / asset_vol
    return m2


@cal_decorator
def cal_winning_rate(ret):
    ret_diff = ret['asset'] - ret['index']
    winning_rate = len(ret_diff[ret_diff > 0]) / len(ret_diff)
    return winning_rate


def cal_ddd(ret):
    if len(ret.columns) == 1:
        ret = ret.dropna(axis=0, how='any')
    else:
        ret = ret.where(~ret.isna(), 0)
    running_max = np.maximum.accumulate(ret.cumsum())
    underwater = ret.cumsum() - running_max
    ddd = np.exp(underwater) - 1
    return ddd


@cal_decorator
def cal_mdd(ret):
    ddd = cal_ddd(ret)
    mdd = -ddd.min()
    return mdd


@cal_decorator
def calculate_mdd_month_number(ret):
    ddd = cal_ddd(ret)
    valley = ddd.idxmin().iloc[0]
    peak = underwater[:valley][underwater[:valley] == 0].dropna().index[-1]
    month_diff = valley.month - peak.month
    year_diff = valley.year - peak.year
    period_diff = max(12 * year_diff + month_diff - 1, 0)
    return period_diff


@cal_decorator
def cal_mdd_recovery_date(ret):
    ddd = cal_ddd(ret)
    valley = ddd.idxmin().iloc[0]
    try:
        recovery_date = ddd[valley:][ddd[valley:] == 0].dropna().index[0]
    except IndexError:
        recovery_date = np.nan
    return recovery_date


@cal_decorator
def cal_mdd_recovery_month_number(ret):
    recovery_date = cal_mdd_recovery_date(ret)
    if isinstance(recovery_date, pd.Timestamp):
        month_diff = recovery_date.month - valley.month
        year_diff = recovery_date.year - valley.year
        period_diff = max(12 * year_diff + month_diff - 1, 0)
    else:
        period_diff = np.nan
    return period_diff


@cal_decorator
def cal_downside_vol(ret, rf, multiplier):
    downside_ret = ret - rf
    downside_ret[downside_ret > 0] = 0
    downside_vol = downside_ret.std() * np.sqrt(multiplier)
    return downside_vol


@cal_decorator
def cal_his_var(ret, cutoff=0.05):
    if len(ret) > 24:
        hist_var = np.quantile(ret, cutoff, interpolation='lower')
    else:
        hist_var = np.nan
    return - hist_var


@cal_decorator
def cal_cond_var(ret, cutoff=0.05):
    var = cal_historical_var(ret, cutoff)
    cond_var = ret[ret <= - var].mean()
    return - cond_var


# 这里用向量化计算+掩码的方式批量处理数据，这种方式是可验证的Python处理的最快方式，目前主要用在回归和回撤的计算上
@cal_decorator
def get_ret_and_mask(asset_ret: pd.DataFrame, index_ret: pd.DataFrame):
    ret = pd.concat([asset_ret, index_ret], axis=1, join='outer')
    funds, index = asset_ret.columns, index_ret.columns[0]
    mask = ret[funds].where(ret[funds].isna(), 0)
    mask = mask.where(~mask.isna(), 1)
    mask1 = ret[index].where(ret[index].isna(), 0)
    mask1 = mask1.where(~mask1.isna(), 1)
    mask = mask.add(mask1, axis=0)
    mask = mask.where(mask < 1, -np.inf)
    mask = softmax(mask.values, axis=0)
    return ret, mask


def softmax(lst, axis=0):
    t = (0, 1)
    if axis == 1:
        t = (1, 0)
    lst_new = np.exp(lst)
    lst_new = lst_new.transpose(t) / lst_new.sum(axis=axis)
    return lst_new.transpose(t)


def ret_risk_indicators(asset_ret, index_ret, freq):
    rf, multiplier = rf_multiplier(freq)
    ret, mask = get_ret_and_mask(asset_ret, index_ret)
    if not isinstance(ret, pd.DataFrame):
        return pd.DataFrame()
    if freq == 'daily':
        annual_ret = cal_annual_ret_daily(ret)
    else:
        annual_ret = cal_annual_data(ret, multiplier, 'ret')







