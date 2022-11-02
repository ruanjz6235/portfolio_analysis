import pandas as pd
from functools import reduce

from ..config import ConfData
from ..const import style_type
from ..util import BaseSelect
from ..query import PortSelect

from ..template.ret_attribution import RetAttr, RetAttr2


# %%
# #############code specifications of portfolio_analysis and return attribution
# An example of single-period portfolio analysis and return attribution standard
def cal_ret_attr_data(dates, style, portfolio, asset_ret=None, **kwargs):
    """
    1. get holding data
    2. generate ret_attr instance
    3. preprocess holding data
    4. get style data (if this has been got before, skip this step)
    5. get return data (if this has been got before, skip this step)
    6. single-period return attribution
    7. save data
    8. when use return attribution data, get single-period data and calculate multi-period data
    """
    ret_attr = RetAttr(portfolio)
    ret_attr.fill_portfolio()
    asset = style_type[style]
    ret_attr.get_stock_ret(dates=dates, asset=asset, asset_ret=asset_ret)
    ret_attr.get_style(style, **kwargs)
    ret_attr_data, style_data, style_allocation = ret_attr.get_daily_attr(ret_attr.portfolio, ret_attr.style_list)

    return (ret_attr_data,
            style_data,
            style_allocation,
            getattr(ret_attr, f'{asset}_ret'))


def cal_barra_attr_data(dates):
    fund_port = PortSelect.get_data(query_name=PortSelect.daily_port, schema='edw', dates=dates)
    base_port = PortSelect.get_data(query_name=PortSelect.daily_base, schema='edw', dates=dates, base='000300')

    # 1. 总收益、基准收益和主动收益，总权重、基准权重和主动权重
    # 2. 总收益和主动收益的因子分解，国家因子，风格因子，行业因子，个股收益
    # 3. 总收益和主动收益的风格与行业分解
    asset = ['weight_x', 'weight_y', 'weight_z']
    factor = ['cntry', 'style', 'indstr', 'exces']
    style = []
    ind = []
    columns = ['fund', 'date', 'code', 'weight']
    port = fund_port[columns].merge(base_port[columns], on=columns[:-1], how='outer')
    port['weight_z'] = port['weight_x'] - port['weight_y']
    port['weight'] = 1
    active_port = port.fillna(0)
    active_port['weight'] = active_port['weight_z']
    for i, port_ in enumerate([port, fund_port, base_port, active_port[columns]]):
        ret_attr = RetAttr(port_)
        ret_attr.fill_portfolio()
        ret_attr.get_stock_ret(dates=dates, asset='stock')
        if i == 0:
            ret_attr_data = ret_attr.portfolio.groupby(['fund', 'date']).apply(ret_attr.get_daily_attr, lst=asset)
            ret_attr_data = ret_attr_data.stack().reset_index()
            ConfData.save(ret_attr_data, 'zhijunfund.fund_base_ret_attr')
            style_data = ret_attr.portfolio.groupby(['fund', 'date']).apply(ret_attr.get_daily_style, lst=asset)
            style_data = style_data.stack().reset_index()
            ConfData.save(style_data, 'zhijunfund.fund_base_style')
        else:
            ret_attr.get_style('barra')
            ret_attr.portfolio['cntry'] = ret_attr.portfolio['country']
            ret_attr.portfolio['style'] = reduce(lambda x, y: x + y, [ret_attr.portfolio[i] for i in style])
            ret_attr.portfolio['indstr'] = reduce(lambda x, y: x + y, [ret_attr.portfolio[i] for i in ind])
            ret_attr.portfolio['exces'] = ret_attr.portfolio['residual_vol'] / ret_attr.portfolio['ret']
            for lst in [factor, style, ind]:
                ret_attr_data = ret_attr.portfolio.groupby(['fund', 'date']).apply(ret_attr.get_daily_attr, lst=lst)
                ret_attr_data = ret_attr_data.stack().reset_index()
                ConfData.save(ret_attr_data, 'zhijunfund.fund_base_ret_attr')
                style_data = ret_attr.portfolio.groupby(['fund', 'date']).apply(ret_attr.get_daily_style, lst=lst)
                style_data = style_data.stack().reset_index()
                ConfData.save(style_data, 'zhijunfund.fund_base_style')


def brinson_use(ret_fund, style_fund, ret_base, style_base, if_cross=True):
    fund_data = ret_fund * style_fund
    base_data = ret_base * style_base
    allc_data = ret_base * style_fund
    slct_data = ret_fund * style_base
    exces = fund_data - base_data
    alloc = allc_data - base_data
    if if_cross:
        selct = slct_data - base_data
        cross = (fund_data + base_data) - (slct_data + allc_data)
    else:
        selct = fund_data - allc_data
    if if_cross:
        return exces, alloc, selct, cross
    else:
        return exces, alloc, selct


def cal_brinson_attr_data(fund_port, base_port, dates):
    ret_attr_fund = RetAttr(fund_port)
    ret_attr_fund.fill_portfolio()
    ret_attr_base = RetAttr(base_port)
    ret_attr_base.fill_portfolio()

    for ind in ['sw', 'zz', 'zjh']:
        if ind == 'sw':
            idx = 38
        elif ind == 'zz':
            idx = 22
        else:
            idx = 28

        ret_attr_fund.get_stock_ret(dates=dates, asset='stock')
        ret_attr_fund.get_style('ind', standard=idx)
        ret_attr_data1 = ret_attr_fund.portfolio.groupby(['fund', 'date']).apply(ret_attr_fund.get_daily_attr)
        style_data1 = ret_attr_fund.portfolio.groupby(['fund', 'date']).apply(ret_attr_fund.get_daily_style)
        mean_price_data1 = ret_attr_data1 / style_data1

        ret_attr_base.ind_style_data = ret_attr_fund.ind_style_data.copy()
        ret_attr_base.stock_ret = ret_attr_fund.stock_ret.copy()

        ret_attr_base.get_stock_ret(dates=dates, asset='stock')
        ret_attr_base.get_style('ind', standard=idx)
        ret_attr_data2 = ret_attr_base.portfolio.groupby(['fund', 'date']).apply(ret_attr_base.get_daily_attr)
        style_data2 = ret_attr_base.portfolio.groupby(['fund', 'date']).apply(ret_attr_base.get_daily_style)
        mean_price_data2 = ret_attr_data2 / style_data2

        # consider two models: brinson based allocation and brinson based position
        # 1. brinson based allocation (one level)
        # 2. brinson based position (two level)
        if_cross = True
        for i in ['brinson_a', 'brinson_p']:
            if i == 'brinson_a':
                weight = pd.Series([1] * len(style_data1), index=style_data1.index)
            else:
                weight = style_data1.sum(axis=1)

            exces, alloc, selct, cross = brinson_use(mean_price_data2,
                                                     style_data2.mul(weight, axis=0),
                                                     mean_price_data1,
                                                     style_data1,
                                                     if_cross)

            return exces, alloc, selct, cross


# %%
# #############code specifications of style_analysis
# An example of single-period style_analysis
def cal_ts_style(portfolio, style_list, **kwargs):
    ret_attr = RetAttr2(portfolio)
    ports = []
    for i, style in enumerate(style_list):
        if not (style.startswith('stock') and style.startswith('bond') and style.startswith('future')):
            asset = style.split('_')[0]
            if asset == style_list[i - 1].split('_')[0]:
                port = ret_attr.get_daily_style_port(asset, **kwargs)
                ret_attr.portfolio = port.copy()
        style_port = ret_attr.get_daily_style_port(style)
        ports.append(style_port)
    ports = pd.concat(ports)
    return ports


def cal_interval_port_style(portfolio, style_list, job_type=None, **kwargs):
    ret_attr = RetAttr2(portfolio)
    ports = []
    for style in style_list:
        if style.split('_')[2] == 'days':
            if style.split('_')[1] != 'all':
                continue
        else:
            if style.split('_')[1] == 'all':
                continue
        ret_attr.get_interval_style_port(style, job_type=job_type, **kwargs)
        ports.append(ret_attr.style_portfolio)
    return pd.concat(ports)
