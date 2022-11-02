#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 19:16:39 2019

@author: yunongwu
"""

import numpy as np
import pandas as pd
import re
from functools import reduce
from .mutistage_campisi_model import *
from .stock_cost import ConfData
from app.framework.injection import database_injector
import logging
from app.common.log import logger
conf = ConfData()


class AssetClass:
    def __init__(self):
        pass

    @staticmethod
    def is_derivative(asset_type):
        if (asset_type == 'derivative') | (asset_type == 'future'):
            return 1
        else:
            return 0


def get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    with db_connection.cursor() as cursor:
        if not AssetClass.is_derivative(asset_type):
            if asset_type == 'stock':
                data_column = ['EndDate', 'SecuCode', 'SecondClassCode', 'ClassName',
                               'MarketPrice', 'MarketInTA', 'ValuationAppreciation']
                FirstClassCode = '1102'
            elif asset_type == 'bond':
                data_column = ['EndDate', 'SecuCode', 'ClassName',
                               'MarketInTA', 'ValuationAppreciation']
                FirstClassCode = '1103'
            else:  # asset_type == 'fund'
                data_column = ['EndDate', 'SecuCode', 'MarketInTA', 'ValuationAppreciation']
                FirstClassCode = '1105'
            cursor.execute(
                """
                SELECT %s
                FROM ZS_FUNDVALUATION
                WHERE ZSCode = %s
                AND EndDate >= to_date('%s', 'yyyy-mm-dd')
                AND EndDate <= to_date('%s', 'yyyy-mm-dd')
                AND FirstClassCode = '%s'
                AND SecuCode is not NULL
                """ % (','.join(data_column), fund_zscode,
                       start_date, end_date, FirstClassCode))
        else:
            data_column = ['EndDate', 'SecuCode', 'SecondClassCode',
                           'MarketInTA', 'ValuationAppreciation']
            if asset_type == 'derivative':
                sub_query = ""
            else:  # asset_type == 'future'
                sub_query = "AND SecondClassCode in ('01', '02', '03', '04', '05', '06', '07', '08', '31', '32')"
            cursor.execute(
                """
                SELECT %s
                FROM ZS_FUNDVALUATION
                WHERE ZSCode = %s
                AND EndDate >= to_date('%s', 'yyyy-mm-dd')
                AND EndDate <= to_date('%s', 'yyyy-mm-dd')
                AND FirstClassCode in ('3102', '3201') %s
                AND ThirdClassCode = '01'
                AND SecuCode is not NULL
                """ % (",".join(data_column), fund_zscode, start_date,
                       end_date, sub_query))
        fund_asset_portfolio = cursor.fetchall()
        if not fund_asset_portfolio:
            fund_asset_portfolio = pd.DataFrame(columns=data_column)
            return fund_asset_portfolio
        fund_asset_portfolio = pd.DataFrame(
            list(fund_asset_portfolio), columns=data_column)
    return fund_asset_portfolio


# %%
def get_asset_portfolio_overall(fund_zscode, start_date, end_date, asset_type):
    if asset_type == 'stock':
        FirstClassCode = ['1102']
        other_FirstClassCode = ['1203']
        other_SecondClassCode = ['01', '03', '06', '09', '11', '13',
                                 '14', '15', '17', '18', '23', '24']
    elif asset_type == 'bond':
        FirstClassCode = ['1103']
        other_FirstClassCode = ['1204']
        other_SecondClassCode = ['10']
    elif asset_type == 'derivative':
        FirstClassCode = ['3102', '3201']
        other_FirstClassCode = ['1031']
        other_SecondClassCode = ['13', '31', '32']
    elif asset_type == 'financial_product':
        FirstClassCode = ['1109']
        other_FirstClassCode = ['1204']
        other_SecondClassCode = ['24']
    elif asset_type == 'deposit_interest':
        FirstClassCode = ['1002']
        other_FirstClassCode = ['1204']
        other_SecondClassCode = ['01']
    elif asset_type == 'reverse_repo_and_interest':
        FirstClassCode = ['1202']
        other_FirstClassCode = ['1204']
        other_SecondClassCode = ['91']
    else:                         # asset_type == 'liquidation_reserve_and_interest'
        FirstClassCode = ['1021']
        other_FirstClassCode = ['1204']
        other_SecondClassCode = ['02']
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode in ('%s')
            AND SecondClassCode is NULL
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date, "','".join(FirstClassCode)))
        asset_portfolio = cursor.fetchall()
        if not asset_portfolio:
            asset_portfolio = pd.DataFrame(columns=data_column)
        else:
            asset_portfolio = pd.DataFrame(
                list(asset_portfolio), columns=data_column)

        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode in ('%s')
            AND SecondClassCode in ('%s')
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date,
                   "','".join(other_FirstClassCode),
                   "','".join(other_SecondClassCode))
        )
        other_asset_portfolio = cursor.fetchall()
        if not other_asset_portfolio:
            other_asset_portfolio = pd.DataFrame(columns=data_column)
        else:
            other_asset_portfolio = pd.DataFrame(
                list(other_asset_portfolio), columns=data_column)

        asset_portfolio = asset_portfolio.append(other_asset_portfolio)
        asset_portfolio = asset_portfolio.fillna(0)
        asset_portfolio = asset_portfolio.groupby('EndDate')[data_column[1:]].sum()
        asset_portfolio = asset_portfolio.reset_index()
        return asset_portfolio


def get_monetary_fund_code(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT SecuCode
            FROM ZJ_STOCKFUND_MARKET
            WHERE SecuCode in %s
            AND SecuCategory = 4
            AND FundType = '货币型'
            """ % (str(tuple(codes)).replace(',)', ')'))
        )
        monetary_funds = cursor.fetchall()
        if not monetary_funds:
            monetary_funds = []
            return monetary_funds
        monetary_funds = pd.DataFrame(list(monetary_funds), columns=['SecuCode'])
        monetary_funds = monetary_funds['SecuCode'].unique().tolist()
    return monetary_funds


def get_monetary_fund_overall(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'SecuCode', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '1105'
            AND SecuCode is NOT NULL
            ORDER BY EndDate asc
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date)
        )
        funds = cursor.fetchall()
        if not funds:
            monetary_fund_overall = pd.DataFrame(
                columns=['EndDate', 'MarketInTA', 'ValuationAppreciation'])
            return monetary_fund_overall
        funds = pd.DataFrame(list(funds), columns=data_column)
        fund_codes = funds['SecuCode'].unique().tolist()

        monetary_funds = get_monetary_fund_code(fund_codes)
        if monetary_funds:
            funds = funds[funds['SecuCode'].isin(monetary_funds)]
            funds = funds.fillna(0)
            monetary_fund_overall = funds.groupby('EndDate')[data_column[2:]].sum()
            monetary_fund_overall = monetary_fund_overall.reset_index()
        else:
            monetary_fund_overall = pd.DataFrame(
                columns=['EndDate', 'MarketInTA', 'ValuationAppreciation'])
    return monetary_fund_overall


def get_fund_dividends(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '1203'
            AND SecondClassCode = '05'
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            ORDER BY EndDate asc
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date))
        fund_dividends = cursor.fetchall()
        if not fund_dividends:
            return pd.DataFrame(columns=data_column)
        fund_dividends = pd.DataFrame(list(fund_dividends), columns=data_column)
    return fund_dividends


def get_monetary_fund_dividends(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'SecuCode', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '1203'
            AND SecondClassCode = '05'
            AND SecuCode is not NULL
            ORDER BY EndDate asc
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date))
        funds = cursor.fetchall()
        if not funds:
            monetary_fund_dividends = pd.DataFrame(
                columns=['EndDate', 'MarketInTA', 'ValuationAppreciation'])
            return monetary_fund_dividends
        funds = pd.DataFrame(list(funds), columns=data_column)
        fund_codes = funds['SecuCode'].unique().tolist()

        monetary_funds = get_monetary_fund_code(fund_codes)
        if monetary_funds:
            funds = funds[funds['SecuCode'].isin(monetary_funds)]
            funds = funds.fillna(0)
            monetary_fund_dividends = funds.groupby('EndDate')[data_column[2:]].sum()
            monetary_fund_dividends = monetary_fund_dividends.reset_index()
        else:
            monetary_fund_dividends = pd.DataFrame(
                columns=['EndDate', 'MarketInTA', 'ValuationAppreciation'])
    return monetary_fund_dividends


def get_fund_portfolio_overall(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '1105'
            AND SecondClassCode is NULL
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            ORDER BY EndDate asc
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date)
        )
        fund_portfolio_overall = cursor.fetchall()
        if not fund_portfolio_overall:
            fund_portfolio_overall = pd.DataFrame(columns=data_column)
        else:
            fund_portfolio_overall = pd.DataFrame(
                list(fund_portfolio_overall), columns=data_column)
        fund_portfolio_overall = fund_portfolio_overall.sort_values(by='EndDate')

    fund_dividends = get_fund_dividends(fund_zscode, start_date, end_date)
    fund_dividends = pd.merge(fund_dividends,
                              fund_portfolio_overall[['EndDate']],
                              on='EndDate', how='right')
    fund_dividends = fund_dividends.fillna(0)
    fund_dividends = fund_dividends.sort_values(by='EndDate')
    fund_dividends = fund_dividends.reset_index(level=0, drop=True)

    monetary_fund_overall = get_monetary_fund_overall(fund_zscode, start_date, end_date)
    monetary_fund_overall = pd.merge(monetary_fund_overall,
                                     fund_portfolio_overall[['EndDate']],
                                     on='EndDate', how='right')
    monetary_fund_overall = monetary_fund_overall.fillna(0)
    monetary_fund_overall = monetary_fund_overall.sort_values(by='EndDate')
    monetary_fund_overall = monetary_fund_overall.reset_index(level=0, drop=True)

    monetary_fund_dividends = get_monetary_fund_dividends(fund_zscode, start_date, end_date)
    monetary_fund_dividends = pd.merge(monetary_fund_dividends,
                                       fund_portfolio_overall[['EndDate']],
                                       on='EndDate', how='right')
    monetary_fund_dividends = monetary_fund_dividends.fillna(0)
    monetary_fund_dividends = monetary_fund_dividends.sort_values(by='EndDate')
    monetary_fund_dividends = monetary_fund_dividends.reset_index(level=0, drop=True)

#    logger.info('fund_portfolio_overall: %s' % (fund_portfolio_overall[['EndDate']]))
#    logger.info('monetary_fund_overall: %s' % (monetary_fund_overall[['EndDate']]))
    fund_portfolio_overall['MarketInTA'] = fund_portfolio_overall['MarketInTA'] + \
                                           fund_dividends['MarketInTA'] - \
                                           monetary_fund_overall['MarketInTA'] - \
                                           monetary_fund_dividends['MarketInTA']
#    logger.info(fund_portfolio_overall[fund_portfolio_overall['EndDate'] == '2019-09-17'])
#    logger.info(monetary_fund_overall[monetary_fund_overall['EndDate'] == '2019-09-17'])
#    logger.info(monetary_fund_dividends[monetary_fund_dividends['EndDate'] == '2019-09-17'])
    fund_portfolio_overall['ValuationAppreciation'] = fund_portfolio_overall['ValuationAppreciation'] + \
                                                      fund_dividends['ValuationAppreciation'] - \
                                                      monetary_fund_overall['ValuationAppreciation'] - \
                                                      monetary_fund_dividends['ValuationAppreciation']
    return fund_portfolio_overall


#%%
def get_security_clearing_deposit(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '3003'
            AND SecondClassCode is NULL
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date)
        )
        clearing_deposit = cursor.fetchall()
        if not clearing_deposit:
            return pd.DataFrame(columns=data_column)
        clearing_deposit = pd.DataFrame(
            list(clearing_deposit), columns=data_column)
    return clearing_deposit


def get_asset_portfolio_overall_method2(fund_zscode, start_date, end_date, asset_type):
    if asset_type == 'security_margin_and_interest':
        FirstClassCode = '1031'
        SecondClassCode = '06'
        other_FirstClassCode = '1204'
        other_SecondClassCode = '91'
    else:              # asset_type == 'refundable_deposits_and_interest'
        FirstClassCode = '1031'
        SecondClassCode = '51'
        other_FirstClassCode = '1204'
        other_SecondClassCode = '15'
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '%s'
            AND SecondClassCode = '%s'
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date,
                   FirstClassCode, SecondClassCode))
        asset_portfolio = cursor.fetchall()
        if not asset_portfolio:
            asset_portfolio = pd.DataFrame(columns=data_column)
        else:
            asset_portfolio = pd.DataFrame(
                list(asset_portfolio), columns=data_column)

        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '%s'
            AND SecondClassCode = '%s'
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date,
                   other_FirstClassCode,
                   other_SecondClassCode))
        other_asset_portfolio = cursor.fetchall()
        if not other_asset_portfolio:
            other_asset_portfolio = pd.DataFrame(columns=data_column)
        else:
            other_asset_portfolio = pd.DataFrame(
                list(other_asset_portfolio), columns=data_column)

        asset_portfolio = asset_portfolio.append(other_asset_portfolio)
        asset_portfolio = asset_portfolio.fillna(0)
        asset_portfolio = asset_portfolio.groupby('EndDate')[
            data_column[1:]].sum()
        asset_portfolio = asset_portfolio.reset_index()
    return asset_portfolio


def get_other_refundable_deposits(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '1031'
            AND SecondClassCode not in ('06', '13', '31', '32', '51')
            AND ThirdClassCode is NULL
            AND SecuCode is NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date)
        )
        other_refundable_deposits = cursor.fetchall()
        if not other_refundable_deposits:
            return pd.DataFrame(columns=data_column)

        other_refundable_deposits = pd.DataFrame(list(other_refundable_deposits),
                                                 columns=data_column)
        other_refundable_deposits = other_refundable_deposits.fillna(0)
        other_refundable_deposits = other_refundable_deposits.groupby('EndDate')[
            ['MarketInTA', 'ValuationAppreciation']].sum()
        other_refundable_deposits = other_refundable_deposits.reset_index()
    return other_refundable_deposits


def get_cash_overall(fund_zscode, start_date, end_date):
    data_column = ['EndDate', 'MarketInTA', 'ValuationAppreciation']
    deposit_and_interest = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='deposit_interest')
    monetary_fund = get_monetary_fund_overall(fund_zscode, start_date, end_date)
    security_clearing_deposit = get_security_clearing_deposit(fund_zscode, start_date, end_date)
    reverse_repo_and_interest = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='reverse_repo_and_interest')
    security_margin_and_interest = get_asset_portfolio_overall_method2(
        fund_zscode, start_date, end_date, asset_type='security_margin_and_interest')
    liquidation_reserve_and_interest = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='liquidation_reserve_and_interest')
    refundable_deposits_and_interest = get_asset_portfolio_overall_method2(
        fund_zscode, start_date, end_date, asset_type='refundable_deposits_and_interest')
    other_refundable_deposits = get_other_refundable_deposits(fund_zscode, start_date, end_date)

    cash = pd.DataFrame(columns=data_column)
    cash = cash.append(deposit_and_interest)
    cash = cash.append(monetary_fund)
    cash = cash.append(security_clearing_deposit)
    cash = cash.append(reverse_repo_and_interest)
    cash = cash.append(security_margin_and_interest)
    cash = cash.append(liquidation_reserve_and_interest)
    cash = cash.append(refundable_deposits_and_interest)
    cash = cash.append(other_refundable_deposits)
    cash_overall = cash.groupby('EndDate')[data_column[1:]].sum()
    cash_overall = cash_overall.reset_index()
#    logger.info(cash_overall[cash_overall['EndDate'] == '2019-09-17'])
    return cash_overall


#%%
def get_equity_ratio_change(stock_portfolio_overall):
    equity_ratio = stock_portfolio_overall[['EndDate', 'MarketInTA']]
    equity_ratio = equity_ratio.rename(columns={'MarketInTA': 'equity_ratio'})
    return equity_ratio


def get_bond_ratio_change(bond_portfolio_overall):
    bond_ratio = bond_portfolio_overall[['EndDate', 'MarketInTA']]
    bond_ratio = bond_ratio.rename(columns={'MarketInTA': 'bond_ratio'})
    return bond_ratio


def get_fund_ratio_change(fund_portfolio_overall):
    fund_ratio = fund_portfolio_overall[['EndDate', 'MarketInTA']]
    fund_ratio = fund_ratio.rename(columns={'MarketInTA': 'fund_ratio'})
    return fund_ratio


def get_derivative_ratio_change(derivative_portfolio_overall):
    derivative_ratio = derivative_portfolio_overall[['EndDate', 'MarketInTA']]
    derivative_ratio = derivative_ratio.rename(columns={'MarketInTA': 'derivative_ratio'})
    return derivative_ratio


def get_financial_products_ratio_change(financial_products_portfolio_overall):
    financial_products_ratio = financial_products_portfolio_overall[['EndDate', 'MarketInTA']]
    financial_products_ratio = financial_products_ratio.rename(columns={'MarketInTA': 'financial_products_ratio'})
    return financial_products_ratio


def get_cash_ratio_change(cash_overall):
    cash_ratio = cash_overall[['EndDate', 'MarketInTA']]
    cash_ratio = cash_ratio.rename(columns={'MarketInTA': 'cash_ratio'})
    return cash_ratio


#%%
def get_fund_portfolio(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'SecuCode', 'FirstClassCode', 'SecondClassCode',
                   'ThirdClassCode', 'ClassName', 'MarketPrice', 'MarketInTA',
                   'ValuationAppreciation']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date)
        )
        fund_portfolio = cursor.fetchall()
        if not fund_portfolio:
            return pd.DataFrame(columns=data_column)
        fund_portfolio = pd.DataFrame(
            list(fund_portfolio), columns=data_column)
    return fund_portfolio


#%%
def get_net_asset_value(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'TotalMarket']
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_NETASSETVALUE
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND Item = 'NetAssetValue'
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date))
        netassetvalue = cursor.fetchall()
        if not netassetvalue:
            return pd.DataFrame(columns=data_column)
        netassetvalue = pd.DataFrame(list(netassetvalue), columns=data_column)
        netassetvalue = netassetvalue.sort_values(by='EndDate')
    return netassetvalue


def get_equity_va_ratio_change(stock_portfolio, gzb_dates):
    """
    通过股票组合和净值日期，计算补齐的收益率dataframe
    :param stock_portfolio:股票持仓的dataframe
    columns=['EndDate', 'SecuCode', 'SecondClassCode', 'ClassName',
    'MarketPrice', 'MarketInTA', 'ValuationAppreciation']
    :param gzb_dates:净值日期的dataframe
    :return:columns=['EndDate', 'equity_va_ratio']
    """
    kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
    ashare_portfolio = stock_portfolio[
        ~stock_portfolio['SecondClassCode'].isin(['74', '81', '82', '83'] + kcshares_index_list)]
    if len(ashare_portfolio) > 0:
        stock_returns = portfolio_whole_return_method2(ashare_portfolio)
        stock_returns = stock_returns.merge(
            ashare_portfolio, on=['EndDate', 'SecuCode', 'MarketInTA'], how='right')
    else:
        stock_returns = pd.DataFrame(
            columns=['EndDate', 'SecuCode', 'MarketInTA', 'log_return'])

    hshare_portfolio = stock_portfolio[
        stock_portfolio['SecondClassCode'].isin(['74', '81', '82', '83']+kcshares_index_list)]
    if len(hshare_portfolio) > 0:
        hshare_portfolio = hshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
        hshare_portfolio['log_return'] = hshare_portfolio.groupby('SecuCode')['MarketPrice'].apply(
            lambda x: np.log(x / x.shift(1)))
        hshare_portfolio = hshare_portfolio.dropna()
        hshare_portfolio = hshare_portfolio.drop('MarketPrice', axis=1)
        stock_returns = stock_returns.append(hshare_portfolio)

    if len(stock_returns) > 0:
        stock_portfolio_return = stock_returns.groupby('EndDate').apply(
            lambda x: (x['log_return'] * x['MarketInTA']).sum())
        stock_portfolio_return = stock_portfolio_return.reset_index()
        stock_portfolio_return = gzb_dates.merge(
            stock_portfolio_return, on='EndDate', how='left')
        stock_portfolio_return = stock_portfolio_return.fillna(0)
        stock_portfolio_return[0] = np.exp(stock_portfolio_return[0].cumsum()) - 1
        stock_portfolio_return = stock_portfolio_return.rename(columns={0: 'equity_va_ratio'})
    else:
        stock_portfolio_return = gzb_dates
        stock_portfolio_return['equity_va_ratio'] = 0

    return stock_portfolio_return


def get_resample_data(bond_port, start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    bond_port['EndDate'] = pd.to_datetime(bond_port['EndDate'])
    start_date = max(start_date, bond_port['EndDate'].min())
    end_date = min(end_date, bond_port['EndDate'].max())
    bond_port = bond_port.fillna(0)
    all_days = pd.date_range(start_date, end_date)
    all_days = pd.DataFrame(all_days).rename(columns={0: 'EndDate'})
    bond_port = pd.merge(all_days, bond_port, how='left', on="EndDate")
    bond_port = bond_port.ffill()
    return bond_port


def get_bond_va_ratio_change(bond_portfolio, gzb_dates):
    start_date = gzb_dates['EndDate'].iloc[0].strftime("%Y-%m-%d")
    end_date = gzb_dates['EndDate'].iloc[-1].strftime("%Y-%m-%d")
    if bond_portfolio is not None:
        bond_marketnv_all = bond_portfolio.fillna(0)
        # bond_marketnv_all = get_resample_data(bond_portfolio, start_date, end_date)
        bond_secucode = bond_marketnv_all.columns[1:]
        bond_info = get_bonds_info(bond_secucode, start_date, end_date)
        bond_info = bond_info.sort_values('EndDate')
        if len(bond_info) > 0:
            bond_names = get_bond_name(bond_secucode)
            bond_return_name = match_bond_name(bond_info, bond_names)
            cum_return = get_cumsum_return(bond_secucode, bond_return_name, bond_marketnv_all)
            cum_return = cum_return.rename(columns={'cum_return': 'bond_va_ratio'})
            cum_return = gzb_dates.merge(cum_return, on='EndDate', how='left')
            cum_return['bond_va_ratio'] = cum_return['bond_va_ratio'].ffill()
            return cum_return
        else:
            cum_return = pd.DataFrame(columns=['EndDate', 'bond_va_ratio'])
            cum_return['EndDate'] = gzb_dates['EndDate']
            return cum_return
    else:
        cum_return = pd.DataFrame(columns=['EndDate', 'bond_va_ratio'])
        cum_return['EndDate'] = gzb_dates['EndDate']
        return cum_return


def get_fund_va_ratio_change(fund_portfolio_overall, netassetvalue):
    fund_va_ratio = netassetvalue.merge(
        fund_portfolio_overall[['EndDate', 'ValuationAppreciation']],
        on='EndDate', how='outer')
    fund_va_ratio['fund_va_ratio'] = fund_va_ratio['ValuationAppreciation'] / \
                                     fund_va_ratio['TotalMarket']
    fund_va_ratio = fund_va_ratio[['EndDate', 'fund_va_ratio']]
    return fund_va_ratio


def get_derivative_va_ratio_change(derivative_portfolio_overall, netassetvalue):
    derivative_va_ratio = netassetvalue.merge(
        derivative_portfolio_overall[['EndDate', 'ValuationAppreciation']],
        on='EndDate', how='outer')
    derivative_va_ratio['derivative_va_ratio'] = derivative_va_ratio['ValuationAppreciation'] / \
                                                 derivative_va_ratio['TotalMarket']
    derivative_va_ratio = derivative_va_ratio[['EndDate', 'derivative_va_ratio']]
    return derivative_va_ratio


def get_financial_products_va_ratio_change(financial_products_portfolio_overall, netassetvalue):
    financial_products_va_ratio = pd.merge(
        financial_products_portfolio_overall[['EndDate', 'ValuationAppreciation']], netassetvalue, on='EndDate',
        how='outer')
    financial_products_va_ratio['financial_products_va_ratio'] = \
        financial_products_va_ratio['ValuationAppreciation'] / \
        financial_products_va_ratio['TotalMarket']
    financial_products_va_ratio = financial_products_va_ratio[
        ['EndDate', 'financial_products_va_ratio']]
    return financial_products_va_ratio


def get_asset_and_valuation_appreciation_ratio_change(request_id, type, fund_zscode, start_date, end_date):
    netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
    if netassetvalue.empty:
        result = {'asset_ratio_change': pd.DataFrame(),
                  'valuation_appreciation_ratio_change': pd.DataFrame()}
        return result
    stock_portfolio = get_fund_astock_portfolio(fund_zscode, start_date, end_date)
    bond_portfolio = obtain_bonds_marketnv(fund_zscode, start_date, end_date)
    fund_portfolio_overall = get_fund_portfolio_overall(fund_zscode, start_date, end_date)
    derivative_portfolio_overall = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='derivative')
    
    financial_products_portfolio_overall = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='financial_product')
    gzb_dates = netassetvalue[['EndDate']]
    asset_ratio_change = get_asset_allocation_change(
        request_id, type, fund_zscode, index_code, start_date, end_date)

    equity_va_ratio = get_equity_va_ratio_change(stock_portfolio, gzb_dates)
    bond_va_ratio = get_bond_va_ratio_change(bond_portfolio, gzb_dates)
    fund_va_ratio = get_fund_va_ratio_change(fund_portfolio_overall, netassetvalue)
    derivative_va_ratio = get_derivative_va_ratio_change(
        derivative_portfolio_overall, netassetvalue)
    financial_products_va_ratio = get_financial_products_va_ratio_change(
        financial_products_portfolio_overall, netassetvalue)
    dfs = [equity_va_ratio, bond_va_ratio, fund_va_ratio,
            derivative_va_ratio, financial_products_va_ratio]
    va_ratio_change = reduce(
        lambda left, right: left.merge(right, on=['EndDate'], how='outer'), dfs)
    va_ratio_change = va_ratio_change.sort_values(by='EndDate')

    if len(asset_ratio_change) > 0:
        result = {'asset_ratio_change': asset_ratio_change,
                  'valuation_appreciation_ratio_change': va_ratio_change}
    else:
        result = {'asset_ratio_change': pd.DataFrame(),
                  'valuation_appreciation_ratio_change': pd.DataFrame()}
    return result


def get_asset_allocation_change(request_id, type, fund_zscode, index_code, start_date, end_date):
    stock_portfolio_overall = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='stock')
    bond_portfolio_overall = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='bond')
    fund_portfolio_overall = get_fund_portfolio_overall(fund_zscode, start_date, end_date)
    derivative_portfolio_overall = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='derivative')
    financial_products_portfolio_overall = get_asset_portfolio_overall(
        fund_zscode, start_date, end_date, asset_type='financial_product')
    cash_overall = get_cash_overall(fund_zscode, start_date, end_date)
    equity_ratio = get_equity_ratio_change(stock_portfolio_overall)
    bond_ratio = get_bond_ratio_change(bond_portfolio_overall)
    fund_ratio = get_fund_ratio_change(fund_portfolio_overall)
    derivative_ratio = get_derivative_ratio_change(derivative_portfolio_overall)
    financial_products_ratio = get_financial_products_ratio_change(financial_products_portfolio_overall)
    cash_ratio = get_cash_ratio_change(cash_overall)
    netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
    gzb_dates = netassetvalue[['EndDate']]
    dfs = [equity_ratio, bond_ratio, fund_ratio, derivative_ratio, cash_ratio, financial_products_ratio]
    asset_ratio_change = reduce(lambda left, right: pd.merge(left, right, on=['EndDate'], how='outer'), dfs)
    asset_ratio_change = pd.merge(gzb_dates, asset_ratio_change, on='EndDate', how='left')
    asset_ratio_change = asset_ratio_change.fillna(0)
    asset_ratio_change.loc[asset_ratio_change['equity_ratio'] < 0, 'equity_ratio'] = 0
    asset_ratio_change.loc[asset_ratio_change['bond_ratio'] < 0, 'bond_ratio'] = 0
    asset_ratio_change.loc[asset_ratio_change['fund_ratio'] < 0, 'fund_ratio'] = 0
    asset_ratio_change.loc[asset_ratio_change['derivative_ratio'] < 0, 'derivative_ratio'] = 0
    asset_ratio_change.loc[asset_ratio_change['financial_products_ratio'] < 0, 'financial_products_ratio'] = 0
    asset_ratio_change.loc[asset_ratio_change['cash_ratio'] < 0, 'cash_ratio'] = 0
    asset_ratio_change['other_ratio'] = 1 - (asset_ratio_change['equity_ratio']
                                             + asset_ratio_change['bond_ratio']
                                             + asset_ratio_change['fund_ratio']
                                             + asset_ratio_change['derivative_ratio']
                                             + asset_ratio_change['financial_products_ratio']
                                             + asset_ratio_change['cash_ratio'])
    asset_ratio_change.loc[asset_ratio_change['other_ratio'] < 0, 'other_ratio'] = 0
#    logger.info(asset_ratio_change[asset_ratio_change['EndDate'] == '2019-09-17'][['fund_ratio', 'cash_ratio', 'other_ratio']])
    if len(asset_ratio_change) == 0:
        asset_ratio_change = pd.DataFrame()
    else:
        asset_ratio_change = complete_index(asset_ratio_change, index_code, start_date, end_date)
    return asset_ratio_change.drop_duplicates()


def get_stock_industry(codes, industry_standard):
    if industry_standard == 'zz':
        standard = 28
    elif industry_standard == 'sw':
        standard = 24
    else:                   # industry_standard == 'zjh':
        standard = 22
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        stock_industries = pd.DataFrame(columns=['SecuCode', 'Industry'])
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SM.SecuCode, LCEI.FirstIndustryName Industry
                        FROM JYDB.SecuMain SM
                        JOIN JYDB.LC_ExgIndustry LCEI ON SM.CompanyCode = LCEI.CompanyCode
                        WHERE SM.SecuCode in %s
                        AND SM.SecuCategory = 1
                        AND LCEI.Standard = %s
                        AND LCEI.CancelDate is NULL
                        """ % (str(tuple(sub_list)).replace(',)', ')'), standard)
                    )
                    stock_industry = cursor.fetchall()
                    if not stock_industry:
                        continue
                    else:
                        stock_industry = pd.DataFrame(
                            list(stock_industry), columns=['SecuCode', 'Industry'])
                    stock_industries = stock_industries.append(stock_industry)
        stock_industries = stock_industries.drop_duplicates(subset=['SecuCode'])
        return stock_industries
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT SM.SecuCode, LCEI.FirstIndustryName Industry
                FROM JYDB.SecuMain SM
                JOIN JYDB.LC_ExgIndustry LCEI ON SM.CompanyCode = LCEI.CompanyCode
                WHERE SM.SecuCode in %s
                AND SM.SecuCategory = 1
                AND LCEI.Standard = %s
                AND LCEI.CancelDate is NULL
                """ % (str(tuple(codes)).replace(',)', ')'), standard)
            )
            stock_industry = cursor.fetchall()
            if not stock_industry:
                return pd.DataFrame(columns=['SecuCode', 'Industry'])
            stock_industries = pd.DataFrame(
                list(stock_industry), columns=['SecuCode', 'Industry'])
        stock_industries = stock_industries.drop_duplicates(subset=['SecuCode'])
        return stock_industries
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'Industry'])


def get_astock_chinames(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        stock_chinames = pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SecuCode, SecuAbbr
                        FROM JYDB.SecuMain
                        WHERE SecuCategory = 1
                        AND SecuMarket in (83, 90)
                        AND SecuCode in %s
                        """ % (str(tuple(sub_list)).replace(',)', ')')))
                    stock_chiname = cursor.fetchall()
                    if not stock_chiname:
                        continue
                    else:
                        stock_chiname = pd.DataFrame(list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
                    stock_chinames = stock_chinames.append(stock_chiname)
            return stock_chinames
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT SecuCode, SecuAbbr
                FROM JYDB.SecuMain
                WHERE SecuCategory = 1
                AND SecuMarket in (83, 90)
                AND SecuCode in %s
                """ % (str(tuple(codes)).replace(',)', ')')))
            stock_chiname = cursor.fetchall()
            if not stock_chiname:
                return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
            stock_chiname = pd.DataFrame(
                list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
            return stock_chiname
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])


def get_hstock_chinames(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        stock_chinames = pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SecuCode, SecuAbbr
                        FROM JYDB.HK_SecuMain
                        WHERE SecuCode in %s
                        """ % (str(tuple(sub_list)).replace(',)', ')')))
                    stock_chiname = cursor.fetchall()
                    if not stock_chiname:
                        continue
                    else:
                        stock_chiname = pd.DataFrame(list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
                    stock_chinames = stock_chinames.append(stock_chiname)
            return stock_chinames
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT SecuCode, SecuAbbr
                FROM JYDB.HK_SecuMain
                WHERE SecuCode in %s
                """ % (str(tuple(codes)).replace(',)', ')')))
            stock_chiname = cursor.fetchall()
            if not stock_chiname:
                return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
            stock_chiname = pd.DataFrame(
                list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
            return stock_chiname
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])


def get_gzstock_chinames(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        stock_chinames = pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SecuCode, SecuAbbr
                        FROM JYDB.SecuMain
                        WHERE SecuCategory = 1
                        AND SecuMarket = 81
                        AND SecuCode in %s
                        """ % (str(tuple(sub_list)).replace(',)', ')')))
                    stock_chiname = cursor.fetchall()
                    if not stock_chiname:
                        continue
                    else:
                        stock_chiname = pd.DataFrame(list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
                    stock_chinames = stock_chinames.append(stock_chiname)
            return stock_chinames
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT SecuCode, SecuAbbr
                FROM JYDB.SecuMain
                WHERE SecuCategory = 1
                AND SecuMarket = 81
                AND SecuCode in %s
                """ % (str(tuple(codes)).replace(',)', ')')))
            stock_chiname = cursor.fetchall()
            if not stock_chiname:
                return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
            stock_chiname = pd.DataFrame(
                list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
            return stock_chiname
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])


def get_kcstock_chinames(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        stock_chinames = pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SecuCode, ChiNameAbbr
                        FROM JYDB.SecuMain
                        WHERE ListedSector = 7
                        AND SecuCode in %s
                        """ % (str(tuple(sub_list)).replace(',)', ')')))
                    stock_chiname = cursor.fetchall()
                    if not stock_chiname:
                        continue
                    else:
                        stock_chiname = pd.DataFrame(list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
                    stock_chinames = stock_chinames.append(stock_chiname)
            return stock_chinames
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT SecuCode, ChiNameAbbr
                FROM JYDB.SecuMain
                WHERE ListedSector = 7
                AND SecuCode in %s
                """ % (str(tuple(codes)).replace(',)', ')')))
            stock_chiname = cursor.fetchall()
            if not stock_chiname:
                return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
            stock_chiname = pd.DataFrame(
                list(stock_chiname), columns=['SecuCode', 'SecuAbbr'])
            return stock_chiname
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])


def get_ahstock_industry(fund_zscode, start_date, end_date, industry_standard):
    stock_portfolio = get_fund_asset_portfolio(
        fund_zscode, start_date, end_date, asset_type='stock')
    kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
    stock_codes = stock_portfolio.SecuCode.unique().tolist()
    if len(stock_codes) > 0:
        hshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(['81', '82', '83'])]
        hshares = hshares['SecuCode'].unique().tolist()
        gzshares = stock_portfolio[stock_portfolio['SecondClassCode'] == '74']
        gzshares = gzshares['SecuCode'].unique().tolist()
        ashares = stock_portfolio[
            ~stock_portfolio['SecondClassCode'].isin(['74', '81', '82', '83']+kcshares_index_list)]
        ashares = ashares['SecuCode'].unique().tolist()
        kcshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(kcshares_index_list)]
        kcshares = kcshares['SecuCode'].unique().tolist()

        if (industry_standard == 'zjh') | (industry_standard == 'sw'):
            stock_industry = get_stock_industry(ashares, industry_standard)
            hstock_industry = get_hshares_industry(hshares, industry_standard)
            stock_industry = stock_industry.append(hstock_industry)
            kcstock_industry = get_kcshares_industry(kcshares, industry_standard)
            stock_industry = stock_industry.append(kcstock_industry)

            stock_portfolio = pd.merge(stock_portfolio, stock_industry, on=['SecuCode'], how='left')
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(gzshares))
                                & (stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-新三板'
            stock_portfolio['Industry'] = stock_portfolio['Industry'].fillna('其他')
        else:
            stock_industry = get_stock_industry(ashares, industry_standard)
            stock_portfolio = pd.merge(stock_portfolio, stock_industry, on=['SecuCode'], how='left')
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(gzshares))
                                & (stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-新三板'
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(hshares))
                                & (stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-港股'
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(kcshares))
                                & (stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-科创板'
            stock_portfolio['Industry'] = stock_portfolio['Industry'].fillna('其他')
    else:
        stock_portfolio = pd.DataFrame(
            columns=['EndDate', 'SecuCode', 'SecondClassCode', 'ClassName', 'MarketPrice',
                     'MarketInTA', 'ValuationAppreciation', 'Industry'])
    return stock_portfolio


def get_industry_allocation(stock_portfolio, gzb_dates):
    industry_allocation = stock_portfolio.groupby(['EndDate', 'Industry'])['MarketInTA'].sum()
    industry_allocation = industry_allocation.reset_index()
    industry_allocation = industry_allocation.pivot_table(
        columns='Industry', values='MarketInTA', index='EndDate')
    column_names = industry_allocation.columns
    others = list(column_names[column_names.str.startswith('其他')])
    column_names = np.array(column_names)
    if len(others) > 0:
        diff = list(column_names[~np.isin(column_names, others)])
        industry_allocation = industry_allocation[diff + others]
    industry_allocation = industry_allocation.reset_index()
    industry_allocation = pd.merge(gzb_dates, industry_allocation, on='EndDate', how='left')
    industry_allocation = industry_allocation.fillna(0)
    return industry_allocation


def get_industry_va_change(stock_portfolio, gzb_dates):
    ashare_portfolio = stock_portfolio[~stock_portfolio['SecondClassCode'].isin(['74', '81', '82', '83'])]
#    logger.info(ashare_portfolio)
    if not ashare_portfolio.empty:
        stock_returns = portfolio_whole_return_method2(ashare_portfolio)
        stock_returns = pd.merge(stock_returns, ashare_portfolio, on=['EndDate', 'SecuCode', 'MarketInTA'], how='inner')
        industry_va_change = stock_returns.groupby(['EndDate', 'Industry']).apply(
            lambda x: (x['MarketInTA'] * x['log_return']).sum())
        industry_va_change = industry_va_change.reset_index()
        industry_va_change = industry_va_change.rename(columns={0: 'portfolio_return'})
    else:
        industry_va_change = pd.DataFrame(columns=['EndDate', 'Industry', 'portfolio_return'])

    hshare_portfolio = stock_portfolio[stock_portfolio['SecondClassCode'].isin(['81', '82', '83'])]
    if len(hshare_portfolio) > 0:
        hshare_portfolio = hshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
        hshare_portfolio['log_return'] = hshare_portfolio.groupby('SecuCode')['MarketPrice'].apply(
            lambda x: np.log(x / x.shift(1)))
        hshare_portfolio = hshare_portfolio.drop('MarketPrice', axis=1)
        hshare_portfolio_return = hshare_portfolio.groupby(['EndDate', 'Industry']).apply(
            lambda x: (x['MarketInTA'] * x['log_return']).sum())
        hshare_portfolio_return = hshare_portfolio_return.reset_index()
        hshare_portfolio_return = hshare_portfolio_return.rename(columns={0: 'portfolio_return'})
        if len(hshare_portfolio_return) > 0:
            industry_va_change = industry_va_change.append(hshare_portfolio_return)

    kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
    kcshare_portfolio = stock_portfolio[stock_portfolio['SecondClassCode'].isin(kcshares_index_list)]
    if len(kcshare_portfolio) > 0:
        kcshare_portfolio = kcshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
        kcshare_portfolio['Industry'] = '其他-科创板'
        kcshare_portfolio['log_return'] = kcshare_portfolio.groupby('SecuCode')['MarketPrice'].apply(
            lambda x: np.log(x / x.shift(1)))
        kcshare_portfolio = kcshare_portfolio.drop('MarketPrice', axis=1)
        kcshare_portfolio_return = kcshare_portfolio.groupby('EndDate').apply(
            lambda x: (x['MarketInTA'] * x['log_return']).sum())
        kcshare_portfolio_return = kcshare_portfolio_return.reset_index()
        kcshare_portfolio_return = kcshare_portfolio_return.rename(columns={0: 'portfolio_return'})
        kcshare_portfolio_return['Industry'] = '其他-科创板'
        if len(kcshare_portfolio_return) > 0:
            industry_va_change = industry_va_change.append(kcshare_portfolio_return)

    gshare_portfolio = stock_portfolio[stock_portfolio['SecondClassCode'] == '74']
    if len(gshare_portfolio) > 0:
        gshare_portfolio = gshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
        gshare_portfolio['Industry'] = '其他-新三板'
        gshare_portfolio['log_return'] = gshare_portfolio.groupby('SecuCode')['MarketPrice'].apply(
            lambda x: np.log(x / x.shift(1)))
        gshare_portfolio = gshare_portfolio.drop('MarketPrice', axis=1)
        gshare_portfolio_return = gshare_portfolio.groupby('EndDate').apply(
            lambda x: (x['MarketInTA'] * x['log_return']).sum())
        gshare_portfolio_return = gshare_portfolio_return.reset_index()
        gshare_portfolio_return = gshare_portfolio_return.rename(columns={0: 'portfolio_return'})
        gshare_portfolio_return['Industry'] = '其他-新三板'
        if len(gshare_portfolio_return) > 0:
            industry_va_change = industry_va_change.append(gshare_portfolio_return)

#    logger.info(industry_va_change)
    industry_va_change = industry_va_change.pivot_table(columns='Industry', values='portfolio_return',
                                                        index='EndDate')

    industry_va_change = industry_va_change.reset_index()
    industry_va_change = pd.merge(gzb_dates, industry_va_change, on='EndDate', how='left')
    industry_va_change = industry_va_change.fillna(0)
    industry_va_change = industry_va_change.set_index('EndDate')
    industry_va_change = np.exp(industry_va_change.cumsum()) - 1
    column_names = industry_va_change.columns
    others = list(column_names[column_names.str.startswith('其他')])
    column_names = np.array(column_names)
    if len(others) > 0:
        diff = list(column_names[~np.isin(column_names, others)])
        industry_va_change = industry_va_change[diff + others]
    industry_va_change = industry_va_change.reset_index()
    return industry_va_change


def get_stock_industry_allocation_and_va_change(request_id, type, fund_zscode,
                                                start_date, end_date, industry_standard):
    stock_portfolio = get_ahstock_industry(fund_zscode, start_date, end_date, industry_standard)
#    logger.info(stock_portfolio)
    if len(stock_portfolio) > 0:
        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        netassetvalue = netassetvalue.rename(columns={'TotalMarket': 'NAV'})
        gzb_dates = netassetvalue[['EndDate']]
        industry_allocation = get_industry_allocation(stock_portfolio, gzb_dates)
#        logger.info(industry_allocation)
        industry_va_change = get_industry_va_change(stock_portfolio, gzb_dates)
#        logger.info(industry_va_change)

        result = {'industry_allocation': industry_allocation,
                  'industry_va_ratio': industry_va_change}
    else:
        result = {}
    return result


#%%
def get_top10_stock(request_id, type, fund_zscode, start_date, end_date):
    stock_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='stock')
    stock_codes = stock_portfolio.SecuCode.unique().tolist()
    kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
    if len(stock_codes) > 0:
        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        gzb_dates = netassetvalue[['EndDate']]
        hshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(['81', '82', '83'])]['SecuCode'].unique().tolist()
        gzshares = stock_portfolio[stock_portfolio['SecondClassCode'] == '74']['SecuCode'].unique().tolist()
        ashares = stock_portfolio[~stock_portfolio['SecondClassCode'].isin(
                                     ['74', '81', '82', '83']+kcshares_index_list)]['SecuCode'].unique().tolist()
        kcshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(kcshares_index_list)]['SecuCode'].unique().tolist()

        ashare_portfolio = stock_portfolio[~stock_portfolio['SecondClassCode'].isin(['74', '81', '82', '83'])]
        # ashare_portfolio = ashare_portfolio.rename(columns={'MarketInTA': 'weight'})
        stock_returns = portfolio_whole_return_method2(ashare_portfolio)
        stock_returns = pd.merge(stock_returns, ashare_portfolio, on=['EndDate', 'SecuCode', 'MarketInTA'], how='inner')

        hshare_portfolio = stock_portfolio[stock_portfolio['SecondClassCode'].isin(['81', '82', '83'])]
        if len(hshare_portfolio) > 0:
            hshare_portfolio = hshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
            # hshare_portfolio = hshare_portfolio.rename(columns={'MarketInTA': 'weight'})
            hshare_portfolio['log_return'] = hshare_portfolio.groupby('SecuCode')['MarketPrice'].apply(
                lambda x: np.log(x / x.shift(1)))
            hshare_portfolio = hshare_portfolio.dropna()
            hshare_portfolio = hshare_portfolio.drop('MarketPrice', axis=1)

        kcshare_portfolio = stock_portfolio[stock_portfolio['SecondClassCode'].isin(kcshares_index_list)]
        if len(kcshare_portfolio) > 0:
            kcshare_portfolio = kcshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
            # kcshare_portfolio = kcshare_portfolio.rename(columns={'MarketInTA': 'weight'})
            kcshare_portfolio['Industry'] = '其他-科创板'

        gshare_portfolio = stock_portfolio[stock_portfolio['SecondClassCode'] == '74']
        if len(gshare_portfolio) > 0:
            gshare_portfolio = gshare_portfolio.sort_values(by=['EndDate', 'SecuCode'])
            # gshare_portfolio = gshare_portfolio.rename(columns={'MarketInTA': 'weight'})
            gshare_portfolio['Industry'] = '其他-新三板'
        stock_weight = stock_portfolio.pivot_table(columns='SecuCode', values='MarketInTA', index='EndDate')
        stock_weight = stock_weight.reset_index()
        stock_weight = pd.merge(gzb_dates, stock_weight, on='EndDate', how='left')

        stock_weight = stock_weight.set_index('EndDate')
        stock_average_weight = stock_weight.mean()
        stock_holding_dates = stock_weight.count()
        stock_average_weight = stock_average_weight.reset_index()
        stock_average_weight = stock_average_weight.rename(columns={'index': 'SecuCode', 0: 'weight'})
        stock_average_weight = stock_average_weight.sort_values(by='weight', ascending=False)
        stock_holding_dates = stock_holding_dates.reset_index()
        stock_holding_dates = stock_holding_dates.rename(columns={'index': 'SecuCode', 0: 'days'})
        stock_top10_weight = stock_average_weight.iloc[0:10]
        stock_top10_weight = pd.merge(stock_top10_weight, stock_holding_dates, on='SecuCode', how='left')

        stock_returns = stock_returns.append(hshare_portfolio)
        stock_returns['va_contribution'] = stock_returns['MarketInTA'] * stock_returns['log_return']
        stock_returns = stock_returns.pivot_table(columns='SecuCode', values='va_contribution', index='EndDate')
        stock_returns = stock_returns.fillna(0)
        stock_returns = np.exp(stock_returns.cumsum()) - 1
        stock_cumulative_returns = stock_returns.iloc[[-1]]
        stock_cumulative_returns = stock_cumulative_returns.reset_index()[
            stock_cumulative_returns.columns].T.reset_index()
        stock_cumulative_returns = stock_cumulative_returns.rename(columns={'index': 'SecuCode', 0: 'va_ratio'})
        stock_positive_va = stock_cumulative_returns[stock_cumulative_returns['va_ratio'] > 0]
        stock_positive_va = stock_positive_va.sort_values(by='va_ratio', ascending=False)

        stock_top10_positive_va = stock_positive_va.iloc[0: 10]
        stock_top10_positive_va = pd.merge(stock_top10_positive_va, stock_average_weight, on='SecuCode', how='left')

        stock_negative_va = stock_cumulative_returns[stock_cumulative_returns['va_ratio'] < 0]
        stock_negative_va = stock_negative_va.sort_values(by='va_ratio')
        stock_top10_negative_va = stock_negative_va.iloc[0: 10]
        stock_top10_negative_va = pd.merge(stock_top10_negative_va, stock_average_weight, on='SecuCode', how='left')

        hshares_names = get_hstock_chinames(hshares)
        gzshares_names = get_gzstock_chinames(gzshares)
        ashares_names = get_astock_chinames(ashares)
        kcshares_names = get_kcstock_chinames(kcshares)
        kcshares_names = kcshares_names.drop_duplicates(subset=['SecuCode'])
        stock_names = ashares_names.append(hshares_names)
        stock_names = stock_names.append(gzshares_names)
        stock_names = stock_names.append(kcshares_names)
        stock_names = stock_names.reset_index(level=0, drop=True)
        stock_top10_weight = pd.merge(stock_top10_weight, stock_names, on='SecuCode', how='left')
#        logger.info(stock_top10_weight)

        stock_top10_positive_va = pd.merge(stock_top10_positive_va, stock_names, on='SecuCode', how='left')
        stock_top10_negative_va = pd.merge(stock_top10_negative_va, stock_names, on='SecuCode', how='left')
        stock_weight_and_va_ratio = {'stock_top10_weight': stock_top10_weight,
                                     'stock_top10_positive_va': stock_top10_positive_va,
                                     'stock_top10_negative_va': stock_top10_negative_va}

    else:
        stock_weight_and_va_ratio = pd.DataFrame()
    return stock_weight_and_va_ratio


#%%
def get_bond_credit_rating(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    data_column = ['SecuCode', 'SecuAbbr', 'EndDate', 'Rating', 'UpdateTime']
    if n > 1000:
        with db_connection.cursor() as cursor:
            bond_ratings = pd.DataFrame(columns=data_column)
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT BC.SecuCode, BC.SecuAbbr, CG.CRDate, CG.CRDesc, CG.UpdateTime
                    FROM JYDB.Bond_Code BC
                    JOIN JYDB.Bond_BDCreditGrading CG
                    ON BC.MainCode = CG.MainCode
                    WHERE BC.SecuCode in %s
                    """ % (str(tuple(sub_list)).replace(',)', ')'))
                    )
                    bond_rating = cursor.fetchall()
                    if not bond_rating:
                        continue
                    bond_rating = pd.DataFrame(list(bond_rating), columns=data_column)
                    bond_ratings = bond_ratings.append(bond_rating)
                    bond_ratings = bond_ratings.sort_values(
                        by=['SecuCode', 'EndDate', 'UpdateTime'])
            return bond_ratings
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT BC.SecuCode, BC.SecuAbbr, CG.CRDate, CG.CRDesc, CG.UpdateTime
                FROM JYDB.Bond_Code BC
                JOIN JYDB.Bond_BDCreditGrading CG
                ON BC.MainCode = CG.MainCode
                WHERE BC.SecuCode in %s
                """ % (str(tuple(codes)).replace(',)', ')'))
            )
            bond_rating = cursor.fetchall()
            if not bond_rating:
                return pd.DataFrame(columns=data_column)
            bond_ratings = pd.DataFrame(list(bond_rating), columns=data_column)
            bond_ratings = bond_ratings.sort_values(
                by=['SecuCode', 'EndDate', 'UpdateTime'])
            return bond_ratings
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'SecuAbbr', 'EndDate', 'Rating', 'UpdateTime'])


def get_bond_chinames(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        with db_connection.cursor() as cursor:
            bond_chinames = pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT SecuCode, SecuAbbr
                    FROM JYDB.Bond_Code
                    WHERE SecuCode in %s
                    """ % (str(tuple(sub_list)).replace(',)', ')'))
                    )
                    bond_chiname = cursor.fetchall()
                    if not bond_chiname:
                        continue
                    bond_chiname = pd.DataFrame(list(bond_chiname), columns=['SecuCode', 'SecuAbbr'])
                    bond_chinames = bond_chinames.append(bond_chiname)
            return bond_chinames
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
               SELECT SecuCode, SecuAbbr
               FROM JYDB.Bond_Code
               WHERE SecuCode in %s
               """ % (str(tuple(codes)).replace(',)', ')'))
            )
            bond_chiname = cursor.fetchall()
            if not bond_chiname:
                return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])
            bond_chinames = pd.DataFrame(list(bond_chiname), columns=['SecuCode', 'SecuAbbr'])
            return bond_chinames
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'SecuAbbr'])


def get_gzb_dates(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EndDate
            FROM ZS_NETASSETVALUE
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND Item = 'NetAssetValue'
            """ % (fund_zscode, start_date, end_date)
        )
        gzb_dates = cursor.fetchall()
        if not gzb_dates:
            return pd.DataFrame(columns=['EndDate'])
        gzb_dates = pd.DataFrame(list(gzb_dates), columns=['EndDate'])
        return gzb_dates


def get_bonds_innercodes(bond_codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            select SECUCODE, INNERCODE
            from JYDB.SECUMAIN
            where SECUCODE in ('%s')
            """ % ("','".join(bond_codes), ))
        bonds_innercodes = cursor.fetchall()
        if not bonds_innercodes:
            return pd.DataFrame(columns=['SecuCode', 'InnerCode'])
        bonds_innercodes = pd.DataFrame(list(bonds_innercodes),
                                        columns=['SecuCode', 'InnerCode'])
    return bonds_innercodes


def get_sub_bonds_data(bond_innercodes, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['INNERCODE', 'ENDDATE', 'VALUEFULLPRICE',
                   'VPYIELD', 'VPADURATION', 'ACCRUINTEREST']
    data_column1 = ['InnerCode', 'EndDate', 'CloseDirtyPrice',
                    'Ytm', 'ModifiedDuration', 'Interest']
    if len(bond_innercodes) > 0:
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT %s
                FROM JYDB.Bond_CBValuation
                WHERE INNERCODE IN (%s)
                AND ENDDATE >= to_date('%s','yyyy-mm-dd')
                AND ENDDATE <= to_date('%s','yyyy-mm-dd')
                """ % (','.join(data_column),
                       ','.join(str(innercode) for innercode in bond_innercodes),
                       start_date, end_date))
            bond_price = cursor.fetchall()
            if not bond_price:
                return pd.DataFrame(columns=data_column1)
            bond_price = pd.DataFrame(bond_price, columns=data_column1)
            return bond_price
    else:
        return pd.DataFrame(columns=data_column1)


def get_bonds_data(bond_codes, start_date, end_date):
    innercodes = get_bonds_innercodes(bond_codes)
    bond_innercodes = innercodes.InnerCode.to_list()
    data_column = ['SecuCode', 'EndDate', 'CloseDirtyPrice',
                   'Ytm', 'ModifiedDuration', 'Interest']
    n = len(bond_codes)
    k = int(n / 1000)
    bond_prices = []
    if k > 0:
        for i in range(k + 1):
            sub_bond_innercodes = bond_innercodes[1000 * i: 1000 * (i + 1)] \
                if 1000 * (i + 1) < n else bond_innercodes[1000 * i: n]
            if len(sub_bond_innercodes) == 0:
                break
            bond_price = get_sub_bonds_data(
                sub_bond_innercodes, start_date, end_date)
            bond_prices.append(bond_price)
        bond_prices = pd.concat(bond_prices, sort=False)
    else:
        bond_prices = get_sub_bonds_data(bond_innercodes, start_date, end_date)
    bond_prices['EndDate'] = pd.to_datetime(bond_prices['EndDate'])
    bond_prices = bond_prices.dropna()
    bond_prices = bond_prices.merge(innercodes, on='InnerCode', how='left')
    bond_prices = bond_prices[data_column]
    bond_prices = bond_prices.sort_values(by=['SecuCode', 'EndDate'])
    return bond_prices


def modified_interest_single(single_bond):
    single_bond['IfInterestDate'] = single_bond['Interest'] - single_bond['Interest'].shift(1)
    single_bond['IfInterestDate'] = single_bond['IfInterestDate'].map(lambda x: 1 if x < 0 else 0)
    single_bond['Interest'] = single_bond['Interest'].shift(1)
    single_bond['CloseDirtyPrice_ture'] = single_bond['CloseDirtyPrice']
    single_bond.loc[single_bond.IfInterestDate == 1, 'CloseDirtyPrice'] = \
        single_bond.loc[single_bond.IfInterestDate == 1, 'CloseDirtyPrice'] + \
        single_bond.loc[single_bond.IfInterestDate == 1, 'Interest']
    return single_bond


def cul_return_single(single_bond_price):
    single_bond_price['CloseDirtyPrice'] = single_bond_price['CloseDirtyPrice'].replace(0, np.nan)
    single_bond_price['SimpleReturn_ture'] = \
        (single_bond_price['CloseDirtyPrice_ture'] -
         single_bond_price['CloseDirtyPrice_ture'].shift(1)) / \
        single_bond_price['CloseDirtyPrice_ture'].shift(1)
    single_bond_price['SimpleReturn_false'] = \
        (single_bond_price['CloseDirtyPrice'] -
         single_bond_price['CloseDirtyPrice'].shift(1)) / \
        single_bond_price['CloseDirtyPrice'].shift(1)
    single_bond_price.loc[single_bond_price.IfInterestDate == 1, 'SimpleReturn_ture'] = \
        single_bond_price.loc[single_bond_price.IfInterestDate == 1, 'SimpleReturn_false']
    single_bond_price = single_bond_price.drop(columns=['SimpleReturn_false'], axis=0)
    single_bond_price = single_bond_price.rename(columns={'SimpleReturn_ture': 'SimpleReturn'})
    single_bond_price = single_bond_price.dropna()
    return single_bond_price


def get_complete_rating(end_dates, bond_ratings):
    """
    评级数据补全
    :param end_dates:基金有债券数据的日期
    :param bond_ratings:债券评级原始数据（已经过预处理）
    :return:处理过
    """
    rating_class = bond_ratings['Rating'].unique()
    rating_class_num = dict(zip(rating_class, range(len(rating_class))))
    rating_class_reverse = dict(zip(range(len(rating_class)), rating_class))
    rating_class_reverse['暂无评级'] = '暂无评级'
    bond_ratings['rating'] = bond_ratings['Rating'].apply(lambda x: rating_class_num[x])
    bond_ratings_pivot = bond_ratings.pivot_table(
        columns='SecuCode', index='EndDate', values='rating')
    bond_ratings_pivot = bond_ratings_pivot.reset_index()
    bond_ratings_pivot = end_dates.merge(bond_ratings_pivot, on='EndDate', how='left')
    bond_ratings_pivot = bond_ratings_pivot.ffill()
    bond_ratings_pivot = bond_ratings_pivot.bfill()
    bond_ratings_pivot = bond_ratings_pivot.fillna("暂无评级")
    bond_ratings_pivot = bond_ratings_pivot.applymap(
        lambda x: rating_class_reverse[x] if x in rating_class_reverse.keys() else x)
    return bond_ratings_pivot


def get_rating_allocation(gzb_dates, bond_portfolio):
    """
    获取评级分布数据
    :param gzb_dates:基金的估值表日期
    :param bond_portfolio: 债券组合和评级数据
    :return: 评级分布数据
    """
    market_in_nv = bond_portfolio[['EndDate', 'Rating', 'MarketInTA']]
    rating_allocation = market_in_nv.groupby(['EndDate', 'Rating'])['MarketInTA'].sum()
    rating_allocation = rating_allocation.reset_index()
    rating_allocation = rating_allocation.pivot_table(columns='Rating', index='EndDate', values='MarketInTA')
    rating_allocation = rating_allocation.reset_index()
    rating_allocation = pd.merge(gzb_dates, rating_allocation, on='EndDate', how='left')
    rating_allocation = rating_allocation.fillna(0)
    return rating_allocation


def get_cum_return(bond_return, MarketInTA, gzb_dates):
    bond_return = bond_return.groupby('SecuCode').apply(modified_interest_single)
    bond_return = bond_return.groupby('SecuCode').apply(cul_return_single)
    bond_return.index = bond_return.index.droplevel(level=0)
    simple_return = bond_return.pivot(index='EndDate', columns='SecuCode', values='SimpleReturn')
    simple_return_index = pd.DataFrame(simple_return.index).rename(columns={0: 'EndDate'})
    MarketInTA_new = pd.merge(simple_return_index, MarketInTA, how='left',
                              left_on='EndDate', right_index=True)
    MarketInTA_new = MarketInTA_new.set_index('EndDate')
    codes_other = set(MarketInTA_new.columns) - set(simple_return.columns)
#    logger.info(codes_other)
    if len(codes_other) > 0:
        simple_return_other = pd.DataFrame(data=0, index=simple_return.index, columns=codes_other)
        simple_return_other = simple_return_other.reset_index().rename(columns={0: 'EndDate'})
        simple_return = simple_return.merge(simple_return_other, on='EndDate', how='left')
        simple_return = simple_return.set_index('EndDate')
    simple_return = simple_return[MarketInTA_new.columns]
    wei_return = simple_return * MarketInTA_new
    wei_return = wei_return.fillna(0)
    cum_return = (wei_return + 1).cumprod() - 1
#    logger.info(cum_return)
#    logger.info(gzb_dates)
    cum_return = gzb_dates.merge(cum_return, left_on='EndDate', right_index=True, how='left')
    cum_return = cum_return.set_index('EndDate')
    cum_return = cum_return.ffill()
    return cum_return


def get_rating_va_change(gzb_dates, bond_ratings_pivot, cum_return, fund_bond_ratings):
    bond_ratings_pivot = gzb_dates.merge(bond_ratings_pivot, on='EndDate', how='left')
    bond_ratings_pivot = bond_ratings_pivot.set_index('EndDate')
    bond_ratings_pivot = bond_ratings_pivot.ffill()
    bond_ratings_pivot = bond_ratings_pivot.bfill()
    rating_va_change = pd.DataFrame(data=None, index=bond_ratings_pivot.index)
    for rating in fund_bond_ratings:
        rating_judge = bond_ratings_pivot.where(~bond_ratings_pivot.applymap(lambda x: x == rating), 1)
        rating_judge = rating_judge.where(rating_judge.applymap(lambda x: x == 1), 0)
        rating_return = rating_judge * cum_return
        rating_va_change[rating] = rating_return.sum(axis=1)
    return rating_va_change


def get_bond_rating_allocation_and_va_change(request_id, type, fund_zscode, start_date, end_date):
    bond_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='bond')
    bond_portfolio = bond_portfolio.sort_values(by=['SecuCode', 'EndDate'])
    bond_codes = bond_portfolio.SecuCode.unique().tolist()
    # bond_secuabbr = bond_portfolio.ClassName.unique().tolist()
    if len(bond_codes) > 0:
        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        gzb_dates = netassetvalue[['EndDate']]
        # bond_rating
        # 获取数据 & 预处理
        MarketInTA = bond_portfolio.pivot_table(columns='SecuCode', values='MarketInTA', index='EndDate')
        bond_ratings_pivot = get_bond_ratings(bond_portfolio)
        if not bond_ratings_pivot.empty:
            bond_ratings = pd.melt(bond_ratings_pivot, id_vars='EndDate', var_name='SecuCode', value_name='Rating')
            bond_portfolio = pd.merge(bond_portfolio, bond_ratings, on=['SecuCode', 'EndDate'], how='left')
            bond_portfolio['Rating'] = bond_portfolio['Rating'].fillna('暂无评级')

            # rating_allocation
            rating_allocation = get_rating_allocation(gzb_dates, bond_portfolio)
            fund_bond_ratings = rating_allocation.columns
            # 计算债券的累计收益率，和评级收益率
            bond_return = get_bonds_data(bond_codes, start_date, end_date)
            if len(bond_return) > 0:
                cum_return = get_cum_return(bond_return, MarketInTA, gzb_dates)
                rating_va_change = get_rating_va_change(gzb_dates, bond_ratings_pivot,
                                                        cum_return, fund_bond_ratings)
            else:
                rating_va_change = pd.DataFrame()
                rating_allocation = pd.DataFrame()
        else:
            rating_va_change = pd.DataFrame()
            rating_allocation = pd.DataFrame()
        result = {'rating_allocation': rating_allocation,
                  'rating_va_ratio': rating_va_change}
    else:
        result = pd.DataFrame()

    return result


# %%
def get_top10_bond(request_id, type, fund_zscode, start_date, end_date):
    bond_portfolio = get_fund_asset_portfolio(
        fund_zscode, start_date, end_date, asset_type='bond')
    bond_portfolio = bond_portfolio.sort_values(by=['SecuCode', 'EndDate'])
#    logger.info(bond_portfolio)
    bond_codes = bond_portfolio.SecuCode.unique().tolist()

    if len(bond_codes) > 0:
        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        netassetvalue = netassetvalue.sort_values(by=['EndDate'])
        gzb_dates = netassetvalue[['EndDate']]
        # MarketInTA
        MarketInTA = bond_portfolio.pivot_table(columns='SecuCode', values='MarketInTA', index='EndDate')
        bond_average_weight = MarketInTA.mean()
        bond_holding_dates = MarketInTA.count()
        bond_average_weight = bond_average_weight.reset_index()
        bond_average_weight = bond_average_weight.rename(columns={'index': 'SecuCode', 0: 'weight'})
        bond_average_weight = bond_average_weight.sort_values(by='weight', ascending=False)
        bond_holding_dates = bond_holding_dates.reset_index()
        bond_holding_dates = bond_holding_dates.rename(columns={'index': 'SecuCode', 0: 'days'})
        bond_top10_weight = bond_average_weight.iloc[0:10]
        bond_top10_weight = pd.merge(bond_top10_weight, bond_holding_dates, on='SecuCode', how='left')
        MarketInTA = MarketInTA.fillna(0)
        bond_return = get_bonds_data(bond_codes, start_date, end_date)

        if len(bond_return) > 0:
            cum_return = get_cum_return(bond_return, MarketInTA, gzb_dates)
            bond_va_return = pd.merge(gzb_dates, cum_return, on='EndDate', how='left')
            bond_va_return = bond_va_return.set_index('EndDate')
            bond_average_va = bond_va_return.mean()
            bond_average_va = bond_average_va.reset_index()
            bond_average_va = bond_average_va.rename(columns={'index': 'SecuCode', 0: 'va_ratio'})

            bond_positive_va = bond_average_va[bond_average_va['va_ratio'] > 0]
            bond_positive_va = bond_positive_va.sort_values(by='va_ratio', ascending=False)
            bond_top10_positive_va = bond_positive_va.iloc[0: 10]
            bond_top10_positive_va = bond_top10_positive_va.merge(
                bond_average_weight, on='SecuCode', how='left')

            bond_negative_va = bond_average_va[bond_average_va['va_ratio'] < 0]
            bond_negative_va = bond_negative_va.sort_values(by='va_ratio')
            bond_top10_negative_va = bond_negative_va.iloc[0: 10]
            bond_top10_negative_va = bond_top10_negative_va.merge(
                bond_average_weight, on='SecuCode', how='left')

            bond_names = bond_portfolio[['SecuCode', 'ClassName']].drop_duplicates(subset=['SecuCode'], keep='last')
            bond_names = bond_names.rename(columns={'ClassName': 'SecuAbbr'})
            bond_top10_weight = pd.merge(bond_top10_weight, bond_names, on='SecuCode', how='left')
            bond_top10_positive_va = pd.merge(bond_top10_positive_va, bond_names, on='SecuCode', how='left')
            bond_top10_negative_va = pd.merge(bond_top10_negative_va, bond_names, on='SecuCode', how='left')

        else:
            bond_names = bond_portfolio[['SecuCode', 'ClassName']].drop_duplicates(subset=['SecuCode'], keep='last')
            bond_names = bond_names.rename(columns={'ClassName': 'SecuAbbr'})
            bond_top10_weight = pd.merge(bond_top10_weight, bond_names, on='SecuCode', how='left')
            bond_top10_positive_va = pd.DataFrame()
            bond_top10_negative_va = pd.DataFrame()
        bond_weight_and_va_ratio = {'bond_top10_weight': bond_top10_weight,
                                    'bond_top10_positive_va': bond_top10_positive_va,
                                    'bond_top10_negative_va': bond_top10_negative_va}
    else:
        bond_weight_and_va_ratio = {}
    return bond_weight_and_va_ratio


# %%
def get_bond_ratings(bond_portfolio):
    bond_codes = bond_portfolio.SecuCode.unique().tolist()
    bond_secuabbr = bond_portfolio.ClassName.unique().tolist()
    bond_ratings = get_bond_credit_rating(bond_codes)
    if bond_ratings.empty:
        return pd.DataFrame()
    bond_ratings = bond_ratings.drop_duplicates(subset=['EndDate', 'SecuCode'], keep='last')
    # 预处理
    if len(bond_ratings[['SecuCode', 'SecuAbbr']].drop_duplicates()) > len(
            bond_ratings[['SecuCode']].drop_duplicates()):
        bond_ratings = bond_ratings[bond_ratings['SecuAbbr'].isin(bond_secuabbr)]
    # 补全评级数据
    MarketInTA = bond_portfolio.pivot_table(columns='SecuCode', values='MarketInTA', index='EndDate')
    end_dates = MarketInTA.index
    end_dates = pd.DataFrame(end_dates, columns=['EndDate'])
    bond_ratings_pivot = get_complete_rating(end_dates, bond_ratings)
    return bond_ratings_pivot


def get_bond_natures(bond_codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(bond_codes)
    data_column = ['SecuCode', 'SecuAbbr', 'BondNature']
    if n > 1000:
        bond_natures = pd.DataFrame(columns=data_column)
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = bond_codes[1000 * i: 1000 * (i + 1)] \
                    if 1000 * (i + 1) < n else bond_codes[1000 * i: n]
                cursor.execute(
                    """
                SELECT BC.SecuCode, BC.SecuAbbr, SC.MS
                FROM JYDB.Bond_Code BC
                JOIN JYDB.CT_SystemConst SC
                ON BC.BondNature = SC.DM
                WHERE BC.SecuCode in %s
                AND SC.LB = 1243
                """ % (str(tuple(sub_list)).replace(',)', ')'))
                )
                bond_nature = cursor.fetchall()
                if not bond_nature:
                    bond_nature = pd.DataFrame(columns=data_column)
                else:
                    bond_nature = pd.DataFrame(list(bond_nature), columns=data_column)
                bond_natures = bond_natures.append(bond_nature)
            return bond_natures
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT BC.SecuCode, BC.SecuAbbr, SC.MS
                FROM JYDB.Bond_Code BC
                JOIN JYDB.CT_SystemConst SC
                ON BC.BondNature = SC.DM
                WHERE BC.SecuCode in %s
                AND SC.LB = 1243
                """ % (str(tuple(bond_codes)).replace(',)', ')'))
            )
            bond_nature = cursor.fetchall()
            if not bond_nature:
                return pd.DataFrame(columns=data_column)
            bond_natures = pd.DataFrame(list(bond_nature), columns=data_column)
            return bond_natures
    elif n == 0:
        return pd.DataFrame(columns=data_column)


def get_bond_remaining_maturity_from_eq(bond_codes, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['SecuCode', 'SecuAbbr', 'EndDate', 'remaining_mat']
    n = len(bond_codes)
    if n > 1000:
        with db_connection.cursor() as cursor:
            bond_yrmats = pd.DataFrame(columns=data_column)
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = bond_codes[1000 * i: 1000 * (i + 1)] \
                    if 1000 * (i + 1) < n else bond_codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT BC.SecuCode, BC.SecuAbbr, EQ.TradingDay, EQ.YrMat
                    FROM JYDB.Bond_Code BC
                    JOIN JYDB.Bond_ExchangeQuote EQ
                    ON BC.InnerCode = EQ.InnerCode
                    WHERE BC.SecuCode in %s
                    AND EQ.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                    AND EQ.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                    AND EQ.YrMat is not NULL
                    """ % (str(tuple(sub_list)).replace(',)', ')'), start_date, end_date)
                    )
                    bond_yrmat = cursor.fetchall()
                    if not bond_yrmat:
                        bond_yrmat = pd.DataFrame(columns=data_column)
                    else:
                        bond_yrmat = pd.DataFrame(list(bond_yrmat), columns=data_column)
                    bond_yrmats = bond_yrmats.append(bond_yrmat)
            return bond_yrmats
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT BC.SecuCode, BC.SecuAbbr, EQ.TradingDay, EQ.YrMat
                FROM JYDB.Bond_Code BC
                JOIN JYDB.Bond_ExchangeQuote EQ
                ON BC.InnerCode = EQ.InnerCode
                WHERE BC.SecuCode in %s
                AND EQ.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                AND EQ.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                AND EQ.YrMat is not NULL
                """ % (str(tuple(bond_codes)).replace(',)', ')'), start_date, end_date)
            )
            bond_yrmat = cursor.fetchall()
            if not bond_yrmat:
                return pd.DataFrame(columns=data_column)
            bond_yrmats = pd.DataFrame(list(bond_yrmat), columns=data_column)
            return bond_yrmats
    elif n == 0:
        return pd.DataFrame(columns=data_column)


def get_bond_remaining_maturity_from_eqfi(bond_codes, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['SecuCode', 'SecuAbbr', 'EndDate', 'remaining_mat']
    n = len(bond_codes)
    if n > 1000:
        bond_yrmats = pd.DataFrame(columns=data_column)
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = bond_codes[1000 * i: 1000 * (i + 1)] \
                    if 1000 * (i + 1) < n else bond_codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT BC.SecuCode, BC.SecuAbbr, EQFI.TradingDay, EQFI.YrMat
                    FROM JYDB.Bond_Code BC
                    JOIN JYDB.Bond_ExchangeQuoteFI EQFI
                    ON BC.InnerCode = EQFI.InnerCode
                    WHERE BC.SecuCode in %s
                    AND EQFI.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                    AND EQFI.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                    AND EQFI.YrMat is not NULL
                    """ % (str(tuple(sub_list)).replace(',)', ')'), start_date, end_date)
                    )
                    bond_yrmat = cursor.fetchall()
                    if not bond_yrmat:
                        bond_yrmat = pd.DataFrame(columns=data_column)
                    else:
                        bond_yrmat = pd.DataFrame(list(bond_yrmat), columns=data_column)
                    bond_yrmats = bond_yrmats.append(bond_yrmat)
            return bond_yrmats
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT BC.SecuCode, BC.SecuAbbr, EQFI.TradingDay, EQFI.YrMat
                FROM JYDB.Bond_Code BC
                JOIN JYDB.Bond_ExchangeQuoteFI EQFI
                ON BC.InnerCode = EQFI.InnerCode
                WHERE BC.SecuCode in %s
                AND EQFI.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                AND EQFI.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                AND EQFI.YrMat is not NULL
                """ % (str(tuple(bond_codes)).replace(',)', ')'), start_date, end_date)
            )
            bond_yrmat = cursor.fetchall()
            if not bond_yrmat:
                return pd.DataFrame(columns=data_column)
            bond_yrmats = pd.DataFrame(list(bond_yrmat), columns=data_column)
            return bond_yrmats
    elif n == 0:
        return pd.DataFrame(columns=data_column)


def get_bond_rating_allocation_change(bond_portfolio, gzb_dates):
    bond_ratings_pivot = get_bond_ratings(bond_portfolio)
    if bond_ratings_pivot.empty:
        return pd.DataFrame()
    bond_ratings = pd.melt(bond_ratings_pivot, id_vars='EndDate', var_name='SecuCode', value_name='Rating')
    bond_portfolio = pd.merge(bond_portfolio, bond_ratings, on=['SecuCode', 'EndDate'], how='left')
    bond_portfolio['Rating'] = bond_portfolio['Rating'].fillna('暂无评级')
    rating_allocation = get_rating_allocation(gzb_dates, bond_portfolio)
    return rating_allocation


def get_bond_nature_allocation_change(bond_portfolio, gzb_dates):
    bond_codes = bond_portfolio.SecuCode.unique().tolist()
    bond_secuabbr = bond_portfolio.ClassName.unique().tolist()

    bond_natures = get_bond_natures(bond_codes)
    if len(bond_natures[['SecuCode', 'SecuAbbr']].drop_duplicates()) > len(
            bond_natures[['SecuCode']].drop_duplicates()):
        bond_natures = bond_natures[bond_natures['SecuAbbr'].isin(bond_secuabbr)]

    bond_portfolio = pd.merge(bond_portfolio, bond_natures, on='SecuCode', how='left')
    bond_portfolio['BondNature'] = bond_portfolio['BondNature'].fillna('暂无分类')

    market_in_nv = bond_portfolio[['EndDate', 'BondNature', 'MarketInTA']]
    bondnature_allocation = market_in_nv.groupby(['EndDate', 'BondNature'])['MarketInTA'].sum()
    bondnature_allocation = bondnature_allocation.reset_index()
    bondnature_allocation = bondnature_allocation.pivot_table(columns='BondNature', index='EndDate',
                                                              values='MarketInTA')

    bondnature_allocation = bondnature_allocation.reset_index()
    bondnature_allocation = pd.merge(gzb_dates, bondnature_allocation, on='EndDate', how='left')
    bondnature_allocation = bondnature_allocation.fillna(0)

    return bondnature_allocation


def get_bond_remaining_maturity_allocation(bond_portfolio, gzb_dates, start_date, end_date):
    bond_codes = bond_portfolio.SecuCode.unique().tolist()
    bond_portfolio = bond_portfolio.rename(columns={'ClassName': 'SecuAbbr'})
    #    bond_secuabbr = bond_portfolio.ClassName.unique().tolist()

    holding_start_dates = bond_portfolio.groupby('SecuCode')['EndDate'].apply(lambda x: x.iloc[0])
    holding_start_dates = holding_start_dates.reset_index()
    holding_start_dates = holding_start_dates.rename(columns={'EndDate': 'start_date'})

    holding_end_dates = bond_portfolio.groupby('SecuCode')['EndDate'].apply(lambda x: x.iloc[-1])
    holding_end_dates = holding_end_dates.reset_index()
    holding_end_dates = holding_end_dates.rename(columns={'EndDate': 'end_date'})

    bond_yrmats = get_bond_remaining_maturity_from_eq(bond_codes, start_date, end_date)
    #    if len(bond_yrmats[['SecuCode', 'SecuAbbr']].drop_duplicates()) > len(bond_yrmats[['SecuCode']].drop_duplicates()):
    #        bond_yrmats = bond_yrmats[bond_yrmats['SecuAbbr'].isin(bond_secuabbr)]

    #    bond_codes_eq = bond_yrmats.SecuCode.unique().tolist()
    #    code_diff = np.setdiff1d(bond_codes, bond_codes_eq)
    bond_yrmats_from_eqfi = get_bond_remaining_maturity_from_eqfi(bond_codes, start_date, end_date)

    bond_yrmats = bond_yrmats.append(bond_yrmats_from_eqfi)
    bond_yrmats = pd.merge(bond_yrmats, holding_start_dates, on='SecuCode', how='left')
    bond_yrmats = pd.merge(bond_yrmats, holding_end_dates, on='SecuCode', how='left')
    bond_yrmats = bond_yrmats[
        (bond_yrmats['EndDate'] >= bond_yrmats['start_date']) & (bond_yrmats['EndDate'] <= bond_yrmats['end_date'])]

    #    if len(bond_yrmats[['SecuCode', 'SecuAbbr']].drop_duplicates()) > len(bond_yrmats[['SecuCode']].drop_duplicates()):
    #        bond_yrmats = bond_yrmats[bond_yrmats['SecuAbbr'].isin(bond_secuabbr)]

    bond_yrmats = bond_yrmats.drop_duplicates(subset=['SecuCode', 'EndDate'])
    if len(bond_yrmats) > 0:
        bond_yrmats = bond_yrmats[~bond_yrmats['remaining_mat'].str.startswith('-')]
        bond_yrmats['remaining_year'] = bond_yrmats['remaining_mat'].str[0].astype('float')
        bond_yrmats['remaining_day'] = bond_yrmats['remaining_mat'].apply(
            lambda x: re.search('年(.*)天', x).group(1)).astype('float')
        bond_yrmats['remaining_maturity'] = bond_yrmats['remaining_year'] + bond_yrmats['remaining_day'] / 365

        bond_yrmats = bond_yrmats.drop(['remaining_mat', 'remaining_year', 'remaining_day'], axis=1)
        bond_portfolio = pd.merge(bond_portfolio, bond_yrmats, on=['SecuCode', 'SecuAbbr', 'EndDate'], how='left')
        bond_portfolio = bond_portfolio.sort_values(by=['SecuCode', 'EndDate'])
        bond_portfolio['remaining_maturity'] = bond_portfolio.groupby('SecuCode')['remaining_maturity'].ffill()
        bond_portfolio['remaining_maturity'] = bond_portfolio.groupby('SecuCode')['remaining_maturity'].bfill()
        bond_portfolio = bond_portfolio.dropna(subset=['remaining_maturity'])
        if len(bond_portfolio) > 0:
            bond_portfolio['weight'] = bond_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x / x.sum())
            bond_portfolio = bond_portfolio.drop(['MarketInTA', 'ValuationAppreciation'], axis=1)
            bond_portfolio_remaining_maturity = bond_portfolio.groupby(['EndDate']).apply(
                lambda x: (x['weight'] * x['remaining_maturity']).sum())
            bond_portfolio_remaining_maturity = bond_portfolio_remaining_maturity.reset_index()
            bond_portfolio_remaining_maturity = bond_portfolio_remaining_maturity.rename(
                columns={0: 'remaining_maturity'})

            return bond_portfolio_remaining_maturity


def get_bond_remaining_maturity_allocation_change(bond_portfolio, gzb_dates, start_date, end_date):
    bond_portfolio_remaining_maturity = get_bond_remaining_maturity_allocation(bond_portfolio, gzb_dates, start_date,
                                                                               end_date)
    if bond_portfolio_remaining_maturity is not None:
        bond_portfolio_remaining_maturity = pd.merge(gzb_dates, bond_portfolio_remaining_maturity, on='EndDate',
                                                     how='left')

    else:
        bond_portfolio_remaining_maturity = pd.DataFrame()

    return bond_portfolio_remaining_maturity


def get_bond_yield_to_maturity_from_eq(bond_codes, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    n = len(bond_codes)

    if n > 1000:
        try:
            bond_ytms = pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = bond_codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else bond_codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT BC.SecuCode, EQ.TradingDay, EQ.YTM_CL/100
                    FROM JYDB.Bond_Code BC
                    JOIN JYDB.Bond_ExchangeQuote EQ
                    ON BC.InnerCode = EQ.InnerCode
                    WHERE BC.SecuCode in %s
                    AND EQ.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                    AND EQ.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                    AND EQ.YTM_CL is not NULL
                    """ % (str(tuple(sub_list)).replace(',)', ')'), start_date, end_date)
                    )
                    bond_ytms = cursor.fetchall()

                    if not bond_ytms:
                        bond_ytms = pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])
                    else:
                        bond_ytms = pd.DataFrame(list(bond_ytms), columns=['SecuCode', 'EndDate', 'ytm'])

                    bond_ytms = bond_ytms.append(bond_ytms)

            return bond_ytms

        finally:
            cursor.close()

    elif n > 0 and n <= 1000:

        try:
            cursor.execute(
                """
                SELECT BC.SecuCode, EQ.TradingDay, EQ.YTM_CL/100
                FROM JYDB.Bond_Code BC
                JOIN JYDB.Bond_ExchangeQuote EQ
                ON BC.InnerCode = EQ.InnerCode
                WHERE BC.SecuCode in %s
                AND EQ.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                AND EQ.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                AND EQ.YTM_CL is not NULL
                """ % (str(tuple(bond_codes)).replace(',)', ')'), start_date, end_date)
            )

            bond_ytms = cursor.fetchall()

            if not bond_ytms:
                return pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])

            bond_ytms = pd.DataFrame(list(bond_ytms), columns=['SecuCode', 'EndDate', 'ytm'])

            return bond_ytms

        finally:
            cursor.close()

    elif n == 0:

        return pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])


def get_bond_yield_to_maturity_from_eqfi(bond_codes, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    n = len(bond_codes)

    if n > 1000:
        try:
            bond_ytms = pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = bond_codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else bond_codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT BC.SecuCode, EQFI.TradingDay, EQFI.YTM_CL/100
                    FROM JYDB.Bond_Code BC
                    JOIN JYDB.Bond_ExchangeQuoteFI EQFI
                    ON BC.InnerCode = EQFI.InnerCode
                    WHERE BC.SecuCode in %s
                    AND EQFI.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                    AND EQFI.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                    AND EQFI.YTM_CL is not NULL
                    """ % (str(tuple(sub_list)).replace(',)', ')'), start_date, end_date)
                    )
                    bond_ytms = cursor.fetchall()

                    if not bond_ytms:
                        bond_ytms = pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])
                    else:
                        bond_ytms = pd.DataFrame(list(bond_ytms), columns=['SecuCode', 'EndDate', 'ytm'])

                    bond_ytms = bond_ytms.append(bond_ytms)

            return bond_ytms

        finally:
            cursor.close()

    elif n > 0 and n <= 1000:

        try:
            cursor.execute(
                """
                SELECT BC.SecuCode, EQFI.TradingDay, EQFI.YTM_CL/100
                FROM JYDB.Bond_Code BC
                JOIN JYDB.Bond_ExchangeQuoteFI EQFI
                ON BC.InnerCode = EQFI.InnerCode
                WHERE BC.SecuCode in %s
                AND EQFI.TradingDay >= to_date('%s', 'yyyy-mm-dd')
                AND EQFI.TradingDay <= to_date('%s', 'yyyy-mm-dd')
                AND EQFI.YTM_CL is not NULL
                """ % (str(tuple(bond_codes)).replace(',)', ')'), start_date, end_date)
            )

            bond_ytms = cursor.fetchall()

            if not bond_ytms:
                return pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])

            bond_ytms = pd.DataFrame(list(bond_ytms), columns=['SecuCode', 'EndDate', 'ytm'])

            return bond_ytms

        finally:
            cursor.close()

    elif n == 0:

        return pd.DataFrame(columns=['SecuCode', 'EndDate', 'ytm'])


def get_bond_yield_to_maturity_allocation(bond_portfolio, gzb_dates, start_date, end_date):
    bond_codes = bond_portfolio.SecuCode.unique().tolist()
    bond_ytms = get_bond_yield_to_maturity_from_eq(bond_codes, start_date, end_date)
    #    bond_codes_eq = bond_ytms.SecuCode.unique().tolist()
    #    code_diff = np.setdiff1d(bond_codes, bond_codes_eq)
    bond_ytms_from_eqfi = get_bond_yield_to_maturity_from_eqfi(bond_codes, start_date, end_date)
    bond_ytms = bond_ytms.append(bond_ytms_from_eqfi)

    holding_start_dates = bond_portfolio.groupby('SecuCode')['EndDate'].apply(lambda x: x.iloc[0])
    holding_start_dates = holding_start_dates.reset_index()
    holding_start_dates = holding_start_dates.rename(columns={'EndDate': 'start_date'})

    holding_end_dates = bond_portfolio.groupby('SecuCode')['EndDate'].apply(lambda x: x.iloc[-1])
    holding_end_dates = holding_end_dates.reset_index()
    holding_end_dates = holding_end_dates.rename(columns={'EndDate': 'end_date'})

    bond_ytms = pd.merge(bond_ytms, holding_start_dates, on='SecuCode', how='left')
    bond_ytms = pd.merge(bond_ytms, holding_end_dates, on='SecuCode', how='left')
    bond_ytms = bond_ytms[
        (bond_ytms['EndDate'] >= bond_ytms['start_date']) & (bond_ytms['EndDate'] <= bond_ytms['end_date'])]

    bond_ytms = bond_ytms.drop_duplicates(subset=['SecuCode', 'EndDate'])

    if len(bond_ytms) > 0:
        bond_portfolio = pd.merge(bond_portfolio, bond_ytms, on=['SecuCode', 'EndDate'], how='left')
        bond_portfolio = bond_portfolio.sort_values(by=['SecuCode', 'EndDate'])
        bond_portfolio['ytm'] = bond_portfolio.groupby('SecuCode')['ytm'].ffill()
        bond_portfolio['ytm'] = bond_portfolio.groupby('SecuCode')['ytm'].bfill()
        bond_portfolio = bond_portfolio.dropna(subset=['ytm'])
        bond_portfolio['weight'] = bond_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x / x.sum())
        bond_portfolio = bond_portfolio.drop(['MarketInTA', 'ValuationAppreciation'], axis=1)
        bond_portfolio = bond_portfolio.reset_index(level=0, drop=True)
        bond_portfolio_yield_to_maturity = bond_portfolio.groupby(['EndDate']).apply(
            lambda x: (x['weight'] * x['ytm']).sum())
        bond_portfolio_yield_to_maturity = bond_portfolio_yield_to_maturity.reset_index()
        bond_portfolio_yield_to_maturity = bond_portfolio_yield_to_maturity.rename(columns={0: 'yield_to_maturity'})

        return bond_portfolio_yield_to_maturity


def get_bond_yield_to_maturity_allocation_change(bond_portfolio, gzb_dates, start_date, end_date):
    bond_portfolio_yield_to_maturity = get_bond_yield_to_maturity_allocation(
        bond_portfolio, gzb_dates, start_date, end_date)
    if bond_portfolio_yield_to_maturity is not None:
#        logger.info(bond_portfolio_yield_to_maturity)
        bond_portfolio_yield_to_maturity = pd.merge(
            gzb_dates, bond_portfolio_yield_to_maturity, on='EndDate', how='left')
    else:
        bond_portfolio_yield_to_maturity = pd.DataFrame()

    return bond_portfolio_yield_to_maturity


def get_bond_allocation_change(request_id, type, fund_zscode, start_date, end_date):
    # bond_portfolio = get_fund_bond_portfolio(fund_zscode, start_date, end_date)
    bond_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='bond')
    bond_codes = bond_portfolio.SecuCode.unique().tolist()

    if len(bond_codes) > 0:
        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        gzb_dates = netassetvalue[['EndDate']]

        rating_allocation = get_bond_rating_allocation_change(bond_portfolio, gzb_dates)

        bondnature_allocation = get_bond_nature_allocation_change(bond_portfolio, gzb_dates)

        bond_portfolio_remaining_maturity = get_bond_remaining_maturity_allocation_change(bond_portfolio, gzb_dates,
                                                                                          start_date, end_date)

        bond_portfolio_yield_to_maturity = get_bond_yield_to_maturity_allocation_change(bond_portfolio, gzb_dates,
                                                                                        start_date, end_date)

        result = {'rating_allocation': rating_allocation,
                  'bondnature_allocation': bondnature_allocation,
                  'remaining_maturity': bond_portfolio_remaining_maturity,
                  'yield_to_maturity': bond_portfolio_yield_to_maturity}

    else:
        result = pd.DataFrame()

    return result


def get_future_productname(contractcodes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    n = len(contractcodes)

    if n > 1000:
        try:
            contract_productnames = pd.DataFrame(columns=['SecuCode', 'ProductName'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = contractcodes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else contractcodes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT CM.ContractCode, CT.ProductName
                    FROM JYDB.Fut_ContractMain CM
                    JOIN JYDB.CT_Product CT
                    ON CT.ProductCode = CM.OptionCode
                    WHERE CT.ProductCategory = 326
                    AND CM.ContractCode in %s
                    """ % (str(tuple(sub_list)).replace(',)', ')'))
                    )
                    contract_productname = cursor.fetchall()
                    if not contract_productname:
                        contract_productname = pd.DataFrame(columns=['SecuCode', 'ProductName'])
                    else:
                        contract_productname = pd.DataFrame(list(contract_productname),
                                                            columns=['SecuCode', 'ProductName'])

                    contract_productnames = contract_productnames.append(contract_productname)

            return contract_productnames

        finally:
            cursor.close()

    elif n > 0 and n <= 1000:

        try:
            cursor.execute(
                """
                SELECT CM.ContractCode, CT.ProductName
                FROM JYDB.Fut_ContractMain CM
                JOIN JYDB.CT_Product CT
                ON CT.ProductCode = CM.OptionCode
                WHERE CT.ProductCategory = 326
                AND CM.ContractCode in %s
                """ % (str(tuple(contractcodes)).replace(',)', ')'))
            )

            contract_productname = cursor.fetchall()

            if not contract_productname:
                return pd.DataFrame(columns=['SecuCode', 'ProductName'])

            contract_productnames = pd.DataFrame(list(contract_productname), columns=['SecuCode', 'ProductName'])

            return contract_productnames

        finally:
            cursor.close()

    elif n == 0:

        return pd.DataFrame(columns=['SecuCode', 'ProductName'])


def get_future_contracttype(contractcodes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    n = len(contractcodes)

    if n > 1000:
        try:
            contracttypes = pd.DataFrame(columns=['SecuCode', 'ContractType'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = contractcodes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else contractcodes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT CM.ContractCode, SC.MS
                    FROM JYDB.Fut_ContractMain CM
                    JOIN JYDB.CT_SystemConst SC
                    ON CM.ContractType = SC.DM
                    WHERE CM.ContractCode in %s
                    AND SC.LB = 1461
                    """ % (str(tuple(sub_list)).replace(',)', ')'))
                    )

                    contracttype = cursor.fetchall()
                    if not contracttype:
                        contracttype = pd.DataFrame(columns=['SecuCode', 'ContractType'])
                    else:
                        contracttype = pd.DataFrame(list(contracttype), columns=['SecuCode', 'ContractType'])

                    contracttypes = contracttypes.append(contracttype)

            return contracttypes

        finally:

            cursor.close()
    elif n > 0 and n <= 1000:

        try:

            cursor.execute(
                """
            SELECT CM.ContractCode, SC.MS
            FROM JYDB.Fut_ContractMain CM
            JOIN JYDB.CT_SystemConst SC
            ON CM.ContractType = SC.DM
            WHERE CM.ContractCode in %s
            AND SC.LB = 1461
            """ % (str(tuple(contractcodes)).replace(',)', ')'))
            )

            contracttype = cursor.fetchall()
            if not contracttype:
                return pd.DataFrame(columns=['SecuCode', 'ContractType'])

            contracttypes = pd.DataFrame(list(contracttype), columns=['SecuCode', 'ContractType'])

            return contracttypes

        finally:
            cursor.close()

    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'ContractType'])


def get_future_contractname(contractcodes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    n = len(contractcodes)

    if n > 1000:
        try:
            contractnames = pd.DataFrame(columns=['SecuCode', 'ContractName'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = contractcodes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else contractcodes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT ContractCode, ContractName
                    FROM JYDB.FUT_CONTRACTMAIN
                    WHERE CONTRACTCODE in %s
                    """ % (str(tuple(sub_list)).replace(',)', ')'))
                    )

                    contractname = cursor.fetchall()
                    if not contractname:
                        contractname = pd.DataFrame(columns=['SecuCode', 'ContractName'])
                    else:
                        contractname = pd.DataFrame(list(contractname), columns=['SecuCode', 'ContractName'])

                    contractnames = contractnames.append(contractname)

            return contractnames

        finally:

            cursor.close()

    elif n > 0 and n <= 1000:

        try:
            cursor.execute(
                """
            SELECT ContractCode, ContractName
            FROM JYDB.FUT_CONTRACTMAIN
            WHERE CONTRACTCODE in %s
            """ % (str(tuple(contractcodes)).replace(',)', ')'))
            )
            contractname = cursor.fetchall()
            if not contractname:
                return pd.DataFrame(columns=['SecuCode', 'ContractName'])

            contractnames = pd.DataFrame(list(contractname), columns=['SecuCode', 'ContractName'])

            return contractnames

        finally:
            cursor.close()

    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'ContractName'])


def get_future_type_allocation_and_va_change(request_id, type, fund_zscode, start_date, end_date):
    # future_portfolio = get_fund_future_portfolio(fund_zscode, start_date, end_date)
    future_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='future')
    future_portfolio.loc[future_portfolio['SecuCode'].str.startswith('0'), 'SecuCode'] = future_portfolio[
                                                                                             'SecuCode'].str[1:]
    future_portfolio['SecuCode'] = future_portfolio['SecuCode'].str.upper()
    contractcodes = future_portfolio['SecuCode'].unique().tolist()

    if len(contractcodes) > 0:

        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        gzb_dates = netassetvalue[['EndDate']]

        contracttypes = get_future_contracttype(contractcodes)

        future_portfolio = pd.merge(future_portfolio, contracttypes, on='SecuCode', how='left')
        future_portfolio['ContractType'] = future_portfolio['ContractType'].fillna('其他')
        future_portfolio = future_portfolio.drop_duplicates().reset_index(drop=True)
        contracttype_allocation = future_portfolio.groupby(['EndDate', 'ContractType'])['MarketInTA'].sum()
        contracttype_allocation = contracttype_allocation.reset_index()
        contracttype_allocation = contracttype_allocation.pivot_table(columns='ContractType', values='MarketInTA',
                                                                      index='EndDate')
        contracttype_allocation = contracttype_allocation.reset_index()
        contracttype_allocation = pd.merge(gzb_dates, contracttype_allocation, on='EndDate', how='left')
        contracttype_allocation = contracttype_allocation.fillna(0)

        future_portfolio = pd.merge(future_portfolio, netassetvalue, on='EndDate', how='left')

        future_portfolio['va_ratio'] = future_portfolio['ValuationAppreciation'] / future_portfolio['TotalMarket']
        future_portfolio['va_ratio'] = future_portfolio['va_ratio'].fillna(0)
        future_portfolio = future_portfolio.drop_duplicates().reset_index(drop=True)
        contracttype_va_change = future_portfolio.groupby(['EndDate', 'ContractType'])['va_ratio'].sum()
        contracttype_va_change = contracttype_va_change.reset_index()
        contracttype_va_change = contracttype_va_change.pivot_table(columns='ContractType', values='va_ratio',
                                                                    index='EndDate')
        contracttype_va_change = contracttype_va_change.reset_index()
        contracttype_va_change = pd.merge(gzb_dates, contracttype_va_change, on='EndDate', how='left')
        contracttype_va_change = contracttype_va_change.sort_values(by='EndDate')
        #        contracttype_va_change = contracttype_va_change.fillna(0)

        productnames = get_future_productname(contractcodes)

        future_portfolio = pd.merge(future_portfolio, productnames, on='SecuCode', how='left')
        future_portfolio['ProductName'] = future_portfolio['ProductName'].fillna('其他')
        future_portfolio = future_portfolio.drop_duplicates().reset_index(drop=True)
        producttype_allocation = future_portfolio.groupby(['EndDate', 'ProductName'])['MarketInTA'].sum()
        producttype_allocation = producttype_allocation.reset_index()
        producttype_allocation = producttype_allocation.pivot_table(columns='ProductName', values='MarketInTA',
                                                                    index='EndDate')
        producttype_allocation = producttype_allocation.reset_index()
        producttype_allocation = pd.merge(gzb_dates, producttype_allocation, on='EndDate', how='left')
        producttype_allocation = producttype_allocation.fillna(0)

        producttype_va_change = future_portfolio.groupby(['EndDate', 'ProductName'])['va_ratio'].sum()
        producttype_va_change = producttype_va_change.reset_index()
        producttype_va_change = producttype_va_change.pivot_table(columns='ProductName', values='va_ratio',
                                                                  index='EndDate')
        producttype_va_change = producttype_va_change.reset_index()
        producttype_va_change = pd.merge(gzb_dates, producttype_va_change, on='EndDate', how='left')
        producttype_va_change = producttype_va_change.sort_values(by='EndDate')
        #        producttype_va_change = producttype_va_change.fillna(0)

        future_weight = future_portfolio.pivot_table(columns='SecuCode', values='MarketInTA', index='EndDate')
        future_weight = future_weight.reset_index()
        future_weight = pd.merge(gzb_dates, future_weight, on='EndDate', how='left')

        future_weight = future_weight.set_index('EndDate')
        future_average_weight = future_weight.mean()
        future_holding_dates = future_weight.count()
        future_average_weight = future_average_weight.reset_index()
        future_average_weight = future_average_weight.rename(columns={'index': 'SecuCode', 0: 'weight'})
        future_average_weight['abs_weight'] = np.abs(future_average_weight['weight'])
        future_average_weight = future_average_weight.sort_values(by='abs_weight', ascending=False)
        future_holding_dates = future_holding_dates.reset_index()
        future_holding_dates = future_holding_dates.rename(columns={'index': 'SecuCode', 0: 'days'})
        future_average_weight = future_average_weight.drop('abs_weight', axis=1)
        future_top10_weight = future_average_weight.iloc[0:10]
        future_top10_weight = pd.merge(future_top10_weight, future_holding_dates, on='SecuCode', how='left')

        #        future_weight = future_weight.fillna(0)

        future_va_ratio = future_portfolio.pivot_table(columns='SecuCode', values='va_ratio', index='EndDate')
        future_va_ratio = future_va_ratio.reset_index()
        future_va_ratio = pd.merge(gzb_dates, future_va_ratio, on='EndDate', how='left')
        future_va_ratio = future_va_ratio.sort_values(by='EndDate')

        future_va_ratio = future_va_ratio.set_index('EndDate')
        future_average_va = future_va_ratio.mean()
        future_average_va = future_average_va.reset_index()
        future_average_va = future_average_va.rename(columns={'index': 'SecuCode', 0: 'va_ratio'})
        future_positive_va = future_average_va[future_average_va['va_ratio'] > 0]
        future_positive_va = future_positive_va.sort_values(by='va_ratio', ascending=False)
        future_top10_positive_va = future_positive_va.iloc[0: 10]
        future_top10_positive_va = pd.merge(future_top10_positive_va, future_average_weight, on='SecuCode', how='left')

        future_negative_va = future_average_va[future_average_va['va_ratio'] < 0]
        future_negative_va = future_negative_va.sort_values(by='va_ratio')
        future_top10_negative_va = future_negative_va.iloc[0: 10]
        future_top10_negative_va = pd.merge(future_top10_negative_va, future_average_weight, on='SecuCode', how='left')

        #        future_va_ratio = future_va_ratio.fillna(0)

        future_names = get_future_contractname(contractcodes)

        future_top10_weight = pd.merge(future_top10_weight, future_names, on='SecuCode', how='left')
        future_top10_positive_va = pd.merge(future_top10_positive_va, future_names, on='SecuCode', how='left')
        future_top10_negative_va = pd.merge(future_top10_negative_va, future_names, on='SecuCode', how='left')

        '''
        future_weight_and_va_ratio = {'future_names': future_names,
                                     'future_weight': future_weight,
                                     'future_va_ratio': future_va_ratio}
        '''

        future_weight_and_va_ratio = {'future_top10_weight': future_top10_weight,
                                      'future_top10_positive_va': future_top10_positive_va,
                                      'future_top10_negative_va': future_top10_negative_va}

        result = {'contracttype_allocation': contracttype_allocation,
                  'contracttype_va_ratio': contracttype_va_change,
                  'producttype_allocation': producttype_allocation,
                  'producttype_va_ratio': producttype_va_change,
                  'future_weight_and_va_ratio': future_weight_and_va_ratio}

    else:
        result = pd.DataFrame()

    return result


# %%
def get_top10_future(request_id, type, fund_zscode, start_date, end_date):
    # future_portfolio = get_fund_future_portfolio(fund_zscode, start_date, end_date)
    future_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='future')
    future_portfolio.loc[future_portfolio['SecuCode'].str.startswith('0'), 'SecuCode'] = future_portfolio[
                                                                                             'SecuCode'].str[1:]
    future_portfolio['SecuCode'] = future_portfolio['SecuCode'].str.upper()
    contractcodes = future_portfolio['SecuCode'].unique().tolist()
    if len(contractcodes) > 0:
        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        gzb_dates = netassetvalue[['EndDate']]
        future_portfolio = pd.merge(future_portfolio, netassetvalue, on='EndDate', how='left')
        future_portfolio['va_ratio'] = future_portfolio['ValuationAppreciation'] / future_portfolio['TotalMarket']
        future_portfolio['va_ratio'] = future_portfolio['va_ratio'].fillna(0)

        future_weight = future_portfolio.pivot_table(columns='SecuCode', values='MarketInTA', index='EndDate')
        future_weight = future_weight.reset_index()
        future_weight = pd.merge(gzb_dates, future_weight, on='EndDate', how='left')

        future_weight = future_weight.set_index('EndDate')
        future_average_weight = future_weight.mean()
        future_holding_dates = future_weight.count()
        future_average_weight = future_average_weight.reset_index()
        future_average_weight = future_average_weight.rename(columns={'index': 'SecuCode', 0: 'weight'})
        future_average_weight['abs_weight'] = np.abs(future_average_weight['weight'])
        future_average_weight = future_average_weight.sort_values(by='abs_weight', ascending=False)
        future_holding_dates = future_holding_dates.reset_index()
        future_holding_dates = future_holding_dates.rename(columns={'index': 'SecuCode', 0: 'days'})
        future_average_weight = future_average_weight.drop('abs_weight', axis=1)
        future_top10_weight = future_average_weight.iloc[0:10]
        future_top10_weight = pd.merge(future_top10_weight, future_holding_dates, on='SecuCode', how='left')

        #        future_weight = future_weight.fillna(0)

        future_va_ratio = future_portfolio.pivot_table(columns='SecuCode', values='va_ratio', index='EndDate')
        future_va_ratio = future_va_ratio.reset_index()
        future_va_ratio = pd.merge(gzb_dates, future_va_ratio, on='EndDate', how='left')
        future_va_ratio = future_va_ratio.sort_values(by='EndDate')

        future_va_ratio = future_va_ratio.set_index('EndDate')
        future_average_va = future_va_ratio.mean()
        future_average_va = future_average_va.reset_index()
        future_average_va = future_average_va.rename(columns={'index': 'SecuCode', 0: 'va_ratio'})
        future_positive_va = future_average_va[future_average_va['va_ratio'] > 0]
        future_positive_va = future_positive_va.sort_values(by='va_ratio', ascending=False)
        future_top10_positive_va = future_positive_va.iloc[0: 10]
        future_top10_positive_va = pd.merge(future_top10_positive_va, future_average_weight, on='SecuCode', how='left')

        future_negative_va = future_average_va[future_average_va['va_ratio'] < 0]
        future_negative_va = future_negative_va.sort_values(by='va_ratio')
        future_top10_negative_va = future_negative_va.iloc[0: 10]
        future_top10_negative_va = pd.merge(future_top10_negative_va, future_average_weight, on='SecuCode', how='left')
        future_names = get_future_contractname(contractcodes)

        future_top10_weight = pd.merge(future_top10_weight, future_names, on='SecuCode', how='left')
        future_top10_positive_va = pd.merge(future_top10_positive_va, future_names, on='SecuCode', how='left')
        future_top10_negative_va = pd.merge(future_top10_negative_va, future_names, on='SecuCode', how='left')
        future_weight_and_va_ratio = {'future_top10_weight': future_top10_weight,
                                      'future_top10_positive_va': future_top10_positive_va,
                                      'future_top10_negative_va': future_top10_negative_va}
    else:
        future_weight_and_va_ratio = {}

    return future_weight_and_va_ratio


# %%
def get_future_type_allocation_change(request_id, type, fund_zscode, start_date, end_date):
    # future_portfolio = get_fund_future_portfolio(fund_zscode, start_date, end_date)
    future_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='future')
    future_portfolio.loc[future_portfolio['SecuCode'].str.startswith('0'), 'SecuCode'] = future_portfolio[
                                                                                             'SecuCode'].str[1:]
    future_portfolio['SecuCode'] = future_portfolio['SecuCode'].str.upper()
    contractcodes = future_portfolio['SecuCode'].unique().tolist()

    if len(contractcodes) > 0:

        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        gzb_dates = netassetvalue[['EndDate']]

        contracttypes = get_future_contracttype(contractcodes) 
    #    logger.info(len(future_portfolio[future_portfolio['EndDate']=='2018-04-10']))
    #    logger.info(contracttypes)
        future_portfolio = pd.merge(future_portfolio, contracttypes, on='SecuCode', how='left')
    #    logger.info(future_portfolio[future_portfolio['EndDate']=='2018-04-10'][['SecuCode', 'MarketInTA', 'ContractType']])
        future_portfolio['ContractType'] = future_portfolio['ContractType'].fillna('其他')
        future_portfolio = future_portfolio.drop_duplicates().reset_index(drop=True)
        contracttype_allocation = future_portfolio.groupby(['EndDate', 'ContractType'])['MarketInTA'].sum()
        contracttype_allocation = contracttype_allocation.reset_index()
        contracttype_allocation = contracttype_allocation.pivot_table(columns='ContractType', values='MarketInTA',
                                                                      index='EndDate')
        contracttype_allocation = contracttype_allocation.reset_index()
        contracttype_allocation = pd.merge(gzb_dates, contracttype_allocation, on='EndDate', how='left')
        contracttype_allocation = contracttype_allocation.fillna(0)

        productnames = get_future_productname(contractcodes)

        future_portfolio = pd.merge(future_portfolio, productnames, on='SecuCode', how='left')
        future_portfolio['ProductName'] = future_portfolio['ProductName'].fillna('其他')
        future_portfolio = future_portfolio.drop_duplicates().reset_index(drop=True)
        producttype_allocation = future_portfolio.groupby(['EndDate', 'ProductName'])['MarketInTA'].sum()
        producttype_allocation = producttype_allocation.reset_index()
        producttype_allocation = producttype_allocation.pivot_table(columns='ProductName', values='MarketInTA',
                                                                    index='EndDate')
        producttype_allocation = producttype_allocation.reset_index()
        producttype_allocation = pd.merge(gzb_dates, producttype_allocation, on='EndDate', how='left')
        producttype_allocation = producttype_allocation.fillna(0)

        future_portfolio['Type'] = np.nan

        future_portfolio.loc[future_portfolio['MarketInTA'] > 0, 'Type'] = '多头'
        future_portfolio.loc[future_portfolio['MarketInTA'] <= 0, 'Type'] = '空头'
        future_portfolio = future_portfolio.drop_duplicates().reset_index(drop=True)
#        logger.info(future_portfolio[future_portfolio['EndDate']=='2018-01-30'])
        type_allocation = future_portfolio.groupby(['EndDate', 'Type'])['MarketInTA'].sum()
        type_allocation = type_allocation.reset_index()
        type_allocation = type_allocation.pivot_table(columns='Type', values='MarketInTA', index='EndDate')
        type_allocation = type_allocation.reset_index()
        type_allocation = pd.merge(gzb_dates, type_allocation, on='EndDate', how='left')
        type_allocation = type_allocation.fillna(0)
        result = {'contracttype_allocation': contracttype_allocation,
                  'producttype_allocation': producttype_allocation,
                  'type_allocation': type_allocation}
    else:
        result = pd.DataFrame()
    return result


# %%
def get_hshares_industry3(codes, industry_standard):
    if industry_standard == 'sw':
        standard = 24
    elif industry_standard == 'zjh':
        standard = 22
    else:           # industry_standard == 'zz':
        standard = 28
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()
    n = len(codes)
    if n > 1000:
        try:
            stock_industries = pd.DataFrame(columns=['SecuCode', 'Industry3Code', 'Industry3Name'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT HKIN.SecuCode, HKIC.IndustryCode, HKIC.IndustryName
                        FROM (
                        SELECT HKSM.SecuCode, HKEI.IndustryNum
                        FROM JYDB.HK_SecuMain HKSM
                        JOIN JYDB.HK_ExgIndustry HKEI ON HKSM.CompanyCode = HKEI.CompanyCode
                        WHERE HKSM.SecuCode in ('%s')
                        AND HKEI.Standard = %s
                        AND HKEI.CancelDate is NULL
                        ) HKIN
                        JOIN JYDB.HK_IndustryCategory HKIC
                        ON HKIN.IndustryNum = HKIC.IndustryNum
                        """ % ("','".join(sub_list), standard)
                    )
                    stock_industry = cursor.fetchall()
                    if not stock_industry:
                        stock_industry = pd.DataFrame(columns=['SecuCode', 'Industry3Code', 'Industry3Name'])
                    else:
                        stock_industry = pd.DataFrame(list(stock_industry),
                                                      columns=['SecuCode', 'Industry3Code', 'Industry3Name'])
                    stock_industries = stock_industries.append(stock_industry)
            stock_industries = stock_industries.drop_duplicates(subset=['SecuCode'])
            return stock_industries
        finally:
            cursor.close()
    elif (n > 0) & (n <= 1000):
        try:
            cursor.execute(
                """
                    SELECT HKIN.SecuCode, HKIC.IndustryCode, HKIC.IndustryName
                    FROM (
                    SELECT HKSM.SecuCode, HKEI.IndustryNum
                    FROM JYDB.HK_SecuMain HKSM
                    JOIN JYDB.HK_ExgIndustry HKEI ON HKSM.CompanyCode = HKEI.CompanyCode
                    WHERE HKSM.SecuCode in ('%s')
                    AND HKEI.Standard = %s
                    AND HKEI.CancelDate is NULL
                    ) HKIN
                    JOIN JYDB.HK_IndustryCategory HKIC
                    ON HKIN.IndustryNum = HKIC.IndustryNum
                """ % ("','".join(codes), standard)
            )
            stock_industry = cursor.fetchall()
            if not stock_industry:
                return pd.DataFrame(columns=['SecuCode', 'Industry3Code', 'Industry3Name'])
            stock_industries = pd.DataFrame(list(stock_industry),
                                            columns=['SecuCode', 'Industry3Code', 'Industry3Name'])
            stock_industries = stock_industries.drop_duplicates(subset=['SecuCode'])
            return stock_industries
        finally:
            cursor.close()
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'Industry3Code', 'Industry3Name'])


def get_industry_name(IndustryCodes, standard):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()
    try:
        cursor.execute(
            """
                SELECT IndustryCode, IndustryName
                from JYDB.HK_IndustryCategory
                where IndustryCode in ('%s')
                and Standard = %s
            """ % ("','".join(str(code) for code in IndustryCodes), standard)
        )
        industry_name = cursor.fetchall()
        if not industry_name:
            return pd.DataFrame(columns=['IndustryCode', 'IndustryName'])
        industry_name = pd.DataFrame(list(industry_name), columns=['IndustryCode', 'IndustryName'])
        return industry_name
    finally:
        cursor.close()


def get_hshares_industry_old(codes, industry_standard):
    hshares_industry3 = get_hshares_industry3(codes, industry_standard)
    if industry_standard == 'sw':
        standard = 24
        hshares_industry3['IndustryCode'] = hshares_industry3['Industry3Code'].apply(lambda x: x[:3] + '000')
    else:          # industry_standard == 'zjh':
        standard = 22
        hshares_industry3['IndustryCode'] = hshares_industry3['Industry3Code'].apply(lambda x: x[:1])
    # elif industry_standard == 'zz':
    #     stock_industry = hshares_industry3[['SecuCode', 'IndustryName']]
    #     return stock_industry
    IndustryCodes = hshares_industry3['IndustryCode'].unique()
    Industries = get_industry_name(IndustryCodes, standard)
    stock_industry = hshares_industry3.merge(Industries, on='IndustryCode', how='left')
    stock_industry = stock_industry[['SecuCode', 'IndustryName']]
    stock_industry = stock_industry.rename(columns={'IndustryName': 'Industry'})
    return stock_industry


def get_hshares_industry(codes, industry_standard):
    hshares_industry3 = get_hshares_industry3(codes, industry_standard)
    if industry_standard == 'sw':
        hshares_industry3['IndustryCode'] = hshares_industry3['Industry3Code'].apply(lambda x: x[:3] + '000')
        stock_industry = hshares_industry3[hshares_industry3['Industry3Code'] == hshares_industry3['IndustryCode']]
        stock_industry = stock_industry.rename(columns={'Industry3Name': 'Industry'})[
            ['SecuCode', 'Industry']].reset_index(drop=True)
    else:          # industry_standard == 'zjh'
        standard = 22
        hshares_industry3['IndustryCode'] = hshares_industry3['Industry3Code'].apply(lambda x: x[:1])
        IndustryCodes = hshares_industry3['IndustryCode'].unique()
        Industries = get_industry_name(IndustryCodes, standard)
        stock_industry = hshares_industry3.merge(Industries, on='IndustryCode', how='left')
        stock_industry = stock_industry[['SecuCode', 'IndustryName']]
        stock_industry = stock_industry.rename(columns={'IndustryName': 'Industry'})
    return stock_industry



#%%
def get_kcshares_industry(codes, industry_standard):
    if industry_standard == 'sw':
        standard = 24
    elif industry_standard == 'zjh':
        standard = 22
    else:           # industry_standard == 'zz':
        standard = 28
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()
    n = len(codes)
    if n > 1000:
        try:
            stock_industries = pd.DataFrame(columns=['SecuCode', 'Industry'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SM.SecuCode, LCSE.FirstIndustryName
                        FROM JYDB.SecuMain SM
                        JOIN JYDB.LC_STIBExgIndustry LCSE
                        ON SM.CompanyCode = LCSE.CompanyCode
                        WHERE SM.SecuCode in ('%s')
                        AND SM.ListedSector = 7
                        AND LCSE.Standard = %s
                        """ % ("','".join(sub_list), standard)
                    )
                    stock_industry = cursor.fetchall()
                    if not stock_industry:
                        stock_industry = pd.DataFrame(columns=['SecuCode', 'Industry'])
                    else:
                        stock_industry = pd.DataFrame(list(stock_industry),
                                                      columns=['SecuCode', 'Industry'])
                    stock_industries = stock_industries.append(stock_industry)
            stock_industries = stock_industries.drop_duplicates(subset=['SecuCode'])
            return stock_industries
        finally:
            cursor.close()
    elif (n > 0) & (n <= 1000):
        try:
            cursor.execute(
                """
                SELECT SM.SecuCode, LCSE.FirstIndustryName
                FROM JYDB.SecuMain SM
                JOIN JYDB.LC_STIBExgIndustry LCSE
                ON SM.CompanyCode = LCSE.CompanyCode
                WHERE SM.SecuCode in ('%s')
                AND SM.ListedSector = 7
                AND LCSE.Standard = %s
                """ % ("','".join(codes), standard)
            )
            stock_industry = cursor.fetchall()
            if not stock_industry:
                return pd.DataFrame(columns=['SecuCode', 'Industry'])
            stock_industries = pd.DataFrame(list(stock_industry),
                                            columns=['SecuCode', 'Industry'])
#            logger.info(stock_industries)
            stock_industries = stock_industries.drop_duplicates(subset=['SecuCode'])
            return stock_industries
        finally:
            cursor.close()
    elif n == 0:
        return pd.DataFrame(columns=['SecuCode', 'Industry'])


# %%
def get_concentration_change(request_id, type, fund_zscode, start_date, end_date, show):
    netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
    gzb_dates = netassetvalue[['EndDate']]

    # stock_portfolio = get_fund_stock_portfolio(fund_zscode, start_date, end_date)
    stock_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='stock')
    if len(stock_portfolio) > 0:
        stock_portfolio = stock_portfolio.sort_values(by=['EndDate', 'MarketInTA'], ascending=[True, False])
        stock_top3_weight = stock_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 3].sum())
        stock_top3_weight = stock_top3_weight.reset_index()
        stock_top3_weight = stock_top3_weight.rename(columns={'MarketInTA': 'top3'})
        stock_concentration = pd.merge(gzb_dates, stock_top3_weight, on='EndDate', how='left')
        #        stock_top3_weight = stock_top3_weight.fillna(0)
        stock_top5_weight = stock_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 5].sum())
        stock_top5_weight = stock_top5_weight.reset_index()
        stock_top5_weight = stock_top5_weight.rename(columns={'MarketInTA': 'top5'})
        stock_concentration = pd.merge(stock_concentration, stock_top5_weight, on='EndDate', how='left')
        #        stock_top5_weight = stock_top5_weight.fillna(0)
        #        stock_top10_weight = stock_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 10].sum())
        #        stock_top10_weight = stock_top10_weight.reset_index()
        #        stock_top10_weight = pd.merge(gzb_dates, stock_top10_weight, on = 'EndDate', how = 'left')
        #        stock_top10_weight = stock_top10_weight.fillna(0)
        stock_weight_all = stock_portfolio.groupby('EndDate')['MarketInTA'].sum()
        stock_weight_all = stock_weight_all.reset_index()
        stock_weight_all = stock_weight_all.rename(columns={'MarketInTA': 'all'})
        stock_concentration = pd.merge(stock_concentration, stock_weight_all, on='EndDate', how='left')
        stock_concentration = stock_concentration.fillna(0)

        stocks_all = stock_portfolio.SecuCode.unique().tolist()

        hshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(['81', '82', '83'])][
            'SecuCode'].unique().tolist()
        kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
        kcshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(kcshares_index_list)][
            'SecuCode'].unique().tolist()
        hshares_industry = get_hshares_industry(hshares, industry_standard='sw')
        kcshares_industry = get_kcshares_industry(kcshares, industry_standard='sw')

        ashares = np.setdiff1d(stocks_all, hshares)
        ashares_industry = get_stock_industry(ashares, industry_standard='sw')
        stock_industries = ashares_industry.append(hshares_industry)
        stock_industries = ashares_industry.append(kcshares_industry)

        stock_portfolio_industry = pd.merge(stock_portfolio, stock_industries, on='SecuCode', how='left')
        stock_portfolio_industry['Industry'] = stock_portfolio_industry['Industry'].fillna('其他')
        industry_weight = stock_portfolio_industry.groupby(['EndDate', 'Industry'])['MarketInTA'].sum()
        industry_weight = industry_weight.reset_index()
        industry_weight = industry_weight.sort_values(by=['EndDate', 'MarketInTA'], ascending=[True, False])

        industry_top3_weight = industry_weight.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 3].sum())
        industry_top3_weight = industry_top3_weight.reset_index()
        industry_top3_weight = industry_top3_weight.rename(columns={'MarketInTA': 'top3'})
        industry_concentration = pd.merge(gzb_dates, industry_top3_weight, on='EndDate', how='left')
        #        industry_top3_weight = industry_top3_weight.fillna(0)
        industry_top5_weight = industry_weight.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 5].sum())
        industry_top5_weight = industry_top5_weight.reset_index()
        industry_top5_weight = industry_top5_weight.rename(columns={'MarketInTA': 'top5'})
        industry_concentration = pd.merge(industry_concentration, industry_top5_weight, on='EndDate', how='left')
        #        industry_top5_weight = industry_top5_weight.fillna(0)
        #        industry_top10_weight = industry_weight.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 10].sum())
        #        industry_top10_weight = industry_top10_weight.reset_index()
        #        industry_top10_weight = pd.merge(gzb_dates, industry_top10_weight, on = 'EndDate', how = 'left')
        #        industry_top10_weight = industry_top10_weight.fillna(0)
        industry_weight_all = industry_weight.groupby('EndDate')['MarketInTA'].sum()
        industry_weight_all = industry_weight_all.reset_index()
        industry_weight_all = industry_weight_all.rename(columns={'MarketInTA': 'all'})
        industry_concentration = pd.merge(industry_concentration, industry_weight_all, on='EndDate', how='left')
        industry_concentration = industry_concentration.fillna(0)

        stock_num_change = stock_portfolio.groupby('EndDate')['SecuCode'].count()
        stock_num_change = stock_num_change.reset_index()
        stock_num_change = pd.merge(gzb_dates, stock_num_change, on='EndDate', how='left')
        stock_num_change = stock_num_change.fillna(0)
        stock_num_change = stock_num_change.rename(columns={'SecuCode': 'security_number'})

        stock_mean_weight = stock_portfolio.groupby('SecuCode')['MarketInTA'].mean()
        stock_mean_weight = stock_mean_weight.reset_index()
        large_weight_stocks = stock_mean_weight[stock_mean_weight['MarketInTA'] > 0.05]
        large_weight_stocks_codes = large_weight_stocks.SecuCode.tolist()
        large_weight_stocks_info = stock_portfolio[stock_portfolio['SecuCode'].isin(large_weight_stocks_codes)][
            ['EndDate', 'SecuCode', 'MarketPrice', 'MarketInTA', 'SecondClassCode']]
        large_weight_stocks_info = large_weight_stocks_info.sort_values(by=['SecuCode', 'EndDate'])
        # logger.info(large_weight_stocks_info[large_weight_stocks_info['SecuCode'] == '002916'])
        large_weight_ashares = \
            large_weight_stocks_info[~large_weight_stocks_info['SecondClassCode'].isin(['74', '81', '82', '83'])][
                'SecuCode'].unique().tolist()
        large_weight_gzshares = large_weight_stocks_info[large_weight_stocks_info['SecondClassCode'] == '74'][
            'SecuCode'].unique().tolist()
        large_weight_hshares = \
            large_weight_stocks_info[large_weight_stocks_info['SecondClassCode'].isin(['81', '82', '83'])][
                'SecuCode'].unique().tolist()
        kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
        large_weight_kcshares = \
            large_weight_stocks_info[large_weight_stocks_info['SecondClassCode'].isin(kcshares_index_list)][
                'SecuCode'].unique().tolist()

        hshares_names = get_hstock_chinames(large_weight_hshares)
        gzshares_names = get_gzstock_chinames(large_weight_gzshares)
        ashares_names = get_astock_chinames(large_weight_ashares)
        kcshares_names = get_kcstock_chinames(large_weight_kcshares)
        kcshares_names = kcshares_names.drop_duplicates(subset=['SecuCode'])

        stock_names = ashares_names.append(hshares_names)
        stock_names = stock_names.append(gzshares_names)
        stock_names = stock_names.append(kcshares_names)
        stock_names = stock_names.reset_index(level=0, drop=True)

        large_stocks_weight = large_weight_stocks_info.groupby(['EndDate', 'SecuCode'])['MarketInTA'].sum().reset_index()
        large_stocks_price = large_weight_stocks_info[['EndDate', 'SecuCode', 'MarketPrice']].drop_duplicates(['EndDate', 'SecuCode'], keep='first')
        if not large_stocks_weight.empty:
            large_weight_stocks_info = large_stocks_weight.merge(large_stocks_price, on=['EndDate', 'SecuCode'], how='left')
            large_weight_stocks_info = large_weight_stocks_info.sort_values(by=['SecuCode', 'EndDate'])
            large_weight_stock_performance = {'stock_names': stock_names,
                                              'large_weight_stocks_info': large_weight_stocks_info[
                                                  ['EndDate', 'SecuCode', 'MarketPrice', 'MarketInTA']]}
        else:
            large_weight_stock_performance = pd.DataFrame()


    else:
        stock_concentration = pd.DataFrame()
        industry_concentration = pd.DataFrame()
        stock_num_change = pd.DataFrame()
        large_weight_stock_performance = pd.DataFrame()

    # bond_portfolio = get_fund_bond_portfolio(fund_zscode, start_date, end_date)
    bond_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='bond')
    if len(bond_portfolio) > 0:
        bond_portfolio = bond_portfolio.sort_values(by=['EndDate', 'MarketInTA'], ascending=[True, False])

        bond_top3_weight = bond_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 3].sum())
        bond_top3_weight = bond_top3_weight.reset_index()
        bond_top3_weight = bond_top3_weight.rename(columns={'MarketInTA': 'top3'})
        bond_concentration = pd.merge(gzb_dates, bond_top3_weight, on='EndDate', how='left')
        #        bond_top3_weight = bond_top3_weight.fillna(0)
        bond_top5_weight = bond_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 5].sum())
        bond_top5_weight = bond_top5_weight.reset_index()
        bond_top5_weight = bond_top5_weight.rename(columns={'MarketInTA': 'top5'})
        bond_concentration = pd.merge(bond_concentration, bond_top5_weight, on='EndDate', how='left')
        #        bond_top5_weight = bond_top5_weight.fillna(0)
        bond_weight_all = bond_portfolio.groupby('EndDate')['MarketInTA'].sum()
        bond_weight_all = bond_weight_all.reset_index()
        bond_weight_all = bond_weight_all.rename(columns={'MarketInTA': 'all'})
        bond_concentration = pd.merge(bond_concentration, bond_weight_all, on='EndDate', how='left')
        bond_concentration = bond_concentration.fillna(0)

        bond_num_change = bond_portfolio.groupby('EndDate')['SecuCode'].count()
        bond_num_change = bond_num_change.reset_index()
        bond_num_change = pd.merge(gzb_dates, bond_num_change, on='EndDate', how='left')
        bond_num_change = bond_num_change.fillna(0)
        bond_num_change = bond_num_change.rename(columns={'SecuCode': 'security_number'})

    else:
        #        bond_top3_weight = pd.DataFrame()
        #        bond_top5_weight = pd.DataFrame()
        #        bond_weight_all = pd.DataFrame()
        bond_concentration = pd.DataFrame()
        bond_num_change = pd.DataFrame()

    # derivative_portfolio = get_fund_derivative_portfolio(fund_zscode, start_date, end_date)
    derivative_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='derivative')
    future_portfolio = derivative_portfolio[
        derivative_portfolio['SecondClassCode'].isin(['01', '02', '03', '04', '05', '06', '07', '08', '31', '32'])]
    if len(future_portfolio) > 0:
        future_portfolio['abs_weight'] = np.abs(future_portfolio['MarketInTA'])
        future_portfolio = future_portfolio.sort_values(by=['EndDate', 'abs_weight'], ascending=[True, False])
        future_top3_weight = future_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 3].sum())
        future_top3_weight = future_top3_weight.reset_index()
        future_top3_weight = future_top3_weight.rename(columns={'MarketInTA': 'top3'})
        future_concentration = pd.merge(gzb_dates, future_top3_weight, on='EndDate', how='left')
        #        future_top3_weight = future_top3_weight.fillna(0)

        future_top5_weight = future_portfolio.groupby('EndDate')['MarketInTA'].apply(lambda x: x.iloc[0: 5].sum())
        future_top5_weight = future_top5_weight.reset_index()
        future_top5_weight = future_top5_weight.rename(columns={'MarketInTA': 'top5'})
        future_concentration = pd.merge(future_concentration, future_top5_weight, on='EndDate', how='left')
        #        future_top5_weight = future_top5_weight.fillna(0)

        future_weight_all = future_portfolio.groupby('EndDate')['MarketInTA'].sum()
        future_weight_all = future_weight_all.reset_index()
        future_weight_all = future_weight_all.rename(columns={'MarketInTA': 'all'})
        future_concentration = pd.merge(future_concentration, future_weight_all, on='EndDate', how='left')
        future_concentration = future_concentration.fillna(0)
    else:
        #        future_top3_weight = pd.DataFrame()
        #        future_top5_weight = pd.DataFrame()
        #        future_weight_all = pd.DataFrame()
        future_concentration = pd.DataFrame()

    if show == 0:
        result = {
        'stock_concentration': stock_concentration,
        'industry_concentration': industry_concentration,
        'stock_num_change': stock_num_change,
        'bond_concentration': bond_concentration,
        'bond_num_change': bond_num_change,
        'future_concentration': future_concentration}
    else:
        result = {
        #              'stock_top3_weight': stock_top3_weight,
        #              'stock_top5_weight': stock_top5_weight,
        #              'stock_top10_weight': stock_top10_weight,
        #              'stock_weight_all': stock_weight_all,
        'stock_concentration': stock_concentration,
        #              'industry_top3_weight': industry_top3_weight,
        #              'industry_top5_weight': industry_top5_weight,
        #              'industry_weight_all': industry_weight_all,
        'industry_concentration': industry_concentration,
        'stock_num_change': stock_num_change,
        'large_weight_stock_performance': large_weight_stock_performance,
        #              'bond_top3_weight': bond_top3_weight,
        #              'bond_top5_weight': bond_top5_weight,
        #              'bond_weight_all': bond_weight_all,
        'bond_concentration': bond_concentration,
        'bond_num_change': bond_num_change,
        #              'future_top3_weight': future_top3_weight,
        #              'future_top5_weight': future_top5_weight,
        #              'future_weight_all': future_weight_all
        'future_concentration': future_concentration
    }

    return result


def complete_index(industry_allocation, index_code, start_date, end_date):
    query = """select tradingday, closeprice / (
    select closeprice from ZJ_Index_Daily_Quote where secucode = '{index}' and tradingday = (
    select min(tradingday) from ZJ_Index_Daily_Quote where tradingday >= to_date('{sd}', 'yyyy-mm-dd'))) index_ret
    from ZJ_Index_Daily_Quote where InnerCode = (select InnerCode
    from JYDB.SecuMain where SecuCode = '{index}' and SecuCategory = 4 and SecuMarket in (83, 90))
    and TradingDay >= to_date('{sd}', 'yyyy-mm-dd') and TradingDay <= to_date('{ed}', 'yyyy-mm-dd')
    order by TradingDay asc""".format(index=index_code, sd=start_date, ed=end_date)
    conn = 'zdj'
    merge_on = ['EndDate']
    merge_how = 'outer'
    query_config = [{'query': query, 'conn': conn, 'merge_on': merge_on, 'merge_how': merge_how, 'rename': {'TRADINGDAY': 'EndDate', 'INDEX_RET': 'ret'}}]
    industry_allocation = conf.complete_df(industry_allocation, query=query_config).sort_values('EndDate').reset_index(drop=True)
    industry_allocation['ret'] = industry_allocation['ret'].ffill()
    industry_allocation = industry_allocation.fillna(0)
    return industry_allocation


def get_stock_industry_allocation_change(request_id, type, fund_zscode, index_code, start_date, end_date, industry_standard):
    # stock_portfolio = get_fund_stock_portfolio(fund_zscode, start_date, end_date)
    stock_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='stock')
    kcshares_index_list = ['C'+str(i) for i in range(1, 10)]

    stock_codes = stock_portfolio.SecuCode.unique().tolist()
    if len(stock_codes) > 0:

        netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
        netassetvalue = netassetvalue.rename(columns={'TotalMarket': 'NAV'})
        gzb_dates = netassetvalue[['EndDate']]

        hshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(['81', '82', '83'])][
            'SecuCode'].unique().tolist()
        gzshares = stock_portfolio[stock_portfolio['SecondClassCode'] == '74']['SecuCode'].unique().tolist()
        ashares = stock_portfolio[~stock_portfolio['SecondClassCode'].isin(['74', '81', '82', '83']+kcshares_index_list)][
            'SecuCode'].unique().tolist()
        kcshares = stock_portfolio[stock_portfolio['SecondClassCode'].isin(kcshares_index_list)]['SecuCode'].unique().tolist()
#        logger.info(kcshares)
        if (industry_standard == 'zjh') | (industry_standard == 'sw'):
            stock_industry = get_stock_industry(ashares, industry_standard)
#            logger.info(stock_industry['Industry'].unique())
            hstock_industry = get_hshares_industry(hshares, industry_standard)
#            logger.info(hstock_industry['Industry'].unique())
            stock_industry = stock_industry.append(hstock_industry)
            kcstock_industry = get_kcshares_industry(kcshares, industry_standard)
            stock_industry = stock_industry.append(kcstock_industry)
#            logger.info(kcstock_industry['Industry'].unique())

            stock_portfolio = pd.merge(stock_portfolio, stock_industry, on=['SecuCode'], how='left')
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(gzshares)) & (
                stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-新三板'
            # stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(hshares)) & (stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-港股'
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(kcshares)) & (
                stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-科创板'
            stock_portfolio['Industry'] = stock_portfolio['Industry'].fillna('其他')

            # logger.info('haha')
        else:
            # logger.info('wawa')
            stock_industry = get_stock_industry(ashares, industry_standard)
            stock_portfolio = pd.merge(stock_portfolio, stock_industry, on=['SecuCode'], how='left')
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(gzshares)) & (
                stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-新三板'
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(hshares)) & (
                stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-港股'
            stock_portfolio.loc[(stock_portfolio['SecuCode'].isin(kcshares)) & (
                stock_portfolio['Industry'].isnull()), 'Industry'] = '其他-科创板'
            stock_portfolio['Industry'] = stock_portfolio['Industry'].fillna('其他')
        industry_allocation = stock_portfolio.groupby(['EndDate', 'Industry'])['MarketInTA'].sum()

        industry_allocation = industry_allocation.reset_index()
        industry_allocation = industry_allocation.rename(columns={'MarketInTA': 'weight'})
        industry_allocation = industry_allocation.pivot_table(columns='Industry', values='weight', index='EndDate')
        column_names = industry_allocation.columns
        others = list(column_names[column_names.str.startswith('其他')])
        column_names = np.array(column_names)
        if len(others) > 0:
            diff = list(column_names[~np.isin(column_names, others)])
            industry_allocation = industry_allocation[diff + others]
        industry_allocation = industry_allocation.reset_index()
        industry_allocation = pd.merge(gzb_dates, industry_allocation, on='EndDate', how='left')
        industry_allocation = industry_allocation.fillna(0)
    else:
        industry_allocation = pd.DataFrame()
    if not industry_allocation.empty:
        industry_allocation = complete_index(industry_allocation, index_code, start_date, end_date)

    return {'industry_allocation':industry_allocation,
            'stock_portfolio': [dict(stock_portfolio.iloc[i]) for i in range(len(stock_portfolio))]}


def get_stock_sector(codes):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    n = len(codes)

    if n > 1000:
        try:
            stock_sectors = pd.DataFrame(columns=['SecuCode', 'ListedSector'])
            k = int(n / 1000)
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                        SELECT SM.SecuCode, SC.MS ListedSector
                        FROM JYDB.SecuMain SM
                        JOIN JYDB.CT_SystemConst SC
                        ON SM.ListedSector = SC.DM
                        WHERE SC.LB = 207
                        AND SM.SecuCode in %s
                        AND SM.SecuCategory = 1
                        """ % (str(tuple(sub_list)).replace(',)', ')'))
                    )
                    stock_sector = cursor.fetchall()
                    if not stock_sector:
                        stock_sector = pd.DataFrame(columns=['SecuCode', 'ListedSector'])
                    else:
                        stock_sector = pd.DataFrame(list(stock_sector), columns=['SecuCode', 'ListedSector'])

                    stock_sectors = stock_sectors.append(stock_sector)

            stock_sectors = stock_sectors.sort_values('ListedSector').drop_duplicates(subset='SecuCode', keep='last')
            return stock_sectors

        finally:
            cursor.close()

    elif n > 0 and n <= 1000:
        try:

            cursor.execute(
                """
                SELECT SM.SecuCode, SC.MS ListedSector
                FROM JYDB.SecuMain SM
                JOIN JYDB.CT_SystemConst SC
                ON SM.ListedSector = SC.DM
                WHERE SC.LB = 207
                AND SM.SecuCode in %s
                AND SM.SecuCategory = 1
                """ % (str(tuple(codes)).replace(',)', ')'))
            )
            stock_sector = cursor.fetchall()

            if not stock_sector:
                return pd.DataFrame(columns=['SecuCode', 'ListedSector'])

            stock_sectors = pd.DataFrame(list(stock_sector), columns=['SecuCode', 'ListedSector'])
            stock_sectors = stock_sectors.sort_values('ListedSector').drop_duplicates(subset='SecuCode', keep='last')
            return stock_sectors

        finally:
            cursor.close()

    elif n == 0:

        return pd.DataFrame(columns=['SecuCode', 'ListedSector'])


def get_stock_sector_allocation_change(request_id, type, fund_zscode, index_code, start_date, end_date):
    netassetvalue = get_net_asset_value(fund_zscode, start_date, end_date)
    gzb_dates = netassetvalue[['EndDate']]

    # stock_portfolio = get_fund_stock_portfolio(fund_zscode, start_date, end_date)
    stock_portfolio = get_fund_asset_portfolio(fund_zscode, start_date, end_date, asset_type='stock')
    stock_codes = stock_portfolio.SecuCode.unique().tolist()
    if len(stock_codes) > 0:
        stock_sectors = get_stock_sector(stock_codes)

        stock_portfolio = pd.merge(stock_portfolio, stock_sectors, on='SecuCode', how='left')
        stock_portfolio.loc[stock_portfolio['SecondClassCode'].isin(['81', '82', '83']), 'ListedSector'] = '港股'

        stock_portfolio['ListedSector'] = stock_portfolio['ListedSector'].fillna('其他')
        sector_allocation = stock_portfolio.groupby(['EndDate', 'ListedSector'])['MarketInTA'].sum()
        sector_allocation = sector_allocation.reset_index()

        sector_allocation = sector_allocation.pivot_table(columns='ListedSector', values='MarketInTA', index='EndDate')
        sector_allocation = sector_allocation.reset_index()
        sector_allocation = pd.merge(gzb_dates, sector_allocation, on='EndDate', how='left')
        sector_allocation = sector_allocation.fillna(0)

    else:
        sector_allocation = pd.DataFrame()
    if not sector_allocation.empty:
        query = """select tradingday, closeprice / (
        select closeprice from ZJ_Index_Daily_Quote where secucode = '{index}' and tradingday = (
        select min(tradingday) from ZJ_Index_Daily_Quote where secucode = '{index}' and tradingday >= to_date('{sd}', 'yyyy-mm-dd'))
        ) - 1 index_ret
        from ZJ_Index_Daily_Quote where InnerCode = (select InnerCode
        from JYDB.SecuMain where SecuCode = '{index}' and SecuCategory = 4 and SecuMarket in (83, 90))
        and TradingDay >= to_date('{sd}', 'yyyy-mm-dd') and TradingDay <= to_date('{ed}', 'yyyy-mm-dd')
        order by TradingDay asc""".format(index=index_code, sd=start_date, ed=end_date)
        conn = 'zdj'
        merge_on = ['EndDate']
        merge_how = 'outer'
        query_config = [{'query': query, 'conn': conn, 'merge_on': merge_on, 'merge_how': merge_how, 'rename': {'TRADINGDAY': 'EndDate', 'INDEX_RET': 'ret'}}]
        sector_allocation = conf.complete_df(sector_allocation, query=query_config).sort_values('EndDate').reset_index(drop=True)
        sector_allocation['ret'] = sector_allocation['ret'].ffill()
        sector_allocation = sector_allocation.fillna(0)

    return sector_allocation


def get_factor_returns(start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
        SELECT TradingDay, country, beta, momentum, market_size, earnings_yield, residual_vol, growth, book_to_price, leverage,
        liquidity, non_linear_size, ind_0, ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, ind_7, ind_8, ind_9, ind_10, ind_11,
        ind_12, ind_13, ind_14, ind_15, ind_16, ind_17, ind_18, ind_19, ind_20, ind_21, ind_22, ind_23, ind_24, ind_25,
        ind_26, ind_27
        FROM FM_WLS_BETA
        WHERE TradingDay > to_date('%s', 'yyyy-mm-dd')
        AND TradingDay <= to_date('%s', 'yyyy-mm-dd')
        """ % (start_date, end_date)
                       )
        result_set = cursor.fetchall()
        if not result_set:
            return None

        result_set = pd.DataFrame(list(result_set),
                                  columns=['TradingDay', 'country', 'beta', 'momentum', 'size', 'earnings_yield',
                                           'residual_vol',
                                           'growth', 'book_to_price', 'leverage', 'liquidity', 'non_linear_size',
                                           'ind_0', 'ind_1', 'ind_2', 'ind_3', 'ind_4',
                                           'ind_5', 'ind_6', 'ind_7', 'ind_8', 'ind_9', 'ind_10', 'ind_11', 'ind_12',
                                           'ind_13', 'ind_14', 'ind_15', 'ind_16',
                                           'ind_17', 'ind_18', 'ind_19', 'ind_20', 'ind_21', 'ind_22', 'ind_23',
                                           'ind_24', 'ind_25', 'ind_26', 'ind_27'])

        result_set = result_set.sort_values(by='TradingDay')

        return result_set

    finally:

        cursor.close()


def get_last_factor_model_date():
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = db_connection.cursor()

    try:
        cursor.execute(
            """
            SELECT max(TradingDay)
            FROM stock_risk_forecast
            """
        )

        result_set = cursor.fetchall()
        if not result_set:
            return None

        last_factor_model_date = pd.DataFrame(list(result_set), columns=['date'])
        last_factor_model_date = last_factor_model_date['date'].iloc[0]

        return last_factor_model_date

    finally:

        cursor.close()


def get_factor_cumulative_return(request_id, type, start_date, end_date):
    style_list = ['beta', 'momentum', 'size', 'earnings_yield', 'residual_vol', 'growth', 'book_to_price', 'leverage',
                  'liquidity', 'non_linear_size']
    style_chinames = ['贝塔', '动量', '规模', '盈利', '波动', '成长', '价值', '杠杆', '流动性', '中盘']

    last_factor_model_date = get_last_factor_model_date()

    if pd.to_datetime(start_date) < last_factor_model_date:

        factor_returns = get_factor_returns(start_date, end_date)
        factor_return_first_date = pd.DataFrame({'TradingDay': [pd.to_datetime(start_date)]})
        factor_returns = factor_returns.append(factor_return_first_date)
        factor_returns = factor_returns.fillna(0)
        factor_returns = factor_returns.sort_values(by='TradingDay')
        factor_returns = factor_returns.set_index('TradingDay')

        factor_returns = np.exp(factor_returns.cumsum()) - 1

        factor_returns = factor_returns[style_list]
        factor_returns.columns = style_chinames
        factor_returns = factor_returns.reset_index()

    else:
        factor_returns = pd.DataFrame()

    return factor_returns


def get_stock_interval_returns(stock_returns, interval_start, interval_end, interval):
    codes = interval['SecuCode'].tolist()

    stock_returns_by_interval = stock_returns[(stock_returns.SecuCode.isin(codes)) &
                                              (stock_returns['tradingDay'] >= interval_start) &
                                              (stock_returns['tradingDay'] <= interval_end)]
    #    stock_returns = await get_interval_stock_returns(codes, interval_start, interval_end)
    stock_returns_by_interval = pd.merge(stock_returns_by_interval, interval[['SecuCode', 'weight']], on='SecuCode',
                                         how='left')

    return stock_returns_by_interval


def get_tradingdays(start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT TradingDate
            FROM JYDB.QT_TradingDayNew
            WHERE IfTradingDay = 1
            AND SecuMarket = 83
            AND TradingDate >= to_date('%s', 'YYYY-MM-DD HH24:MI:SS')
            AND TradingDate <= to_date('%s', 'YYYY-MM-DD HH24:MI:SS')
            """ % (start_date, end_date))
        tradingdays = cursor.fetchall()
        if not tradingdays:
            return pd.DataFrame(columns=['EndDate'])
        tradingdays = pd.DataFrame(list(tradingdays), columns=['EndDate'])
        tradingdays = tradingdays.sort_values(by='EndDate')
    return tradingdays


#%%
def get_stock_returns_year(codes, year):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    n = len(codes)
    if n > 1000:
        stock_returns = pd.DataFrame(columns=['TradingDay', 'SecuCode', 'log_return'])
        k = int(n / 1000)
        with db_connection.cursor() as cursor:
            for i in range(0, k + 1):
                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]
                if len(sub_list) > 0:
                    cursor.execute(
                        """
                    SELECT TradingDay, code, log_return
                    FROM stock_daily_quote
                    WHERE code in %s
                    AND TradingDay >= to_date('%s-01-01', 'YYYY-MM-DD HH24:MI:SS')
                    AND TradingDay <= to_date('%s-12-31', 'YYYY-MM-DD HH24:MI:SS')
                    """ % (str(tuple(sub_list)).replace(',)', ')'), year, year))
                    stock_return = cursor.fetchall()
                    if not stock_return:
                        stock_return = pd.DataFrame(columns=['TradingDay', 'SecuCode', 'log_return'])
                    else:
                        stock_return = pd.DataFrame(list(stock_return),
                                                    columns=['TradingDay', 'SecuCode', 'log_return'])
                    stock_returns = stock_returns.append(stock_return)
            stock_returns = stock_returns.sort_values(by=['TradingDay', 'SecuCode'])
            return stock_returns
    elif (n > 0) & (n <= 1000):
        with db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT TradingDay, code, log_return
                FROM stock_daily_quote
                WHERE code in %s
                AND TradingDay >= to_date('%s-01-01', 'YYYY-MM-DD HH24:MI:SS')
                AND TradingDay <= to_date('%s-12-31', 'YYYY-MM-DD HH24:MI:SS')
                """ % (str(tuple(codes)).replace(',)', ')'), year, year)
            )
            stock_return = cursor.fetchall()
            if not stock_return:
                return pd.DataFrame(columns=['TradingDay', 'SecuCode', 'log_return'])
            stock_returns = pd.DataFrame(list(stock_return), columns=['TradingDay', 'SecuCode', 'log_return'])
            stock_returns = stock_returns.sort_values(by=['TradingDay', 'SecuCode'])
            return stock_returns
    else:
        return pd.DataFrame(columns=['TradingDay', 'SecuCode', 'log_return'])


def portfolio_whole_return_method2(stock_portfolio):
    start_date = stock_portfolio['EndDate'].min()
    end_date = stock_portfolio['EndDate'].max()
#    logger.info(start_date)
#    logger.info(end_date)
    tradingdays = get_tradingdays(start_date, end_date)
    holdings = stock_portfolio[['EndDate', 'SecuCode', 'MarketInTA']]
    holdings = pd.merge(holdings, tradingdays, on='EndDate', how='inner')
    if len(holdings) > 0:
        holdings = holdings.sort_values(by=['EndDate', 'SecuCode'])
        return_all = []
        holdings['year'] = holdings['EndDate'].apply(lambda x: x.year)
        for year in list(holdings['year'].unique()):
#            logger.info(year)
            holdings_year = holdings[holdings['year'] == year]
            secucodes_year = holdings_year['SecuCode'].unique()
            return_year = get_stock_returns_year(secucodes_year, year)
            return_all.append(return_year)
        stock_returns_all = pd.concat(return_all)
        stock_returns_all = stock_returns_all.rename(columns={'TradingDay': 'EndDate'})
        holdings = pd.merge(holdings, stock_returns_all, on=['EndDate', 'SecuCode'], how='left')
        holdings['log_return'] = holdings['log_return'].fillna(0)
    else:
        holdings = pd.DataFrame()
    return holdings


# %%
def get_fund_astock_portfolio(fund_zscode, start_date, end_date):
    db_connection = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    data_column = ['EndDate', 'SecuCode', 'SecondClassCode', 'ClassName',
                   'MarketPrice', 'MarketInTA', 'ValuationAppreciation']
    kcshares_index_list = ['C'+str(i) for i in range(1, 10)]
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT %s
            FROM ZS_FUNDVALUATION
            WHERE ZSCode = %s
            AND EndDate >= to_date('%s', 'yyyy-mm-dd')
            AND EndDate <= to_date('%s', 'yyyy-mm-dd')
            AND FirstClassCode = '1102'
            AND SecondClassCode is not Null
            AND SecondClassCode not in ('74', '81', '82', '83', '%s')
            AND SecuCode is not NULL
            """ % (','.join(data_column), fund_zscode,
                   start_date, end_date, "','".join(kcshares_index_list)))
        fund_stock_portfolio = cursor.fetchall()
        if not fund_stock_portfolio:
            return pd.DataFrame(columns=data_column)
        fund_stock_portfolio = pd.DataFrame(
            list(fund_stock_portfolio), columns=data_column)
        return fund_stock_portfolio
