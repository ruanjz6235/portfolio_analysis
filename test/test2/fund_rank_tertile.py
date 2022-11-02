#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 10:48:31 2019

@author: g
"""

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import warnings
from app.framework.injection import database_injector
warnings.filterwarnings("ignore")
import logging
from app.common.log import logger
#import cx_Oracle as cx


def calculate_accumulated_return(asset_return):

    if asset_return is not None:

        accumulated_return = asset_return.sum()
        accumulated_return = np.exp(accumulated_return) - 1

    else:

        accumulated_return = np.nan

    return accumulated_return




def calculate_annualized_return(asset_return):

    if asset_return is not None:

        multiplier = 12

        annualized_return = multiplier * asset_return.mean()
        annualized_return = np.exp(annualized_return) - 1

    else:

        annualized_return = np.nan

    return annualized_return



def calculate_annualized_excessive_return(asset_return, benchmark_return):

    if asset_return is not None:

        if len(asset_return)==len(benchmark_return):

            multiplier = 12

            annualized_return = multiplier * (asset_return - benchmark_return).mean()
            annualized_return = np.exp(annualized_return) - 1

        else:
            return np.nan

    else:

        annualized_return = np.nan

    return annualized_return



def calculate_annualized_volatility(asset_return):

    if asset_return is not None:

        multiplier = 12

#        asset_return = np.exp(asset_return) - 1

        annualized_vol = asset_return.std(ddof = 1) * np.sqrt(multiplier)

        #we assume that a volatility less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if annualized_vol < 0.001:
            annualized_vol = np.nan

    else:

        annualized_vol = np.nan

    return annualized_vol



def calculate_sharpe_ratio(asset_return):

    if asset_return is not None:

        multiplier = 12

        annualized_return = multiplier * asset_return.mean()

        vol = asset_return.std(ddof = 1) * np.sqrt(multiplier)

        #we assume that a volatility less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if vol < 0.001 :

            sharpe_ratio = np.nan

        else:

            sharpe_ratio = (annualized_return - 0.03) / vol

    else:
        sharpe_ratio = np.nan


    return sharpe_ratio



def calculate_maximum_drawdown(asset_return):

    if asset_return is not None:

        running_max = np.maximum.accumulate(asset_return.cumsum())

        underwater = asset_return.cumsum() - running_max

        underwater = np.exp(underwater) - 1

        mdd = -underwater.min()

    else:

        mdd = np.nan

    return mdd



def calculate_beta(asset_return, benchmark_return):

    if asset_return is not None:

        multiplier = 12

        if len(asset_return)==len(benchmark_return):

            asset_return = asset_return - 0.03/multiplier

            benchmark_return = benchmark_return - 0.03/multiplier

            cov = asset_return.cov(benchmark_return)

            beta = cov/benchmark_return.var()

            #we assume that a beta less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
            if beta < 0.001:
                return np.nan

        else:
            return np.nan

    else:
        return np.nan

    return beta



def calculate_information_ratio(asset_return, benchmark_return):

    if asset_return is not None and len(asset_return)==len(benchmark_return):

        multiplier = 12

        active_return = asset_return - benchmark_return

        tracking_error = (active_return.std(ddof = 1))* np.sqrt(multiplier)
        #we assume that a tracking error less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if tracking_error < 0.001:
            tracking_error = np.nan

        asset_annualized_return = multiplier * asset_return.mean()
        index_annualized_return = multiplier * benchmark_return.mean()

        information_ratio = (asset_annualized_return - index_annualized_return)/tracking_error

    else:
        information_ratio = np.nan

    return information_ratio

def calculate_tracking_error(asset_return, benchmark_return):

    multiplier = 12

    if asset_return is not None and len(asset_return)==len(benchmark_return):

        active_return = asset_return - benchmark_return

        tracking_error = (active_return.std(ddof = 1))* np.sqrt(multiplier)

        #we assume that a tracking error less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if tracking_error < 0.001:
            return np.nan

    else:
        tracking_error = np.nan

    return tracking_error



def calculate_sortino_ratio(asset_return):

    if asset_return is not None:

        multiplier = 12

        downside_return = asset_return - 0.03/multiplier

        downside_return[downside_return > 0] = 0
        downside_volatility = downside_return.std(ddof = 1) * np.sqrt(multiplier)

        annualized_return = multiplier * asset_return.mean()

        sortino_ratio = (annualized_return - 0.03) / downside_volatility

        #we assume that a volatility less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if downside_volatility < 0.001:
            sortino_ratio = np.nan

    else:
        sortino_ratio = np.nan

    return sortino_ratio




def calculate_treynor_ratio(asset_return, benchmark_return):

    if asset_return is not None and len(asset_return)==len(benchmark_return):

        multiplier = 12

        beta = calculate_beta(asset_return, benchmark_return)

        annualized_return = multiplier * asset_return.mean()

        #risk-free rate assumed to be 0.03
        treynor_ratio = (annualized_return - 0.03) / beta

    else:
        treynor_ratio = np.nan

    return treynor_ratio



def calculate_jensens_alpha(asset_return, benchmark_return):

    if asset_return is not None and len(asset_return)==len(benchmark_return):

        multiplier = 12

        beta = calculate_beta(asset_return, benchmark_return)

        asset_annualized_return = multiplier * asset_return.mean()

        benchmark_annualized_return = multiplier * benchmark_return.mean()

        #risk-free rate assumed to be 0.03
        rf = 0.03
        jensens_alpha = asset_annualized_return - (rf + beta * (benchmark_annualized_return - rf))

    else:
        jensens_alpha = np.nan

    return jensens_alpha



def calculate_m2(asset_return, benchmark_return):

    if asset_return is not None and len(asset_return)==len(benchmark_return):

        multiplier = 12

        asset_annualized_return = multiplier * asset_return.mean()

        asset_annualized_vol = asset_return.std(ddof = 1) * np.sqrt(multiplier)

        benchmark_annualized_vol = benchmark_return.std(ddof = 1) * np.sqrt(multiplier)

        #risk-free rate assumed to be 0.03
        rf = 0.03
        #we assume that a volatility less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if asset_annualized_vol < 0.001:
            m2 = np.nan
        else:
            m2 = rf + (asset_annualized_return - rf) * benchmark_annualized_vol/asset_annualized_vol

    else:
        m2 = np.nan

    return m2



def calculate_downside_volatility(asset_return, benchmark_return):

    multiplier = 12

    if asset_return is not None and len(asset_return)==len(benchmark_return):

        downside_return = asset_return - benchmark_return
        downside_return[downside_return > 0] = 0
        downside_volatility = downside_return.std(ddof = 1) * np.sqrt(multiplier)
        #we assume that a volatility less than 0.001 is due to broken data. if it is not set to nan, corresponding ratios will be inflated
        if downside_volatility < 0.001:
            downside_volatility = np.nan

    else:
        downside_volatility = np.nan

    return downside_volatility



def get_fund_pool():

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()

    try:
        cursor.execute(
                        """
                        SELECT DISTINCT INNERCODE
                        FROM JYDB.SF_POFBASICINFO INFO
                        """
                        )

        fund_pool = cursor.fetchall()

        if not fund_pool:

            pd.DataFrame(columns=['InnerCode'])

        fund_pool = pd.DataFrame(list(fund_pool),columns = ['InnerCode'])

        return fund_pool

    finally:
        cursor.close()







def get_threeyear_benchmark_return(cul_date):

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    #benchmark is set as CSI300 (innercode=3145)
    #note that cul_date is always the beginning of a month as specified in get_dates_to_insert
    #ADD_MONTHS -37, to ensure that 37 month-end closeprices are retrieved
    try:
        cursor.execute( """
                                SELECT  INNERCODE,TRADINGDAY,CLOSEPRICE FROM ZDJ.ZJ_INDEX_DAILY_QUOTE
                                WHERE INNERCODE=3145
                                AND TRADINGDAY < TO_DATE('%s','yyyy-mm-dd')
                                AND ADD_MONTHS(TO_DATE('%s','yyyy-mm-dd'),-37) <= TRADINGDAY
                                """% (cul_date,cul_date)
                                    )

        benchmark_return = cursor.fetchall()

        if not benchmark_return:

            return pd.DataFrame(columns = ['InnerCode','EndDate','ClosePrice'])

        benchmark_return = pd.DataFrame(list(benchmark_return),columns = ['InnerCode','EndDate','ClosePrice'])

        benchmark_return['EndDate'] = pd.to_datetime(benchmark_return['EndDate'])

        benchmark_return = benchmark_return.set_index('EndDate')

    finally:
        cursor.close()


    result = benchmark_return.iloc[benchmark_return.reset_index().groupby(benchmark_return.index.strftime('%Y-%m'))['EndDate'].idxmax()]

    result['EndDate'] = result.index

    result['EndDate'] = result['EndDate'].map(lambda x : pd.to_datetime(str(x + relativedelta(months=1))[:8]+'01') - relativedelta(days=1))

    result = result.set_index('EndDate')

    result['return'] = np.log(result['ClosePrice']/result['ClosePrice'].shift(1))

    result = result.dropna()

    result = result['return']

    #assuming that benchmark monthly return data are continuous
    if len(result) > 36:
        result = result[(len(result)-36):]

    return result



def get_threeyear_fund_return(codes,cul_date):

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    codes = list(map(str,codes))

    n = len(codes)

    if n > 1000:
        try:
            fund_returns = pd.DataFrame()

            k = int(n/1000)

            for i in range(0,k+1):

                sub_list = codes[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else codes[1000 * i: n]

                if len(sub_list) != 0:
                    #fund monthly returns are not always calculated on the last day of a month,
                    #in anticipation of that, cul_date is always the beginning of a month as specified in get_dates_to_insert
                    #ADD_MONTHS is set at -36, therefore the length of resultant monthly returns should always be 36
                    cursor.execute( """
                                    SELECT  INNERCODE,YEAR,MONTH,COMPLEX_MONTHLY_RETURN FROM ZDJ.JY_FUND_MONTHLY_RET
                                    WHERE INNERCODE IN %s
                                    AND ENDDATE < TO_DATE('%s','yyyy-mm-dd')
                                    AND ADD_MONTHS(TO_DATE('%s','yyyy-mm-dd'),-36) <= ENDDATE
                                    """% (str(tuple(sub_list)).replace(',)',')'),cul_date,cul_date)
                                    )

                    fund_return = cursor.fetchall()

                    if not fund_return:
                        continue

                    fund_return = pd.DataFrame(list(fund_return),columns = ['InnerCode','Year','Month','MonthReturn'])

                    fund_returns = fund_returns.append(fund_return)

                if fund_returns.empty:
                    return pd.DataFrame(columns = ['InnerCode','EndDate','MonthReturn'])

        finally:

            cursor.close()


    elif n > 0 and n <= 1000:

        try:
            #fund monthly returns are not always calculated on the last day of a month,
            #in anticipation of that, cul_date is always the beginning of a month as specified in get_dates_to_insert
            #ADD_MONTHS is set at -36, therefore the length of resultant monthly returns should always be 36
            cursor.execute(
            """
            SELECT  INNERCODE,YEAR,MONTH,COMPLEX_MONTHLY_RETURN FROM ZDJ.JY_FUND_MONTHLY_RET
            WHERE INNERCODE IN %s
            AND ENDDATE < TO_DATE('%s','yyyy-mm-dd')
            AND ADD_MONTHS(TO_DATE('%s','yyyy-mm-dd'),-36) <= ENDDATE
            """% (str(tuple(codes)).replace(',)',')'),cul_date,cul_date)
            )


            fund_returns = cursor.fetchall()

            if not fund_returns:

                return pd.DataFrame(columns = ['InnerCode','EndDate','MonthReturn'])

            fund_returns = pd.DataFrame(list(fund_returns),columns = ['InnerCode','Year','Month','MonthReturn'])

        finally:

            cursor.close()

    elif n==0:

        return pd.DataFrame(columns = ['InnerCode','EndDate','MonthReturn'])


    #fund_returns.columns = ['InnerCode','Year','Month','MonthReturn']

    fund_returns['Day'] = 1
    fund_returns = fund_returns[(fund_returns['Month'] != 0) & (fund_returns['Day'] != 0)]
    fund_returns['EndDate'] = pd.to_datetime(fund_returns[['Year','Month','Day']])

    #fund_returns['EndDate'] = fund_returns['EndDate'].map(lambda x : pd.to_datetime(str(x + relativedelta(months=1))[:8]+'01') - relativedelta(days=1))
    fund_returns['EndDate'] = fund_returns['EndDate'].apply(lambda x: x + relativedelta(months=1))
    fund_returns['EndDate'] = fund_returns['EndDate'].apply(lambda x: x + relativedelta(days=-1))

    fund_returns = fund_returns.drop(['Year','Month','Day'], axis = 1)

    #first filter: exclude innercodes with less than 2 month's data
    month_nums = fund_returns.groupby('InnerCode').apply(lambda x:len(x['MonthReturn'])).reset_index().rename(columns={0:'CountNum'})

    effect_innercode = list(month_nums[month_nums.CountNum>=2].InnerCode.unique())

    fund_returns = fund_returns[fund_returns.InnerCode.isin(effect_innercode)]

    #second filter: exclude funds with only 0 returns
    zero_nums = fund_returns.groupby('InnerCode').apply(lambda x:len(x[x.MonthReturn!=0])).reset_index().rename(columns={0:'CountNum'})

    effect_innercode = list(zero_nums[zero_nums.CountNum>=2].InnerCode.unique())

    fund_returns = fund_returns[fund_returns.InnerCode.isin(effect_innercode)]


    start = pd.to_datetime(str(pd.to_datetime(cul_date))[:8]+'01') - relativedelta(days=1) - pd.Timedelta('3 Y')
    #in case start represets a Febuary in a leap year. this is an alternative treatment to the one used in calculate_metrics()
    start = pd.to_datetime(str(pd.to_datetime(start) + relativedelta(months=1))[:8]+'01') - relativedelta(days=1)
    #truncate the possible one extra month
    fund_returns = fund_returns[fund_returns.EndDate > start]


    #outlier handling
    fund_returns.loc[fund_returns['MonthReturn']>5,'MonthReturn'] = fund_returns.MonthReturn.mean()

    fund_returns.loc[fund_returns['MonthReturn']<-5,'MonthReturn'] = fund_returns.MonthReturn.mean()

    #fund_returns['MonthReturn'] = np.exp(fund_returns['MonthReturn'])-1

    fund_returns = fund_returns.set_index('EndDate')

    return fund_returns


def calculate_metrics(codes,cul_date):

    fund_return = get_threeyear_fund_return(codes,cul_date)

    benchmark = get_threeyear_benchmark_return(cul_date)

    benchmark = benchmark.sort_index(ascending=False)

    month_nums = fund_return.groupby('InnerCode').apply(lambda x:len(x['MonthReturn'])).reset_index().rename(columns={0:'CountNum'})

    effect_innercode = list(month_nums[month_nums.CountNum>=2].InnerCode.unique())

    fund_return = fund_return[fund_return.InnerCode.isin(effect_innercode)]

    if len(fund_return) > 0:

        ret_res_idxs = pd.DataFrame(columns=['InnerCode','AnnuReturn_1y','AnnuReturn_cy','AnnuReturn_3y','AnnuExReturn_1y','AnnuExReturn_cy','AnnuExReturn_3y','AnnuVol_1y','AnnuVol_cy','AnnuVol_3y','SharpeRatio_1y','SharpeRatio_cy','SharpeRatio_3y','MaximumDrawdown_1y','MaximumDrawdown_cy','MaximumDrawdown_3y','Beta_1y','Beta_cy','Beta_3y','AnnuDownVol_1y','AnnuDownVol_cy','AnnuDownVol_3y','AnnuTrack_1y','AnnuTrack_cy','AnnuTrack_3y','AccuReturn_1y','AccuReturn_cy','AccuReturn_3y','Jensen_1y','Jensen_cy','Jensen_3y','Info_1y','Info_cy','Info_3y','Sortino_1y','Sortino_cy','Sortino_3y','Treynor_1y','Treynor_cy','Treynor_3y','M2_1y','M2_cy','M2_3y'])

        for innercode in fund_return.InnerCode.unique():

            sub_fund = fund_return[fund_return.InnerCode==innercode]

            asset_return = sub_fund['MonthReturn']

            asset_return = asset_return.sort_index(ascending=False)

            benchmark_return = benchmark[benchmark.index>=asset_return.index.min()]

            #in case asset_return does not have the latest update (which the benchmark_return will always have)
            if len(benchmark_return) > len(asset_return):
                benchmark_return = benchmark_return[(len(benchmark_return) - len(asset_return)):]

            values = np.array(
                                [[innercode],[np.nan],[np.nan],
                                [calculate_annualized_return(asset_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_annualized_excessive_return(asset_return,benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_annualized_volatility(asset_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_sharpe_ratio(asset_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_maximum_drawdown(asset_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_beta(asset_return,benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_downside_volatility(asset_return, benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_tracking_error(asset_return, benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_accumulated_return(asset_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_jensens_alpha(asset_return, benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_information_ratio(asset_return, benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_sortino_ratio(asset_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_treynor_ratio(asset_return, benchmark_return)],
                                [np.nan],
                                [np.nan],
                                [calculate_m2(asset_return, benchmark_return)]]
                                ).T

            columns=['InnerCode','AnnuReturn_1y','AnnuReturn_cy','AnnuReturn_3y','AnnuExReturn_1y','AnnuExReturn_cy','AnnuExReturn_3y','AnnuVol_1y','AnnuVol_cy','AnnuVol_3y','SharpeRatio_1y','SharpeRatio_cy','SharpeRatio_3y','MaximumDrawdown_1y','MaximumDrawdown_cy','MaximumDrawdown_3y','Beta_1y','Beta_cy','Beta_3y','AnnuDownVol_1y','AnnuDownVol_cy','AnnuDownVol_3y','AnnuTrack_1y','AnnuTrack_cy','AnnuTrack_3y','AccuReturn_1y','AccuReturn_cy','AccuReturn_3y','Jensen_1y','Jensen_cy','Jensen_3y','Info_1y','Info_cy','Info_3y','Sortino_1y','Sortino_cy','Sortino_3y','Treynor_1y','Treynor_cy','Treynor_3y','M2_1y','M2_cy','M2_3y']

            ret_res_idx =  pd.DataFrame(values,columns=columns)

            ret_res_idxs = ret_res_idxs.append(ret_res_idx)

            ret_res_idxs['InnerCode'] = ret_res_idxs['InnerCode'].astype(int)

        ret_res_idxs = ret_res_idxs.set_index('InnerCode')

        #second loop for 1-year metrics
        for innercode in ret_res_idxs.index:

            sub_fund = fund_return[fund_return.InnerCode==innercode]

            asset_return = sub_fund['MonthReturn']

            #start = pd.to_datetime(fund_return.index.max()) - pd.Timedelta('1 Y')  #assuming that at least one of the fund has up-to-date monthly returns
            start = pd.to_datetime(str(pd.to_datetime(cul_date))[:8]+'01') - relativedelta(days=1) - pd.Timedelta(1, unit='Y')

            #special treatment in case 'start' represents a leap year Febuary, e.g.('2017-02-28 - Timedelta('1Y')) would be '2016-02-28' instead of '2016-02-29'
            if divmod(int(str(start)[:4]),4)[1] == 0 and str(start)[5:7] == '02':
                start = start + pd.Timedelta('1 days')

            asset_return = asset_return[asset_return.index > start]
            asset_return = asset_return.sort_index(ascending=False)

            if len(asset_return) < 2:

                continue

            benchmark_return = benchmark[benchmark.index>=asset_return.index.min()]

            #in case asset_return does not have the latest update (which the benchmark_return will always have)
            if len(benchmark_return) > len(asset_return):
                benchmark_return = benchmark_return[(len(benchmark_return) - len(asset_return)):]


            ret_res_idxs.loc[innercode,'AnnuReturn_1y'] = calculate_annualized_return(asset_return)
            ret_res_idxs.loc[innercode,'AnnuExReturn_1y'] = calculate_annualized_excessive_return(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AnnuVol_1y'] = calculate_annualized_volatility(asset_return)
            ret_res_idxs.loc[innercode,'SharpeRatio_1y'] = calculate_sharpe_ratio(asset_return)
            ret_res_idxs.loc[innercode,'MaximumDrawdown_1y'] = calculate_maximum_drawdown(asset_return)
            ret_res_idxs.loc[innercode,'Beta_1y'] = calculate_beta(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AnnuDownVol_1y'] = calculate_downside_volatility(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AnnuTrack_1y'] = calculate_tracking_error(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AccuReturn_1y'] = calculate_accumulated_return(asset_return)
            ret_res_idxs.loc[innercode,'Jensen_1y'] = calculate_jensens_alpha(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'Info_1y'] = calculate_information_ratio(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'Sortino_1y'] = calculate_sortino_ratio(asset_return)
            ret_res_idxs.loc[innercode,'Treynor_1y'] = calculate_treynor_ratio(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'M2_1y'] = calculate_m2(asset_return,benchmark_return)


        #third loop for current-year metrics
        for innercode in ret_res_idxs.index:

            sub_fund = fund_return[fund_return.InnerCode==innercode]

            asset_return = sub_fund['MonthReturn']

            start = pd.to_datetime(str(pd.to_datetime(cul_date))[:4]+'-01-01') - relativedelta(days=1)

            asset_return = asset_return[asset_return.index > start]
            asset_return = asset_return.sort_index(ascending=False)

            if len(asset_return) < 2:

                continue

            benchmark_return = benchmark[benchmark.index>=asset_return.index.min()]

            #in case asset_return does not have the latest update (which the benchmark_return will always have)
            if len(benchmark_return) > len(asset_return):
                benchmark_return = benchmark_return[(len(benchmark_return) - len(asset_return)):]


            ret_res_idxs.loc[innercode,'AnnuReturn_cy'] = calculate_annualized_return(asset_return)
            ret_res_idxs.loc[innercode,'AnnuExReturn_cy'] = calculate_annualized_excessive_return(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AnnuVol_cy'] = calculate_annualized_volatility(asset_return)
            ret_res_idxs.loc[innercode,'SharpeRatio_cy'] = calculate_sharpe_ratio(asset_return)
            ret_res_idxs.loc[innercode,'MaximumDrawdown_cy'] = calculate_maximum_drawdown(asset_return)
            ret_res_idxs.loc[innercode,'Beta_cy'] = calculate_beta(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AnnuDownVol_cy'] = calculate_downside_volatility(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AnnuTrack_cy'] = calculate_tracking_error(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'AccuReturn_cy'] = calculate_accumulated_return(asset_return)
            ret_res_idxs.loc[innercode,'Jensen_cy'] = calculate_jensens_alpha(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'Info_cy'] = calculate_information_ratio(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'Sortino_cy'] = calculate_sortino_ratio(asset_return)
            ret_res_idxs.loc[innercode,'Treynor_cy'] = calculate_treynor_ratio(asset_return,benchmark_return)
            ret_res_idxs.loc[innercode,'M2_cy'] = calculate_m2(asset_return,benchmark_return)


        ret_res_idxs['EndDate'] = fund_return.index.max()

        ret_res_idxs['TotalNumber_1y'] = len(ret_res_idxs['AnnuReturn_1y'].dropna())
        ret_res_idxs['TotalNumber_cy'] = len(ret_res_idxs['AnnuReturn_cy'].dropna())
        ret_res_idxs['TotalNumber_3y'] = len(ret_res_idxs['AnnuReturn_3y'].dropna())

        ret_res_idxs['InnerCode'] = ret_res_idxs.index

        return  ret_res_idxs

    else:

        return pd.DataFrame(columns=['InnerCode','AnnuReturn_1y','AnnuReturn_cy','AnnuReturn_3y','AnnuExReturn_1y','AnnuExReturn_cy','AnnuExReturn_3y','AnnuVol_1y','AnnuVol_cy','AnnuVol_3y','SharpeRatio_1y','SharpeRatio_cy','SharpeRatio_3y','MaximumDrawdown_1y','MaximumDrawdown_cy','MaximumDrawdown_3y','Beta_1y','Beta_cy','Beta_3y','AnnuDownVol_1y','AnnuDownVol_cy','AnnuDownVol_3y','AnnuTrack_1y','AnnuTrack_cy','AnnuTrack_3y','AccuReturn_1y','AccuReturn_cy','AccuReturn_3y','Jensen_1y','Jensen_cy','Jensen_3y','Info_1y','Info_cy','Info_3y','Sortino_1y','Sortino_cy','Sortino_3y','Treynor_1y','Treynor_cy','Treynor_3y','M2_1y','M2_cy','M2_3y'])




def calculate_fund_rank(fund_performance):

    result = fund_performance.copy()

    result['AnnuReturn_1y_Rank'] = result['AnnuReturn_1y'].rank(method='min', ascending=False)

    result['AnnuReturn_cy_Rank'] = result['AnnuReturn_cy'].rank(method='min', ascending=False)

    result['AnnuReturn_3y_Rank'] = result['AnnuReturn_3y'].rank(method='min', ascending=False)

    result['AnnuExReturn_1y_Rank'] = result['AnnuExReturn_1y'].rank(method='min', ascending=False)

    result['AnnuExReturn_cy_Rank'] = result['AnnuExReturn_cy'].rank(method='min', ascending=False)

    result['AnnuExReturn_3y_Rank'] = result['AnnuExReturn_3y'].rank(method='min', ascending=False)

    result['AnnuVol_1y_Rank'] = result['AnnuVol_1y'].rank(method='min', ascending=True)

    result['AnnuVol_cy_Rank'] = result['AnnuVol_cy'].rank(method='min', ascending=True)

    result['AnnuVol_3y_Rank'] = result['AnnuVol_3y'].rank(method='min', ascending=True)

    result['SharpeRatio_1y_Rank'] = result['SharpeRatio_1y'].rank(method='min', ascending=False)

    result['SharpeRatio_cy_Rank'] = result['SharpeRatio_cy'].rank(method='min', ascending=False)

    result['SharpeRatio_3y_Rank'] = result['SharpeRatio_3y'].rank(method='min', ascending=False)

    result['MaximumDrawdown_1y_Rank'] = result['MaximumDrawdown_1y'].rank(method='min', ascending=True)

    result['MaximumDrawdown_cy_Rank'] = result['MaximumDrawdown_cy'].rank(method='min', ascending=True)

    result['MaximumDrawdown_3y_Rank'] = result['MaximumDrawdown_3y'].rank(method='min', ascending=True)

    result['Beta_1y_Rank'] = result['Beta_1y'].rank(method='min', ascending=False)

    result['Beta_cy_Rank'] = result['Beta_cy'].rank(method='min', ascending=False)

    result['Beta_3y_Rank'] = result['Beta_3y'].rank(method='min', ascending=False)

    result['AnnuDownVol_1y_Rank'] = result['AnnuDownVol_1y'].rank(method='min', ascending=True)

    result['AnnuDownVol_cy_Rank'] = result['AnnuDownVol_cy'].rank(method='min', ascending=True)

    result['AnnuDownVol_3y_Rank'] = result['AnnuDownVol_3y'].rank(method='min', ascending=True)

    result['AnnuTrack_1y_Rank'] = result['AnnuTrack_1y'].rank(method='min', ascending=True)

    result['AnnuTrack_cy_Rank'] = result['AnnuTrack_cy'].rank(method='min', ascending=True)

    result['AnnuTrack_3y_Rank'] = result['AnnuTrack_3y'].rank(method='min', ascending=True)

    result['AccuReturn_1y_Rank'] = result['AccuReturn_1y'].rank(method='min', ascending=False)

    result['AccuReturn_cy_Rank'] = result['AccuReturn_cy'].rank(method='min', ascending=False)

    result['AccuReturn_3y_Rank'] = result['AccuReturn_3y'].rank(method='min', ascending=False)

    result['Jensen_1y_Rank'] = result['Jensen_1y'].rank(method='min', ascending=False)

    result['Jensen_cy_Rank'] = result['Jensen_cy'].rank(method='min', ascending=False)

    result['Jensen_3y_Rank'] = result['Jensen_3y'].rank(method='min', ascending=False)

    result['Info_1y_Rank'] = result['Info_1y'].rank(method='min', ascending=False)

    result['Info_cy_Rank'] = result['Info_cy'].rank(method='min', ascending=False)

    result['Info_3y_Rank'] = result['Info_3y'].rank(method='min', ascending=False)

    result['Sortino_1y_Rank'] = result['Sortino_1y'].rank(method='min', ascending=False)

    result['Sortino_cy_Rank'] = result['Sortino_cy'].rank(method='min', ascending=False)

    result['Sortino_3y_Rank'] = result['Sortino_3y'].rank(method='min', ascending=False)

    result['Treynor_1y_Rank'] = result['Treynor_1y'].rank(method='min', ascending=False)

    result['Treynor_cy_Rank'] = result['Treynor_cy'].rank(method='min', ascending=False)

    result['Treynor_3y_Rank'] = result['Treynor_3y'].rank(method='min', ascending=False)

    result['M2_1y_Rank'] = result['M2_1y'].rank(method='min', ascending=False)

    result['M2_cy_Rank'] = result['M2_cy'].rank(method='min', ascending=False)

    result['M2_3y_Rank'] = result['M2_3y'].rank(method='min', ascending=False)


    return result



'''
def get_tertile_funds(request_id,type,tertile,metric,cul_date):


    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    metric_rank = str(metric)+'_RANK'
    try:
        cursor.execute(
        """
        SELECT INNERCODE,ENDDATE,%s,TOTALNUMBER%s
        FROM  ZS_FUND_RANK_TERTILE
        WHERE ENDDATE<=TO_DATE('%s','yyyy-mm-dd')
        AND ADD_MONTHS(TO_DATE('%s','yyyy-mm-dd'),-1) < ENDDATE
        """%(metric_rank,str(metric)[-3:],cul_date,cul_date)
        )

        result = cursor.fetchall()

        if not result:
            return pd.DataFrame()

        result = pd.DataFrame(list(result), columns = ['InnerCode','EndDate','Rank','TotalNumber'])

    finally:
        cursor.close()

    tertile_1 = int(result['TotalNumber'].sample()/3)

    tertile_2 = int(result['TotalNumber'].sample()*2/3)

    if tertile == 'top':
        result = result[result.Rank<=tertile_1]

    elif tertile == 'middle':
        result = result[result.Rank<=tertile_2]
        result = result[result.Rank>tertile_1]

    elif tertile == 'bottom':
        result = result[result.Rank>tertile_2]

    else:
        return pd.DataFrame()

    return result
'''




def fund_rank_tertile_to_database(result):

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    data = result.copy()

    data = data.where(pd.notnull(data), None)

    try:

        values = list(zip(data['InnerCode'],data['EndDate'],data['AnnuReturn_1y'],data['AnnuReturn_cy'],data['AnnuReturn_3y'],data['AnnuExReturn_1y'],data['AnnuExReturn_cy'],data['AnnuExReturn_3y'],data['AnnuVol_1y'],data['AnnuVol_cy'],data['AnnuVol_3y'],data['SharpeRatio_1y'],data['SharpeRatio_cy'],data['SharpeRatio_3y'],data['MaximumDrawdown_1y'],data['MaximumDrawdown_cy'],data['MaximumDrawdown_3y'],data['Beta_1y'],data['Beta_cy'],data['Beta_3y'],data['AnnuDownVol_1y'],data['AnnuDownVol_cy'],data['AnnuDownVol_3y'],data['AnnuTrack_1y'],data['AnnuTrack_cy'],data['AnnuTrack_3y'],data['AccuReturn_1y'],data['AccuReturn_cy'],data['AccuReturn_3y'],data['Jensen_1y'],data['Jensen_cy'],data['Jensen_3y'],data['Info_1y'],data['Info_cy'],data['Info_3y'],data['Sortino_1y'],data['Sortino_cy'],data['Sortino_3y'],data['Treynor_1y'],data['Treynor_cy'],data['Treynor_3y'],data['M2_1y'],data['M2_cy'],data['M2_3y'],data['AnnuReturn_1y_Rank'],data['AnnuReturn_cy_Rank'],data['AnnuReturn_3y_Rank'],data['AnnuExReturn_1y_Rank'],data['AnnuExReturn_cy_Rank'],data['AnnuExReturn_3y_Rank'],data['AnnuVol_1y_Rank'],data['AnnuVol_cy_Rank'],data['AnnuVol_3y_Rank'],data['SharpeRatio_1y_Rank'],data['SharpeRatio_cy_Rank'],data['SharpeRatio_3y_Rank'],data['MaximumDrawdown_1y_Rank'],data['MaximumDrawdown_cy_Rank'],data['MaximumDrawdown_3y_Rank'],data['Beta_1y_Rank'],data['Beta_cy_Rank'],data['Beta_3y_Rank'],data['AnnuDownVol_1y_Rank'],data['AnnuDownVol_cy_Rank'],data['AnnuDownVol_3y_Rank'],data['AnnuTrack_1y_Rank'],data['AnnuTrack_cy_Rank'],data['AnnuTrack_3y_Rank'],data['AccuReturn_1y_Rank'],data['AccuReturn_cy_Rank'],data['AccuReturn_3y_Rank'],data['Jensen_1y_Rank'],data['Jensen_cy_Rank'],data['Jensen_3y_Rank'],data['Info_1y_Rank'],data['Info_cy_Rank'],data['Info_3y_Rank'],data['Sortino_1y_Rank'],data['Sortino_cy_Rank'],data['Sortino_3y_Rank'],data['Treynor_1y_Rank'],data['Treynor_cy_Rank'],data['Treynor_3y_Rank'],data['M2_1y_Rank'],data['M2_cy_Rank'],data['M2_3y_Rank'],data['TotalNumber_1y'],data['TotalNumber_cy'],data['TotalNumber_3y']))

        cursor.executemany(
                    """BEGIN
                    insert into ZS_FUND_RANK_TERTILE(INNERCODE,ENDDATE,ANNURETURN_1Y,ANNURETURN_CY,ANNURETURN_3Y,ANNUEXRETURN_1Y,ANNUEXRETURN_CY,ANNUEXRETURN_3Y,ANNUVOL_1Y,ANNUVOL_CY,ANNUVOL_3Y,SHARPERATIO_1Y,SHARPERATIO_CY,SHARPERATIO_3Y,MAXIMUMDRAWDOWN_1Y,MAXIMUMDRAWDOWN_CY,MAXIMUMDRAWDOWN_3Y,BETA_1Y,BETA_CY,BETA_3Y,ANNUDOWNVOL_1Y,ANNUDOWNVOL_CY,ANNUDOWNVOL_3Y,ANNUTRACK_1Y,ANNUTRACK_CY,ANNUTRACK_3Y,ACCURETURN_1Y,ACCURETURN_CY,ACCURETURN_3Y,JENSEN_1Y,JENSEN_CY,JENSEN_3Y,INFO_1Y,INFO_CY,INFO_3Y,SORTINO_1Y,SORTINO_CY,SORTINO_3Y,TREYNOR_1Y,TREYNOR_CY,TREYNOR_3Y,M2_1Y,M2_CY,M2_3Y,ANNURETURN_1Y_RANK,ANNURETURN_CY_RANK,ANNURETURN_3Y_RANK,ANNUEXRETURN_1Y_RANK,ANNUEXRETURN_CY_RANK,ANNUEXRETURN_3Y_RANK,ANNUVOL_1Y_RANK,ANNUVOL_CY_RANK,ANNUVOL_3Y_RANK,SHARPERATIO_1Y_RANK,SHARPERATIO_CY_RANK,SHARPERATIO_3Y_RANK,MAXIMUMDRAWDOWN_1Y_RANK,MAXIMUMDRAWDOWN_CY_RANK,MAXIMUMDRAWDOWN_3Y_RANK,BETA_1Y_RANK,BETA_CY_RANK,BETA_3Y_RANK,ANNUDOWNVOL_1Y_RANK,ANNUDOWNVOL_CY_RANK,ANNUDOWNVOL_3Y_RANK,ANNUTRACK_1Y_RANK,ANNUTRACK_CY_RANK,ANNUTRACK_3Y_RANK,ACCURETURN_1Y_RANK,ACCURETURN_CY_RANK,ACCURETURN_3Y_RANK,JENSEN_1Y_RANK,JENSEN_CY_RANK,JENSEN_3Y_RANK,INFO_1Y_RANK,INFO_CY_RANK,INFO_3Y_RANK,SORTINO_1Y_RANK,SORTINO_CY_RANK,SORTINO_3Y_RANK,TREYNOR_1Y_RANK,TREYNOR_CY_RANK,TREYNOR_3Y_RANK,M2_1Y_RANK,M2_CY_RANK,M2_3Y_RANK,TOTALNUMBER_1Y,TOTALNUMBER_CY,TOTALNUMBER_3Y) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58,:59,:60,:61,:62,:63,:64,:65,:66,:67,:68,:69,:70,:71,:72,:73,:74,:75,:76,:77,:78,:79,:80,:81,:82,:83,:84,:85,:86,:87,:88,:89);
                    EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                    UPDATE ZS_FUND_RANK_TERTILE
                    SET ENDDATE=:2,ANNURETURN_1Y=:3,ANNURETURN_CY=:4,ANNURETURN_3Y=:5,ANNUEXRETURN_1Y=:6,ANNUEXRETURN_CY=:7,ANNUEXRETURN_3Y=:8,ANNUVOL_1Y=:9,ANNUVOL_CY=:10,ANNUVOL_3Y=:11,SHARPERATIO_1Y=:12,SHARPERATIO_CY=:13,SHARPERATIO_3Y=:14,MAXIMUMDRAWDOWN_1Y=:15,MAXIMUMDRAWDOWN_CY=:16,MAXIMUMDRAWDOWN_3Y=:17,BETA_1Y=:18,BETA_CY=:19,BETA_3Y=:20,ANNUDOWNVOL_1Y=:21,ANNUDOWNVOL_CY=:22,ANNUDOWNVOL_3Y=:23,ANNUTRACK_1Y=:24,ANNUTRACK_CY=:25,ANNUTRACK_3Y=:26,ACCURETURN_1Y=:27,ACCURETURN_CY=:28,ACCURETURN_3Y=:29,JENSEN_1Y=:30,JENSEN_CY=:31,JENSEN_3Y=:32,INFO_1Y=:33,INFO_CY=:34,INFO_3Y=:35,SORTINO_1Y=:36,SORTINO_CY=:37,SORTINO_3Y=:38,TREYNOR_1Y=:39,TREYNOR_CY=:40,TREYNOR_3Y=:41,M2_1Y=:42,M2_CY=:43,M2_3Y=:44,ANNURETURN_1Y_RANK=:45,ANNURETURN_CY_RANK=:46,ANNURETURN_3Y_RANK=:47,ANNUEXRETURN_1Y_RANK=:48,ANNUEXRETURN_CY_RANK=:49,ANNUEXRETURN_3Y_RANK=:50,ANNUVOL_1Y_RANK=:51,ANNUVOL_CY_RANK=:52,ANNUVOL_3Y_RANK=:53,SHARPERATIO_1Y_RANK=:54,SHARPERATIO_CY_RANK=:55,SHARPERATIO_3Y_RANK=:56,MAXIMUMDRAWDOWN_1Y_RANK=:57,MAXIMUMDRAWDOWN_CY_RANK=:58,MAXIMUMDRAWDOWN_3Y_RANK=:59,BETA_1Y_RANK=:60,BETA_CY_RANK=:61,BETA_3Y_RANK=:62,ANNUDOWNVOL_1Y_RANK=:63,ANNUDOWNVOL_CY_RANK=:64,ANNUDOWNVOL_3Y_RANK=:65,ANNUTRACK_1Y_RANK=:66,ANNUTRACK_CY_RANK=:67,ANNUTRACK_3Y_RANK=:68,ACCURETURN_1Y_RANK=:69,ACCURETURN_CY_RANK=:70,ACCURETURN_3Y_RANK=:71,JENSEN_1Y_RANK=:72,JENSEN_CY_RANK=:73,JENSEN_3Y_RANK=:74,INFO_1Y_RANK=:75,INFO_CY_RANK=:76,INFO_3Y_RANK=:77,SORTINO_1Y_RANK=:78,SORTINO_CY_RANK=:79,SORTINO_3Y_RANK=:80,TREYNOR_1Y_RANK=:81,TREYNOR_CY_RANK=:82,TREYNOR_3Y_RANK=:83,M2_1Y_RANK=:84,M2_CY_RANK=:85,M2_3Y_RANK=:86,TOTALNUMBER_1Y=:87,TOTALNUMBER_CY=:88,TOTALNUMBER_3Y=:89
                    WHERE INNERCODE=:1;
                    END;
                    """,values
                    )

        db_connection.commit()

        return {'if_success':'success'}


    finally:
        cursor.close()



def get_latest_fund_rank_date():

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()

    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT ENDDATE FROM
            (SELECT EndDate
            FROM ZDJ.ZS_FUND_RANK_TERTILE
            ORDER BY EndDate DESC)

            WHERE rownum = 1
            """
        )
        latest_fund_rank_date = cursor.fetchall()

        #set an earliest date
        if not latest_fund_rank_date:
            return pd.DataFrame([pd.to_datetime('2019-08-31')],columns=['EndDate'])

        latest_fund_rank_date = pd.DataFrame(list(latest_fund_rank_date), columns = ['EndDate'])

        return latest_fund_rank_date

    finally:
        cursor.close()




def get_dates_to_insert():

    current_date = pd.Timestamp.today()

    #ad hoc setting for testing
    #current_date = pd.to_datetime('2019-10-11')

    latest_fund_rank_date = get_latest_fund_rank_date()

    dates_to_insert = pd.DataFrame(columns = ['EndDate'])

    dates_to_insert['EndDate'] = pd.date_range(start=str(pd.to_datetime(latest_fund_rank_date['EndDate'][0])+pd.offsets.MonthEnd())[:10], end=str(current_date)[:10])

    result = []

    #if still in the current month
    if dates_to_insert.empty:
        result.append(pd.to_datetime(str(current_date)[:8]+'01'))
    else:
        #only the first days of a month are included
        for x in dates_to_insert['EndDate']:
            if x.day == 1:
                result.append(x)
        result = sorted(result)

    return result





def feed_fund_rank_tertile(request_id, type):

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()

    try:
        date_list = get_dates_to_insert()

        cursor.execute(
                """
                select distinct INNERCODE from ZDJ.ZS_FUND_RANK_TERTILE
                """
                )

        innercode_old = cursor.fetchall()

        if not innercode_old:
            innercode_old = pd.DataFrame()
        else:
            innercode_old = pd.DataFrame(list(innercode_old), columns = ['INNERCODE'])

        codes = get_fund_pool()

        codes = codes['InnerCode'].tolist()

        #codes = codes[-100:]

        #the for loop here is only for test.
        #the formal setting of this function is to only update the table based on the latest date
        for x in list([date_list[-1]]):

            #returns=get_threeyear_fund_return(codes,str(x)[:10])

            result=calculate_metrics(codes,str(x)[:10])

            rank=calculate_fund_rank(result)

            fund_rank_tertile_to_database(rank)


        innercode_to_delete = []

        for x in innercode_old['INNERCODE']:
            if x not in rank.index:
                innercode_to_delete.append(x)

        n = len(innercode_to_delete)

        if n > 1000:

             k = int(n/1000)

             for i in range(0,k+1):
                 sub_list = innercode_to_delete[1000 * i: 1000 * (i + 1)] if 1000 * (i + 1) < n else innercode_to_delete[1000 * i: n]

                 if len(sub_list) != 0:

                    cursor.execute(
                        """
                        delete from ZDJ.ZS_FUND_RANK_TERTILE
                        where INNERCODE in %s
                        """%(str(tuple(sub_list)).replace(',)',')'))
                        )
                    db_connection.commit()

        elif n > 0 and n <= 1000:

            cursor.execute(
                """
                delete from ZDJ.ZS_FUND_RANK_TERTILE
                where INNERCODE in %s
                """%(str(tuple(innercode_to_delete)).replace(',)',')'))
                )
            db_connection.commit()

        elif n==0:
            pass


        return {'if_success':'success'}

    finally:
        cursor.close()


'''
def create_fund_rank_tertile_database():

    #db_connection = database_injector.get_resource()
    #cursor = db_connection.cursor()
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    cursor = conn.cursor()


    try:

        cursor.execute("""
                CREATE TABLE ZS_FUND_RANK_TERTILE(
                "ID" number(18) NOT NULL PRIMARY KEY,
                "INNERCODE" number(18) DEFAULT NULL,
                "ENDDATE" date DEFAULT NULL,

                "ANNURETURN_1Y" number(18,8) DEFAULT NULL,
                "ANNURETURN_CY" number(18,8) DEFAULT NULL,
                "ANNURETURN_3Y" number(18,8) DEFAULT NULL,
                "ANNUEXRETURN_1Y" number(18,8) DEFAULT NULL,
                "ANNUEXRETURN_CY" number(18,8) DEFAULT NULL,
                "ANNUEXRETURN_3Y" number(18,4) DEFAULT NULL,
                "ANNUVOL_1Y" number(18,8) DEFAULT NULL,
                "ANNUVOL_CY" number(18,8) DEFAULT NULL,
                "ANNUVOL_3Y" number(18,8) DEFAULT NULL,
                "SHARPERATIO_1Y" number(18,8) DEFAULT NULL,
                "SHARPERATIO_CY" number(18,8) DEFAULT NULL,
                "SHARPERATIO_3Y" number(18,8) DEFAULT NULL,
                "MAXIMUMDRAWDOWN_1Y" number(18,8) DEFAULT NULL,
                "MAXIMUMDRAWDOWN_CY" number(18,8) DEFAULT NULL,
                "MAXIMUMDRAWDOWN_3Y" number(18,8) DEFAULT NULL,
                "BETA_1Y" number(18,8) DEFAULT NULL,
                "BETA_CY" number(18,8) DEFAULT NULL,
                "BETA_3Y" number(18,8) DEFAULT NULL,
                "ANNUDOWNVOL_1Y" number(18,8) DEFAULT NULL,
                "ANNUDOWNVOL_CY" number(18,8) DEFAULT NULL,
                "ANNUDOWNVOL_3Y" number(18,8) DEFAULT NULL,
                "ANNUTRACK_1Y" number(18,8) DEFAULT NULL,
                "ANNUTRACK_CY" number(18,8) DEFAULT NULL,
                "ANNUTRACK_3Y" number(18,8) DEFAULT NULL,
                "ACCURETURN_1Y" number(18,8) DEFAULT NULL,
                "ACCURETURN_CY" number(18,8) DEFAULT NULL,
                "ACCURETURN_3Y" number(18,8) DEFAULT NULL,
                "JENSEN_1Y" number(18,8) DEFAULT NULL,
                "JENSEN_CY" number(18,8) DEFAULT NULL,
                "JENSEN_3Y" number(18,8) DEFAULT NULL,
                "INFO_1Y" number(18,8) DEFAULT NULL,
                "INFO_CY" number(18,8) DEFAULT NULL,
                "INFO_3Y" number(18,8) DEFAULT NULL,
                "SORTINO_1Y" number(18,8) DEFAULT NULL,
                "SORTINO_CY" number(18,8) DEFAULT NULL,
                "SORTINO_3Y" number(18,8) DEFAULT NULL,
                "TREYNOR_1Y" number(18,8) DEFAULT NULL,
                "TREYNOR_CY" number(18,8) DEFAULT NULL,
                "TREYNOR_3Y" number(18,8) DEFAULT NULL,
                "M2_1Y" number(18,8) DEFAULT NULL,
                "M2_CY" number(18,8) DEFAULT NULL,
                "M2_3Y" number(18,8) DEFAULT NULL,

                "ANNURETURN_1Y_RANK" number(18) DEFAULT NULL,
                "ANNURETURN_CY_RANK" number(18) DEFAULT NULL,
                "ANNURETURN_3Y_RANK" number(18) DEFAULT NULL,
                "ANNUEXRETURN_1Y_RANK" number(18) DEFAULT NULL,
                "ANNUEXRETURN_CY_RANK" number(18) DEFAULT NULL,
                "ANNUEXRETURN_3Y_RANK" number(18) DEFAULT NULL,
                "ANNUVOL_1Y_RANK" number(18) DEFAULT NULL,
                "ANNUVOL_CY_RANK" number(18) DEFAULT NULL,
                "ANNUVOL_3Y_RANK" number(18) DEFAULT NULL,
                "SHARPERATIO_1Y_RANK" number(18) DEFAULT NULL,
                "SHARPERATIO_CY_RANK" number(18) DEFAULT NULL,
                "SHARPERATIO_3Y_RANK" number(18) DEFAULT NULL,
                "MAXIMUMDRAWDOWN_1Y_RANK" number(18) DEFAULT NULL,
                "MAXIMUMDRAWDOWN_CY_RANK" number(18) DEFAULT NULL,
                "MAXIMUMDRAWDOWN_3Y_RANK" number(18) DEFAULT NULL,
                "BETA_1Y_RANK" number(18) DEFAULT NULL,
                "BETA_CY_RANK" number(18) DEFAULT NULL,
                "BETA_3Y_RANK" number(18) DEFAULT NULL,
                "ANNUDOWNVOL_1Y_RANK" number(18) DEFAULT NULL,
                "ANNUDOWNVOL_CY_RANK" number(18) DEFAULT NULL,
                "ANNUDOWNVOL_3Y_RANK" number(18) DEFAULT NULL,
                "ANNUTRACK_1Y_RANK" number(18) DEFAULT NULL,
                "ANNUTRACK_CY_RANK" number(18) DEFAULT NULL,
                "ANNUTRACK_3Y_RANK" number(18) DEFAULT NULL,
                "ACCURETURN_1Y_RANK" number(18) DEFAULT NULL,
                "ACCURETURN_CY_RANK" number(18) DEFAULT NULL,
                "ACCURETURN_3Y_RANK" number(18) DEFAULT NULL,
                "JENSEN_1Y_RANK" number(18) DEFAULT NULL,
                "JENSEN_CY_RANK" number(18) DEFAULT NULL,
                "JENSEN_3Y_RANK" number(18) DEFAULT NULL,
                "INFO_1Y_RANK" number(18) DEFAULT NULL,
                "INFO_CY_RANK" number(18) DEFAULT NULL,
                "INFO_3Y_RANK" number(18) DEFAULT NULL,
                "SORTINO_1Y_RANK" number(18) DEFAULT NULL,
                "SORTINO_CY_RANK" number(18) DEFAULT NULL,
                "SORTINO_3Y_RANK" number(18) DEFAULT NULL,
                "TREYNOR_1Y_RANK" number(18) DEFAULT NULL,
                "TREYNOR_CY_RANK" number(18) DEFAULT NULL,
                "TREYNOR_3Y_RANK" number(18) DEFAULT NULL,
                "M2_1Y_RANK" number(18) DEFAULT NULL,
                "M2_CY_RANK" number(18) DEFAULT NULL,
                "M2_3Y_RANK" number(18) DEFAULT NULL,
                "TOTALNUMBER_1Y" number(18) DEFAULT NULL,
                "TOTALNUMBER_CY" number(18) DEFAULT NULL,
                "TOTALNUMBER_3Y" number(18) DEFAULT NULL,

                "UPDATETIME" date DEFAULT NULL,
                CONSTRAINT FUND_RANK_TERTILE_CODE UNIQUE (INNERCODE))
                """)


        cursor.execute("""
                CREATE SEQUENCE FUND_RANK_TERTILE_ID_SEQ START WITH 1 INCREMENT BY 1
                """)
        cursor.execute("""
                CREATE OR REPLACE TRIGGER FUND_RANK_TERTILE_ID
                BEFORE INSERT ON ZS_FUND_RANK_TERTILE
                FOR EACH ROW

                BEGIN
                SELECT FUND_RANK_TERTILE_ID_SEQ.NEXTVAL
                INTO :new.ID
                FROM dual;
                END;
                """)
        cursor.execute("""
                CREATE OR REPLACE TRIGGER FUND_RANK_TERTILE_UT
                BEFORE INSERT OR UPDATE ON ZS_FUND_RANK_TERTILE
                FOR EACH ROW

                BEGIN
                SELECT sysdate INTO :NEW.UpdateTime FROM dual;
                END;
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INNERCODE is '聚源私募基金代码'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ENDDATE is '估值日期'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNURETURN_1Y is '近一年年化绝对收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNURETURN_CY is '今年以来年化绝对收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNURETURN_3Y is '近三年年化绝对收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUEXRETURN_1Y is '近一年年化超额收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUEXRETURN_CY is '今年以来年化超额收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUEXRETURN_3Y is '近三年年化超额收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUVOL_1Y is '近一年年化波动率'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUVOL_CY is '今年以来年化波动率'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUVOL_3Y is '近三年年化波动率'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SHARPERATIO_1Y is '近一年年化夏普率'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SHARPERATIO_CY is '今年以来年化夏普率'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SHARPERATIO_3Y is '近三年年化夏普率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.MAXIMUMDRAWDOWN_1Y is '近一年最大回撤'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.MAXIMUMDRAWDOWN_CY is '今年以来最大回撤'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.MAXIMUMDRAWDOWN_3Y is '近三年最大回撤'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.BETA_1Y is '近一年贝塔'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.BETA_CY is '今年以来贝塔'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.BETA_3Y is '近三年贝塔'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUDOWNVOL_1Y is '近一年年化下行波动率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUDOWNVOL_CY is '今年以来年化下行波动率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUDOWNVOL_3Y is '近三年年化下行波动率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUTRACK_1Y is '近一年年化跟踪误差'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUTRACK_CY is '今年以来年化跟踪误差'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUTRACK_3Y is '近三年年化跟踪误差'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ACCURETURN_1Y is '近一年累计收益'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ACCURETURN_CY is '今年以来累计收益'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ACCURETURN_3Y is '近三年累计收益'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.JENSEN_1Y is '近一年年化詹森指数'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.JENSEN_CY is '今年以来年化詹森指数'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.JENSEN_3Y is '近三年年化詹森指数'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INFO_1Y is '近一年年化信息比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INFO_CY is '今年以来年化信息比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INFO_3Y is '近三年年化信息比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SORTINO_1Y is '近一年年化索提诺比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SORTINO_CY is '今年以来年化索提诺比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SORTINO_3Y is '近三年年化索提诺比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TREYNOR_1Y is '近一年年化特雷诺比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TREYNOR_CY is '今年以来年化特雷诺比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TREYNOR_3Y is '近三年年化特雷诺比率'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.M2_1Y is '近一年年化M2测度'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.M2_CY is '今年以来年化M2测度'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.M2_3Y is '近三年年化M2测度'
                        """)


        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNURETURN_1Y_RANK is '近一年年化绝对收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNURETURN_CY_RANK is '今年以来年化绝对收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNURETURN_3Y_RANK is '近三年年化绝对收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUEXRETURN_1Y_RANK is '近一年年化超额收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUEXRETURN_CY_RANK is '今年以来年化超额收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUEXRETURN_3Y_RANK is '近三年年化超额收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUVOL_1Y_RANK is '近一年年化波动率排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUVOL_CY_RANK is '今年以来年化波动率排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUVOL_3Y_RANK is '近三年年化波动率排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SHARPERATIO_1Y_RANK is '近一年年化夏普率排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SHARPERATIO_CY_RANK is '今年以来年化夏普率排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SHARPERATIO_3Y_RANK is '近三年年化夏普率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.MAXIMUMDRAWDOWN_1Y_RANK is '近一年最大回撤排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.MAXIMUMDRAWDOWN_CY_RANK is '今年以来最大回撤排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.MAXIMUMDRAWDOWN_3Y_RANK is '近三年最大回撤排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.BETA_1Y_RANK is '近一年贝塔排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.BETA_CY_RANK is '今年以来贝塔排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.BETA_3Y_RANK is '近三年贝塔排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUDOWNVOL_1Y_RANK is '近一年年化下行波动率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUDOWNVOL_CY_RANK is '今年以来年化下行波动率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUDOWNVOL_3Y_RANK is '近三年年化下行波动率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUTRACK_1Y_RANK is '近一年年化跟踪误差排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUTRACK_CY_RANK is '今年以来年化跟踪误差排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ANNUTRACK_3Y_RANK is '近三年年化跟踪误差排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ACCURETURN_1Y_RANK is '近一年累计收益排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ACCURETURN_CY_RANK is '今年以来累计收益排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.ACCURETURN_3Y_RANK is '近三年累计收益排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.JENSEN_1Y_RANK is '近一年年化詹森指数排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.JENSEN_CY_RANK is '今年以来年化詹森指数排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.JENSEN_3Y_RANK is '近三年年化詹森指数排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INFO_1Y_RANK is '近一年年化信息比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INFO_CY_RANK is '今年以来年化信息比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.INFO_3Y_RANK is '近三年年化信息比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SORTINO_1Y_RANK is '近一年年化索提诺比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SORTINO_CY_RANK is '今年以来年化索提诺比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.SORTINO_3Y_RANK is '近三年年化索提诺比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TREYNOR_1Y_RANK is '近一年年化特雷诺比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TREYNOR_CY_RANK is '今年以来年化特雷诺比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TREYNOR_3Y_RANK is '近三年年化特雷诺比率排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.M2_1Y_RANK is '近一年年化M2测度排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.M2_CY_RANK is '今年以来年化M2测度排名'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.M2_3Y_RANK is '近三年年化M2测度排名'
                        """)

        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TOTALNUMBER_1Y is '近一年有效基金总数'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TOTALNUMBER_CY is '今年以来有效基金总数'
                        """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_FUND_RANK_TERTILE.TOTALNUMBER_3Y is '近三年有效基金总数'
                        """)

        return {'if_success':'success'}


    finally:
        cursor.close()
'''




'''
def create_month_return_rank_database():

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()


    try:

        cursor.execute("""
                CREATE TABLE ZS_MONTH_RETURN_RANK(
                "ID" number(18) NOT NULL PRIMARY KEY,
                "INNERCODE" number(18) DEFAULT NULL,
                "MONTH" number(18) DEFAULT NULL,
                "ACCURR" number(18,8) DEFAULT NULL,
                "CALDATE" date DEFAULT NULL,
                "INSERTTIME" date DEFAULT NULL,
                "JSID" number(18) DEFAULT NULL,
                "RANK" number(18) DEFAULT NULL,
                "TOTAL" number(18) DEFAULT NULL,
                "UPDATETIME" date DEFAULT NULL,
                CONSTRAINT MONTH_RETURN_RANK_CODE_MONTH UNIQUE (INNERCODE,MONTH))
                """)


        cursor.execute("""
                CREATE SEQUENCE MONTH_RETURN_RANK_ID_SEQ START WITH 1 INCREMENT BY 1
                """)
        cursor.execute("""
                CREATE OR REPLACE TRIGGER MONTH_RETURN_RANK_ID
                BEFORE INSERT ON ZS_MONTH_RETURN_RANK
                FOR EACH ROW

                BEGIN
                SELECT MONTH_RETURN_RANK_ID_SEQ.NEXTVAL
                INTO :new.ID
                FROM dual;
                END;
                """)
        cursor.execute("""
                CREATE OR REPLACE TRIGGER MONTH_RETURN_RANK_UT
                BEFORE INSERT OR UPDATE ON ZS_MONTH_RETURN_RANK
                FOR EACH ROW

                BEGIN
                SELECT sysdate INTO :NEW.UpdateTime FROM dual;
                END;
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_MONTH_RETURN_RANK.INNERCODE is '聚源内部代码'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_MONTH_RETURN_RANK.MONTH is '日期'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_MONTH_RETURN_RANK.ACCURR is '累计收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_MONTH_RETURN_RANK.CALDATE is '计算日期'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_MONTH_RETURN_RANK.RANK is '月度累计收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_MONTH_RETURN_RANK.TOTAL is '月度总有效基金数'
                """)


        return {'if_success':'success'}


    finally:
        cursor.close()
'''



def month_return_rank_to_database(request_id,type):

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    result = get_month_return()

    data = calculate_month_rank(result)

    data = data.where(pd.notnull(data), None)

    try:

        values = list(zip(data['INNERCODE'],data['MONTH'],data['ACCURR'],data['CALDATE'],data['INSERTTIME'],data['JSID'],data['RANK'],data['TOTAL']))


        cursor.executemany(
                """BEGIN
                insert into ZS_MONTH_RETURN_RANK(INNERCODE,MONTH,ACCURR,CALDATE,INSERTTIME,JSID,RANK,TOTAL) values(:1,:2,:3,:4,:5,:6,:7,:8);
                EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                UPDATE ZS_MONTH_RETURN_RANK
                SET ACCURR=:3, CALDATE=:4, RANK=:7, TOTAL=:8
                WHERE INNERCODE=:1 AND MONTH=:2;
                END;
                """,values
                )
        db_connection.commit()

        return {'if_success':'success'}


    finally:
        cursor.close()



def get_month_return():

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    #the start_date is set in such a way that data 6 months before current month will also be included, in case previous returns have been updated
    current_date = pd.Timestamp.today()
    start_date = current_date - pd.Timedelta(6, unit='M')
    start_date = int(str(start_date)[:7].replace('-',''))

    try:
        cursor.execute(
                """
            select INNERCODE,MONTH,CALDATE,ACCURR,INSERTTIME,JSID from JYDB.SF_POFMONTHRETURN
            where MONTH >= %s

            """%(start_date)

                )


        month_return = cursor.fetchall()

        if  not month_return:

            return pd.DataFrame()


        month_return = pd.DataFrame(list(month_return), columns = ['INNERCODE', 'MONTH', 'CALDATE','ACCURR','INSERTTIME','JSID'])
        month_return['CALDATE'] = pd.to_datetime(month_return['CALDATE'])
        temp = month_return.groupby(['INNERCODE','MONTH'])['CALDATE'].max().reset_index()
        result = pd.merge(month_return,temp,how='right',on=['INNERCODE','MONTH','CALDATE'])

        return  result

    finally:
        cursor.close()



def calculate_month_rank(result):

    total = result.groupby('MONTH').apply(lambda x: len(x['INNERCODE'])).reset_index().rename(columns={0:'TOTAL'})

    result['RANK'] = result.groupby('MONTH')['ACCURR'].rank(method='min', ascending=False)

    result = pd.merge(result,total,how='left',on=['MONTH'])

    return result


'''
def create_year_return_rank_database():

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()


    try:

        cursor.execute("""
                CREATE TABLE ZS_YEAR_RETURN_RANK(
                "ID" number(18) NOT NULL PRIMARY KEY,
                "INNERCODE" number(18) DEFAULT NULL,
                "YEAR" number(18) DEFAULT NULL,
                "ACCUYIELD" number(18,8) DEFAULT NULL,
                "CALDATE" date DEFAULT NULL,
                "INSERTTIME" date DEFAULT NULL,
                "JSID" number(18) DEFAULT NULL,
                "RANK" number(18) DEFAULT NULL,
                "TOTAL" number(18) DEFAULT NULL,
                "UPDATETIME" date DEFAULT NULL,
                CONSTRAINT YEAR_RETURN_RANK_CODE_YEAR UNIQUE (INNERCODE,YEAR))
                """)


        cursor.execute("""
                CREATE SEQUENCE YEAR_RETURN_RANK_ID_SEQ START WITH 1 INCREMENT BY 1
                """)
        cursor.execute("""
                CREATE OR REPLACE TRIGGER YEAR_RETURN_RANK_ID
                BEFORE INSERT ON ZS_YEAR_RETURN_RANK
                FOR EACH ROW

                BEGIN
                SELECT YEAR_RETURN_RANK_ID_SEQ.NEXTVAL
                INTO :new.ID
                FROM dual;
                END;
                """)
        cursor.execute("""
                CREATE OR REPLACE TRIGGER YEAR_RETURN_RANK_UT
                BEFORE INSERT OR UPDATE ON ZS_YEAR_RETURN_RANK
                FOR EACH ROW

                BEGIN
                SELECT sysdate INTO :NEW.UpdateTime FROM dual;
                END;
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_YEAR_RETURN_RANK.INNERCODE is '聚源内部代码'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_YEAR_RETURN_RANK.YEAR is '日期'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_YEAR_RETURN_RANK.ACCUYIELD is '累计收益'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_YEAR_RETURN_RANK.CALDATE is '计算日期'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_YEAR_RETURN_RANK.RANK is '年度累计收益排名'
                """)
        cursor.execute("""
                COMMENT ON COLUMN ZS_YEAR_RETURN_RANK.TOTAL is '年度总有效基金数'
                """)


        return {'if_success':'success'}


    finally:
        cursor.close()
'''




def year_return_rank_to_database(request_id,type):

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    result = get_year_return()

    data = calculate_year_rank(result)

    data = data.where(pd.notnull(data), None)

    try:

        values = list(zip(data['INNERCODE'],data['YEAR'],data['ACCUYIELD'],data['CALDATE'],data['INSERTTIME'],data['JSID'],data['RANK'],data['TOTAL']))


        cursor.executemany(
                """BEGIN
                insert into ZS_YEAR_RETURN_RANK(INNERCODE,YEAR,ACCUYIELD,CALDATE,INSERTTIME,JSID,RANK,TOTAL) values(:1,:2,:3,:4,:5,:6,:7,:8);
                EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                UPDATE ZS_YEAR_RETURN_RANK
                SET ACCUYIELD=:3, CALDATE=:4, RANK=:7, TOTAL=:8
                WHERE INNERCODE=:1 AND YEAR=:2;
                END;
                """,values
                )
        db_connection.commit()

        return {'if_success':'success'}


    finally:
        cursor.close()



def get_year_return():

    db_connection = database_injector.get_resource()
    cursor = db_connection.cursor()
    #conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '47.100.200.178:1521/helowin', encoding = 'UTF-8')
    #cursor = conn.cursor()

    #the start_date is set in such a way that data from the year before current year will also be included, in case previous-year returns are updated
    current_date = pd.Timestamp.today()
    start_date = current_date - pd.Timedelta(1, unit='Y')
    start_date = int(str(start_date)[:4])

    try:
        cursor.execute(
                """
            SELECT INNERCODE,YEARS,CALDATE,ACCUYIELD,INSERTTIME,JSID  FROM JYDB.SF_POFYEARRETURN
            WHERE YEARS >= %s
            """%(start_date)

                )


        year_return = cursor.fetchall()

        if  not year_return:

            return pd.DataFrame()


        year_return = pd.DataFrame(list(year_return), columns = ['INNERCODE', 'YEAR', 'CALDATE','ACCUYIELD','INSERTTIME','JSID'])
        year_return['CALDATE'] = pd.to_datetime(year_return['CALDATE'])

        #there mgiht be multiple calculations for the same INNERCODE in a same YEAR. only the one with the latest CALDATE is kept
        temp = year_return.groupby(['INNERCODE','YEAR'])['CALDATE'].max().reset_index()
        result = pd.merge(year_return,temp,how='right',on=['INNERCODE','YEAR','CALDATE'])

        return  result

    finally:
        cursor.close()



def calculate_year_rank(result):

    total = result.groupby('YEAR').apply(lambda x: len(x['INNERCODE'])).reset_index().rename(columns={0:'TOTAL'})

    result['RANK'] = result.groupby('YEAR')['ACCUYIELD'].rank(method='min', ascending=False)

    result = pd.merge(result,total,how='left',on=['YEAR'])

    return result
