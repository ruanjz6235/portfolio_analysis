#%%
# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime
import cx_Oracle as cx
import app.common.log as logger


def get_clients_period_return(client_ids, start_date, end_date):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """
    select client_id, init_date, unit_nv, daily_return FROM zdj.hv_return where client_id in %s
    and init_date >= to_date('%s', 'yyyy-mm-dd') and init_date <= to_date('%s', 'yyyy-mm-dd')
    order by client_id, init_date asc""" % (str(tuple(client_ids)).replace(',)', ')'), start_date, end_date)
    return pd.read_sql(query, conn).rename(columns={'CLIENT_ID': 'client_id', 'INIT_DATE': 'EndDate',
                                                    'UNIT_NV': 'client', 'DAILY_RETURN': 'client_return'})


def get_scenarios_return(request_id, type, client_id, start_date, end_date, index_code, scenario_type):
    if len(end_date) == 0:
        return {}
    scenarios = get_fund_scenarios(start_date, end_date, scenario_type)
    if scenarios.empty:
        start, end = start_date, end_date
    else:
        scenarios['Start_Date'] = scenarios['Start_Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        scenarios['End_Date'] = scenarios['End_Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        start, end = scenarios['Start_Date'].min(), scenarios['End_Date'].max()
    fund_return = get_clients_period_return([client_id], start_date, end_date)
    if fund_return.empty:
        return {}
    fund_return['EndDate'] = fund_return['EndDate'].apply(lambda x: x.strftime('%Y-%m-%d'))
    fund_start, fund_end = fund_return['EndDate'].iloc[0], fund_return['EndDate'].iloc[-1]
    fund_return['client'] = fund_return['client'] / fund_return['client'].iloc[0] - 1
    fund_cum_return = fund_return[['EndDate', 'client']]
    # index_codes = ['000300', '000016', '000905', '000985', 'H11001']
    index_start, index_end = fund_cum_return['EndDate'].iloc[0], fund_cum_return['EndDate'].iloc[-1]
    # for index_code in index_codes:
    index_return = get_index_return(index_code, index_start, index_end)
    index_cum_return = index_return[['EndDate', 'index']]
    fund_cum_return = pd.merge(fund_cum_return, index_cum_return, on='EndDate', how='inner')
    fund_index_cum_return = fund_cum_return.ffill().bfill().sort_values('EndDate').set_index('EndDate')
    if fund_index_cum_return.empty:
        result = {'fund_index_cum_return': pd.DataFrame(),
                  'scenarios_return': pd.DataFrame()}
        return result
    scenarios = scenarios[(scenarios['End_Date'] >= fund_start) & (scenarios['Start_Date'] <= fund_end)]
    scenarios_return = []
    if scenarios.empty:
        scenarios_return = []
    else:
        for start in scenarios['Start_Date']:
            scenario = scenarios[scenarios['Start_Date'] == start]
            end, scenario_name = scenario['End_Date'].iloc[0], scenario['Scenario'].iloc[0]
            period_return = fund_index_cum_return[(fund_index_cum_return.index >= start)
                                                  & (fund_index_cum_return.index <= end)] + 1
            scenario_return = period_return.iloc[-1] / period_return.iloc[0] - 1
            scenario_return['start_date'] = period_return.index[0]
            scenario_return['end_date'] = period_return.index[-1]
            scenario_return = pd.DataFrame(scenario_return, columns=[scenario_name]).T.reset_index()
            scenarios_return.append(scenario_return)
        scenarios_return = pd.concat(scenarios_return).reset_index(drop=True)
    fund_index_cum_return = fund_index_cum_return.reset_index()
    result = {'fund_index_cum_return': fund_index_cum_return,
              'scenarios_return': [dict(scenarios_return.iloc[i]) for i in range(len(scenarios_return))]}
    return result


def get_fund_scenarios(start_date, end_date, scenario_type):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """SELECT Scenario_Name, Start_Date, End_Date FROM ZS_FUND_SCENARIOS WHERE scenario_type = {i}
    AND End_Date >= to_date('{sd}', 'yyyy-mm-dd') AND Start_Date <= to_date('{ed}', 'yyyy-mm-dd')
    AND scenario_status = 'active' ORDER BY Start_Date asc""".format(i=scenario_type, sd=start_date, ed=end_date)
    return pd.read_sql(query, conn).rename(columns={'SCENARIO_NAME': 'Scenario', 'START_DATE': 'Start_Date',
                                                    'END_DATE': 'End_Date'})


def get_index_return(index_code, start_date, end_date):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select TradingDay, ClosePrice from zdj.ZJ_INDEX_DAILY_QUOTE where SecuCode = '{code}'
    and TradingDay >= to_date('{sd}', 'yyyy-mm-dd') and TradingDay <= to_date('{ed}', 'yyyy-mm-dd')
    order by TradingDay asc""".format(code=index_code, sd=start_date, ed=end_date)
    index_return = pd.read_sql(query, conn).rename(columns={'TRADINGDAY': 'EndDate'})
    index_return['index_return'] = index_return['CLOSEPRICE'] / index_return['CLOSEPRICE'].shift(1)
    index_return['index'] = index_return['CLOSEPRICE'] / index_return['CLOSEPRICE'].iloc[0] - 1
    index_return['EndDate'] = index_return['EndDate'].apply(lambda x: x.strftime('%Y-%m-%d'))
    return index_return[['EndDate', 'index', 'index_return']]



