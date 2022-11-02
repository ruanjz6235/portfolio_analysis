import pandas as pd
import numpy as np
import cx_Oracle as cx
import datetime as dt
from dateutil.relativedelta import relativedelta
from .hv_return_new import get_dynamic_drawdown
import statsmodels.api as sm
from pyhive import hive
from .hv_return_new import get_client_id
conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
cursor = conn.cursor()


# 滚动收益，滚动最大回撤，滚动日波动率
def get_index_return(index_code, start, end):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select i.SecuCode, i.TradingDay, i.ClosePrice from ZDJ.ZJ_INDEX_DAILY_QUOTE i
    inner join JYDB.SecuMain SM on i.InnerCode = SM.InnerCode where i.SeCuCode = '{code}' and SM.SecuCategory = 4
    and i.TradingDay >= to_date('{start}', 'yyyy-mm-dd') and i.TradingDay <= to_date('{end}', 'yyyy-mm-dd')
    """.format(code=index_code, start=start, end=end)
    cursor.execute(query)
    index_return = cursor.fetchall()
    index_return = pd.DataFrame(list(index_return), columns=['SecuCode', 'EndDate', 'ClosePrice'])
    index_return['index_return'] = (index_return['ClosePrice']/index_return['ClosePrice'].shift(1)).fillna(1) - 1
    return index_return[['EndDate', 'index_return']]


def get_calendar(x, freq):
    if freq != 'daily':
        if freq == 'monthly':
            calendar = x.strftime('%Y-%m')
        elif freq == 'quarter':
            calendar = str(x.year) + '-' + ('' if len(str(3*x.quarter)) == 2 else '0') + str(3*x.quarter)
        elif freq == 'weekly':
            calendar = str(x.year) + '-' + ('' if len(str(x.isocalendar()[1])) == 2 else '0') + str(x.isocalendar()[1])
        else:
            calendar = str(x.year)
    else:
        calendar = x.strftime('%Y-%m-%d')
    return calendar


def get_index_freq_data(index_code, start, end, freq, data_type):
    if freq not in ['daily', 'weekly']:
        if freq == 'monthly':
            start, end = start + '-01', (pd.to_datetime(end + '-01') + pd.offsets.MonthEnd()).strftime('%Y-%m-%d')
        elif freq == 'quarter':
            start, end = (pd.to_datetime(start + '-01') - pd.DateOffset(months=2)).strftime('%Y-%m-%d'), \
                         (pd.to_datetime(end + '-01') + pd.offsets.MonthEnd()).strftime('%Y-%m-%d')
        else:
            start, end = start + '-01-01', end + '-12-31'
    index_return = get_index_return(index_code, start, end)
    if index_return.empty:
        return pd.DataFrame(columns=['calendar', 'index_return'])
    else:
        index_return['calendar'] = index_return['EndDate'].apply(lambda x: get_calendar(x, freq))
        if data_type == 'return_type':
            index_freq_return = index_return.groupby('calendar')['index_return'].apply(lambda x: (x+1).prod()-1)
        elif data_type == 'mdd_type':
            index_return = index_return.rename(columns={'EndDate': 'init_date'})
            index_freq_return = index_return.groupby('calendar')[['init_date', 'index_return']].apply(get_return_mdd)
            index_freq_return = index_freq_return.apply(lambda x: x[0]).rename('index_return')
        else:                 # data_type == 'vol_type'
            index_freq_return = index_return.groupby('calendar')['index_return'].std().rename('index_return')
        return index_freq_return.reset_index()


def get_monthly_data(request_id, type, client_id, index_code, start_date, end_date, freq, data_type):
    if len(end_date) == 0:
        return pd.DataFrame()
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select client_id, init_date, daily_return from hv_return where client_id = '{client}'
    and init_date >= to_date('{start}', 'yyyy-mm-dd') and init_date <= to_date('{end}', 'yyyy-mm-dd')
    and daily_return is not null""".format(client=client_id, start=start_date, end=end_date)
    client_return = pd.read_sql(query, conn).rename(columns={'CLIENT_ID': 'client_id', 'INIT_DATE': 'init_date',
                                                             'DAILY_RETURN': 'daily_return'})
    if client_return.empty:
        result = pd.DataFrame()
    else:
        if data_type == 'return_type':
            client_monthly_return = get_clients_freq_data(client_return, freq, data_type)[['calendar', 'client_return']]
            start, end = client_monthly_return['calendar'].min(), client_monthly_return['calendar'].max()
            index_monthly_return = get_index_freq_data(index_code, start, end, freq, data_type)
        elif data_type == 'vol_type':
            client_monthly_return = get_clients_freq_data(client_return, freq, data_type)[['calendar', 'client_return']]
            start, end = client_monthly_return['calendar'].min(), client_monthly_return['calendar'].max()
            index_monthly_return = get_index_freq_data(index_code, start, end, freq, data_type)
        else:                      # data_type == 'mdd_type'
            client_monthly_return = get_clients_freq_data(client_return, freq, data_type)[['calendar', 'client_return']]
            start, end = client_monthly_return['calendar'].min(), client_monthly_return['calendar'].max()
            index_monthly_return = get_index_freq_data(index_code, start, end, freq, data_type)
        client_monthly_return = client_monthly_return.dropna()
        monthly_return = client_monthly_return.merge(index_monthly_return, on=['calendar'], how='inner')
        return_distribution = get_monthly_distribution(monthly_return)
        return_distribution_zero = get_monthly_return_distribution_by_zero(monthly_return)
        result = {'monthly_return': monthly_return, 'return_distribution': return_distribution,
                  'return_distribution_by_zero': return_distribution_zero}
    return result


def get_clients_freq_data(client_return, freq, data_type):
    client_return['calendar'] = client_return['init_date'].apply(lambda x: get_calendar(x, freq))
    if data_type == 'return_type':
        client_monthly = client_return.groupby('calendar')['daily_return'].apply(lambda x: (x+1).prod()-1)
    elif data_type == 'mdd_type':
        client_return = client_return.rename(columns={'EndDate': 'init_date'})
        client_monthly = client_return.groupby('calendar')[['init_date', 'daily_return']].apply(get_return_mdd)
        client_monthly = client_monthly.apply(lambda x: x[0])
    else:
        client_monthly = client_return.groupby('calendar')['daily_return'].std()
    return client_monthly.rename('client_return').reset_index()


def get_monthly_distribution(client_index_monthly, num_bin=8):
    client_monthly = (100 * client_index_monthly['client_return']).tolist()
    index_monthly = (100 * client_index_monthly['index_return']).tolist()
    monthly = client_monthly + index_monthly
    bin_range = np.int((np.int(np.abs(np.min(monthly)) + np.abs(np.max(monthly))) + 1) / num_bin) + 1
    bin_lims = np.array([np.int(np.min(monthly)) - 1 + bin_range * i for i in np.arange(num_bin + 1)])
    hist1_num, _ = np.histogram(client_monthly, bins=bin_lims)
    hist2_num, _ = np.histogram(index_monthly, bins=bin_lims)
    hist1 = hist1_num / hist1_num.sum()
    hist2 = hist2_num / hist2_num.sum()
    for i in np.arange(num_bin) + 1:
        if hist1[num_bin - i] != 0 or hist2[num_bin - i] != 0:
            break
    if i != 1:
        hist1 = hist1[: - i + 1]
        hist2 = hist2[: - i + 1]
        hist1_num = hist1_num[: - i + 1]
        hist2_num = hist2_num[: - i + 1]
        bin_lims = bin_lims[: - i + 1]
    returns = pd.DataFrame({'fund_return_distribution': list(hist1), 'index_return_distribution': list(hist2),
                            'fund_return_num': list(hist1_num), 'index_return_num': list(hist2_num)})
    bin_lims = pd.DataFrame({'bin_lims': list(bin_lims / 100)})
    return_distribution = {'return_distribution': returns, 'bin_lims': bin_lims}
    return return_distribution


def get_monthly_return_distribution_by_zero(fund_index_return):
    fund_lower_than_zero_num = len(
        fund_index_return[fund_index_return['client_return'] <= 0])
    fund_higher_than_zero_num = len(
        fund_index_return[fund_index_return['client_return'] > 0])
    index_lower_than_zero_num = len(
        fund_index_return[fund_index_return['index_return'] <= 0])
    index_higher_than_zero_num = len(
        fund_index_return[fund_index_return['index_return'] > 0])
    total_num = len(fund_index_return)
    fund_lower_than_zero_ratio = fund_lower_than_zero_num / total_num
    fund_higher_than_zero_ratio = fund_higher_than_zero_num / total_num
    index_lower_than_zero_rato = index_lower_than_zero_num / total_num
    index_higher_than_zero_ratio = index_higher_than_zero_num / total_num
    return_distribution_by_zero = pd.DataFrame(
        {'用户小于0月份数': [fund_lower_than_zero_num],
         '用户大于0月份数': [fund_higher_than_zero_num],
         '指数小于0月份数': [index_lower_than_zero_num],
         '指数大于0月份数': [index_higher_than_zero_num],
         '用户小于0比例': [fund_lower_than_zero_ratio],
         '用户大于0比例': [fund_higher_than_zero_ratio],
         '指数小于0比例': [index_lower_than_zero_rato],
         '指数大于0比例': [index_higher_than_zero_ratio]})
    return return_distribution_by_zero


#################################################################################################################
# 区间收益,区间最大回撤
def get_interval_data(request_id, type, client_id, index_code, end_date, data_type):
    if len(end_date) == 0:
        return pd.DataFrame()
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select init_date, daily_return from hv_return where client_id = '{client}'
    and init_date <= to_date('{end}', 'yyyy-mm-dd')""".format(client=client_id, end=end_date)
    client_return = pd.read_sql(query, conn).rename(columns={'INIT_DATE': 'EndDate', 'DAILY_RETURN': 'client_return'})
    if client_return.empty:
        return pd.DataFrame()
    start_date = client_return['EndDate'].min().strftime('%Y-%m-%d')
    index_return = get_index_return(index_code, start_date, end_date)
    client_index_return = client_return.merge(index_return, on='EndDate', how='outer')

    end_date = pd.to_datetime(end_date)
    fund_end_date = client_return.iloc[-1]['EndDate']
    end_date = min(end_date, fund_end_date)
    client_index_return = client_index_return[client_index_return['EndDate'] <= end_date]
    year_start = pd.to_datetime(dt.datetime(end_date.year, 1, 1, 0, 0))
    one_month_before = end_date + relativedelta(months=-1)
    three_months_before = end_date + relativedelta(months=-3)
    six_months_before = end_date + relativedelta(months=-6)
    one_year_before = end_date + relativedelta(years=-1)
    two_years_before = end_date + relativedelta(years=-2)

    start_names = ['year_start', 'one_month', 'three_months', 'six_months', 'one_year', 'two_years', 'since_founded']
    start_list = [year_start, one_month_before, three_months_before, six_months_before, one_year_before,
                  two_years_before, start_date]
    interval_returns = list()
    for start_name, start in zip(start_names, start_list):
        interval_return = client_index_return[client_index_return['EndDate'] >= start]
        time_start = interval_return['EndDate'].min()
        interval_return.loc[interval_return['EndDate'] == time_start, ['client_return', 'index_return']] = 0
        if interval_return.empty:
            interval_returns.append([start_name] + [np.nan, np.nan])
            continue
        interval_return = interval_return.fillna(0)
        if data_type == 'return_type':
            interval_return = ((interval_return[['client_return', 'index_return']] + 1).prod() - 1).to_list()
        else:
            interval_return = get_return_mdd(interval_return[['EndDate', 'client_return', 'index_return']].rename(
                columns={'EndDate': 'init_date'}))
        interval_returns.append([start_name] + interval_return)
    interval_returns = pd.DataFrame(interval_returns, columns=['intervals', 'client', 'index'])
    return {'interval_returns': interval_returns, 'start': start_date}


##################################################################################################################
# 时段接口
def get_fund_period(request_id, type, client_id):
    whole_period = get_fund_return_whole_period(client_id)
    start_date = whole_period['start_date'].iloc[0]
    end_date = whole_period['end_date'].iloc[0]
    if end_date:
        year_start = max(pd.to_datetime(dt.datetime(end_date.year, 1, 1, 0, 0)), start_date).strftime('%Y-%m-%d')
        one_month = max(end_date + relativedelta(months=-1), start_date).strftime('%Y-%m-%d')
        three_month = max(end_date + relativedelta(months=-3), start_date).strftime('%Y-%m-%d')
        six_month = max(end_date + relativedelta(months=-6), start_date).strftime('%Y-%m-%d')
        one_year = max(end_date + relativedelta(years=-1), start_date).strftime('%Y-%m-%d')
        two_years = max(end_date + relativedelta(years=-2), start_date).strftime('%Y-%m-%d')
        fund_periods = pd.DataFrame(
            data=[[start_date.strftime('%Y-%m-%d'), year_start, one_month, three_month, six_month, one_year, two_years],
                  [end_date.strftime('%Y-%m-%d')] * 7],
            index=['start_date', 'end_date'],
            columns=['since_founded', 'year_start', 'one_month', 'three_months', 'six_months', 'one_year', 'two_years'])
    else:
        fund_periods = pd.DataFrame(
            data=[[np.nan] * 7, [np.nan] * 7], index=['start_date', 'end_date'],
            columns=['since_founded', 'year_start', 'one_month', 'three_months', 'six_months', 'one_year', 'two_years'])
    return {key: dict(value) for key, value in dict(fund_periods).items()}


def get_fund_return_whole_period(client_id):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """
    SELECT min(init_date) as start_date, max(init_date) as end_date FROM hv_return
    WHERE client_id = '{client_id}'""".format(client_id=client_id)
    fund_return_whole_period = pd.read_sql(query, conn).rename(columns={'START_DATE': 'start_date',
                                                                        'END_DATE': 'end_date'})
    return fund_return_whole_period


#################################################################################################################
# 区间最大回撤
def get_return_mdd(assets_return):
    mdd = get_dynamic_drawdown(assets_return).set_index('init_date').min().to_list()
    return [-x for x in mdd]


#################################################################################################################
# 收益风险指标
def get_hv_return_and_risk_indicators(request_id, type, client_id, index_code, start_date, end_date, freq):
    if len(end_date) == 0:
        return pd.DataFrame()
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select init_date, daily_return from hv_return where client_id = '{client}'
    and init_date >= to_date('{start}', 'yyyy-mm-dd') and init_date <= to_date('{end}', 'yyyy-mm-dd')
    and daily_return is not null""".format(client=client_id, start=start_date, end=end_date)
    client_return = pd.read_sql(query, conn).rename(columns={'INIT_DATE': 'init_date', 'DAILY_RETURN': 'daily_return'})
    if client_return.empty:
        return pd.DataFrame()
    start_date, end_date = client_return['init_date'].min(), client_return['init_date'].max()
    asset_return = get_clients_freq_data(client_return, freq, 'return_type')[['calendar', 'client_return']]
    if freq == 'weekly':
        start = (start_date - pd.Timedelta(days=start_date.isocalendar()[2])).strftime('%Y-%m-%d')
        end = (end_date + pd.Timedelta(days=7-start_date.isocalendar()[2])).strftime('%Y-%m-%d')
    else:
        start, end = asset_return['calendar'].min(), asset_return['calendar'].max()
    index_return = get_index_freq_data(index_code, start, end, freq, 'return_type')
    if len(asset_return) > 0 and len(index_return) > 0:
        client_index_monthly = pd.merge(asset_return, index_return, on='calendar', how='inner')
        asset_return_merged_array = client_index_monthly['client_return']
        index_return_merged_array = client_index_monthly['index_return']
    else:
        asset_return_merged_array = pd.Series()
        index_return_merged_array = pd.Series()

    if len(asset_return) > 0:
        asset_return_array = asset_return['client_return']
    else:
        asset_return_array = pd.DataFrame()
    if len(index_return) > 0:
        index_return_array = index_return['index_return']
    else:
        index_return_array = pd.DataFrame()
    # 区间收益率
    if freq != 'daily':
        asset_daily_return = client_return[['init_date', 'daily_return']].copy().rename(
            columns={'daily_return': 'client_return'}).set_index('init_date')
        index_daily_return = get_index_return(
            index_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        ).rename(columns={'EndDate': 'init_date'}).set_index('init_date')
    else:
        asset_daily_return, index_daily_return = asset_return.copy(), index_return.copy()
        asset_daily_return['calendar'] = pd.to_datetime(asset_daily_return['calendar'])
        index_daily_return['calendar'] = pd.to_datetime(index_daily_return['calendar'])
        asset_daily_return = asset_daily_return.rename(columns={'calendar': 'init_date'}).set_index('init_date')
        index_daily_return = index_daily_return.rename(columns={'calendar': 'init_date'}).set_index('init_date')
    asset_daily_return.iloc[0], index_daily_return.iloc[0] = 0, 0
    asset_interval_return = (asset_daily_return['client_return'] + 1).prod() - 1
    index_interval_return = (index_daily_return['index_return'] + 1).prod() - 1
    if len(asset_daily_return) > 2:
        start_date = asset_daily_return.index.sort_values()[0]
        end_date = asset_daily_return.index.sort_values()[-1]
        day_num = (end_date - start_date).days
        # 年化收益率
        asset_annualized_return = (1 + asset_interval_return) ** (1 / (day_num / 365)) - 1
        index_annualized_return = (1 + index_interval_return) ** (1 / (day_num / 365)) - 1
    else:
        asset_annualized_return = np.nan
        index_annualized_return = np.nan
    asset_daily_return, index_daily_return = asset_daily_return.reset_index(), index_daily_return.reset_index()
    # alpha, beta
    asset_alpha, asset_beta = calculate_alpha_and_beta(
        asset_return_merged_array, index_return_merged_array, freq)
    index_alpha = 0
    index_beta = 1
    # 夏普比率
    asset_sharpe_ratio = calculate_sharpe_ratio(asset_return_array, freq)
    index_sharpe_ratio = calculate_sharpe_ratio(index_return_array, freq)
    # 信息比率
    asset_information_ratio = calculate_information_ratio(
        asset_return_merged_array, index_return_merged_array, freq)
    index_information_ratio = np.nan
    # 索提诺比率
    asset_sortino_ratio = calculate_sortino_ratio(asset_return_array, freq)
    index_sortino_ratio = calculate_sortino_ratio(index_return_array, freq)
    # 特雷诺比率
    asset_treynor_ratio = calculate_treynor_ratio(
        asset_return_merged_array, index_return_merged_array, freq)
    index_treynor_ratio = index_annualized_return - 0.03
    # m2
    asset_m2 = calculate_m2(asset_return_array, index_return_array, freq)
    index_m2 = index_annualized_return
    # 胜率
    asset_winning_rate = calculate_winning_rate(
        asset_return_merged_array, index_return_merged_array)
    index_winning_rate = 0
    # 最大回撤
    if not asset_daily_return.empty:
        asset_maximum_drawdown = get_return_mdd(asset_daily_return)[0]
    else:
        asset_maximum_drawdown = np.nan
    if not index_daily_return.empty:
        index_maximum_drawdown = get_return_mdd(index_daily_return)[0]
    else:
        index_maximum_drawdown = np.nan
    # 詹森指数
    asset_jensens_alpha = calculate_calmar_ratio(asset_annualized_return, asset_maximum_drawdown)
    index_jensens_alpha = calculate_calmar_ratio(index_annualized_return, index_maximum_drawdown)
    
    asset_maximum_drawdown_months_number = calculate_maximum_drawdown_month_number(asset_daily_return)
    index_maximum_drawdown_months_number = calculate_maximum_drawdown_month_number(index_daily_return)

    asset_maximum_drawdown_recovery_date = calculate_maximum_drawdown_recovery_date(asset_daily_return)
    index_maximum_drawdown_recovery_date = calculate_maximum_drawdown_recovery_date(index_daily_return)

    asset_mdd_recovery_months_num = calculate_maximum_drawdown_recovery_months_number(asset_daily_return)
    index_mdd_recovery_months_num = calculate_maximum_drawdown_recovery_months_number(index_daily_return)

    asset_annualized_volatility = calculate_annualized_volatility(asset_return_array, freq)
    index_annualized_volatility = calculate_annualized_volatility(index_return_array, freq)

    asset_downside_volatility = calculate_downside_volatility(asset_return_array, freq)
    index_downside_volatility = calculate_downside_volatility(index_return_array, freq)

    asset_tracking_error = calculate_tracking_error(asset_return_merged_array, index_return_merged_array, freq)
    index_tracking_error = 0

    asset_historical_var = calculate_historical_var(asset_return_array, cutoff=0.05)
    index_historical_var = calculate_historical_var(index_return_array, cutoff=0.05)

    asset_cvar = calculate_conditional_var(asset_return_array, cutoff=0.05)
    index_cvar = calculate_conditional_var(index_return_array, cutoff=0.05)

    return_and_risk_indicators = pd.DataFrame(
        {'item': ['asset', 'index'],
         'interval_return': [asset_interval_return, index_interval_return],
         'annualized_return': [asset_annualized_return, index_annualized_return],
         'alpha': [asset_alpha, index_alpha],
         'sharpe_ratio': [asset_sharpe_ratio, index_sharpe_ratio],
         'information_ratio': [asset_information_ratio, index_information_ratio],
         'sortino_ratio': [asset_sortino_ratio, index_sortino_ratio],
         'treynor_ratio': [asset_treynor_ratio, index_treynor_ratio],
         'jensens_alpha': [asset_jensens_alpha, index_jensens_alpha],
         'm2': [asset_m2, index_m2],
         'winning_rate': [asset_winning_rate, index_winning_rate],
         'maximum_drawdown': [asset_maximum_drawdown, index_maximum_drawdown],
         'maximum_drawdown_months_number': [asset_maximum_drawdown_months_number, index_maximum_drawdown_months_number],
         'maximum_drawdown_recovery_date': [asset_maximum_drawdown_recovery_date, index_maximum_drawdown_recovery_date],
         'maximum_drawdown_recovery_months_number': [asset_mdd_recovery_months_num, index_mdd_recovery_months_num],
         'annualized_volatility': [asset_annualized_volatility, index_annualized_volatility],
         'beta': [asset_beta, index_beta],
         'downside_volatility': [asset_downside_volatility, index_downside_volatility],
         'tracking_error': [asset_tracking_error, index_tracking_error],
         'historical_var': [asset_historical_var, index_historical_var],
         'historical_cvar': [asset_cvar, index_cvar]})
    return_and_risk_indicators['maximum_drawdown_recovery_date'] = \
        return_and_risk_indicators['maximum_drawdown_recovery_date'].fillna(
            pd.to_datetime('1900-01-01'))
    return return_and_risk_indicators


def calculate_alpha_and_beta(asset_return, index_return, freq):
    if len(asset_return) > 1 and len(index_return) > 1:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        rf = 0.03 / multiplier
        y = asset_return - rf
        x = index_return - rf
        x = sm.add_constant(x)
        model = sm.OLS(y, x).fit()
        alpha = model.params[0]
        alpha = np.exp(multiplier * alpha) - 1
        if len(y) > 1:
            beta = model.params[1]
        else:
            beta = np.nan
    else:
        alpha = np.nan
        beta = np.nan
    return alpha, beta


def calculate_sharpe_ratio(asset_return, freq):
    if not asset_return.empty:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        annualized_return = multiplier * asset_return.mean()
        annualized_return = np.exp(annualized_return) - 1
        annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
        sharpe_ratio = (annualized_return - 0.03) / annualized_vol
    else:
        sharpe_ratio = np.nan
    return sharpe_ratio

    
def calculate_calmar_ratio(asset_return, mdd):
    if not np.isnan(mdd):
        sharpe_ratio = (asset_return - 0.03) / mdd
    else:
        sharpe_ratio = np.nan
    return sharpe_ratio


def calculate_information_ratio(asset_return, index_return, freq):
    if len(asset_return) > 0 and len(index_return) > 0:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        active_return = asset_return - index_return
        tracking_error = (active_return.std(ddof=1)) * np.sqrt(multiplier)
        asset_annualized_return = multiplier * asset_return.mean()
        index_annualized_return = multiplier * index_return.mean()
        information_ratio = (asset_annualized_return
                             - index_annualized_return) / tracking_error
    else:
        information_ratio = np.nan
    return information_ratio


def calculate_sortino_ratio(asset_return, freq):
    if not asset_return.empty:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        target_return = 0.03 / multiplier
        downside_return = asset_return - target_return
        downside_return[downside_return > 0] = 0
        downside_volatility = downside_return.std(ddof=1) * np.sqrt(multiplier)
        annualized_return = multiplier * asset_return.mean()
        sortino_ratio = (annualized_return - 0.03) / downside_volatility
    else:
        sortino_ratio = np.nan
    return sortino_ratio


def calculate_treynor_ratio(asset_return, index_return, freq):
    if len(asset_return) > 1 and len(index_return) > 1:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        rf = 0.03 / multiplier
        if len(asset_return[asset_return.values != 0]) == 0:
            return np.nan
        y = asset_return - rf
        x = index_return - rf
        x = sm.add_constant(x)
        model = sm.OLS(y, x).fit()
        beta = model.params[1]
        annualized_return = multiplier * asset_return.mean()
        treynor_ratio = (annualized_return - 0.03) / beta
    else:
        treynor_ratio = np.nan
    return treynor_ratio


def calculate_jensens_alpha(asset_return, index_return, freq):
    if len(asset_return) > 1 and len(index_return) > 1:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        rf = 0.03 / multiplier
        y = asset_return - rf
        x = index_return - rf
        x = sm.add_constant(x)
        model = sm.OLS(y, x).fit()
        beta = model.params[1]
        asset_annualized_return = multiplier * asset_return.mean()
        index_annualized_return = multiplier * index_return.mean()
        jensens_alpha = asset_annualized_return \
                        - (0.03 + beta * (index_annualized_return - 0.03))
    else:
        jensens_alpha = np.nan
    return jensens_alpha


def calculate_m2(asset_return, index_return, freq):
    if len(asset_return) > 0 and len(index_return) > 0:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        asset_annualized_return = multiplier * asset_return.mean()
        asset_annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
        index_annualized_vol = index_return.std(ddof=1) * np.sqrt(multiplier)
        m2 = 0.03 + (asset_annualized_return - 0.03) * index_annualized_vol \
             / asset_annualized_vol
    else:
        m2 = np.nan
    return m2


def calculate_winning_rate(asset_return, index_return):
    if not asset_return.empty:
        return_diff = asset_return - index_return
        winning_rate = len(return_diff[return_diff > 0]) / len(return_diff)
    else:
        winning_rate = np.nan
    return winning_rate


def calculate_maximum_drawdown(asset_return):
    if not asset_return.empty:
        asset_return = asset_return.set_index('EndDate')
        running_max = np.maximum.accumulate(asset_return.cumsum())
        underwater = asset_return.cumsum() - running_max
        underwater = np.exp(underwater) - 1
        mdd = -underwater.min().values[0]
    else:
        mdd = np.nan
    return mdd


def calculate_maximum_drawdown_month_number(asset_return):
    if len(asset_return) > 1:
        asset_return = asset_return.set_index('init_date')
        asset_return = asset_return.dropna(axis=0, how='any')
        running_max = np.maximum.accumulate(asset_return.cumsum())
        underwater = asset_return.cumsum() - running_max
        underwater = np.exp(underwater) - 1
        valley = underwater.idxmin().iloc[0]
        peak = underwater[:valley][underwater[:valley] == 0].dropna().index[-1]
        month_diff = valley.month - peak.month
        year_diff = valley.year - peak.year
        period_diff = max(12 * year_diff + month_diff, 0)
    else:
        period_diff = np.nan
    return period_diff


def calculate_maximum_drawdown_recovery_date(asset_return):
    if len(asset_return) > 1:
        asset_return = asset_return.set_index('init_date')
        asset_return = asset_return.dropna(axis=0, how='any')
        running_max = np.maximum.accumulate(asset_return.cumsum())
        underwater = asset_return.cumsum() - running_max
        underwater = np.exp(underwater) - 1
        valley = underwater[underwater.columns[-1]].idxmin()
        try:
            recovery_date = underwater[valley:][underwater[valley:] == 0].dropna().index[0]
        except IndexError:
            recovery_date = np.nan
    else:
        recovery_date = np.nan
    return recovery_date


def calculate_maximum_drawdown_recovery_months_number(asset_return):
    if len(asset_return) > 1:
        asset_return = asset_return.set_index('init_date')
        asset_return = asset_return.dropna(axis=0, how='any')
        running_max = np.maximum.accumulate(asset_return.cumsum())
        underwater = asset_return.cumsum() - running_max
        underwater = np.exp(underwater) - 1
        valley = underwater.idxmin().iloc[0]
        try:
            recovery_date = underwater[valley:][underwater[valley:] == 0].dropna().index[0]
        except IndexError:
            recovery_date = None
        if recovery_date is not None:
            month_diff = recovery_date.month - valley.month
            year_diff = recovery_date.year - valley.year
            period_diff = max(12 * year_diff + month_diff, 0)
        else:
            period_diff = np.nan
    else:
        period_diff = np.nan
    return period_diff


def calculate_downside_volatility(asset_return, freq):
    if freq == 'weekly':
        multiplier = 52
    elif freq == 'monthly':
        multiplier = 12
    else:  # freq == 'daily'
        multiplier = 252
    target_return = 0.03 / multiplier
    if not asset_return.empty:
        downside_return = asset_return - target_return
        downside_return[downside_return > 0] = 0
        downside_volatility = downside_return.std(ddof=1) * np.sqrt(multiplier)
    else:
        downside_volatility = np.nan
    return downside_volatility


def calculate_historical_var(asset_return, cutoff=0.05):
    if len(asset_return) > 24:
        hist_var = np.quantile(asset_return, cutoff, interpolation='lower')
    else:
        hist_var = np.nan
    return -hist_var


def calculate_conditional_var(asset_return, cutoff=0.05):
    if len(asset_return) > 24:
        var = np.quantile(asset_return, cutoff, interpolation='lower')
        conditional_var = asset_return[asset_return <= var].mean()
    else:
        conditional_var = np.nan
    return -conditional_var


def calculate_annualized_volatility(asset_return, freq):
    if not asset_return.empty:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
    else:
        annualized_vol = np.nan
    return annualized_vol


def calculate_tracking_error(asset_return, index_return, freq):
    if len(asset_return) > 0 and len(index_return) > 0:
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        active_return = asset_return - index_return
        tracking_error = (active_return.std(ddof=1)) * np.sqrt(multiplier)
    else:
        tracking_error = np.nan
    return tracking_error


def get_benchmark_correlation(asset_return, index_return):
    if len(asset_return) > 0 and len(index_return) > 0:
        benchmark_correlation = asset_return.corr(index_return)
    else:
        benchmark_correlation = np.nan
    return benchmark_correlation












