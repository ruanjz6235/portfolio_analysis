import pandas as pd
import numpy as np
import cx_Oracle as cx
from pyhive import hive
hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR', port=21050,
                            database='bizdm', auth='LDAP')
hive_cursor = hive_conn.cursor()
conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
cursor = conn.cursor()


def get_zscodes():
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    zscodes = pd.read_sql("""select zgf.zscode, zgf.innercode from zs_gzb_fund zgf inner join zs_gzb_fund_extend pbi
    on zgf.zscode = pbi.zscode where pbi.ListedState = 1""", conn)
    return np.array(zscodes['ZSCODE']), zscodes.rename(columns={'ZSCODE': 'client_id'})


def get_groups(clients, k):
    r = len(clients) % k
    if r == 0:
        clients_group = list(clients.reshape(len(clients) // k, k))
    elif len(clients) // k:
        clients_group = list(clients[:-r].reshape(len(clients) // k, k)) + [clients[-r:]]
    else:
        clients_group = [clients[-r:]]
    return clients_group


# %%
def get_zscode_return_label(request_id, type):
    clients, codes = get_zscodes()
    groups = get_groups(clients, 1000)
    today = pd.datetime.today().date()
    index_list = ['annureturn', 'annuexreturn', 'annuvol', 'sharperatio', 'maximumdrawdown', 'beta', 'annudownvol',
                  'annutrack', 'accureturn', 'jensen', 'info', 'sortino', 'treynor', 'm2']
    for m, i in enumerate(groups):
        print(m)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        query = """select zscode, EndDate, exp(log_return) - 1 dr from ZS_FUND_CUNITNV_RET
        where zscode in ({clients}) and EndDate >= to_date('{start}', 'yyyy-mm-dd') order by EndDate asc
        """.format(start='2018-12-25', clients=str(list(i))[1:-1])
        clients_return = pd.read_sql(query, conn)
        if clients_return.empty:
            continue
        clients_return.columns = ['client_id', 'init_date', 'daily_return']
        clients_return['daily_return'] = np.log(clients_return['daily_return'] + 1)
        query = """select InnerCode, TradingDay, ClosePrice from zdj.zj_index_daily_quote where InnerCode = 3145
        and TradingDay >= to_date('{start}', 'yyyy-mm-dd') order by TradingDay asc""".format(start='2018-12-25')
        benchmark = pd.read_sql(query, conn)
        benchmark.columns = ['InnerCode', 'TradingDay', 'ClosePrice']
        benchmark['daily_return'] = np.log(benchmark['ClosePrice'] / benchmark['ClosePrice'].shift(1))
        start = ['{y}-{m}-{d}'.format(y=today.year-1, m=today.month, d=today.day), '{y}-01-01'.format(y=today.year),
                 '{y}-{m}-{d}'.format(y=today.year-2, m=today.month, d=today.day)]
        index_type = ['_1y', '_cy', '_3y']
        clients_index, all_list = [], []
        for j, s in enumerate(start):
            print(index_type[j])
            index_list_n1 = [index + index_type[j] for index in index_list]
            sub_return = clients_return[clients_return['init_date'] >= s]
            sub_return = sub_return.sort_values(['client_id', 'init_date']).reset_index(drop=True)
            sub_index = sub_return.groupby('client_id').apply(
                lambda x: get_index(x, benchmark['daily_return'], index_list_n1)).reset_index()
            for k in index_list_n1:
                sub_index0 = sub_index[~sub_index[k].isna()].sort_values(k, ascending=False)
                sub_index1 = sub_index[sub_index[k].isna()]
                sub_index0[k+'_rank'] = range(1, 1 + len(sub_index0))
                sub_index1[k+'_rank'] = len(clients)
                sub_index = pd.concat([sub_index0, sub_index1]).reset_index(drop=True)
            sub_index['totalnumber' + index_type[j]] = len(clients)
            all_list.append(list(sub_index.columns))
            clients_index.append(sub_index)
        sub_data = pd.merge(clients_index[0], clients_index[1], on=['client_id'], how='inner')
        clients_data = pd.merge(sub_data, clients_index[2], on=['client_id'], how='inner')
        clients_data['EndDate'] = today.strftime('%Y-%m-%d')
        all_list = np.array(all_list).T.reshape(1, len(all_list) * len(all_list[0]))[0]
        clients_data = codes.merge(clients_data, on='client_id', how='inner')
        clients_data = clients_data[[all_list[2], 'INNERCODE', 'EndDate'] + list(all_list[3:])]
        for ll in clients_data.columns[3:45]:
            clients_data[ll] = clients_data[ll].apply(lambda x: min(round(x, 4), 100))
        clients_data['INNERCODE'] = clients_data['INNERCODE'].fillna(clients_data['client_id'])
        clients_data = clients_data.where(pd.notnull(clients_data), None)
        for ll in ['client_id', 'INNERCODE'] + list(clients_data.columns[45:]):
            clients_data[ll] = clients_data[ll].apply(lambda x: float(x))
        values = list(tuple(clients_data.loc[ii]) for ii in clients_data.index)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.execute(f'delete from zs_return_rank_tertile where zscode in ({str(list(i))[1:-1]})')
        conn.commit()
        cursor.executemany(
            """insert into zs_return_rank_tertile (zscode, innercode, EndDate, annureturn_1y, annureturn_cy, annureturn_3y,
            annuexreturn_1y, annuexreturn_cy, annuexreturn_3y, annuvol_1y, annuvol_cy, annuvol_3y,
            sharperatio_1y, sharperatio_cy, sharperatio_3y, maximumdrawdown_1y, maximumdrawdown_cy, maximumdrawdown_3y,
            beta_1y, beta_cy, beta_3y, annudownvol_1y, annudownvol_cy, annudownvol_3y, annutrack_1y, annutrack_cy,
            annutrack_3y, accureturn_1y, accureturn_cy, accureturn_3y, jensen_1y, jensen_cy, jensen_3y, info_1y,
            info_cy, info_3y, sortino_1y, sortino_cy, sortino_3y, treynor_1y, treynor_cy, treynor_3y, m2_1y, m2_cy,
            m2_3y, annureturn_1y_rank, annureturn_cy_rank, annureturn_3y_rank, annuexreturn_1y_rank,
            annuexreturn_cy_rank, annuexreturn_3y_rank, annuvol_1y_rank, annuvol_cy_rank, annuvol_3y_rank,
            sharperatio_1y_rank, sharperatio_cy_rank, sharperatio_3y_rank, maximumdrawdown_1y_rank,
            maximumdrawdown_cy_rank, maximumdrawdown_3y_rank, beta_1y_rank, beta_cy_rank, beta_3y_rank,
            annudownvol_1y_rank, annudownvol_cy_rank, annudownvol_3y_rank, annutrack_1y_rank, annutrack_cy_rank,
            annutrack_3y_rank, accureturn_1y_rank, accureturn_cy_rank, accureturn_3y_rank, jensen_1y_rank,
            jensen_cy_rank, jensen_3y_rank, info_1y_rank, info_cy_rank, info_3y_rank, sortino_1y_rank, sortino_cy_rank,
            sortino_3y_rank, treynor_1y_rank, treynor_cy_rank, treynor_3y_rank, m2_1y_rank, m2_cy_rank, m2_3y_rank,
            totalnumber_1y, totalnumber_cy, totalnumber_3y)
            values (:1, :2, to_date(:3, 'yyyy-mm-dd'), :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17,
            :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38,
            :39, :40, :41, :42, :43, :44, :45, :46, :47, :48, :49, :50, :51, :52, :53, :54, :55, :56, :57, :58, :59,
            :60, :61, :62, :63, :64, :65, :66, :67, :68, :69, :70, :71, :72, :73, :74, :75, :76, :77, :78, :79, :80,
            :81, :82, :83, :84, :85, :86, :87, :88, :89, :90)
            """, values
        )
        conn.commit()
    return 'success'


# %%
def get_index(asset_return_original, benchmark_return, index_list):
    asset_return_original = asset_return_original[~asset_return_original['daily_return'].isna()]
    start, end = asset_return_original.iloc[0]['init_date'], asset_return_original.iloc[-1]['init_date']
    asset_return = asset_return_original['daily_return']
    days = (end-start).days + 1
    accumulated_return = calculate_accumulated_return(asset_return)
    annualized_return = calculate_annualized_return(asset_return, days)
    annualized_excessive_return = calculate_annualized_excessive_return(asset_return, benchmark_return, days)
    annualized_volatility = calculate_annualized_volatility(asset_return)
    sharpe_ratio = calculate_sharpe_ratio(asset_return)
    maximum_drawdown = calculate_maximum_drawdown(asset_return)
    beta = calculate_beta(asset_return, benchmark_return)
    information_ratio = calculate_information_ratio(asset_return, benchmark_return)
    tracking_error = calculate_tracking_error(asset_return, benchmark_return)
    sortino_ratio = calculate_sortino_ratio(asset_return)
    treynor_ratio = calculate_treynor_ratio(asset_return, benchmark_return, beta)
    jensens_alpha = calculate_jensens_alpha(asset_return, benchmark_return, beta)
    m2 = calculate_m2(asset_return, benchmark_return)
    downside_volatility = calculate_downside_volatility(asset_return, benchmark_return)
    return pd.Series([annualized_return, annualized_excessive_return, annualized_volatility, sharpe_ratio,
                      maximum_drawdown, beta, downside_volatility, tracking_error, accumulated_return, jensens_alpha,
                      information_ratio, sortino_ratio, treynor_ratio, m2],
                     index=index_list)


# %%
def calculate_accumulated_return(asset_return):
    if len(asset_return) > 0:
        accumulated_return = asset_return.sum()
        accumulated_return = np.exp(accumulated_return) - 1
    else:
        accumulated_return = np.nan
    return accumulated_return


def calculate_annualized_return(asset_return, days):
    if len(asset_return) > 0:
        multiplier = 365 / days
        annualized_return = multiplier * asset_return.sum()
        annualized_return = np.exp(annualized_return) - 1
    else:
        annualized_return = np.nan
    return annualized_return


def calculate_annualized_excessive_return(asset_return, benchmark_return, days):
    if len(asset_return) > 0:
        if len(asset_return) == len(benchmark_return):
            multiplier = 365 / days
            annualized_return = multiplier * (asset_return - benchmark_return).sum()
            annualized_return = np.exp(annualized_return) - 1
        else:
            return np.nan
    else:
        annualized_return = np.nan
    return annualized_return


def calculate_annualized_volatility(asset_return):
    if len(asset_return) > 0:
        multiplier = 252
        annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
        if annualized_vol < 0.001:
            annualized_vol = np.nan
    else:
        annualized_vol = np.nan
    return annualized_vol


def calculate_sharpe_ratio(asset_return):
    if len(asset_return) > 0:
        multiplier = 252
        annualized_return = multiplier * asset_return.mean()
        annualized_return = np.exp(annualized_return) - 1
        vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
        if vol < 0.001:
            sharpe_ratio = np.nan
        else:
            sharpe_ratio = (annualized_return - 0.03) / vol
    else:
        sharpe_ratio = np.nan
    return sharpe_ratio


def calculate_maximum_drawdown(asset_return):
    if len(asset_return) > 0:
        running_max = np.maximum.accumulate(asset_return.cumsum())
        underwater = asset_return.cumsum() - running_max
        underwater = np.exp(underwater) - 1
        mdd = -underwater.min()
    else:
        mdd = np.nan
    return mdd


def calculate_beta(asset_return, benchmark_return):
    if len(asset_return) > 0:
        multiplier = 252
        if len(asset_return) == len(benchmark_return):
            asset_return = asset_return - 0.03 / multiplier
            benchmark_return = benchmark_return - 0.03 / multiplier
            cov = asset_return.cov(benchmark_return)
            beta = cov / benchmark_return.var()
            if beta < 0.001:
                return np.nan
        else:
            return np.nan
    else:
        return np.nan
    return beta


def calculate_information_ratio(asset_return, benchmark_return):
    if len(asset_return) > 0 and len(asset_return) == len(benchmark_return):
        multiplier = 252
        active_return = asset_return - benchmark_return
        tracking_error = (active_return.std(ddof=1)) * np.sqrt(multiplier)
        if tracking_error < 0.001:
            tracking_error = np.nan
        asset_annualized_return = multiplier * asset_return.mean()
        index_annualized_return = multiplier * benchmark_return.mean()
        information_ratio = (asset_annualized_return - index_annualized_return) / tracking_error
    else:
        information_ratio = np.nan
    return information_ratio


def calculate_tracking_error(asset_return, benchmark_return):
    multiplier = 252
    if len(asset_return) > 0 and len(asset_return) == len(benchmark_return):
        active_return = asset_return - benchmark_return
        tracking_error = (active_return.std(ddof=1)) * np.sqrt(multiplier)
        if tracking_error < 0.001:
            return np.nan
    else:
        tracking_error = np.nan
    return tracking_error


def calculate_sortino_ratio(asset_return):
    if len(asset_return) > 0:
        multiplier = 252
        downside_return = asset_return - 0.03 / multiplier
        downside_return[downside_return > 0] = 0
        downside_volatility = downside_return.std(ddof=1) * np.sqrt(multiplier)
        annualized_return = multiplier * asset_return.mean()
        sortino_ratio = (annualized_return - 0.03) / downside_volatility
        if downside_volatility < 0.001:
            sortino_ratio = np.nan
    else:
        sortino_ratio = np.nan
    return sortino_ratio


def calculate_treynor_ratio(asset_return, benchmark_return, beta):
    if len(asset_return) > 0 and len(asset_return) == len(benchmark_return):
        multiplier = 252
        # beta = calculate_beta(asset_return, benchmark_return)
        annualized_return = multiplier * asset_return.mean()
        treynor_ratio = (annualized_return - 0.03) / beta
    else:
        treynor_ratio = np.nan
    return treynor_ratio


def calculate_jensens_alpha(asset_return, benchmark_return, beta):
    if len(asset_return) > 0 and len(asset_return) == len(benchmark_return):
        multiplier = 252
        # beta = calculate_beta(asset_return, benchmark_return)
        asset_annualized_return = multiplier * asset_return.mean()
        benchmark_annualized_return = multiplier * benchmark_return.mean()
        rf = 0.03
        jensens_alpha = asset_annualized_return - (rf + beta * (benchmark_annualized_return - rf))
    else:
        jensens_alpha = np.nan
    return jensens_alpha


def calculate_m2(asset_return, benchmark_return):
    if len(asset_return) > 0 and len(asset_return) == len(benchmark_return):
        multiplier = 252
        asset_annualized_return = multiplier * asset_return.mean()
        asset_annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
        benchmark_annualized_vol = benchmark_return.std(ddof=1) * np.sqrt(multiplier)
        rf = 0.03
        if asset_annualized_vol < 0.001:
            m2 = np.nan
        else:
            m2 = rf + (asset_annualized_return - rf) * benchmark_annualized_vol / asset_annualized_vol
    else:
        m2 = np.nan
    return m2


def calculate_downside_volatility(asset_return, benchmark_return):
    multiplier = 252
    if len(asset_return) > 0 and len(asset_return) == len(benchmark_return):
        downside_return = asset_return - benchmark_return
        downside_return[downside_return > 0] = 0
        downside_volatility = downside_return.std(ddof=1) * np.sqrt(multiplier)
        if downside_volatility < 0.001:
            downside_volatility = np.nan
    else:
        downside_volatility = np.nan
    return downside_volatility
