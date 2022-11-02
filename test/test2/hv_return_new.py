import pandas as pd
import numpy as np
from pyhive import hive
import cx_Oracle as cx
from app.common.log import logger
conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
# cursor = conn.cursor()
# logger = logging.getLogger('hv_return_new')


# 数据迁移
def get_clients(request_id, type):
    hive_conn = hive.Connection(host='10.52.40.222', port=21050, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                database='bizdm', auth='LDAP')
    query = """select * from bizdm.t_org_cust_highnav_cust_info"""
    clients = pd.read_sql(query, hive_conn)
    clients.loc[clients['cust_attr_nm'] == '产品（机构端）', 'cust_attr_nm'] = '产品'
    clients = clients.where(pd.notnull(clients), None)
    values = list(tuple(clients.loc[i]) for i in clients.index)
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    cursor.executemany(
        """
        begin
        insert into t_org_cust_highnav_cust_info (biz_dt, biz_dt_nm, suprs_brn_org_no, suprs_brn_org_nm, brn_org_no,
        brn_org_nm, src_cust_no, cust_nm, age, cust_attr_cd, cust_attr_nm, cust_status_cd, cust_status_nm,
        ordi_accnt_mrk, crdt_accnt_mrk, opt_accnt_mrk, prod_accnt_mrk, open_dt, cncl_dt, in_dt, cust_risk_lvl_cd,
        cust_risk_lvl_nm, tot_ast, mon12_max_ast, mon12_avg_ast, etl_ld_tm, etl_ld_job, etl_date)
        values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22,
        :23, :24, :25, :26, :27, :28);
        exception
        when dup_val_on_index then
        update t_org_cust_highnav_cust_info
        set biz_dt=:1, biz_dt_nm=:2, suprs_brn_org_no=:3, suprs_brn_org_nm=:4, brn_org_no=:5, brn_org_nm=:6,
        cust_nm=:8, age=:9, cust_attr_cd=:10, cust_attr_nm=:11, cust_status_cd=:12, cust_status_nm=:13,
        ordi_accnt_mrk=:14, crdt_accnt_mrk=:15, opt_accnt_mrk=:16, prod_accnt_mrk=:17, open_dt=:18, cncl_dt=:19,
        in_dt=:20, cust_risk_lvl_cd=:21, cust_risk_lvl_nm=:22, tot_ast=:23, mon12_max_ast=:24, mon12_avg_ast=:25,
        etl_ld_tm=:26, etl_ld_job=:27, etl_date=:28
        where src_cust_no=:7;
        end;
        """, values
    )
    conn.commit()
    return {'success': 'success'}


def get_tradingdays_oracle():
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select TradingDate from JYDB.QT_TradingDayNew where TradingDate >= '2019-01-01'
    and TradingDate <= '{today}' and iftradingday = 1 and secumarket in (83, 90)""".format(
        today=pd.datetime.today().strftime('%Y-%m-%d'))
    return pd.read_sql(query, conn)['TradingDate'].tolist()


def get_tradingdays_hive():
    hive_conn = hive.Connection(host='10.52.40.222', port=10000, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                database='gildata', auth='LDAP')
    query = """select TradingDate from JYDB_QT_TradingDayNew where TradingDate >= '2019-01-01'
    and TradingDate <= '{today}' and iftradingday = 1 and secumarket in (83, 90)""".format(
        today=pd.datetime.today().strftime('%Y-%m-%d'))
    return sorted(pd.read_sql(query, hive_conn)['tradingdate'].tolist())


def get_groups(clients):
    r = len(clients) % 50
    if r == 0:
        groups = list(clients.reshape(len(clients) // 50, 50))
    else:
        groups = list(clients[:-r].reshape(len(clients) // 50, 50)) + [clients[-r:]]
    return groups


def get_original_return(date, clients, last=None):
    hive_conn = hive.Connection(host='10.52.40.222', port=21050, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                database='bizdm', auth='LDAP')
    today = date.strftime('%Y%m%d')
    query = """select src_cust_no client_id, tot_ast asset, tot_nett netting, tot_prft profit
    from bizdm.t_org_cust_d_highnav_cust_ast where etl_date = '{dd}'""".format(dd=today)
    today_data = pd.read_sql(query, hive_conn)
    if not last:
        today_data = get_new_returns_1(today_data, 0)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
    else:
        last = last.strftime('%Y-%m-%d')
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """select client_id, unit_nv, share_num from hv_return
        where init_date = to_date('{dd}', 'yyyy-mm-dd')""".format(dd=last)
        last_data = pd.read_sql(query, conn).rename(columns={'CLIENT_ID': 'client_id', 'SHARE_NUM': 'SHARE'})
        all_data = today_data.merge(last_data, on='client_id', how='outer')
        all_data_11 = all_data[all_data['UNIT_NV'].isna()]
        all_data_12 = all_data[(all_data['SHARE'] == 0) & (all_data['netting'] == 0)]
        all_data_13 = all_data[(all_data['UNIT_NV'] == 0) & (all_data['netting'] == 0)]
        all_data_1 = pd.concat(
            [get_new_returns_1(all_data_11, 0), get_new_returns_1(all_data_12, 1), get_new_returns_1(all_data_13, 0)])
        all_data_2 = all_data[(~all_data['client_id'].isin(all_data_1['client_id'])) & (~all_data['asset'].isna())]
        all_data_2 = get_new_returns_2(all_data_2)
        # 用户终止销户,就不算之后的收益率了
        # all_data_3 = all_data[~all_data['asset'].isna()]
        all_data = pd.concat([all_data_1, all_data_2]).reset_index(drop=True)
        # today_data_new = all_data[['client_id', 'unit_nv', 'share', 'daily_return']]
        # n_today_data = last_data[~last_data['client_id'].isin(today_data['client_id'].unique())][
        #     ['client_id', 'UNIT_NV', 'SHARE']].rename(columns={'UNIT_NV': 'unit_nv', 'SHARE': 'share'})
        # n_today_data['daily_return'] = 0
        all_data1 = all_data[all_data['client_id'].isin(all_data_11['client_id'].tolist())]
        all_data2 = all_data[~all_data['client_id'].isin(all_data_11['client_id'].tolist())]
        all_data2 = clean_return_data3(all_data2)
        all_data = pd.concat([all_data1, all_data2]).reset_index(drop=True)
        today_data = all_data[['client_id', 'unit_nv', 'share', 'daily_return']]
    today_data = today_data[today_data['client_id'].isin(clients)]
    today_data['date'] = date
    today_data = today_data[['client_id', 'date', 'unit_nv', 'share', 'daily_return']]
    today_data = today_data.where(pd.notnull(today_data), None)
    today_data['unit_nv'] = today_data['unit_nv'].apply(lambda x: float(x))
    today_data['share'] = today_data['share'].apply(lambda x: float(x))
    today_data['date'] = today_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    values = list(tuple(today_data.loc[i]) for i in today_data.index)
    cursor.executemany(
        """
        begin
        insert into hv_return (client_id, init_date, unit_nv, share_num, daily_return)
        values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5);
        exception
        when dup_val_on_index then
        update hv_return
        set unit_nv=:3, share_num=:4, daily_return=:5
        where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd');
        end;
        """, values
    )
    conn.commit()


def get_client_id():
    hive_conn = hive.Connection(host='10.52.40.222', port=21050, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                           database='bizdm', auth='LDAP')
    query = """select src_cust_no from bizdm.t_org_cust_highnav_cust_info"""
    clients = pd.read_sql(query, hive_conn)['src_cust_no'].tolist()
    return clients


# 高净值客户收益率计算
def get_hv_return(request_id, type):
    clients = get_client_id()
    logger.info('finish clients')
    clients = list(client for client in clients if client not in ['321000782', '1800041995'])
    tradingdays = get_tradingdays_hive()
    for i in range(tradingdays.index(pd.to_datetime('2019-01-02')), len(tradingdays)):
        logger.info(tradingdays[i])
        if i == 0:
            get_original_return(tradingdays[i], clients)
        else:
            get_original_return(tradingdays[i], clients, tradingdays[i - 1])
    return {'success': 'success'}


# 特殊基金
def get_other_return(clients):
    hive_conn = hive.Connection(host='10.52.40.222', port=21050, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                database='bizdm', auth='LDAP')
    cursor = hive_conn.cursor()
    query = """select src_cust_no client_id, biz_dt init_date, tot_ast asset
    from bizdm.t_org_cust_d_highnav_cust_ast where src_cust_no in ({dd}) order by src_cust_no, etl_date asc""".format(dd=str(clients)[1:-1])
    data_use0 = pd.read_sql(query, hive_conn)
    for i in clients:
        data_use = data_use0[data_use0['client_id'] == i]
        data_use['share'] = data_use['asset'].iloc[0]
        data_use['unit_nv'] = data_use['asset'] / data_use['share']
        data_use['daily_return'] = data_use['unit_nv'] / data_use['unit_nv'].shift(1) - 1
        data_use['daily_return'] = data_use['daily_return'].fillna(0)
        data_use = data_use[['client_id', 'init_date', 'unit_nv', 'share', 'daily_return']]
        today_data = data_use.copy()
        today_data = today_data.where(pd.notnull(today_data), None)
        today_data['unit_nv'] = today_data['unit_nv'].apply(lambda x: float(x))
        today_data['share'] = today_data['share'].apply(lambda x: float(x))
        today_data['init_date'] = today_data['init_date'].apply(lambda x: str(x)[:4]+'-'+str(x)[4:6]+'-'+str(x)[6:8])
        values = list(tuple(today_data.loc[i]) for i in today_data.index)
        cursor.executemany(
            """
            begin
            insert into hv_return (client_id, init_date, unit_nv, share_num, daily_return)
            values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5);
            exception
            when dup_val_on_index then
            update hv_return
            set unit_nv=:3, share_num=:4, daily_return=:5
            where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd');
            end;
            """, values
        )
        conn.commit()


# one client
def get_hv_return_one(request_id, type, clients):
    tradingdays = get_tradingdays_hive()
    for i in range(tradingdays.index(pd.to_datetime('2019-01-02')), len(tradingdays)):
        logger.info(tradingdays[i])
        if i == 0:
            get_original_return_one(tradingdays[i], clients)
        else:
            get_original_return_one(tradingdays[i], clients, tradingdays[i - 1])
    return {'success': 'success'}


def get_original_return_one(date, clients, last=None):
    hive_conn = hive.Connection(host='10.52.40.222', port=21050, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                database='bizdm', auth='LDAP')
    today = date.strftime('%Y%m%d')
    query = """select src_cust_no client_id, tot_ast asset, tot_nett netting, tot_prft profit
    from bizdm.t_org_cust_d_highnav_cust_ast where etl_date = '{dd}' and src_cust_no in ({clients})
    """.format(dd=today, clients=str(clients)[1:-1])
    today_data = pd.read_sql(query, hive_conn)
    if not last:
        today_data = get_new_returns_1(today_data, 0)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
    else:
        last = last.strftime('%Y-%m-%d')
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """select client_id, unit_nv, share_num from hv_return where init_date = to_date('{dd}', 'yyyy-mm-dd')
        and client_id in ({clients})""".format(dd=last, clients=str(clients)[1:-1])
        last_data = pd.read_sql(query, conn).rename(columns={'CLIENT_ID': 'client_id', 'SHARE_NUM': 'SHARE'})
        all_data = today_data.merge(last_data, on='client_id', how='outer')
        all_data_11 = all_data[all_data['UNIT_NV'].isna()]
        all_data_12 = all_data[(all_data['SHARE'] == 0) & (all_data['netting'] == 0)]
        all_data_13 = all_data[(all_data['UNIT_NV'] == 0) & (all_data['netting'] == 0)]
        all_data_1 = pd.concat(
            [get_new_returns_1(all_data_11, 0), get_new_returns_1(all_data_12, 1), get_new_returns_1(all_data_13, 0)])
        all_data_2 = all_data[(~all_data['client_id'].isin(all_data_1['client_id'])) & (~all_data['asset'].isna())]
        all_data_2 = get_new_returns_2(all_data_2)
        # 用户终止销户,就不算之后的收益率了
        # all_data_3 = all_data[~all_data['asset'].isna()]
        all_data = pd.concat([all_data_1, all_data_2]).reset_index(drop=True)
        # today_data_new = all_data[['client_id', 'unit_nv', 'share', 'daily_return']]
        # n_today_data = last_data[~last_data['client_id'].isin(today_data['client_id'].unique())][
        #     ['client_id', 'UNIT_NV', 'SHARE']].rename(columns={'UNIT_NV': 'unit_nv', 'SHARE': 'share'})
        # n_today_data['daily_return'] = 0
        all_data1 = all_data[all_data['client_id'].isin(all_data_11['client_id'].tolist())]
        all_data2 = all_data[~all_data['client_id'].isin(all_data_11['client_id'].tolist())]
        all_data2 = clean_return_data3(all_data2)
        all_data = pd.concat([all_data1, all_data2]).reset_index(drop=True)
        today_data = all_data[['client_id', 'unit_nv', 'share', 'daily_return']]
    today_data = today_data[today_data['client_id'].isin(clients)]
    today_data['date'] = date
    today_data = today_data[['client_id', 'date', 'unit_nv', 'share', 'daily_return']]
    today_data = today_data.where(pd.notnull(today_data), None)
    today_data['unit_nv'] = today_data['unit_nv'].apply(lambda x: float(x))
    today_data['share'] = today_data['share'].apply(lambda x: float(x))
    today_data['date'] = today_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    values = list(tuple(today_data.loc[i]) for i in today_data.index)
    cursor.executemany(
        """
        begin
        insert into hv_return (client_id, init_date, unit_nv, share_num, daily_return)
        values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5);
        exception
        when dup_val_on_index then
        update hv_return
        set unit_nv=:3, share_num=:4, daily_return=:5
        where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd');
        end;
        """, values
    )
    conn.commit()


# %%
# 数据清洗
def get_new_returns_2(data_all):
    sub_data = data_all[(abs(data_all['netting'] / data_all['asset']) > 0.5)]
    sub_data = clean_return_data(sub_data)
    data_all = data_all[~data_all['client_id'].isin(sub_data['client_id'].unique())]
    data_all['share'] = data_all['netting'] / data_all['UNIT_NV'] + data_all['SHARE']
    data_all['unit_nv'] = data_all['asset'] / data_all['share']
    data_all['daily_return'] = data_all['unit_nv'] / data_all['UNIT_NV'] - 1
    return pd.concat([data_all, sub_data]).reset_index(drop=True)


def get_new_returns_1(data_all, last):
    if last == 0:
        data_all['unit_nv'] = 1
        data_all['share'] = data_all['asset'] / data_all['unit_nv']
        data_all['daily_return'] = np.nan
    else:
        data_all['unit_nv'] = data_all['UNIT_NV']
        data_all['share'] = data_all['asset'] / data_all['unit_nv']
        data_all['share'] = data_all['share'].fillna(0)
        data_all['daily_return'] = 0
    return data_all


def clean_return_data(sub_data):
    sub_data1 = sub_data[(sub_data['netting'] + sub_data['profit']) < 0]
    sub_data2 = sub_data[(sub_data['netting'] + sub_data['profit']) >= 0]
    sub_data1['daily_return'] = sub_data1['profit'] / (sub_data1['UNIT_NV'] * sub_data1['SHARE'])
    sub_data2['daily_return'] = sub_data2['profit'] / sub_data2['asset']
    sub_data = pd.concat([sub_data1, sub_data2])
    sub_data['unit_nv'] = sub_data['UNIT_NV'] * (sub_data['daily_return'] + 1)
    sub_data['share'] = sub_data['asset'] / sub_data['unit_nv']
    return sub_data


def log_def(x):
    if len(x[np.isinf(x['share'])]) > 0:
        logger.info(x[np.isinf(x['share'])])
    if len(x[np.isinf(x['unit_nv'])]) > 0:
        logger.info(x[np.isinf(x['unit_nv'])])
    if len(x[x['share'].isna()]) > 0:
        logger.info(x[x['share'].isna()])
    if len(x[x['unit_nv'].isna()]) > 0:
        logger.info(x[x['unit_nv'].isna()])


def clean_return_data2(data_all):
    if len(data_all[(data_all['asset'] < 1000)
                    &((data_all['unit_nv']/data_all['UNIT_NV'] >= 1.5)|(data_all['unit_nv'] >= 10)
                     |(data_all['unit_nv']/data_all['UNIT_NV'] <= -1.5)|(data_all['unit_nv'] <= -10))]):
        data_all['unit_nv'] = data_all['UNIT_NV']
        data_all['share'] = data_all['asset'] / data_all['unit_nv']
        data_all['daily_return'] = 0
    return data_all


def clean_return_data3(data_all):
    data_all.loc[data_all['share'].isna(), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[data_all['share'].isna(), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[data_all['share'].isna(), 'daily_return'] = 0
    data_all.loc[data_all['unit_nv'].isna(), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[data_all['unit_nv'].isna(), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[data_all['unit_nv'].isna(), 'daily_return'] = 0
    data_all.loc[data_all['daily_return'].isna(), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[data_all['daily_return'].isna(), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[data_all['daily_return'].isna(), 'daily_return'] = 0
    data_all.loc[np.isinf(data_all['share']), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[np.isinf(data_all['share']), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[np.isinf(data_all['share']), 'daily_return'] = 0
    data_all.loc[np.isinf(data_all['unit_nv']), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[np.isinf(data_all['unit_nv']), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[np.isinf(data_all['unit_nv']), 'daily_return'] = 0
    data_all.loc[np.isinf(data_all['daily_return']), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[np.isinf(data_all['daily_return']), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[np.isinf(data_all['daily_return']), 'daily_return'] = 0
    data_all.loc[abs(data_all['daily_return'] > 0.22), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[abs(data_all['daily_return'] > 0.22), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[abs(data_all['daily_return'] > 0.22), 'daily_return'] = 0
    data_all.loc[abs(data_all['daily_return'] < -0.22), 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[abs(data_all['daily_return'] < -0.22), 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[abs(data_all['daily_return'] < -0.22), 'daily_return'] = 0
    data_all.loc[data_all['share'] > 50000000000, 'unit_nv'] = data_all['UNIT_NV']
    data_all.loc[data_all['share'] > 50000000000, 'share'] = data_all['asset'] / data_all['unit_nv']
    data_all.loc[data_all['share'] > 50000000000, 'daily_return'] = 0
    data_all = data_all[~data_all['share'].isna()]
    data_all = data_all[~data_all['unit_nv'].isna()]
    data_all = data_all[~np.isinf(data_all['share'])]
    data_all = data_all[~np.isinf(data_all['unit_nv'])]
    data_all = data_all[abs(data_all['share']) <= 50000000000]
    data_all = data_all[abs(data_all['unit_nv']) <= 500]
    data_all = data_all[abs(data_all['daily_return']) <= 0.3]
    return data_all


# 收益曲线
def get_client_performance(request_id, type, client_id, index_code, start_date, end_date):
    if len(end_date) == 0:
        return {'all_return': pd.DataFrame(), 'all_drawdown': pd.DataFrame()}
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select init_date, daily_return from hv_return where init_date > to_date('{sd}', 'yyyy-mm-dd')
    and init_date <= to_date('{ed}', 'yyyy-mm-dd') and client_id = '{client}' order by init_date asc
    """.format(sd=start_date, ed=end_date, client=client_id)
    client_return = pd.read_sql(query, conn).rename(columns={'INIT_DATE': 'init_date', 'DAILY_RETURN': client_id})
    if client_return.empty:
        return {'all_return': pd.DataFrame(), 'all_drawdown': pd.DataFrame()}
    query = """select TradingDay, ClosePrice from ZJ_Index_Daily_Quote where InnerCode = (select InnerCode
    from JYDB.SecuMain where SecuCode = '{index}' and SecuCategory = 4 and SecuMarket in (83, 90))
    and TradingDay >= to_date('{sd}', 'yyyy-mm-dd') and TradingDay <= to_date('{ed}', 'yyyy-mm-dd')
    order by TradingDay asc""".format(index=index_code, sd=start_date, ed=end_date)
    index_return = pd.read_sql(query, conn).rename(columns={'TRADINGDAY': 'init_date', 'CLOSEPRICE': index_code})
    client_return['client'] = (client_return[client_id] + 1).cumprod()
    index_return['index'] = index_return[index_code] / index_return[index_code].iloc[0]
    all_return = client_return.merge(index_return, on='init_date', how='inner').ffill().fillna(1)
    log_return = np.log((all_return[['client', 'index']] / all_return[['client', 'index']].shift(1)).fillna(1))
    log_return['init_date'] = all_return['init_date']
    all_drawdown = get_dynamic_drawdown(log_return)
    all_return = (all_return.set_index('init_date') - 1).reset_index()
    return {'all_return': all_return, 'all_drawdown': all_drawdown}


def get_dynamic_drawdown(assets_return):
    asset_return = assets_return.copy()
    return_list = list(asset_return.columns[~np.isin(asset_return.columns, ['init_date'])])
    drawdown_list = return_list
    gr = asset_return[['init_date'] + return_list].set_index('init_date')
    gr = gr.dropna(axis=0, how='any')
    running_max = np.maximum.accumulate(gr.cumsum())
    underwater = gr.cumsum() - running_max
    underwater = np.exp(underwater) - 1
    underwater = underwater.reset_index().rename(columns=dict(zip(return_list, drawdown_list)))
    return underwater


# 资产变化
def get_asset_scale(request_id, type, client_id, start_date, end_date):
    if len(end_date) == 0:
        return {}
    hive_conn = hive.Connection(host='10.52.40.222', port=21050, username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                database='bizdm', auth='LDAP')
    query = """select biz_dt init_date, tot_ast, ordi_ast+crdt_net_ast+opt_net_ast+prod_mktval ordi_ast,
    tot_ast/(ordi_ast+crdt_net_ast+opt_net_ast+prod_mktval) leverage from bizdm.t_org_cust_d_highnav_cust_ast
    where src_cust_no = '{client}' and etl_date >= '{sd}' and etl_date <= '{ed}' and ordi_ast+crdt_net_ast+opt_net_ast+prod_mktval > 0
    order by etl_date asc""".format(
    client=client_id, sd=start_date.replace('-', ''), ed=end_date.replace('-', ''))
    asset_scale = pd.read_sql(query, hive_conn)
    asset_scale['init_date'] = pd.to_datetime(asset_scale['init_date'].apply(lambda x: str(x)[:4]+'-'+str(x)[4:6]+'-'+str(x)[6:8]))
    return asset_scale










