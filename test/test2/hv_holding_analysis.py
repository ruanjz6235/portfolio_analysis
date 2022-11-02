import pandas as pd
import numpy as np
import cx_Oracle as cx
import datetime as dt
from dateutil.relativedelta import relativedelta
from pyhive import hive
from app.common.log import logger
# conn = hive.Connection(host='10.8.13.120', port=10000, username='hdfs', database='default')
hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR', port=21050,
                            database='bizdm', auth='LDAP')
hive_cursor = hive_conn.cursor()
conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
cursor = conn.cursor()


def get_tradingdays_oracle():
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select TradingDate from JYDB.QT_TradingDayNew where TradingDate >= to_date('2019-01-01', 'yyyy-mm-dd')
    and TradingDate <= to_date('{today}', 'yyyy-mm-dd') and iftradingday = 1 and secumarket in (83, 90)""".format(
        today=pd.datetime.today().strftime('%Y-%m-%d'))
    return pd.read_sql(query, conn)['TRADINGDATE'].tolist()[-15:]


def get_client_ids():
    hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR', port=21050,
                                database='bizdm', auth='LDAP')
    hive_cursor = hive_conn.cursor()
    query = """select src_cust_no from bizdm.T_ORG_CUST_HIGHNAV_CUST_INFO where cust_status_nm = '正常'"""
    clients = np.array(pd.read_sql(query, hive_conn)['src_cust_no'])
    return clients


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
# 大类资产
def get_class_data(request_id, type):
    tradingdays = get_tradingdays_oracle()
    for date in tradingdays:
        date_hive = date.strftime('%Y%m%d')
        logger.info(date_hive+'--------------------------')
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = f"""select src_cust_no client_id, ast_cgy_tp asset_class, sum(hld_mktval) market_value
        from bizdm.t_org_cust_d_highnav_cust_hld where etl_date = '{date_hive}' group by src_cust_no, ast_cgy_tp"""
        sub_class_data = pd.read_sql(query, hive_conn)
        query = f"""select src_cust_no client_id, '现金' asset_class, tot_fund_bal market_value
        from bizdm.t_org_cust_d_highnav_cust_ast where etl_date = '{date_hive}'"""
        clients_money = pd.read_sql(query, hive_conn)
        logger.info('process')
        clients_money = clients_money[~clients_money['market_value'].isna()]
        sub_class_data = pd.concat([sub_class_data, clients_money]).reset_index(drop=True)
        sub_class_data['init_date'] = date.strftime('%Y-%m-%d')
        sub_all = sub_class_data.groupby(['client_id'])['market_value'].sum().rename('mv').reset_index()
        sub_class_data = sub_class_data.merge(sub_all, on=['client_id'], how='left')
        sub_class_data = sub_class_data[sub_class_data['mv'] != 0]
        sub_class_data['ratio'] = sub_class_data['market_value'] / sub_class_data['mv']
        sub_class_data = sub_class_data[['client_id', 'init_date', 'asset_class', 'market_value', 'ratio']]
        sub_class_data['asset_class'] = sub_class_data['asset_class'].apply(lambda x: str(x))
        logger.info('save')
        values = list(zip(*(sub_class_data[i] for i in sub_class_data.columns)))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.execute(f"""delete from HV_ASSET_ALLOCATION where init_date = to_date('{date.strftime('%Y-%m-%d')}', 'yyyy-mm-dd')""")
        conn.commit()
        cursor.executemany(
            """
            BEGIN
            INSERT INTO HV_ASSET_ALLOCATION (CLIENT_ID, INIT_DATE, ASSET_TYPE, MARKET_VALUE, RATIO)
            VALUES (:1, TO_DATE(:2, 'yyyy-mm-dd'), :3, :4, :5);
            EXCEPTION
            WHEN DUP_VAL_ON_INDEX then
            update hv_asset_allocation
            set market_value=:4, ratio=:5
            where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd') and asset_type=:3;
            end;
            """, values
        )
        conn.commit()
    return {'success': 'success'}


# %%
# 股票行业
def get_industry_data_old(request_id, type):
    clients = get_client_ids()
    groups = get_groups(clients, 50)
    codes = get_stock_codes()
    inds = [get_industry(codes, 'sw'), get_industry(codes, 'zjh'),
            get_industry(codes, 'zz'), get_industry(codes, 'bk')]
    logger.info('start_stock_industry')
    sd = pd.datetime.today().date() - pd.DateOffset(days=20)
    sd = pd.to_datetime('2019-01-01')
    for j, i in enumerate(groups):
        logger.info(j)
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = """select src_cust_no client_id, biz_dt init_date, sec_code code, hld_mktval market_value
        from bizdm.t_org_cust_d_highnav_cust_hld where src_cust_no in ({clients}) and ast_cgy_tp = '股票'
        and etl_date >= '{sd}' and sec_nm is not null and hld_mktval is not null""".format(
            clients=str(list(i))[1:-1], sd=sd.strftime('%Y%m%d'))
        sub_class_data = pd.read_sql(query, hive_conn)
        sub_class_data['init_date'] = pd.to_datetime(sub_class_data['init_date'].apply(
            lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8]))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where client_id in ({clients}) and init_date >= to_date('{sd}', 'yyyy-mm-dd')
        group by client_id, init_date""".format(clients=str(list(i))[1:-1], sd=sd.strftime('%Y-%m-%d'))
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})        
        for ind, ind_d in enumerate(inds):
            logger.info(ind)
            ind_d = ind_d.drop_duplicates(['code'])
            sub_data = sub_class_data.merge(ind_d, on='code', how='left')
            sub_data['Industry'] = sub_data['Industry'].fillna('其他-新三板')
            sub_data = sub_data.groupby(['client_id', 'init_date', 'Industry'])['market_value'].sum().reset_index()
            sub_data['ind'] = ind + 1
            sub_data = sub_data.merge(sub_all, on=['client_id', 'init_date'], how='left')
            sub_data = sub_data[sub_data['mv'] != 0]
            sub_data['ratio'] = sub_data['market_value'] / sub_data['mv']
            sub_data = sub_data[['client_id', 'init_date', 'ind', 'Industry', 'market_value', 'ratio']]
            sub_data['init_date'] = sub_data['init_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            sub_data['ind'] = sub_data['ind'].apply(lambda x: str(x))
            sub_data = sub_data[~sub_data['ratio'].isna()]
            values_all = list(zip(sub_data['client_id'], sub_data['init_date'], sub_data['ind'],
                              sub_data['Industry'], sub_data['market_value'], sub_data['ratio']))
            conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
            cursor = conn.cursor()
            cursor.execute(f"""delete from hv_industry where client_id in ({str(sub_data['client_id'].drop_duplicates().tolist())[1:-1]})""")
            conn.commit()
            cursor.executemany(
                """
                begin
                insert into hv_industry (client_id, init_date, industry_type, industry_name, market_value, ratio)
                values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5, :6);
                exception
                when dup_val_on_index then
                update hv_industry
                set market_value=:5, ratio=:6
                where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd') and industry_type=:3 and industry_name=:4;
                end;
                """, values_all
            )
            conn.commit()
    return {'success': 'success'}


def get_industry_data(request_id, type):
    codes = get_stock_codes()
    inds = [get_industry(codes, 'sw'), get_industry(codes, 'zjh'),
            get_industry(codes, 'zz'), get_industry(codes, 'bk')]
    tradingdays = get_tradingdays_oracle()
    logger.info('start_stock_industry')
    for date in tradingdays:
        date_hive = date.strftime('%Y%m%d')
        logger.info(date_hive+'--------------------------')
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = f"""select src_cust_no client_id, sec_code code, hld_mktval market_value
        from bizdm.t_org_cust_d_highnav_cust_hld where ast_cgy_tp = '股票' and etl_date = '{date_hive}'
        and sec_nm is not null and hld_mktval is not null"""
        sub_class_data = pd.read_sql(query, hive_conn)
        logger.info('hld')
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = f"""select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where init_date = to_date('{date_hive}', 'yyyymmdd') group by client_id, init_date"""
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
        cursor.execute(f"""delete from hv_industry where init_date = to_date('{date_hive}', 'yyyymmdd')""")
        conn.commit()
        for ind, ind_d in enumerate(inds):
            logger.info(ind)
            sub_data = sub_class_data.merge(ind_d, on='code', how='left')
            sub_data['Industry'] = sub_data['Industry'].fillna('其他-新三板')
            sub_data = sub_data.groupby(['client_id', 'Industry'])['market_value'].sum().reset_index()
            sub_data['ind'] = ind + 1
            sub_data = sub_data.merge(sub_all, on='client_id', how='inner')
            sub_data = sub_data[sub_data['mv'] != 0]
            sub_data['ratio'] = sub_data['market_value'] / sub_data['mv']
            sub_data = sub_data[['client_id', 'init_date', 'ind', 'Industry', 'market_value', 'ratio']]
            sub_data['init_date'] = sub_data['init_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            sub_data['ind'] = sub_data['ind'].apply(lambda x: str(x))
            sub_data = sub_data[~sub_data['ratio'].isna()]
            values_all = list(zip(sub_data['client_id'], sub_data['init_date'], sub_data['ind'],
                              sub_data['Industry'], sub_data['market_value'], sub_data['ratio']))
            conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
            cursor = conn.cursor()
            cursor.executemany(
                """
                begin
                insert into hv_industry (client_id, init_date, industry_type, industry_name, market_value, ratio)
                values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5, :6);
                exception
                when dup_val_on_index then
                update hv_industry
                set market_value=:5, ratio=:6
                where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd') and industry_type=:3 and industry_name=:4;
                end;
                """, values_all
            )
            conn.commit()
    return {'success': 'success'}


def get_stock_codes():
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select SecuCode from JYDB.SecuMain where SecuMarket in (83, 90) and SecuCategory = 1"""
    a_inner_codes = pd.read_sql(query, conn).rename(columns={'SECUCODE': 'SecuCode'})
    codes = a_inner_codes['SecuCode'].tolist()
    query = """select SecuCode from JYDB.HK_SecuMain where SecuMarket = 72 and SecuCategory in (3, 51, 52, 53, 55)"""
    hk_inner_codes = pd.read_sql(query, conn).rename(columns={'SECUCODE': 'SecuCode'})
    hk_codes = hk_inner_codes['SecuCode'].tolist()
    codes.extend(hk_codes)
    return codes


# 港股三级代码，申万的industry_standard = 'sw'，中证港股没有分类
def get_hshares_industry3(codes, industry_standard):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    if industry_standard == 'zz':
        query = """
        select SecuCode, '其他-港股' as IndustryName
        from JYDB.HK_SecuMain
        where SecuCode in ('{InnerCode}')
        """.format(InnerCode="','".join(str(code) for code in codes))
        hshares_industry3 = pd.read_sql(query, conn)
    else:
        if industry_standard == 'sw':
            standard = 24
        else:                          # industry_standard == 'zjh':
            standard = 22
        query = """
        SELECT HKIN.SecuCode, HKIC.IndustryCode as Industry3Code, HKIC.IndustryName
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
        """ % ("','".join(str(i) for i in codes), standard)
        hshares_industry3 = pd.read_sql(query, conn)
    return hshares_industry3


# 一级代码行业名称
def get_industry_name(IndustryCodes, standard):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """
    SELECT IndustryCode, IndustryName
    from JYDB.HK_IndustryCategory
    where IndustryCode in ('%s')
    and Standard = %s
    """ % ("','".join(str(code) for code in IndustryCodes), standard)
    industry_name = pd.read_sql(query, conn)
    return industry_name


# 取三级代码，将三级代码转换成一级代码，申万三级代码的前三个字符就是一级代码
def get_hshares_industry(codes, industry_standard):
    hshares_industry3 = get_hshares_industry3(codes, industry_standard)
    if industry_standard in ['sw', 'zjh']:
        if industry_standard == 'sw':
            hshares_industry3['INDUSTRYCODE'] = hshares_industry3['INDUSTRY3CODE'].apply(lambda x: x[:3] + '000')
            stock_industry = hshares_industry3[hshares_industry3['INDUSTRY3CODE'] == hshares_industry3['INDUSTRYCODE']]
            stock_industry = stock_industry.rename(columns={'INDUSTRYNAME': 'Industry', 'SECUCODE': 'code'})[
                ['code', 'Industry']].reset_index(drop=True)
        else:                  # industry_standard == 'zjh'
            standard = 22
            hshares_industry3 = hshares_industry3.rename(columns={'INDUSTRYNAME': 'INDUSTRY3NAME'})
            hshares_industry3['INDUSTRYCODE'] = hshares_industry3['INDUSTRY3CODE'].apply(lambda x: x[:1])
            IndustryCodes = hshares_industry3['INDUSTRYCODE'].unique()
            Industries = get_industry_name(IndustryCodes, standard)
            stock_industry = hshares_industry3.merge(Industries, on='INDUSTRYCODE', how='left')
            stock_industry = stock_industry.rename(columns={'INDUSTRYNAME': 'Industry', 'SECUCODE': 'code'})[
                ['code', 'Industry']]
    elif industry_standard == 'zz':
        stock_industry = hshares_industry3.rename(columns={'INDUSTRYNAME': 'Industry', 'SECUCODE': 'code'})
    else:                # industry_standard == 'bk'
        stock_industry = pd.DataFrame(hshares_industry3['SECUCODE'].rename('code'))
        stock_industry['Industry'] = '港股'
    stock_industry = stock_industry.drop_duplicates(subset=['code'])
    return stock_industry


# 科创版行业分类，同样申万有科创版行业分类，中证没有科创版行业分类
def get_kcshares_industry(codes, industry_standard):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    if industry_standard in ['zz', 'bk']:
        if industry_standard == 'zz':
            name = '其他-科创板'
        else:
            name = '科创板'
        query = """
        select SecuCode, '{name}' as kc_Industry
        from JYDB.SecuMain
        where SecuCode in ('{InnerCode}')
        and ListedSector = 7
        """.format(name=name, InnerCode="','".join(str(i) for i in codes))
        kcshares_industry = pd.read_sql(query, conn)
    else:
        if industry_standard == 'sw':
            standard = 24
        else:                             # industry_standard == 'zjh':
            standard = 22
        query = """
        SELECT SM.SecuCode, LCSE.FirstIndustryName as kc_Industry
        FROM JYDB.SecuMain SM
        JOIN JYDB.LC_STIBExgIndustry LCSE
        ON SM.CompanyCode = LCSE.CompanyCode
        WHERE SM.SecuCode in ('%s')
        AND SM.ListedSector = 7
        AND LCSE.Standard = %s
        """ % ("','".join(str(i) for i in codes), standard)
        kcshares_industry = pd.read_sql(query, conn)
    kcshares_industry = kcshares_industry.rename(columns={'SECUCODE': 'code', 'KC_INDUSTRY': 'Industry'})
    kcshares_industry = kcshares_industry.drop_duplicates(subset=['code'])
    return kcshares_industry


# a股行业分类
def get_astock_industry(codes, industry_standard):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    if industry_standard == 'bk':
        query = """select SecuCode, ListedSector Industry from JYDB.SecuMain where SecuCode in %s
        and SecuCategory in (1, 2)""" % (str(tuple(codes)).replace(',)', ')'))
        ashares_industry = pd.read_sql(query, conn).rename(columns={'SECUCODE': 'code', 'INDUSTRY': 'Industry'})
        sector = ['主板', '中小板', '新三板', '其他', '大宗交易系统', '创业板', '科创板']
        ashares_industry['Industry'] = ashares_industry['Industry'].apply(lambda x: sector[x-1])
    else:
        if industry_standard == 'zz':
            standard = 28
        elif industry_standard == 'zjh':
            standard = 22
        else:      # industry_standard == 'sw':
            standard = 24
        query = """
        SELECT SM.SecuCode, LCEI.FirstIndustryName as Industry
        FROM JYDB.SecuMain SM
        JOIN JYDB.LC_ExgIndustry LCEI ON SM.CompanyCode = LCEI.CompanyCode
        WHERE SM.SecuCode in %s
        AND SM.SecuCategory in (1, 2)
        AND LCEI.Standard = %s
        """ % (str(tuple(codes)).replace(',)', ')'), standard)
        ashares_industry = pd.read_sql(query, conn).rename(columns={'SECUCODE': 'code', 'INDUSTRY': 'Industry'})
    ashares_industry = ashares_industry.drop_duplicates(subset=['code'])
    return ashares_industry


def get_industry(codes, industry_standard):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    groups = get_groups(np.array(codes), 1000)
    industry = []
    for i in groups:
        sub_industry = pd.concat([get_hshares_industry(i, industry_standard),
                                  get_kcshares_industry(i, industry_standard),
                                  get_astock_industry(i, industry_standard)])
        industry.append(sub_industry)
    industry = pd.concat(industry).reset_index(drop=True)
    return industry


# %%
# 前十大重仓股票
def get_max10(request_id, type):
    clients = get_client_ids()
    groups = get_groups(clients, 1000)
    logger.info('start_max_10')
    for j, i in enumerate(groups):
        logger.info(j)
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = """select src_cust_no client_id, biz_dt init_date, sec_code code, sec_nm name, hld_mktval market_value
        from bizdm.t_org_cust_d_highnav_cust_hld where src_cust_no in ({clients}) and ast_cgy_tp = '股票'
        and etl_date >= '20190101' and sec_nm is not null and hld_mktval is not null""".format(clients=str(list(i))[1:-1])
        sub_data = pd.read_sql(query, hive_conn)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where client_id in ({clients}) group by client_id, init_date""".format(clients=str(list(i))[1:-1])
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
        sub_data['init_date'] = pd.to_datetime(sub_data['init_date'].apply(
            lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-'+str(x)[6:8]))
        sub_data = sub_data.merge(sub_all, on=['client_id', 'init_date'], how='left')
        sub_data = sub_data[sub_data['mv'] != 0]
        sub_data['ratio'] = sub_data['market_value'] / sub_data['mv']
        today = pd.datetime.today().date()
        st = today - pd.DateOffset(years=4)
        m3, m6, y1 = today - pd.DateOffset(months=3), today - pd.DateOffset(months=6), today - pd.DateOffset(years=1)
        dates = [st, m3, m6, y1]
        all_data1 = []
        sub_data = sub_data[~sub_data['ratio'].isna()]
        for st_name, start in enumerate(dates):
            data1 = sub_data[sub_data['init_date'] >= start]
            if data1.empty:
                continue
            sub_data1 = data1.groupby(['client_id', 'code', 'name']).agg(
                {'ratio': 'mean', 'init_date': 'count'}).reset_index().rename(columns={'init_date': 'days'})
            sub_data1['times'] = st_name + 1
            all_data1.append(sub_data1)
        if len(all_data1) == 0:
            continue
        all_data1 = pd.concat(all_data1).groupby(['client_id', 'times']).apply(
            lambda x: x.sort_values('ratio', ascending=False).iloc[:10]).reset_index(drop=True)
        all_data1 = all_data1[['client_id', 'times', 'code', 'name', 'ratio', 'days']]
        all_data1['times'] = all_data1['times'].apply(lambda x: str(x))
        values = list(zip(all_data1['client_id'], all_data1['times'], all_data1['code'],
                          all_data1['name'], all_data1['ratio'], all_data1['days']))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.executemany(
            """
            begin
            insert into hv_max10 (client_id, time_type, SecuCode, SecuName, mean_ratio, days)
            values (:1, :2, :3, :4, :5, :6);
            exception
            when dup_val_on_index then
            update hv_max10
            set mean_ratio=:5, days=:6
            where client_id=:1 and time_type=:2 and SecuCode=:3 and SecuName=:4;
            end;
            """, values
        )
        conn.commit()
    return {'success': 'success'}


# %%
# 债券评级
def get_bond_rating(codes):
    groups = get_groups(codes, 1000)
    bond_ratings = []
    for i in groups:
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """
        SELECT BC.SecuCode, CG.CRDate, CG.CRDesc FROM JYDB.Bond_Code BC JOIN JYDB.Bond_BDCreditGrading CG
        ON BC.MainCode = CG.MainCode WHERE BC.SecuCode in %s and CG.CRDate >= to_date('2019-01-01', 'yyyy-mm-dd')
        """ % (str(tuple(i)).replace(',)', ')'))
        bond_rating = pd.read_sql(query, conn)
        bond_ratings.append(bond_rating)
    bond_ratings = pd.concat(bond_ratings).reset_index(drop=True)
    return bond_ratings.rename(columns={'SECUCODE': 'code', 'CRDATE': 'init_date', 'CRDESC': 'name'})


# 债券简称
def get_bond_chinames(codes):
    groups = get_groups(codes, 1000)
    bond_chinames = []
    for i in groups:
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """SELECT SecuCode, SecuAbbr FROM JYDB.Bond_Code WHERE SecuCode in %s
        """ % (str(tuple(i)).replace(',)', ')'))
        bond_chiname = pd.read_sql(query, conn)
        bond_chinames.append(bond_chiname)
    bond_chinames = pd.concat(bond_chinames).reset_index(drop=True)
    return bond_chinames


# 债券种类
def get_bond_natures(codes):
    groups = get_groups(codes, 1000)
    bond_natures = []
    for i in groups:
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """
        SELECT BC.SecuCode, SC.MS BondNature FROM JYDB.Bond_Code BC JOIN JYDB.CT_SystemConst SC ON BC.BondNature = SC.DM
        WHERE BC.SecuCode in %s AND SC.LB = 1243""" % (str(tuple(i)).replace(',)', ')'))
        bond_nature = pd.read_sql(query, conn)
        bond_natures.append(bond_nature)
    bond_natures = pd.concat(bond_natures).reset_index(drop=True)
    return bond_natures.rename(columns={'SECUCODE': 'code', 'BONDNATURE': 'name'})


def get_bond_type(request_id, type):
    hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                port=21050, database='bizdm', auth='LDAP')
    query = """select distinct sec_code code from bizdm.t_org_cust_d_highnav_cust_hld where ast_cgy_tp = '债券' and sec_nm is not null
    and hld_mktval is not null"""
    codes = pd.read_sql(query, hive_conn)
    zl = get_bond_natures(codes['code'].values)
    pj = get_bond_rating(codes['code'].values)
    tradingdays = get_tradingdays_oracle()
    logger.info('start_bond_type')
    for date in tradingdays:
        date_hive = date.strftime('%Y%m%d')
        logger.info(date_hive+'--------------------------')
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = f"""select src_cust_no client_id, sec_code code, hld_mktval market_value from bizdm.t_org_cust_d_highnav_cust_hld
        where ast_cgy_tp = '债券' and etl_date = '{date_hive}' and sec_nm is not null and hld_mktval is not null"""
        sub_class_data = pd.read_sql(query, hive_conn)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = f"""select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where init_date = to_date('{date_hive}', 'yyyymmdd') group by client_id, init_date"""
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
        sub_data = sub_class_data.merge(pj, on='code', how='left').sort_values(['client_id', 'code']).reset_index(drop=True)
        sub_data['name'] = sub_data.groupby(['client_id', 'code'])['name'].ffill()
        sub_data['name'] = sub_data['name'].fillna('暂无评级')
        sub_data_2 = sub_class_data.merge(zl, on='code', how='left')
        sub_data_2['name'] = sub_data_2['name'].fillna('其他')
        datas = [sub_data, sub_data_2]
        cursor.execute(f"""delete from hv_bond_type where init_date = to_date('{date_hive}', 'yyyymmdd')""")
        conn.commit()
        for k, data in enumerate(datas):
            logger.info('data_{i}'.format(i=k))
            data = data.groupby(['client_id', 'name'])['market_value'].sum().reset_index()
            data['ind'] = k+1
            data = data.merge(sub_all, on='client_id', how='left')
            data = data[data['mv'] != 0]
            data['ratio'] = data['market_value'] / data['mv']
            data = data[['client_id', 'init_date', 'ind', 'name', 'market_value', 'ratio']]
            data['init_date'] = data['init_date'].ffill().bfill().apply(lambda x: x.strftime('%Y-%m-%d'))
            data['ind'] = data['ind'].apply(lambda x: str(x))
            data = data[~data['ratio'].isna()]
            values = list(zip(data['client_id'], data['init_date'], data['ind'],
                              data['name'], data['market_value'], data['ratio']))
            conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
            cursor = conn.cursor()
            cursor.executemany(
                """
                begin
                insert into hv_bond_type (client_id, init_date, industry_type, industry_name, market_value, ratio)
                values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5, :6);
                exception
                when dup_val_on_index then
                update hv_bond_type
                set market_value=:5, ratio=:6
                where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd') and industry_type=:3 and industry_name=:4;
                end;
                """, values
            )
            conn.commit()
    return {'success': 'success'}


# %%
# 期权资产占比
def get_option_type(request_id, type):
    tradingdays = get_tradingdays_oracle()
    for date in tradingdays:
        date_hive = date.strftime('%Y%m%d')
        logger.info(date_hive+'--------------------------')
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = f"""select src_cust_no client_id, sec_code code, sec_nm name, hld_mktval market_value from bizdm.t_org_cust_d_highnav_cust_hld
        where etl_date = '{date_hive}' and ast_cgy_tp = '衍生品' and sec_nm is not null and hld_mktval is not null"""
        sub_class_data = pd.read_sql(query, hive_conn)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = f"""select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where init_date = to_date('{date_hive}', 'yyyymmdd') group by client_id, init_date"""
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
        sub_class_data = sub_class_data.merge(sub_all, on='client_id', how='inner')
        if sub_class_data.empty:
            continue
        sub_data1 = sub_class_data.copy()
        sub_data1.loc[sub_data1['name'].str.contains('50ETF'), 'industry_name'] = '50ETF'
        sub_data1.loc[sub_data1['name'].str.contains('300ETF'), 'industry_name'] = '300ETF'
        sub_data1 = sub_data1.groupby(['client_id', 'init_date', 'industry_name'])['market_value'].sum().reset_index()
        sub_data1['industry_type'] = 1
        sub_all1 = sub_data1.groupby(['client_id', 'init_date'])['market_value'].sum().rename('mv').reset_index()
        data1 = sub_data1.merge(sub_all1, on=['client_id', 'init_date'], how='left')
        data1 = data1[data1['mv'] != 0]
        data1['ratio'] = data1['market_value'] / data1['mv']
        sub_data2 = sub_class_data.copy()
        sub_data2.loc[sub_data2['name'].str.contains('购'), 'industry_name'] = '认购'
        sub_data2.loc[sub_data2['name'].str.contains('沽'), 'industry_name'] = '认沽'
        sub_data2 = sub_data2.groupby(['client_id', 'init_date', 'industry_name'])['market_value'].sum().reset_index()
        sub_data2['industry_type'] = 2
        sub_all2 = sub_data2.groupby(['client_id', 'init_date'])['market_value'].sum().rename('mv').reset_index()
        data2 = sub_data2.merge(sub_all2, on=['client_id', 'init_date'], how='left')
        data2 = data2[data2['mv'] != 0]
        data2['ratio'] = data2['market_value'] / data2['mv']
        data0 = pd.concat([data1, data2]).reset_index(drop=True)
        data0 = data0[['client_id', 'init_date', 'industry_type', 'industry_name', 'market_value', 'ratio']]
        data0['init_date'] = data0['init_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        data0 = data0[~data0['ratio'].isna()]
        data0['industry_type'] = data0['industry_type'].apply(lambda x: str(x))
        values = list(zip(data0['client_id'], data0['init_date'], data0['industry_type'],
                          data0['industry_name'], data0['market_value'], data0['ratio']))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.executemany(
            """
            begin
            insert into hv_option_type (client_id, init_date, industry_type, industry_name, market_value, ratio)
            values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4, :5, :6);
            exception
            when dup_val_on_index then
            update hv_option_type
            set market_value=:5, ratio=:6
            where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd') and industry_type=:3 and industry_name=:4;
            end;
            """, values
        )
        conn.commit()
    return {'success': 'success'}


# %%
# 前五大债券持仓
def get_max5(request_id, type):
    clients = get_client_ids()
    groups = get_groups(clients, 1000)
    logger.info('start_option')
    for j, i in enumerate(groups):
        logger.info(j)
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = """select src_cust_no client_id, biz_dt init_date, sec_code code, sec_nm name, hld_mktval market_value
        from bizdm.t_org_cust_d_highnav_cust_hld where src_cust_no in ({clients}) and ast_cgy_tp = '债券'
        and etl_date >= '20190101' and sec_nm is not null and hld_mktval is not null""".format(clients=str(list(i))[1:-1])
        sub_class_data = pd.read_sql(query, hive_conn).sort_values(['client_id', 'init_date'])
        sub_class_data['init_date'] = pd.to_datetime(sub_class_data['init_date'].apply(
            lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8]))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = """select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where client_id in ({clients}) group by client_id, init_date""".format(clients=str(list(i))[1:-1])
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
        sub_data = sub_class_data.merge(sub_all, on=['client_id', 'init_date'], how='left')
        sub_data = sub_data[sub_data['mv'] != 0]
        sub_data['ratio'] = sub_data['market_value'] / sub_data['mv']
        sub_data = sub_data[~sub_data['ratio'].isna()]
        today = pd.datetime.today().date()
        st = today - pd.DateOffset(years=4)
        m3, m6, y1 = today - pd.DateOffset(months=3), today - pd.DateOffset(months=6), today - pd.DateOffset(years=1)
        dates = [st, m3, m6, y1]
        all_data1 = []
        for st_name, start in enumerate(dates):
            data1 = sub_data[sub_data['init_date'] >= start]
            if data1.empty:
                continue
            sub_data1 = data1.groupby(['client_id', 'code', 'name']).agg(
                {'ratio': 'mean', 'init_date': 'count'}).reset_index().rename(columns={'init_date': 'days'})
            sub_data1['times'] = st_name + 1
            all_data1.append(sub_data1)
        if len(all_data1) == 0:
            continue
        all_data1 = pd.concat(all_data1).groupby(['client_id', 'times']).apply(
            lambda x: x.sort_values('ratio', ascending=False).iloc[:5]).reset_index(drop=True)
        all_data1 = all_data1[['client_id', 'times', 'code', 'name', 'ratio', 'days']]
        values = list(zip(all_data1['client_id'], all_data1['times'], all_data1['code'],
                          all_data1['name'], all_data1['ratio'], all_data1['days']))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.executemany(
            """
            begin
            insert into hv_max5 (client_id, time_type, SecuCode, SecuName, mean_ratio, days)
            values (:1, :2, :3, :4, :5, :6);
            exception
            when dup_val_on_index then
            update hv_max5
            set mean_ratio=:5, days=:6
            where client_id=:1 and time_type=:2 and SecuCode=:3 and SecuName=:4;
            end;
            """, values
        )
        conn.commit()
    return {'success': 'success'}


# %%
# 股票集中度
def get_stock_concern(request_id, type):
    tradingdays = get_tradingdays_oracle()
    for date in tradingdays:
        date_hive = date.strftime('%Y%m%d')
        logger.info(date_hive+'--------------------------')
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = f"""select src_cust_no client_id, sec_code code, hld_mktval market_value from bizdm.t_org_cust_d_highnav_cust_hld
        where etl_date = '{date_hive}' and ast_cgy_tp = '股票' and sec_nm is not null and hld_mktval is not null"""
        sub_class_data = pd.read_sql(query, hive_conn)
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        query = f"""select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
        where init_date = to_date('{date_hive}', 'yyyymmdd') group by client_id, init_date"""
        sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
        sub_data = sub_class_data.merge(sub_all, on='client_id', how='left')
        sub_data = sub_data[sub_data['mv'] != 0]
        sub_data['ratio'] = sub_data['market_value'] / sub_data['mv']
        sub_data = sub_data[~sub_data['ratio'].isna()]
        sub_data = sub_data.sort_values(['client_id', 'init_date']).reset_index(drop=True)
        sub_data['init_date'] = sub_data['init_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        if sub_data.empty:
            continue
        sub_data_all = []
        sub_data = sub_data.sort_values(['client_id', 'init_date', 'ratio'], ascending=False)
        for k in range(1, 5):
            logger.info(k)
            if k < 4:
                if k == 1:
                    sub_data0 = sub_data.groupby(['client_id', 'init_date'])['ratio'].apply(
                        lambda x: x[:3].sum()).reset_index()
                elif k == 2:
                    sub_data0 = sub_data.groupby(['client_id', 'init_date'])['ratio'].apply(
                        lambda x: x[:5].sum()).reset_index()
                else:
                    sub_data0 = sub_data.groupby(['client_id', 'init_date'])['ratio'].sum().reset_index()
            else:
                sub_data0 = sub_data.groupby(['client_id', 'init_date'])['ratio'].count().reset_index()
            sub_data0['name'] = k
            sub_data_all.append(sub_data0)
        sub_data_all = pd.concat(sub_data_all)[['client_id', 'init_date', 'name', 'ratio']]
        values = list(zip(sub_data_all['client_id'], sub_data_all['init_date'],
                          sub_data_all['name'], sub_data_all['ratio']))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.executemany(
            """
            begin
            insert into hv_concern (client_id, init_date, industry_name, ratio)
            values (:1, to_date(:2, 'yyyy-mm-dd'), :3, :4);
            exception
            when dup_val_on_index then
            update hv_concern
            set ratio=:4
            where client_id=:1 and init_date=to_date(:2, 'yyyy-mm-dd') and industry_name=:3;
            end;
            """, values
        )
        conn.commit()
    return {'success': 'success'}


# %%
# 前十大重仓个股--页面接口
def get_max10_port(request_id, type, client_id, start_date, end_date):
    if len(end_date) == 0:
        return []
    hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                port=21050, database='bizdm', auth='LDAP')
    sd, ed = start_date.replace('-', ''), end_date.replace('-', '')
    query = """select sec_code code, sec_nm name, biz_dt, hld_mktval mv from bizdm.t_org_cust_d_highnav_cust_hld
    where src_cust_no = '{cl}' and ast_cgy_tp = '股票' and etl_date >= '{sd}' and etl_date <= '{ed}' and sec_nm is not null
    and hld_mktval is not null order by etl_date asc""".format(cl=client_id, sd=sd, ed=ed)
    sub_data = pd.read_sql(query, hive_conn).rename(columns={'biz_dt': 'init_date'})
    if sub_data.empty:
        return []
    secu_info = sub_data.drop_duplicates(subset=['code'], keep='last')[['code', 'name']].reset_index(drop=True)
    sub_data['init_date'] = pd.to_datetime(sub_data['init_date'].apply(
        lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8]))
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select init_date, sum(market_value) mvs from zdj.hv_asset_allocation where client_id = '{clients}'
    and init_date >= to_date('{sd}', 'yyyy-mm-dd') and init_date <= to_date('{ed}', 'yyyy-mm-dd') group by init_date
    """.format(clients=client_id, sd=start_date, ed=end_date)
    sub_all = pd.read_sql(query, conn).rename(columns={'INIT_DATE': 'init_date', 'MVS': 'mvs'}).sort_values('init_date')
    if sub_all.empty:
        return []
    sub_data = sub_data.merge(sub_all, on='init_date', how='left')
    sub_data['mv'] = sub_data['mv'].fillna(0)
    sub_data['ratio'] = sub_data['mv'] / sub_data['mvs']
    ratio_data = sub_data.groupby(['code'])['ratio'].mean().reset_index()
    days = len(sub_data['init_date'].unique())
    days_data = sub_data.groupby(['code'])['init_date'].count().rename('days').reset_index()
    all_data = ratio_data.merge(days_data, on=['code'], how='left')
    all_data = all_data.merge(secu_info, on=['code'], how='left')
    all_data['ratio'] = all_data['ratio'] * all_data['days'] / days
    all_data = all_data.sort_values('ratio', ascending=False).reset_index(drop=True).iloc[:10]
    all_data = all_data[['code', 'name', 'ratio', 'days']]
    all_data['ratio'] = all_data['ratio'].astype('float')
    all_data['days'] = all_data['days'].astype('int')
    result = [dict(all_data.iloc[i]) for i in range(len(all_data))]
    return result


# %%
# 前五大重仓债券--页面接口
def get_max5_port(request_id, type, client_id, start_date, end_date):
    if len(end_date) == 0:
        return []
    hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                port=21050, database='bizdm', auth='LDAP')
    sd, ed = start_date.replace('-', ''), end_date.replace('-', '')
    query = """select sec_code code, sec_nm name, biz_dt, hld_mktval mv from bizdm.t_org_cust_d_highnav_cust_hld
    where src_cust_no = '{cl}' and ast_cgy_tp = '债券' and etl_date >= '{sd}' and etl_date <= '{ed}' and sec_nm is not null
    and hld_mktval is not null order by etl_date asc""".format(cl=client_id, sd=sd, ed=ed)
    sub_data = pd.read_sql(query, hive_conn).rename(columns={'biz_dt': 'init_date'})
    if sub_data.empty:
        return []
    secu_info = sub_data.drop_duplicates(subset=['code'], keep='last')[['code', 'name']].reset_index(drop=True)
    sub_data['init_date'] = pd.to_datetime(sub_data['init_date'].apply(
        lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8]))
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select init_date, sum(market_value) mvs from zdj.hv_asset_allocation where client_id = '{clients}'
    and init_date >= to_date('{sd}', 'yyyy-mm-dd') and init_date <= to_date('{ed}', 'yyyy-mm-dd') group by init_date
    """.format(clients=client_id, sd=start_date, ed=end_date)
    sub_all = pd.read_sql(query, conn).rename(columns={'INIT_DATE': 'init_date', 'MVS': 'mvs'})
    if sub_all.empty:
        return []
    sub_data = sub_data.merge(sub_all, on='init_date', how='left')
    sub_data['mv'] = sub_data['mv'].fillna(0)
    sub_data['ratio'] = sub_data['mv'] / sub_data['mvs']
    ratio_data = sub_data.groupby(['code'])['ratio'].mean().reset_index()
    days = len(sub_data['init_date'].unique())
    days_data = sub_data.groupby(['code'])['init_date'].count().rename('days').reset_index()
    all_data = ratio_data.merge(days_data, on=['code'], how='left')
    all_data = all_data.merge(secu_info, on=['code'], how='left')
    all_data['ratio'] = all_data['ratio'] * all_data['days'] / days
    all_data = all_data.sort_values('ratio', ascending=False).reset_index(drop=True).iloc[:5]
    all_data = all_data[['code', 'name', 'ratio', 'days']]
    all_data['ratio'] = all_data['ratio'].astype('float')
    all_data['days'] = all_data['days'].astype('int')
    return [dict(all_data.iloc[i]) for i in range(len(all_data))]


# %%
# 前十大盈利亏损个股--页面接口
def get_max10_profit(request_id, type, client_id, start_date, end_date):
    if len(end_date) == 0:
        return []
    hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                port=21050, database='bizdm', auth='LDAP')
    sd, ed = start_date.replace('-', ''), end_date.replace('-', '')
    query = """select sec_code code, sec_nm name, biz_dt, hld_prft mv20, hld_mktval mv1
    from bizdm.t_org_cust_d_highnav_cust_hld where src_cust_no = '{cl}' and ast_cgy_tp = '股票' and etl_date >= '{sd}'
    and etl_date <= '{ed}' and sec_nm is not null and hld_mktval is not null order by etl_date asc""".format(
        cl=client_id, sd=sd, ed=ed)
    sub_data = pd.read_sql(query, hive_conn).rename(columns={'biz_dt': 'init_date'})
    if sub_data.empty:
        return []
    sub_data['mv2'] = sub_data.groupby('code')['mv20'].apply(get_daily_data)
    secu_info = sub_data.drop_duplicates(subset=['code'], keep='last')[['code', 'name']].reset_index(drop=True)
    sub_data['init_date'] = sub_data['init_date'].apply(lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8])
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select init_date, sum(market_value) mvs from zdj.hv_asset_allocation where client_id = '{clients}'
    and init_date >= to_date('{sd}', 'yyyy-mm-dd') and init_date <= to_date('{ed}', 'yyyy-mm-dd') group by init_date
    """.format(clients=client_id, sd=start_date, ed=end_date)
    sub_all = pd.read_sql(query, conn).rename(columns={'INIT_DATE': 'init_date', 'MVS': 'mvs'})
    if sub_all.empty:
        return []
    sub_all['init_date'] = sub_all['init_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    sub_data = sub_data.merge(sub_all, on='init_date', how='left')
    sub_data['mv1'] = sub_data['mv1'].fillna(0)
    sub_data['mv2'] = sub_data['mv2'].fillna(0)
    sub_data['ratio1'] = sub_data['mv1'] / sub_data['mvs']
    sub_data['ratio2'] = sub_data['mv2'] / sub_data['mvs']
    ratio1_data = sub_data.groupby(['code'])['ratio1'].mean().reset_index()
    ratio2_data = sub_data.groupby(['code'])['ratio2'].apply(lambda x: (1+x).prod()-1).reset_index()
    days = len(sub_data['init_date'].unique())
    days_data = sub_data.groupby(['code'])['init_date'].count().rename('days').reset_index()
    ratio1_data = ratio1_data.merge(days_data, on=['code'], how='left')
    ratio1_data['ratio1'] = ratio1_data['ratio1'] * ratio1_data['days'] / days
    ratio1_data = ratio1_data.merge(secu_info, on=['code'], how='left')
    ratio_data = ratio1_data[['code', 'name', 'ratio1']].merge(ratio2_data, on=['code'], how='inner')
    d01 = ratio_data[ratio_data['ratio2'] >= 0].sort_values('ratio2', ascending=False).reset_index(drop=True)
    d02 = ratio_data[ratio_data['ratio2'] < 0].sort_values('ratio2').reset_index(drop=True)
    d1, d2 = d01.iloc[:10], d02.iloc[:10]
#     wining = pd.DataFrame([['盈利', len(d01), len(ratio_data), d01['ratio1'].sum()],
#                            ['亏损', len(d02), len(ratio_data), d02['ratio1'].sum()]],
#                            columns=['type', 'num', 'all_num', 'ratio1'])
    wining = pd.DataFrame([['盈利', len(d01), len(ratio_data), len(d01)/len(ratio_data)],
                           ['亏损', len(d02), len(ratio_data), len(d02)/len(ratio_data)]],
                           columns=['type', 'num', 'all_num', 'ratio1'])

    return {'pdata': [dict(d1.iloc[i]) for i in range(len(d1))], 'ndata': [dict(d2.iloc[i]) for i in range(len(d2))],
            'wining': [dict(wining.iloc[i]) for i in range(len(wining))]}


def get_daily_data(x):
    x0 = x - x.shift(1)
    return x0.fillna(x.iloc[0])




