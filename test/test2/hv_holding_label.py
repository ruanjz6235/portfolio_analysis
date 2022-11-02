import pandas as pd
import numpy as np
import cx_Oracle as cx
import datetime as dt
from dateutil.relativedelta import relativedelta
from pyhive import hive
from app.common.log import logger
from .get_style_holdings import get_fi_typelist, get_fi_ratinglist
# conn = hive.Connection(host='10.8.13.120', port=10000, username='hdfs', database='default')
hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR', port=21050,
                            database='bizdm', auth='LDAP')
hive_cursor = hive_conn.cursor()
conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
cursor = conn.cursor()


def get_client_ids():
    hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR', port=21050,
                                database='bizdm', auth='LDAP')
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


def get_tradingdays_oracle(sd):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    query = """select TradingDate from JYDB.QT_TradingDayNew where TradingDate >= to_date('{sd}', 'yyyy-mm-dd')
    and TradingDate <= to_date('{today}', 'yyyy-mm-dd') and iftradingday = 1 and secumarket in (83, 90)""".format(
        sd=sd.strftime('%Y-%m-%d'), today=pd.datetime.today().strftime('%Y-%m-%d'))
    return pd.read_sql(query, conn)['TRADINGDATE'].tolist()


# %%
def get_holding_label(request_id, type):
    clients = get_client_ids()
    groups = get_groups(clients, 1000)
    logger.info('start_stock_label')
    sd = pd.datetime.today().date() - pd.DateOffset(years=1)
    for j, i in enumerate(groups):
        logger.info(j)
        stock_label = get_stock_label(i, sd)
        bond_label = get_bond_label(i, sd)
        option_label = get_option_label(i, sd)
        total_label = pd.DataFrame()
        for k, sub_label in enumerate([stock_label, bond_label, option_label]):
            if k == 0:
                total_label = sub_label.copy()
            else:
                total_label = total_label.merge(sub_label, on=['client_id'], how='outer')
        total_label = total_label.fillna(0)
        label_values = list(zip(
            total_label['client_id'], total_label['weight'], total_label['con'], total_label['num'],
            total_label['cb_label'], total_label['interest'], total_label['credit'], total_label['high_rating'],
            total_label['low_rating'], total_label['short_duration'], total_label['long_duration'],
            total_label['option']))
        conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
        cursor = conn.cursor()
        cursor.executemany(
            """
            begin
            insert into hv_holding_label (client_id, weight, con, num, cb_label, interest, credit, high_rating,
            low_rating, short_duration, long_duration, option_label)
            values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12);
            exception
            when dup_val_on_index then
            update hv_holding_label
            set weight=:2, con=:3, num=:4, cb_label=:5, interest=:6, credit=:7, high_rating=:8, low_rating=:9,
            short_duration=:10, long_duration=:11, option_label=:12
            where client_id=:1;
            end;
            """, label_values
        )
        conn.commit()
    return {'success': 'success'}


def get_asset_weight(asset_type, i, sd):
    tradingdays = get_tradingdays_oracle(sd)
    tradingdays = np.array([d.strftime('%Y%m%d') for d in tradingdays])
    dates_group = get_groups(tradingdays, 21)
    sub_class_data = []
    for sub_dates in dates_group:
        hive_conn = hive.Connection(host='10.52.40.222', username='fundrating', password='6B2O02sP1OhYoLlX12OR',
                                    port=21050, database='bizdm', auth='LDAP')
        query = """select src_cust_no client_id, biz_dt init_date, sec_code code, hld_mktval market_value
        from bizdm.t_org_cust_d_highnav_cust_hld where src_cust_no in ({clients}) and ast_cgy_tp = '{asset}'
        and etl_date in ({sd}) and sec_nm is not null and hld_mktval is not null""".format(
            clients=str(list(i))[1:-1], asset=asset_type, sd=str(list(sub_dates))[1:-1])
        sub2_class_data = pd.read_sql(query, hive_conn)
        sub_class_data.append(sub2_class_data)
    sub_class_data = pd.concat(sub_class_data)
    if sub_class_data.empty:
        return pd.DataFrame(columns=['client_id', 'init_date', 'code', 'market_value', 'mv', 'ratio'])
    sub_class_data['init_date'] = pd.to_datetime(sub_class_data['init_date'].apply(
        lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8]))
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select client_id ids, init_date dates, sum(market_value) mv from zdj.hv_asset_allocation
    where client_id in ({clients}) and init_date >= to_date('{sd}', 'yyyy-mm-dd')
    group by client_id, init_date""".format(clients=str(list(i))[1:-1], sd=sd.strftime('%Y-%m-%d'))
    sub_all = pd.read_sql(query, conn).rename(columns={'IDS': 'client_id', 'DATES': 'init_date', 'MV': 'mv'})
    sub_all = sub_all[~sub_all['mv'].isna()]
    sub_data = sub_class_data.merge(sub_all, on=['client_id', 'init_date'], how='inner')
    sub_data = sub_data[sub_data['mv'] != 0]
    sub_data['ratio'] = sub_data['market_value'] / sub_data['mv']
    return sub_data


def get_stock_label(i, sd):
    sub_data = get_asset_weight('股票', i, sd)[['client_id', 'init_date', 'ratio']]
    if sub_data.empty:
        return pd.DataFrame(columns=['client_id', 'weight', 'con', 'num'])
    sub_data['init_date'] = sub_data['init_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    data = sub_data[~sub_data['ratio'].isna()]
    weight = data.groupby(['client_id', 'init_date'])['ratio'].sum().reset_index().groupby('client_id')[
        'ratio'].mean().rename('weight').reset_index()
    sub10 = data.groupby(['client_id', 'init_date'])['ratio'].apply(lambda x: x.sort_values().iloc[:5]).reset_index()
    con = sub10.groupby(['client_id', 'init_date'])['ratio'].sum().reset_index().groupby('client_id')[
        'ratio'].mean().rename('con').reset_index()
    num = data.groupby(['client_id', 'init_date'])['ratio'].count().reset_index().groupby('client_id')[
        'ratio'].mean().rename('num').reset_index()
    sub_all_data = weight.merge(con, on=['client_id'], how='outer')
    sub_all_data = sub_all_data.merge(num, on=['client_id'], how='outer')
    return sub_all_data


def get_bond_label(i, sd):
    bond_data = get_asset_weight('债券', i, sd)[['client_id', 'init_date', 'code', 'ratio']]
    if bond_data.empty:
        return pd.DataFrame(columns=['client_id', 'cb_label', 'interest', 'credit', 'high_rating', 'low_rating',
                                     'short_duration', 'long_duration'])
    cb_code = get_cb_code()
    cb_data = bond_data[bond_data['code'].isin(cb_code)]
    if cb_data.empty:
        cb_label = pd.DataFrame(columns=['client_id', 'cb_label'])
    else:
        cb_label = cb_data.groupby(['client_id', 'init_date'])['ratio'].sum().reset_index().groupby('client_id')[
            'ratio'].mean().rename('cb_label').reset_index()
    fi_data = bond_data[~bond_data['code'].isin(cb_code)]
    if fi_data.empty:
        fi_data = pd.DataFrame(columns=['client_id', 'interest', 'credit', 'high_rating', 'low_rating',
                                        'short_duration', 'long_duration'])
        bond_label = cb_label.merge(fi_data, on=['client_id'], how='outer')
        return bond_label
    fiType, fiRating, fiDuration = get_fi_data0(list(fi_data['code'].unique()), sd.strftime('%Y-%m-%d'))
    if fiType.empty:
        fiType_label = fi_data[['client_id']].drop_duplicates()
        fiType_label['interest'] = 0
        fiType_label['credit'] = 0
    else:
        t_l = get_bond_type(fi_data, fiType)
        fiType_label = t_l.groupby(['client_id', 'init_date', 'type_new'])['ratio'].sum().reset_index().groupby(
            ['client_id', 'type_new'])['ratio'].mean().reset_index()
        fiType_label = fiType_label.pivot(columns='type_new', index='client_id', values='ratio').reset_index().fillna(0)
        other1 = list(set(['interest', 'credit']) - set(fiType_label.columns))
        if len(other1) > 0:
            fiType_label[other1[0]] = 0
    if fiRating.empty:
        fiRating_label = fi_data[['client_id']].drop_duplicates()
        fiRating_label['high_rating'] = 0
        fiRating_label['low_rating'] = 0
    else:
        rating = get_rating(fi_data, fiRating)
        r_l = fi_data.merge(rating, on=['code', 'init_date'], how='inner')
        fiRating_label = r_l.groupby(['client_id', 'init_date', 'rating_type'])['ratio'].sum().reset_index().groupby(
            ['client_id', 'rating_type'])['ratio'].mean().reset_index()
        fiRating_label = fiRating_label.pivot(columns='rating_type', index='client_id', values='ratio').reset_index().fillna(0)
        other2 = list(set(['high_rating', 'low_rating']) - set(fiRating_label.columns))
        if len(other2) > 0:
            for m in other2:
                fiRating_label[m] = 0
    if fiDuration.empty:
        fiDuration_label = fi_data[['client_id']].drop_duplicates()
        fiDuration_label['short_duration'] = 0
        fiDuration_label['long_duration'] = 0
    else:
        duration = get_duration(fi_data, fiDuration)
        d_l = fi_data.merge(duration, on=['code', 'init_date'], how='inner')
        fiDuration_label = d_l.groupby(['client_id', 'init_date', 'd_type'])['ratio'].sum().reset_index().groupby(
            ['client_id', 'd_type'])['ratio'].mean().reset_index()
        fiDuration_label = fiDuration_label.pivot(columns='d_type', index='client_id', values='ratio').reset_index().fillna(0)
        other3 = list(set(['short_duration', 'long_duration']) - set(fiDuration_label.columns))
        if len(other3) > 0:
            for n in other3:
                fiDuration_label[n] = 0
    bond_label_data = [cb_label, fiType_label, fiRating_label, fiDuration_label]
    bond_label = pd.DataFrame()
    for j, p in enumerate(bond_label_data):
        if j == 0:
            bond_label = p.copy()
        else:
            bond_label = bond_label.merge(p, on=['client_id'], how='outer')
    return bond_label.fillna(0)


def get_bond_type(fi_data, fiType):
    fiTypeList = get_fi_typelist()
    interest_type = ['央行票据', '国库现金管理', '国债现货', '同业存单']
    credit_type = list(set(fiTypeList) - set(interest_type))
    t_l = fi_data.merge(fiType, on='code', how='inner')
    t_l.loc[t_l['bond_type'].isin(interest_type), 'type_new'] = 'interest'
    t_l.loc[t_l['bond_type'].isin(credit_type), 'type_new'] = 'credit'
    t_l['type_new'] = t_l['type_new'].fillna('other_type')
    return t_l


def get_duration(fi_data, fiDuration):
    weight_dates = fi_data[['code', 'init_date']].drop_duplicates()
    duration = weight_dates.merge(fiDuration, on=['code', 'init_date'], how='outer').sort_values(
        ['code', 'init_date']).reset_index(drop=True)
    duration = duration.groupby('code').apply(get_complete_duration).reset_index(drop=True)
    duration.loc[duration['duration'] <= 3, 'd_type'] = 'short_duration'
    duration.loc[duration['duration'] > 3, 'd_type'] = 'long_duration'
    return duration


def get_complete_duration(duration):
    duration_nan = duration[duration["duration"].isna()]
    duration_term = duration[~duration["duration"].isna()]
    if duration_term.empty:
        return pd.DataFrame(columns=['code', 'init_date', 'duration'])
    for i in duration_nan['init_date']:
        before = duration_term[duration_term['init_date'] < i]
        after = duration_term[duration_term['init_date'] > i]
        if before.empty:
            i2 = min(after['init_date'])
            d2 = duration[duration['init_date'] == i2]["duration"].iloc[0]
            duration.loc[duration['init_date'] == i, "duration"] = (i2-i).days/360+d2
        elif after.empty:
            i1 = max(before['init_date'])
            d1 = duration[duration['init_date'] == i1]["duration"].iloc[0]
            duration.loc[duration['init_date'] == i, "duration"] = d1-(i-i1).days/360
        else:
            i1 = max(before['init_date'])
            i2 = min(after['init_date'])
            d1 = duration[duration['init_date'] == i1]["duration"].iloc[0]
            d2 = duration[duration['init_date'] == i2]["duration"].iloc[0]
            duration.loc[duration['init_date'] == i, "duration"] = (d2*(i-i1).days+d1*(i2-i).days)/(i2-i1).days
    return duration


def get_rating(fi_data, fiRating):
    fiRatingList = get_fi_ratinglist()
    high_rating = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA']
    low_rating = list(set(fiRatingList) - set(high_rating))
    fiRating.loc[fiRating['rating'].isin(high_rating), 'rating_type'] = 1
    fiRating.loc[fiRating['rating'].isin(low_rating), 'rating_type'] = -1
    rating = match_fi_rating(fi_data, fiRating)
    return rating


def match_fi_rating(fi_data, fiRating):
    weight_dates = fi_data[['init_date']].drop_duplicates()
    ratings = fiRating.pivot(columns='code', values='rating_type', index='init_date').reset_index()
    ratings = weight_dates.merge(ratings, on='init_date', how='outer').sort_values('init_date').set_index('init_date')
    ratings = ratings.ffill().bfill().reset_index()
    rating = pd.melt(ratings, id_vars='init_date', var_name='code', value_name='rating_type')
    rating.loc[rating['rating_type'] == 1, 'rating_type'] = 'high_rating'
    rating.loc[rating['rating_type'] == -1, 'rating_type'] = 'low_rating'
    return rating


def get_bond_codes(SecuCode):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    cursor.execute("""select SecuCode, InnerCode from JYDB.SecuMain where SecuCode in (%s)
    and SecuCategory in (5,6,7,8,9,11,12,17,18,19,23,28,29,30,31,32,33,36,37,38)""" % (str(SecuCode)[1:-1]))
    dataset1 = cursor.fetchall()
    data1 = pd.DataFrame(list(dataset1), columns=["code", "InnerCode"])
    InnerCode = data1['InnerCode']
    return data1, InnerCode


def get_fi_type(data1, InnerCode):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    cursor.execute("""select  BD.InnerCode, CT.MS from JYDB.Bond_BasicInfoN BD inner join JYDB.CT_SystemConst CT
    on BD.BondNature=CT.DM where BD.InnerCode in (%s) and CT.LB=1243""" % (",".join(format(i) for i in InnerCode)))
    dataset2 = cursor.fetchall()
    data2 = pd.DataFrame(list(dataset2), columns=["InnerCode", "bond_type"])
    fiType = data2.merge(data1, on='InnerCode', how='inner')
    fiType = fiType[~fiType['bond_type'].isna()].drop_duplicates(subset=['code'])
    return fiType[['code', 'bond_type']]


def get_fi_rating(data1, InnerCode, end_day):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    cursor.execute("""select MainCode, CRDate, CT.MS from JYDB.Bond_BDCreditGrading BD 
    inner join JYDB.CT_SystemConst CT on BD.CRCode = CT.DM where CT.LB in (1407,1372,1704) and MainCode in (%s)
    and CRDate <= to_date('%s', 'yyyy-mm-dd')""" % (",".join(format(i) for i in InnerCode), end_day))
    dataset2 = cursor.fetchall()
    data2 = pd.DataFrame(list(dataset2), columns=["InnerCode", "init_date", "rating"])
    fiRating = data2.merge(data1, on='InnerCode', how='inner')
    fiRating = fiRating[~fiRating['rating'].isna()].drop_duplicates(subset=['code', 'init_date'])
    return fiRating[["code", "init_date", "rating"]]


def get_fi_duration(data1, InnerCode):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    cursor = conn.cursor()
    cursor.execute("""select innerCode, TradingDay, ModifiedDuration_CL from JYDB.Bond_ExchangeQuote
    where InnerCode in (%s)""" % (",".join(format(i) for i in InnerCode), ))
    dataset2 = cursor.fetchall()
    data2 = pd.DataFrame(list(dataset2), columns=["InnerCode", "init_date", "duration"])
    fiDuration = data2.merge(data1, on='InnerCode', how='inner')
    fiDuration = fiDuration[~fiDuration['duration'].isna()].drop_duplicates(subset=['code', 'init_date'])
    return fiDuration[["code", "init_date", "duration"]].sort_values(['code', 'init_date']).reset_index(drop=True)


def get_fi_data0(SecuCode, end_day):
    data1, InnerCode = get_bond_codes(SecuCode)
    fiType = get_fi_type(data1, InnerCode)
    fiRating = get_fi_rating(data1, InnerCode, end_day)
    fiDuration = get_fi_duration(data1, InnerCode)
    return fiType, fiRating, fiDuration


def get_cb_code():
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = """select bc.SecuCode from JYDB.Bond_Code bc inner join JYDB.Bond_ConBDBasicInfo bi
    on bc.InnerCode = bi.InnerCode"""
    return pd.read_sql(query, conn)['SECUCODE'].tolist()


def get_option_label(i, sd):
    option_data = get_asset_weight('衍生品', i, sd)[['client_id', 'init_date', 'ratio']]
    if option_data.empty:
        return pd.DataFrame(columns=['client_id', 'option'])
    option_label = option_data.groupby(['client_id', 'init_date'])['ratio'].sum().reset_index().groupby('client_id')[
        'ratio'].mean().rename('option').reset_index()
    return option_label












