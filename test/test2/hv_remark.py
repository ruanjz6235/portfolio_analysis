from __future__ import division
import pandas as pd
from .hv_return_risk import *
from .brinson import *


# 产品概览
def remark_1(request_id, type, client_id):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    query = f"""select client_id, init_date, daily_return from hv_return where client_id = '{client_id}'
    and daily_return is not null order by init_date asc"""
    client_return = pd.read_sql(query, conn).rename(columns={'CLIENT_ID': 'client_id', 'INIT_DATE': 'init_date',
                                                             'DAILY_RETURN': 'daily_return'})
    remark = ''
    if client_return.empty:
        return '暂无数据'
    start_date, end_date = client_return['init_date'].min(), client_return['init_date'].max()
    if end_date <= pd.datetime.today().date() - pd.DateOffset(days=60):
        return '近期未更新数据'
    index_return = get_index_return('000300', '2019-01-01', pd.datetime.today().date().strftime('%Y-%m-%d'))
    if start_date >= pd.datetime.today().date() - pd.DateOffset(years=1):
        time_period, time_type = '不满1年', '开户以来'
    elif (start_date <= pd.datetime.today().date() - pd.DateOffset(years=1)) & (start_date >= pd.datetime.today().date() - pd.DateOffset(years=2)):
        time_period, time_type = '超过1年', '近1年'
        start_date = pd.datetime.today().date() - pd.DateOffset(years=1)
    else:
        time_period, time_type = '超过2年', '近2年'
        start_date = pd.datetime.today().date() - pd.DateOffset(years=2)
    remark += f'该用户开户{time_period}，{time_type}收益率为'
    ci = client_return[client_return['init_date'] >= start_date]
    ii = index_return[index_return['EndDate'] >= start_date]
    cdr = get_return_mdd(ci[['init_date', 'daily_return']])[0]
    idr = get_return_mdd(ii[['EndDate', 'index_return']].rename(columns={'EndDate': 'init_date'}))[0]
    if time_period != '不满1年':
        query = f"""select annureturn_1y_rank/totalnumber_1y rrank1, annureturn_cy_rank/totalnumber_cy rrankc,
        annureturn_3y_rank/totalnumber_3y rrank3, maximumdrawdown_1y_rank/totalnumber_1y mrank1,
        maximumdrawdown_cy_rank/totalnumber_cy mrankc, maximumdrawdown_3y_rank/totalnumber_3y mrank3
        from zdj.zs_client_rank_tertile where client_id = '{client_id}'"""
        rank = pd.read_sql(query, conn)
        if not rank.empty:
            if time_period == '超过1年':
                rank_data = [rank['RRANK1'].iloc[0], rank['MRANK1'].iloc[0]]
            else:
                rank_data = [rank['RRANK3'].iloc[0], rank['MRANK3'].iloc[0]]
        else:
            rank_data = []
    else:
        rank_data = []
    remark += '{cr}%，同期沪深300收益为{ir}%，'.format(cr=round(100*((1+ci['daily_return']).prod()-1), 2),
                                              ir=round(100*((1+ii['index_return']).prod()-1), 2))
    if len(rank_data) > 0:
        if rank_data[0] < 1/3:
            crr = '收益排名靠前。'
        elif rank_data[0] < 2/3:
            crr = '收益排名居中。'
        else:
            crr = '收益排名靠后。'
        if rank_data[1] < 1 / 3:
            cdrr = '，回撤小于大多数用户。'
        elif rank_data[1] < 2 / 3:
            cdrr = '，回撤在市场上居中。'
        else:
            cdrr = '，回撤大于大多数用户。'
    else:
        crr = ''
        cdrr = ''
    remark += crr
    remark += f'{time_type}最大回撤为{round(100*cdr, 2)}%，同期沪深300最大回撤为{round(100*idr, 2)}%'
    remark += cdrr
    return remark + '。'


# 收益与风险
def remark_2(request_id, type, client_id):
    remark = '开户以来，该用户在'
    monthly_return_data = get_monthly_data(request_id, type, client_id, '000300', '2019-01-01',
                                           pd.datetime.today().date().strftime('%Y-%m-%d'), 'monthly', 'return_type')
    if len(monthly_return_data) == 0:
        return ''
    ratio_data = monthly_return_data['return_distribution_by_zero']
    monthly_return = monthly_return_data['monthly_return']
    mean_std = monthly_return.mean().tolist() + monthly_return.std().tolist()
    remark += '{cl}%的月份收益大于0，同期沪深300在{il}%的月份收益大于0；用户平均月度收益为{c}%，沪深300同期平均月度收益为{i}%，'.format(
        cl=round(100*ratio_data['用户大于0比例'][0], 2), il=round(100*ratio_data['指数大于0比例'][0], 2),
        c=round(100*mean_std[0], 2), i=round(100*mean_std[1], 2))
    if abs(mean_std[0]-mean_std[1]) >= (mean_std[2]+mean_std[3]):
        if mean_std[0] > mean_std[1]:
            remark += '用户月度收益显著高于沪深300月度收益'
        else:
            remark += '用户月度收益显著低于沪深300月度收益'
    else:
        remark += '用户与指数月度收益相近'
    remark += '。风险方面，'
    monthly_vol_data = get_monthly_data(request_id, type, client_id, '000300', '2019-01-01',
                                        pd.datetime.today().date().strftime('%Y-%m-%d'), 'monthly', 'vol_type')
    monthly_vol = monthly_vol_data['monthly_return']
    mean_std1 = monthly_vol.mean().tolist() + monthly_return.std().tolist()
    remark += '用户平均月度波动为{c}%，沪深300同期平均月度波动为{i}%，'.format(c=round(100*mean_std1[0], 2), i=round(100*mean_std1[1], 2))
    if abs(mean_std1[0]-mean_std1[1]) >= (mean_std1[2]+mean_std1[3]):
        if mean_std1[0] > mean_std1[1]:
            remark += '用户波动显著高于沪深300波动'
        else:
            remark += '用户波动显著低于沪深300波动'
    else:
        remark += '用户与指数波动相近'
    return remark + '。'


def remark_3(request_id, type, client_id):
    conn = cx.connect('zdj', 'xtKFE8k3ctqbYDOz', '10.55.57.53:1521/fundrate', encoding='UTF-8')
    remark = '开户以来，该用户'
    query = f"""select INIT_DATE, ASSET_TYPE, RATIO from HV_ASSET_ALLOCATION where CLIENT_ID = '{client_id}'"""
    class_data = pd.read_sql(query, conn)
    if class_data.empty:
        return '没有持有任何资产'
    dates = class_data['INIT_DATE'].drop_duplicates().tolist()
    count = class_data.groupby('ASSET_TYPE')['INIT_DATE'].count().rename('count').reset_index()
    ratio_old = class_data.groupby('ASSET_TYPE')['RATIO'].mean().rename('ratio_old').reset_index()
    ratio = ratio_old.merge(count, on=['ASSET_TYPE'], how='inner')
    ratio['ratio'] = ratio['ratio_old'] * ratio['count'] / len(dates)
    ratio = ratio.set_index('ASSET_TYPE')['ratio'].sort_values(ascending=False)
    ratio_a = ratio[ratio.values >= 0.3]
    if len(ratio_a) != 0:
        asset_type_words = '主要持有' + '、'.join(ratio_a.index.tolist())
    else:
        asset_type_words = '持有的大类资产权重都不超过30%'
    remark += asset_type_words
    if '股票' in ratio.index:
        stock_ratio = ratio['股票']
        remark += f'，股票平均持仓权重为{round(100*stock_ratio, 2)}%'
        query = f"""select init_date, industry_name, ratio from hv_industry where client_id = '{client_id}' and industry_type = 1"""
        industry_data = pd.read_sql(query, conn)
        if len(industry_data) != 0:
            count = industry_data.groupby('INDUSTRY_NAME')['INIT_DATE'].count().rename('count').reset_index()
            ratio_old = industry_data.groupby('INDUSTRY_NAME')['RATIO'].mean().rename('ratio_old').reset_index()
            ratio = ratio_old.merge(count, on=['INDUSTRY_NAME'], how='inner')
            ratio['ratio'] = ratio['ratio_old'] * ratio['count'] / len(dates)
            ratio = ratio.set_index('INDUSTRY_NAME')['ratio'].sort_values(ascending=False).iloc[:3]
            ratio0 = [str(round(100*r, 2))+'%' for r in ratio.tolist()]
            remark += '，最偏爱的行业为' + '、'.join(ratio.index.tolist()) + '，持有权重分别为' + '、'.join(ratio0)
    return remark + '。'


def remark_4(request_id, type, client_id):
    brinson = fund_all_multi_brinson(request_id, type, client_id, '2019-01-01',
                                     pd.datetime.today().strftime('%Y-%m-%d'), '000300')
    if len(brinson) == 0:
        return ''
    brinson = pd.concat([pd.DataFrame(pd.Series(i)).T for i in brinson])
    total = pd.Series(brinson.iloc[0]).tolist()
    brinson_use = brinson[brinson['excess_effect'] > 0].iloc[1:4]
    industry_use = '、'.join(brinson_use['Industry'].tolist())
    excess_use = '、'.join([str(round(100*r, 2))+'%' for r in brinson_use['excess_effect'].tolist()])
    remark = f'相较于沪深300，用户的总超额收益为{round(100*total[1], 2)}%，给用户带来超额收益最多的行业为{industry_use}，超额收益分别为{excess_use}。'
    return remark


def client_remark(request_id, type, client_id):
    remark1 = remark_1(request_id, type, client_id)
    remark2 = remark_2(request_id, type, client_id)
    remark3 = remark_3(request_id, type, client_id)
    remark4 = remark_4(request_id, type, client_id)
    remark = [remark1, remark2, remark3, remark4]
    return '<p>'+'</p><p>'.join([r for r in remark if len(remark) > 0])+'</p>'










