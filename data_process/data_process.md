#私募基金数据清洗入参

##字段命名规则(词根)
整个项目(包含数据清洗和计算)均需按照命名规则命名。如果没有该词根，可继续补充，但是补充后需要按照命名规则进行命名。

如果同时出现多个code，再去考虑是fund_code还是bond_code等，否则同一张表中只有一个code字段时，都用code命名；
如果有多个date，再去考虑是start_date, end_date, report_date, list_date等，否则一张表中只有一个date时，都用date命名。

基金ID：fund

基金代码(证监会代码，或备案号)：fund_code

备案时间：list_date

基金名称：fund_name

基金简称：fund_abbr

基金单位净值：nv

基金累计净值：cum_nv

基金复权净值：complex_nv

基金日收益率：ret

基金月、季、年收益率：monthly_ret / quarter_ret / annual_ret

净值公布日（交易日）：date(TradingDay, EndDate均合并为date)

报告期：report_date

公告日：issue_date

交易时间戳：time

成交价：price

收盘价：close

持有期、久期：duration

持仓：holding

成交量：volume

股票代码：code

债券代码：bond_code

资产类别：asset

标准、分类方法（如申万、证监会等）：standard

行业：ind

经理：manager

管理人：

成立日期：start_date

基金状态：list_state

投资策略：strategy

投资子策略：sub_strategy

区间收益：

年化收益：

阿尔法：alpha

贝塔：beta

信息比率：ir

索提诺：sortino

特雷诺：

卡玛：calma

回撤：drawdown

最大回撤：mdd

收益贡献：va

波动率：vol

下行波动：down_vol

场景：scenario

时段：period

##表结构及源表(后续若有需要再添加)
私募排排很多表的表结构中都有isvalid字段，表示记录是否有效，清洗后可只保留有效的数据，所以也不需要这个字段了
###1. 私募基金基本信息表(估值表or私募排排)
字段：
基金ID、基金名称、基金简称、备案号、
备案时间、成立时间、清算日期、封闭期、开放期、基金状态、
基金经理、投资策略、投资子策略、指数ID、基金投资范围、投资限制、投资理念、
基金类型、基金管理人、公司类型

源表：pvn_fund_info, pvn_fund_strategy, pvn_fund_manager_mapping, pvn_fund_status
###2. 基金净值(估值表or私募排排)
字段：
基金ID、净值公布日、
单位净值、累计净值(分红不投)、复权净值(分红再投)、动态回撤(距离历史新高的距离)、
分红(无分红为null)、拆分(无拆分为null)

源表：
pvn_nav, pvn_distribution,
ZS_FundNVGrowthRate
###3. 基金持仓表(估值表)
字段：
基金ID、净值公布日、
资产类别、资产代码、权重(MarketInNV, MarketInTA)、收益贡献(ValuationAppreciation)、收盘价(MarketPrice)

源表：ZS_FundValuation, pvn_fund_portfolio
(这一块关于资产类别分类一块，可以再单独讨论一下)
###4. 股票行业、指数、前复权价格
###5. 债券种类、债券评级、剩余年限、到期收益率、债券价格
###6. 期货合约类型、合约标的、合约方向
###7. 股票晨星风格箱数据
###8. 股票、债券、期货市场场景数据

##收益率清洗
私募排排基于净值计算出来的数据基本都是周频数据。
以"幻方量化专享7号"为例：该基金在2019年11月15日成立后，部分时候是周频公布，部分时候是两三天公布，部分时候是一天公布，
需要保留原始收益表，供直接展示使用，同时常规数据默认是周频数据，如果没有周频数据，就按照公布频率，月频就算月频。
###1. 频率识别(日频、周频、月频)
识别基金净值公布日近期的基金公布频率，并给基金净值日打上日频、周频还是月频的标签。

这里有一定的计算量。apply的相关用法，使用```np.apply_along_axis```和```np.apply_over_axes```方法替代```dataframe.apply```

滚动数据计算，该轮子在util.BaseProcess.rolling_window中，可使用
```python
def rolling_window(array, window):
    shape = array.shape[:-1] + (array.shape[-1] - window + 1, window)
    strides = array.strides + (array.strides[-1],)
    return np.lib.stride_tricks.as_strided(array, shape=shape, strides=strides)
```
计算逻辑：
获取每条净值数据的前10条数据和后10条数据，按照瀑布流法：

如果前一自然周之内，或后一自然周之内有两条以上数据，则为日频数据，打上标签d；

如果前一自然周之内，或后一自然周之内有一条数据，则为周频数据，打上标签w；

如果前一自然月之内，或后一自然月之内有一条数据，则为月频数据，打上标签m；

如果前一自然月和后一自然月之内均无数据，则为月频以上数据，打上标签y。

###2. 缺失值补充(日频、周频、月频)
需要做缺失值填充的基金包括：

如果前后标签都为w，但两个日期间隔不止一周，存在缺失值这种方法可以用dataframe.resample来解决，出现nan，则有缺失值；

如果前后标签不一样，按照频率较低的来统计，判断是否存在缺失值。

一般情况下，私募基金最常见的净值公布频率为周频和月频。
```python
def count_miss(ret_df):
    """
    基本逻辑：如果既出现周频，又出现月频标签，月频标签的最大日期和最小日期相差6个月以内，说明月频数据仍不是主要公布频率，仍应按照周频数据来计算
    """
    if isinstance(ret_df, np.array):
        ret_df = pd.DataFrame(ret_df)
    ret_label = ret_df['label'].unique()
    if 'y' in ret_label:
        ret_df = ret_df.resample(on='date', freq='m')['nv'].last()
    elif 'm' in ret_label:
        if 'w' not in ret_label:
            ret_df = ret_df.resample(on='date', freq='m')['nv'].last()
        else:
            max_dt, min_dt = ret_df[ret_df['label'] == 'm']['date'].max(), ret_df[ret_df['label'] == 'm']['date'].min()
            if max_dt <= pd.DateOffset(months=6) + min_dt:
                ret_df = ret_df.resample(on='date', freq='w')['nv'].last()
            else:
                ret_df = ret_df.resample(on='date', freq='m')['nv'].last()
    elif 'w' in ret_label:
        ret_df = ret_df.resample(on='date', freq='w')['nv'].last()
    else:
        pass
    return ret_df
```
获取到了所有缺失值，再进行回归填充(缺失净值少于126个交易日/26周/6个月则填充，多于126个交易日/26周/6个月当做两只基金)
```python
from data_transform import DataTransform
import statsmodels.api as sm
def fill_miss(asset_df, index_df):
    """asset_df, index_df均为净值序列"""
    asset_df_new = asset_df[~asset_df['nv'].isna()]
    asset_df_new = DataTransform(asset_df_new)
    asset_df_new, index_df_new = asset_df_new.align([index_df])
    asset_df_new['ret'] = asset_df_new['nv'] / asset_df_new['nv'].shift(1) - 1
    index_df_new['ret'] = index_df_new['nv'] / asset_df_new['nv'].shift(1) - 1
    index_df_new = sm.add_constant(index_df_new)
    alpha, beta = sm.OLS(asset_df_new, index_df_new).fit().params
    asset_df.loc[asset_df['ret'].isna(), 'ret'] = alpha + index_df['ret'] * beta
    return asset_df
```
###3. 极值处理
极值判断
array中大于均值+3倍标准差，或小于均值-3倍标准差的，做极值处理，极值处理可以处理为3sigma，也可以同缺失值一同处理
```python
def fill_3sigma_na(df, method):
    mean, sigma = df['ret'].mean(), df['ret'].std()
    if method == '3sigma':
        df.loc[df['ret'] > mean + 3 * sigma, 'ret'] = mean + 3 * sigma
        df.loc[df['ret'] < mean - 3 * sigma, 'ret'] = mean - 3 * sigma
    elif method == 'na':
        df.loc[df['ret'] > mean + 3 * sigma, 'ret'] = np.nan
        df.loc[df['ret'] < mean - 3 * sigma, 'ret'] = np.nan
    return df
```

###4. 周、月收益率计算
经过缺失值处理过后的收益率一般都是周收益率或月收益率。周收益率和月收益率的计算均按照每个周期最后一个净值数据作为这只基金的在这个周期内的最后的净值


###5. 展示
私募排排数据展示中

1. 业绩走势、场景分析模块展示原始收益率(不清洗)

2. 收益与风险模块的收益率源数据均为清洗后的数据

2.1 鉴于基金至少有月度收益率，故在展示滚动收益率的时候，月、季、年都有数据

2.2 区间收益率、月度波动、区间最大回撤、滚动最大回撤、收益风险指标均应按照清洗后的结果进行计算。








