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









