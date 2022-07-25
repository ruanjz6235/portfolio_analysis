#代码规范:

###import模块:

import内容依次是
1. Python内部模块
2. 三方包(如果可以，尽量少用三方包，因为会有依赖)
3. 依次从./中的config.py, const.py, data_transform.py, util.py, query.py中导入所需模块
4. ./template/*.py
5. ./engine/*.py
6. ./cronjob/*.py
7. ./port/*.py

按照顺序import 每一点写完后空一行

###基础类:

如果后续需要继承，基础类应以Base为开头命名，如BaseSelect, BaseProcess

基础类只实现基础功能，上层中写到有可能在其他地方需要的功能应当移到更加基础的文件或类中，尽量避免出现相互引用的混乱情况，最终的代码应当形成树状结构，
即底层类、中间类、上层类，定时任务或接口文件的多层结构，每一层只从上一层中引用所需类。

其中，基础类中:
1. BaseSelect实现的是get_dates(定时任务的日期获取方法)，get_data(数据统一获取形式)，complete_df(补充完整dataframe的列)
2. BaseProcess中目前只有一个方法，就是滚动模型方法，这里可以用于滚动收益率计算、tm模型等
3. ConfData主要只实现数据连接，数据保存
4. const.py中的NameConst和Schema类分别确定字段命名和字段类型
5. DataTransform目前专注于透视表转换、重命名、清除数据、数据合并等功能，目前暂可视为dataframe的子类，和dataframe处理类似

###template类:
目前实现了收益与风险类、持仓分析、收益归因类、情景分析类、标签类的计算内容，主要框架已形成，引用的代码规范可见engine

这部分内容统一用类来写，一方面形成一个良好的维护结构，一方面意义很明显。主要的类有:
1. RetAttr: 入参是持仓和计算时间，可以实现get_style(获取风格数据)，fill_portfolio(填充持仓数据)，get_stock_ret(获取价格数据)，
   get_daily_attr(收益归因日度计算结果)，get_daily_style(持仓分析日度计算结果)，get_cum_attr(收益归因展示)
2. 



###engine类:
这部分中有template模块使用方法:
1. ret_attribution.py中，cal_ret_attr_data用于多处收益归因的计算，同时cal_barra_attr_data和cal_brinson_attr_data
   是对RetAttr的个性化使用方法案例。
2. 

###cronjob和port类就相对比较简单，略去



#使用文档
###




