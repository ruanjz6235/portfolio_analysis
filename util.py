import pandas as pd
import numpy as np
from functools import lru_cache, reduce, wraps
from copy import deepcopy

from .config import ConfData
from .const import nc


def time_decorator(func):
    @wraps(func)
    def timer(*args, **kwargs):
        start = datetime.datetime.now()
        result = func(*args, **kwargs)
        end = datetime.datetime.now()
        print(f'“{func.__name__}” run time: {end - start}.')
        return result

    return timer


def groupby_wrapper(groupby_name):

    def groupby_func(func):
        def new_func(df, **kwargs):
            return df.groupby(groupby_name).apply(func, **kwargs)

        return new_func

    return groupby_func


class BaseSelect(ConfData):
    # cronjob
    @classmethod
    def get_dates(cls, **kwargs):
        last_date = kwargs.get('last_date')
        today = pd.to_datetime('today').normalize()
        if not kwargs.get('if_source'):
            return np.array([int(x.strftime('%Y%m%d')) for x in pd.date_range(last_date, today)])
        zjfund = cls.get_conn('zhijunfund')
        tar_con, sou_con = kwargs.get('tar_con', zjfund), kwargs.get('sou_con', zjfund)
        target, source = kwargs.get('target', 'T_CUST_D_STK_TRD_IDX'), kwargs.get('source', 'T_EVT_SEC_DLV_JOUR')
        date_name = kwargs.get('dt', 'etl_date')
        tar_dt, sou_dt = kwargs.get('tar_dt', date_name), kwargs.get('sou_dt', date_name)
        if not last_date:
            query = f"""select max({tar_dt}) from {target}"""
            last_date = pd.read_sql(query, tar_con)
            last_date = last_date.iloc[0].iloc[0]
            if last_date is None:
                last_date = '20150101'
            else:
                last_date = str(int(last_date))
        query = f"""select distinct {sou_dt} from {source} where {sou_dt} >= {last_date} order by {sou_dt} asc"""
        dates = pd.read_sql(query, sou_con)[sou_dt].astype('int').values
        return dates

    @classmethod
    # select data from table partitioned by date
    def get_days_data(cls, dates, query, schema, **kwargs):
        days_data = []
        for date in dates:
            print(date)
            day_data = pd.read_sql(query.format(date=date, **kwargs), cls.get_conn(schema))
            days_data.append(day_data)
        days_data = pd.concat(days_data)
        return days_data

    @classmethod
    def get_data(cls, query_name, schema, **kwargs):
        """
        if select portfolio data by date，kwargs = {'query': 'fund_port', 'schema': 'edw', 'dates': dates,...}
        the key 'query' and 'schema' is required, but 'dates' is not necessary
        """
        dates = kwargs.get('dates')
        query = getattr(cls, query_name)
        # we need keep the return form consistent
        if not dates:
            return pd.read_sql(query.format(**kwargs), ConfData.get_conn(schema))
        else:
            kwargs.pop('dates')
            return cls.get_days_data(dates, query, schema, **kwargs)

    @classmethod
    def complete_df(cls, df, **kwargs):
        # qs是一个对象，columns=['query', 'conn', 'merge_on', 'merge_how']
        qs = kwargs.get('query')
        for query in qs:
            conn = cls.get_conn(query.get('conn', 'zhijunfund'))
            merge_on = query.get('merge_on', ['src_cust_no'])
            merge_how = query.get('merge_how', 'outer')
            cols = query.get('cols', {})
            data = pd.read_sql(query['query'], conn).rename(columns=cols)
            columns = data.columns
            df = df.merge(data, on=merge_on, how=merge_how)
            df[columns] = df[columns].ffill().bfill()
        return df


class ExampleSelect(BaseSelect):
    # newest industry
    def get_stock_ind(self, codes):
        query = f"""select secucode, name_abbr, first_industry, info_date from stock_industry
        where secucode in ({str(codes)[1:-1]}) and standard = 38 and secumarket in (83, 90)"""
        ind = pd.read_sql(query, self.get_conn('zhijunfund'))
        ind.columns = ['code', 'name', 'ind', 'info_date']
        ind_new = ind.groupby('code')['info_date'].max().rename('info').reset_index()
        ind = ind.merge(ind_new, on='code', how='outer')
        ind = ind[ind['info_date'] == ind['info']]
        return ind[['code', 'name', 'ind']]

    def get_stock_close(self, dates):
        stock_close_query = """select biz_dt, sec_code, cls_price, last_price
        from T_PRD_EXCH_SEC_QUOTE where etl_date = '{date}' and trd_cgy_cd in ('1', '2')"""
        stock_close = self.get_days_data(dates, stock_close_query, 'edw')
        stock_close.columns = ['date', 'code', 'close', 'preclose']
        return stock_close

    def get_client_day_trd(self, zs_code, date):
        query = f"""select etl_date, mtch_tm, biz_flg, sec_code, mtch_price, mtch_qty
        from T_EVT_SEC_DLV_JOUR where etl_date = '{date}' and src_cust_no = '{str(zs_code)}'"""
        day_trading = pd.read_sql(query, self.get_conn('edw'))
        day_trading.columns = ['date', 'time', 'flag', 'code', 'price', 'volume']
        day_trading['flag'] = day_trading['flag'].apply(lambda x: str(int(x)))
        return day_trading

    def get_client_trd(self, zs_code, dates):
        trading_query = """select etl_date, mtch_tm, biz_flg, sec_code, mtch_price, mtch_qty
        from T_EVT_SEC_DLV_JOUR where etl_date = '{date}' and src_cust_no = {str(zs_code)}"""
        trading = self.get_days_data(dates, trading_query, 'edw', zs_code=zs_code)
        trading.columns = ['date', 'time', 'flag', 'code', 'price', 'volume']
        trading['flag'] = trading['flag'].apply(lambda x: str(int(x)))
        return trading

    def get_day_trd(self, dates):
        trading_query = """select etl_date, src_cust_no, mtch_tm, biz_flg, sec_code, mtch_price, mtch_qty
        from T_EVT_SEC_DLV_JOUR where etl_date = {date}"""
        trading = self.get_days_data(dates, trading_query, 'edw')
        trading.columns = ['date', 'client', 'time', 'flag', 'code', 'price', 'volume']
        trading['flag'] = trading['flag'].apply(lambda x: str(int(x)))
        return trading

    def get_client_trd_idx(self, zs_code, dates):
        trading_query = """select * from T_CUST_D_STK_TRD_IDX where biz_dt = {date} and src_cust_no = '{zs_code}'
        and idx_code in ('hld_mktval', 'buy_mtch_amt', 'sell_mtch_amt', 'mtch_amt')"""
        trading = self.get_days_data(dates, trading_query, 'zhijunfund', zs_code=zs_code)
        del trading['id'], trading['updatetime']
        return trading

    def get_client_hld(self, zs_code, dates):
        holding_query = """select biz_dt, sec_code, hld_mktval from t_org_cust_d_highnav_cust_hld
        where etl_date = '{date}' and src_cust_no = '{zs_code}' and ast_attr_nm = '股票'"""
        holding = self.get_days_data(dates, holding_query, 'bizdm', zs_code=zs_code)
        holding.columns = ['date', 'code', 'holding_mv']
        return holding

    def get_client_hld_all(self, zs_code):
        holding_query = f"""select biz_dt, sec_code, hld_mktval from t_org_cust_d_highnav_cust_hld
        where src_cust_no = '{zs_code}' and ast_cgy_tp = '股票' order by biz_dt, sec_code asc"""
        holding = pd.read_sql(holding_query, self.get_conn('bizdm'))
        holding.columns = ['date', 'code', 'holding_mv']
        return holding

    def get_client_trd_all(self, zs_code):
        query = f"""select evt_dt, mtch_tm, biz_flg, sec_code, mtch_price, mtch_qty
        from T_EVT_SEC_DLV_JOUR where src_cust_no = '{str(zs_code)}'"""
        day_trading = pd.read_sql(query, self.get_conn('edw'))
        day_trading.columns = ['date', 'time', 'flag', 'code', 'price', 'volume']
        day_trading['flag'] = day_trading['flag'].apply(lambda x: str(int(x)))
        return day_trading

    def get_zs_codes(self):
        query = """select distinct zscode, client_id from zscode_client where client_id is not null"""
        return pd.read_sql(query, self.get_conn('zhijunfund'))['client_id'].astype('int').tolist()

    def complete_trading(self, trading, zs_code):
        query = """select distinct biz_dt, biz_dt_nm, suprs_brn_org_no, suprs_brn_org_nm, brn_org_no, brn_org_nm,
        cust_nm, ast_cgy_tp, ast_attr_cd, sec_code, trd_cgy_cd, sec_cgy_cd from t_org_cust_d_highnav_cust_hld
        where src_cust_no = '{zs_code}'""".format(zs_code=zs_code)
        conn = 'bizdm'
        merge_on = ['sec_code', 'biz_dt']
        merge_how = 'left'
        query_config = [{'query': query, 'conn': conn, 'merge_on': merge_on, 'merge_how': merge_how}]
        trading = self.complete_df(trading, query=query_config)
        return trading


class BaseProcess:
    @classmethod
    def get_groups(cls, array, k):
        y, r = len(array) // k, len(array) % k
        array_group = list(array[: len(array) - r].reshape(y, k))
        if r != 0:
            array_group.append(array[len(array) - r:])
        return array_group

    @classmethod
    def shift_array(cls, array: np.array, k: int, axis=0):
        if axis != 0:
            array = array.T

        len_array = len(array)
        new_array = array[max(0, k): len_array + min(0, k)]
        mask = deepcopy(array)
        mask.fill(np.nan)

        if k > 0:
            new_array = np.concatenate([mask[abs(k)], new_array])
        else:
            new_array = np.concatenate([new_array, mask[abs(k)]])

        if axis != 0:
            new_array = new_array.T
        return new_array

    @classmethod
    def model_data(cls,
                   data,
                   x_func=[(['000300'], lambda x: x-0.03/12, ['excess']),
                           (['excess'], lambda x: x ** 2, ['excess_2']),
                           (['excess'], lambda x: max(x, 0), ['excess_3'])],
                   rf=0.03/12):
        old = np.hstack(np.array(x_func)[:, 0])
        ys = data.columns[~np.isin(data.columns, old)]

        xs = np.hstack(np.array(x_func)[:, 2])
        data['const'] = 1
        data[xs] = np.nan
        data[ys] = data[ys] - rf

        dt = np.dtype({'names': data.columns, 'formats': ['O'] * len(data.columns)})
        raw = np.array(data.values, dtype=dt)

        for sub in x_func:
            cols1, func, cols2 = sub[0], sub[1], sub[2]
            assert len(set(cols1) - set(data.columns)) == 0
            array = func(raw[cols1])
            # array = np.apply_along_axis(func, 0, raw[cols1])
            for k, col in enumerate(cols2):
                raw[col] = array[k]

        return raw[ys], raw[['const'] + list(xs)]

    @classmethod
    def get_freq_calendar(cls, date, freq):
        if freq.lower() == 'w':
            calendar = date[:4] + str(pd.to_datetime(date).isocalendar()[1])
        elif freq.lower() == 'm':
            calendar = date[:6]
        elif freq.lower() == 'q':
            calendar = date[:4] + '0' + str((int(date[4:6]) - 1) // 3 + 1)
        elif freq.lower() == 'y':
            calendar = date[:4]
        else:
            raise KeyError('freq not in w, m, q, y')
        return calendar

    @classmethod
    def interpolation(cls, df, method={'method': 'linear'}, **kwargs):
        # interpolate
        if type(method) == str:
            try:
                df = df.interpolate(**method)
            except KeyError:
                raise KeyError('this method is not in df.interpolation')
        else:
            try:
                df = method['method'](df, **kwargs)
            except TypeError:
                raise TypeError("custom variable 'method' is not callable")
        return df

    @classmethod
    def rolling_window(cls, array, window):
        shape = array.shape[:-1] + (array.shape[-1] - window + 1, window)
        strides = array.strides + (array.strides[-1],)
        return np.lib.stride_tricks.as_strided(array, shape=shape, strides=strides)

    @classmethod
    def rolling_model(cls,
                      func,
                      df,
                      x_funcs=[[(['000300'], lambda x: x-0.03/12, ['excess']),
                                (['excess'], lambda x: x ** 2, ['excess_2']),
                                (['excess'], lambda x: max(x, 0), ['excess_3'])]],
                      freq='Q',
                      window=24,
                      dt_type='ret',
                      cal_type=0):
        """
        data is price_type, index is 'date'-like, and df is a pivot table
        """
        # resample
        df = df.resample(freq).last()

        # price_type or ret_type
        if dt_type == 'ret':
            df = df / df.shift(1) - 1
        calendars = df.index.map(cls.get_freq_calendar, freq=freq)
        params = []

        # rolling_model
        if type(x_funcs[0]) == tuple:
            x_funcs = [x_funcs]
        else:
            assert type(x_funcs[0]) == list

        for x_func in x_funcs:
            y, x = cls.model_data(df, x_func)
            if cal_type == 1:
                for i, calendar in emumerate(calendars.tolist()):
                    sub_y = y[i: i + window]
                    sub_x = x[i: i + window]
                    param = func(sub_y, sub_x)
                    params.append([calendar] + param)
                params = pd.DataFrame(params)
            else:
                df_window = cls.rolling_window(pd.DataFrame(df).values, window)
                params = np.apply_along_axis(func, 1, df_window)
                params = pd.DataFrame(params)
                params['calendar'] = calendars
        return pd.DataFrame(params)

    @classmethod
    def fit_index(cls, y, x):
        """fitting from index ret to fund ret, used in interpolation"""
        pass
