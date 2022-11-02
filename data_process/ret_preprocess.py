import numpy as np
import pandas as pd
import statsmodels.api as sm
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm

from ..config import ConfData
from ..data_transform import DataTransform
from ..util import BaseProcess, BaseSelect, groupby_wrapper, time_decorator
from ..const import nc
from ..query import RetSelect, InfoSelect


class RetPreProcess:
    @classmethod
    def get_data_label(cls, ret_df):
        dates = ret_df[nc.date_name]
        dates_window = BaseProcess.rolling_window(dates, 11)
        # dates_shift = np.insert(dates_window[..., 1:], dates_window.shape[1] - 1, 0, axis=1)
        dates_shift = BaseProcess.shift_array(dates_window, k=-1, axis=1)
        shift_change = [(pd.to_timedelta(d)[:5].mean().days, pd.to_timedelta(d)[5:10].mean().days)
                        for d in dates_shift - dates_window]

        def get_label(shift_dt):
            before, after = shift_dt[0], shift_dt[1]
            if before <= 3.5 or after <= 3.5:
                return 'd'
            elif before <= 7 or after <= 7:
                return 'w'
            elif before <= 31 or after <= 31:
                return 'm'
            else:
                return 'y'

        label = np.apply_along_axis(get_label, 1, shift_change)
        label = np.append([np.nan] * 10, label)
        ret_df[nc.label_name] = label
        return ret_df

    @classmethod
    def count_miss(cls, ret_df):
        """
        出参：包含月、季、年缺失收益率的完整收益率序列日历
        """
        def len_(df, label):
            return len(df[df[nc.label_name] == label])

        def get_dates_new(df, freq, ds):
            if isinstance(df, np.array):
                df = pd.DataFrame(df, columns=[nc.date_name, nc.nv_name])
            df_new = df.resample(freq, on=nc.date_name)[nc.date_name].last()
            ds_other = df_new[df_new[nc.date_name].isna()].index.tolist()
            ds = ds.extend(ds_other)
            return sorted(ds)

        dates = ret_df[nc.date_name]
        lab = {'y': len_(ret_df, 'y'), 'm': len_(ret_df, 'm'), 'w': len_(ret_df, 'w'), 'd': len_(ret_df, 'd')}
        a = len(ret_df)
        if a < 30:
            return ret_df
        if (lab['y'] + lab['m']) / a >= 0.5:
            dates = get_dates_new(ret_df, 'm', dates)
            label_tp = 'm'
        elif lab['w'] / a >= 0.8:
            dates = get_dates_new(ret_df, 'w', dates)
            label_tp = 'w'
        elif lab['d'] / a >= 0.9:
            dates = get_dates_new(ret_df, 'd', dates)
            label_tp = 'd'
        else:
            # len_lab = sorted(lab.items(), key=lambda x: x[1], reverse=True)
            if lab['y'] + lab['m'] > lab['w']:
                dates = get_dates_new(ret_df, 'm', dates)
                label_tp = 'm'
            elif lab['w'] > 4 * (lab['y'] + lab['m']):
                dates = get_dates_new(ret_df, 'w', dates)
                label_tp = 'w'
            else:
                dates = get_dates_new(ret_df, 'w', dates)
                label_tp = 'w'
        ret_df_new = np.array([np.nan] * len(dates), dtype=ret_df.dtype)
        ret_df_new[nc.fund_name] = ret_df[nc.fund_name][0]
        ret_df_new[nc.date_name] = dates
        ret_df_new[nc.label_name] = label_tp
        return np.append(ret_df, ret_df_new)

    @classmethod
    def fill_miss(cls, asset_df, index_df):
        """asset_df和index_df均为净值序列"""
        asset_df_new = asset_df[~asset_df[nc.complex_nv_name].isna()]
        asset_df_new = DataTransform(asset_df_new)
        asset_df_new, index_df_new = asset_df_new.align(index_df)

        anv, inv = asset_df_new[nc.complex_nv_name], index_df_new[nc.complex_nv_name]
        asset_df_new[nc.ret_name] = anv / BaseProcess.shift_array(anv, 1) - 1
        index_df_new[nc.ret_name] = inv / BaseProcess.shift_array(inv, 1) - 1

        index_df_new = sm.add_constant(index_df_new[nc.ret_name])
        alpha, beta = sm.OLS(asset_df_new[nc.ret_name], index_df_new).fit().params
        asset_df.loc[asset_df[nc.ret_name].isna(), nc.ret_name] = alpha + index_df[nc.ret_name] * beta
        return asset_df

    @time_decorator
    @groupby_wrapper(groupby_name=nc.code_name)
    def get_miss_data(self, asset_df, index_df):
        asset_df[nc.label_name] = np.nan
        columns = asset_df.columns
        dt = np.dtype({'names': columns, 'formats': ['O'] * len(columns)})
        asset_df = np.array(list(zip(*(asset_df[i] for i in asset_df.columns))), dtype=dt)

        asset_df = self.get_data_label(asset_df)
        asset_df = self.count_miss(asset_df)
        return self.fill_miss(asset_df, index_df)

    @classmethod
    def fit_index(cls, df, index_df, keep_label=True):
        code_groups = BaseProcess.get_groups(df[nc.code_name].unique(), 100)
        df = Parallel(n_jobs=multiprocessing.cpu_count())(
            delayed(cls.get_miss_data)(df[df[nc.code_name].isin(group)], index_df)
            for group in tqdm(code_groups)
        )
        if not keep_label:
            del df[nc.label_name]
        return df


if __name__ == '__main__':
    index_price = RetSelect.get_data('index_price', schema='zhijunfund', code=['000300'])
    codes_old = InfoSelect.get_data('fund_codes', 'simuwang')['fund'].values
    codes_old = BaseProcess.get_groups(codes_old, 1000)
    # x, y = len(codes_old) // 1000, len(codes_old) % 1000
    # codes = list(codes_old[: len(codes_old) - y].reshape(x, 1000))
    # codes.append(codes_old[len(codes_old) - y:])
    for sub_codes in codes:
        fund_ret = RetSelect.get_data('fund_ret', 'simuwang', codes=str(list(sub_codes))[1:-1])
        fund_ret_new = RetPreProcess.fit_index(fund_ret, index_price)
        ConfData.save(fund_ret_new, 'zhijunfund.fund_ret')
        print(i)
