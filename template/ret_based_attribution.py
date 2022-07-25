import pandas as pd
import numpy as np

from ..data_transform import DataTransform
from ..util import (BaseSelect,
                    BaseProcess)


def tm_hm_model(fund_and_index_return_by_window, rf=0.03 / 12):
    y = fund_and_index_return_by_window['MonthlyReturn'] - rf
    fund_and_index_return_by_window['bench_rf'] = fund_and_index_return_by_window['log_return'] - rf
    fund_and_index_return_by_window['bench_rf_2'] = fund_and_index_return_by_window['bench_rf'] ** 2
    fund_and_index_return_by_window['bench_rf_3'] = fund_and_index_return_by_window['bench_rf'].apply(
        lambda bench_rf: max(bench_rf, 0))

    x_tm = fund_and_index_return_by_window[['bench_rf', 'bench_rf_2']]
    x_tm = sm.add_constant(x_tm)
    model_tm = sm.OLS(y, x_tm).fit()
    [alpha_tm, beta1_tm, beta2_tm] = model_tm.params
    [p1_tm, p2_tm, p3_tm] = model_tm.pvalues
    [t1_tm, t2_tm, t3_tm] = model_tm.tvalues
    r2_tm = model_tm.rsquared
    params1 = [alpha_tm, p1_tm, t1_tm, beta1_tm, p2_tm, t2_tm, beta2_tm, p3_tm, t3_tm, r2_tm]

    x_hm = fund_and_index_return_by_window[['bench_rf', 'bench_rf_3']]
    x_hm = sm.add_constant(x_hm)
    model_hm = sm.OLS(y, x_hm).fit()
    [alpha_hm, beta1_hm, beta2_hm] = model_hm.params
    [p1_hm, p2_hm, p3_hm] = model_hm.pvalues
    [t1_hm, t2_hm, t3_hm] = model_hm.tvalues
    r2_hm = model_hm.rsquared
    params2 = [alpha_hm, p1_hm, t1_hm, beta1_hm, p2_hm, t2_hm, beta2_hm, p3_hm, t3_hm, r2_hm]
    params = params1 + params2
    return params