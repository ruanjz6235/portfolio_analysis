import pandas as pd
import numpy as np
import scipy

from ..config import ConfData
from ..data_transform import DataTransform
from ..util import (BaseSelect,
                    BaseProcess)


class RegressProcess(BaseProcess):
    @classmethod
    def linear_reg(cls, y, x):
        """
        raw.columns: ['fund', 'date', 'ret', '000300']
        x_funcs: [[('000300', lambda x: x-0.03/12, 'excess'), ('excess', lambda x: x ** 2, 'excess_2')]]
        """
        xs = []
        model = sm.OLS(y, x).fit()
        xs.append(model.params)
        xs.append(model.pvalues)
        xs.append(model.tvalues)
        xs.append([model.rsquared] * len(xs))
        return np.array(xs)

    @classmethod
    def tm_hm_model(cls, df, method='linear', **kwargs):
        func, freq, window, dt_type, cal_type = cls.linear_reg, 'm', 12, 'ret', 0
        x_funcs = [[([index], lambda x: x-0.03/12, 'tm_excess'),
                    (['tm_excess'], lambda y: np.apply_along_axis(lambda x: x ** 2, 0, y), 'tm_excess2')],
                   [([index], lambda x: x-0.03/12, 'hm_excess'),
                    (['tm_excess'], lambda y: np.apply_along_axis(lambda x: max(x, 0), 0, y), 'hm_excess2')]]
        df = cls.interpolation(df, method=method, **kwargs)
        params = cls.rolling_model(func, df, x_funcs, freq, window, dt_type, cal_type)
        return params
