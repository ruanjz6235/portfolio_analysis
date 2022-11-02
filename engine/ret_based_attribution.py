import pandas as pd

from ..util import BaseSelect, BaseProcess
from ..query import RetSelect

from ..template.ret_based_attribution import RegressProcess


def get_rolling_tm_hm(codes, index_data):
    method = BaseProcess.fit_index
    kwargs = {'x': '000300'}

    fund_data = BaseSelect.get_data(RetSelect.fund_ret,
                                    schema='zhijunfund',
                                    codes=codes).set_index(['date', 'fund']).unstack()
    ret_data = pd.concat([fund_data, index_data], axis=1)
    RegressProcess.tm_hm_model(ret_data, method, **kwargs)
