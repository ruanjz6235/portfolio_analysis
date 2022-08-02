import pandas as pd
import numpy as np

from ..util import BaseSelect, BaseProcess
from ..query import RetSelect

from ..template.ret_based_attribution import RegressProcess


def get_rolling_tm_hm(fund_data, index_data):
    method = BaseProcess.fit_index
    kwargs = {'x': index_data}

    ret_data = pd.concat([fund_data, index_data], axis=1)
    params = RegressProcess.tm_hm_model(ret_data, method, **kwargs)
    return params
