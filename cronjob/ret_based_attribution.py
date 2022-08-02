import pandas as pd
import numpy as np

from ..config import ConfData
from ..util import BaseSelect, BaseProcess
from ..query import RetSelect, InfoSelect

from ..engine.ret_based_attribution import get_rolling_tm_hm


# %%
index_data = BaseSelect.get_data(RetSelect.index_ret,
                                 schema='zhijunfund',
                                 code='000300').set_index(['date'])
codes = BaseSelect.get_data(InfoSelect.fund_codes, schema='zhijunfund')['fund'].unique()


# %%
# tm-hm model
def get_tm_hm_data():
    batch = 500
    a, b = codes // batch, codes % batch
    batch_codes = np.vstack([codes[: len(codes) - b].reshape(batch, a), codes[len(codes) - b:]])
    for sub_codes in batch_codes:
        fund_data = BaseSelect.get_data(RetSelect.fund_ret,
                                        schema='zhijunfund',
                                        codes=sub_codes).set_index(['date', 'fund']).unstack()
        params = get_rolling_tm_hm(fund_data, index_data)
        ConfData.save(params, 'zhijunfund.tm_hm_model')

