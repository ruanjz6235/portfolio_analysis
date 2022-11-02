
import numpy as np

from ..util import BaseSelect
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
    if b == 0:
        batch_codes = codes[: -b].reshape(batch, a)
    else:
        batch_codes = np.vstack([codes[: -b].reshape(batch, a), codes[-b:]])
    for sub_codes in batch_codes:
        get_rolling_tm_hm(sub_codes, index_data)

