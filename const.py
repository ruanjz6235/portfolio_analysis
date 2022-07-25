import pandas as pd
import numpy as np

from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType, DoubleType


class NameConst:
    fund_name = 'fund'
    code_name = 'code'
    flag_name = 'flag'
    type_name = 'type'
    hold_type_name = 'hold_type'

    date_name = 'date'
    time_name = 'time'
    price_name = 'price'
    close_name = 'close'
    preclose_name = 'preclose'
    volume_name = 'volume'

    hold_name = 'holding'
    days_name = 'days'
    dur_name = 'duration'
    hold_mv_name = 'holding_mv'
    count_name = 'count'
    codes_nm = ['fund', 'code']
    realized_nm = ['gx', 'rn', 'dx', 'hg', 'pg', 'qz', 'pt']
    realize_nm = ['pt_realize', 'hg_realize', 'dx_realize', 'qz_realize', 'pg_realize']
    hold_nm = ['holding', 'duration']
    turn_nm = ['buy_turn', 'sell_turn', 'turnover']


nc = NameConst()
columns = [x for x in NameConst.__dict__.keys() if x.find('name') >= 0]
columns_string = ['fund', 'code', 'flag', 'hold_type']
columns_double1 = ['date', 'time', 'price', 'volume', 'close', 'preclose', 'type', 'count']
columns_double2 = nc.realize_nm + nc.realized_nm + nc.hold_nm + nc.turn_nm


class Schema:
    def __init__(self, cols_string, cols_double):
        SCHEMA = []
        for i, list_ in enumerate([cols_string, cols_double]):
            for name in list_:
                if i == 0:
                    name = [x for x in NameConst.__dict__.keys() if x.find(name) >= 0][0]
                    type_ = StringType()
                    field = StructField(getattr(nc, name), type_, True)
                elif i == 1:
                    name = [x for x in NameConst.__dict__.keys() if x.find(name) >= 0][0]
                    type_ = DoubleType()
                    field = StructField(getattr(nc, name), type_, True)
                else:
                    type_ = DoubleType()
                    field = StructField(name, type_, True)
                SCHEMA.append(field)
        self.SCHEMA = StructType(SCHEMA)
