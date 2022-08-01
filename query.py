import pandas as pd
import numpy as np

from .config import ConfData
from .util import BaseSelect


def get_tradingdays(start_date, end_date):
    query = f"""select TradingDate from QT_TradingDayNew where IfTradingDay = 1 and SecuMarket = 83
    and TradingDate >= '{start_date}' and TradingDate >= '{end_date}'"""
    tradingdays = pd.read_sql(query, ConfData.get_conn('zhijunfund'))
    tradingdays['TradingDate'] = tradingdays['TradingDate'].apply(lambda x: x.strftime('%Y%m%d'))
    return tradingdays['TradingDate'].unique()


class StyleSelect(BaseSelect):
    barra_style = """select * from FM_FactorExposure where date = {date}"""
    ind_style = """select * from stock_industry where date = {date} and standard = {standard}"""
    ms_style = """select * from stock_industry where date = {date} and standard = {standard}"""


class RetSelect(BaseSelect):
    a_ret = """select * from a_ret_table"""
    kc_ret = """select * from kc_ret_table"""
    h_ret = """select * from h_ret_table"""
    index_ret = """"""
    fund_ret = """"""


class PortSelect(BaseSelect):
    funds_port = """select fund, date, code, weight, style from fund_portfolio where fund in ({funds})"""
    daily_port = """select fund, date, code, weight, style from fund_portfolio where date = '{date}'"""
    daily_base = """select fund, date, code, weight, style from base_portfolio where date = '{date}'"""
    base_attr = """select code, date, code, weight, style from base_portfolio where date = '{date}'"""


class InfoSelect(BaseSelect):
    fund_codes = """"""
