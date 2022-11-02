#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/22 5:49 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : lunzi.py
# @IDE     : PyCharm
# @REMARKS : 说明文字


def get_tradingdays():
    query = f"""select * from QT_TradingDayNew"""
    tradingdays = pd.read_sql(query, ConfData.get_conn('zhijunfund'))
    tradingdays['TradingDate'] = tradingdays['TradingDate'].apply(lambda x: x.strftime('%Y%m%d'))
    return tradingdays['TradingDate'].unique()