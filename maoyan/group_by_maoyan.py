#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from maoyan.common.AreaCodeDecoder import AreaCodeDecoder


def group_by_maoyan():
    df = pd.read_csv("deal_source_data/xxx.csv", encoding="gbk")
    df['totalPrice'] = df['totalPrice'].str.replace("万", "").replace(".", "").astype(float) * 10000
    rename_dict = {
        "data": "观影人次(猫眼)",
        "totalPrice": "电影票房(猫眼)"
    }
    df = df.rename(columns=rename_dict).dropna()
    area_code_decoder = AreaCodeDecoder()
    df["areacode"] = df["name"].map(area_code_decoder.name_to_areacode)
    df_melt = df.melt(id_vars=["areacode", "name"], value_vars=['观影人次(猫眼)', '电影票房(猫眼)'], var_name='item_short_name',
                      value_name='fvalue')
    pivot_data = pd.pivot_table(df, index=["areacode", "name"], values=['fvalue'], aggfunc='mean')
    print(df)


if __name__ == '__main__':
    group_by_maoyan()
