# -*- encoding: utf-8 -*-
# user:LWM
import pandas as pd
from tqdm import tqdm
import DataUtil, SqlUtil
import warnings
from pandas.core.common import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


# 获取股票基本数据，市盈率等，写入SQL
def update_stock_basic_info_by_akshare():
    codes, names = DataUtil.get_all_exist_stock_code()
    exist_codes = list(set(SqlUtil.get_all_codes_by_basic_info()))
    for idx, code in tqdm(enumerate(codes)):
        if code in exist_codes:
            continue
        df = DataUtil.get_basic_info_by_code(code)
        df['code'] = [code] * df.shape[0]
        # 从数据库中获取基本面信息
        df2 = SqlUtil.get_stock_basic_info(code)
        df3 = pd.concat([df2, df])
        df3.sort_values(by=['trade_date'], ascending=False, inplace=True)
        df3.drop_duplicates(subset=['trade_date'], keep='last', inplace=True)
        # 删除旧的数据信息
        SqlUtil.drop_stock_basic_info_with_code(code)
        # 插入新增的数据信息
        SqlUtil.insert_sql(df, 'stock_basic_info')


if __name__ == '__main__':
    update_stock_basic_info_by_akshare()
