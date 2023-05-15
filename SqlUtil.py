# -*- encoding: utf-8 -*-
# user:LWM

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from collections import Counter
import psycopg2

pd.set_option('display.max_columns', 10)  # 显示所有列
pd.set_option('display.max_columns', 20)  # 最多显示20列
plt.rcParams['font.sans-serif'] = ['SimHei']

engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/astock')


def insert_sql(data, db_name, if_exists='append'):
    try:
        data.to_sql(db_name, engine, index=False, if_exists=if_exists)
    except:
        print('wrong with insert StaticsData to sql')
        pass


def get_lasted_date_by_code(code):
    sql = "select * from stock_data where code='{}' order by date desc limit 5".format(code)
    df = pd.read_sql(sql, engine)
    if df.empty:
        return '20221001'
    beg = df['date'][0].replace('-', '')
    return beg


def last_date_dapan_zt_features():
    sql = "select * from dapan_zt_features order by date desc limit 5"
    df = pd.read_sql(sql, engine)
    beg = df['date'][0]
    return beg


def get_lasted_date_by_codes(codes):
    sql = "select code,date from stock_data where code in ('{0}') order by date desc".format("','".join(codes))
    df = pd.read_sql(sql, engine)
    dates = []
    for code in codes:
        tmp = df[df['code'] == code]
        if tmp.empty:
            return '19900101'
        dates.append(df['date'][0].replace('-', ''))
    return sorted(dates)


def get_all_sql_exist_stock_code():
    sql = "select distinct(code) from stock_data "
    df = pd.read_sql(sql, engine)
    return df['code'].values.tolist()


def get_exist_codes_tech_indicator():
    try:
        sql = "select distinct(code) from stock_tech_indicators "
        df = pd.read_sql(sql, engine)
    except:
        return []
    return set(df['code'].values.tolist())


def get_ip_data():
    try:
        sql = "select * from ip_data"
        df = pd.read_sql(sql, engine)
    except:
        df = pd.DataFrame()
    return df


def drop_ip_data(invalid_hosts):
    conn = psycopg2.connect(database="astock", user="postgres", password="admin", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    sql = "delete from ip_data where host in ('{0}')".format("','".join(invalid_hosts))
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


def get_exist_sacs_info():
    try:
        sql = "select * from stock_concept_features "
        df = pd.read_sql(sql, engine)
    except:
        return pd.DataFrame()
    return df


def get_all_basic_info_exist_stock_code():
    sql = "select distinct(code) from stock_basic_feature "
    df = pd.read_sql(sql, engine)
    return df['code'].values.tolist()


def get_stock_data_from_sql(code, beg, end):
    sql = "select * from stock_data where code='{0}' and date>='{1}' and date <= '{2}' order by date asc".format(code,
                                                                                                                 beg,
                                                                                                                 end)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    return df


def get_stock_data_all(code):
    sql = "select * from stock_data where code='{0}'order by date asc".format(code)
    df = pd.read_sql(sql, engine)
    df.sort_values(by=['date'], inplace=True, ascending=True)
    df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_single_dapan_data_all(code):
    sql = "select * from dapan_data where code='{0}' order by date asc".format(code)
    df = pd.read_sql(sql, engine)
    df.dropna(axis=1,inplace=True)
    df.sort_values(by=['date'], inplace=True, ascending=True)
    df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_dapan_data_all():
    sql = "select * from dapan_data order by date asc"
    df = pd.read_sql(sql, engine)
    df.sort_values(by=['date'], inplace=True, ascending=True)
    df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_batch_dapan_data_by_code(codes, date):
    sql = "select * from dapan_data where code in ('{0}') and date='{1}' order by date asc".format("','".join(codes),
                                                                                                   date)
    df = pd.read_sql(sql, engine)
    df.sort_values(by=['date'], inplace=True, ascending=True)
    df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_all_by_date(date):
    sql = "select * from stock_data where date='{0}'".format(date)
    df = pd.read_sql(sql, engine)
    df.sort_values(by=['code'], inplace=True, ascending=True)
    df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_tech_indicators(code):
    try:
        sql = "select * from stock_tech_indicators where code='{}' order by date asc".format(code)
        df = pd.read_sql(sql, engine)
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
        df.index = range(df.shape[0])
    except:
        return pd.DataFrame()
    return df


def get_stock_tech_indicators_dates(code):
    try:
        sql = "select distinct(date) from stock_tech_indicators where code='{}' order by date asc".format(code)
        df = pd.read_sql(sql, engine)
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
        df.index = range(df.shape[0])
    except:
        return pd.DataFrame()
    return df


def get_all_dapan_tech_indicators(code):
    sql = "select * from dapan_tech_indicators where code = '{}';".format(code)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_batch_stock_tech_indicators_single_date(codes, date):
    sql = "select * from stock_tech_indicators where code in ('{0}') and date ='{1}' ".format("','".join(codes), date)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


def drop_stock_tech_indicators_with_code(code):
    conn = psycopg2.connect(database="astock", user="postgres", password="admin", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    sql = "delete from stock_tech_indicators where code='{}'".format(code)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


def get_stock_data_2day_from_sql(code, beg):
    sql = "select * from stock_data where code='{0}' and date>='{1}'  order by date limit 2".format(code, beg)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_batch_2day_from_sql(codes, beg):
    sql = "select * from stock_data where code in ('{0}') and date>='{1}'  order by date limit {2}".format(
        "','".join(codes), beg,
        2 * len(codes))
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_batch_ndays_back(codes, end, n):
    sql = "select * from stock_data where code in ('{0}') and date >='{1}' order by date limit {2}".format(
        "','".join(codes), end, n * len(codes))
    print(sql)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_batch_3day_from_sql(codes, beg):
    sql = "select * from stock_data where code in ('{0}') and date>='{1}'  order by date limit {2}".format(
        "','".join(codes), beg,
        3 * len(codes))
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_batch_10day_from_sql(codes, beg, end):
    sql = "select * from stock_data where code in ('{0}') and date>='{1}' and date<='{2}' order by date".format(
        "','".join(codes), beg, end)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_batch_with_date(date):
    sql = "select * from stock_data where date='{0}'  order by code ".format(date)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_batch_with_codes_date(codes, date):
    sql = "select * from stock_data where code in ('{0}') and date='{1}'  order by code ".format("','".join(codes),
                                                                                                 date)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_news_from_sql(time):
    sql = "select * from stock_news where time>='{0}' order by time asc".format(time)
    print(sql)
    df = pd.read_sql(sql, engine)
    return df


def get_stock_data_batch_from_sql(codes, beg, end):
    sql = "select * from stock_data where code in ('{0}') and date>='{1}' and date <= '{2}' order by date asc".format(
        "','".join(codes),
        beg,
        end)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_single_from_sql(code, beg, end):
    sql = "select * from stock_data where code = '{0}' and date>='{1}' and date <= '{2}' order by date asc".format(
        code,
        beg,
        end)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code', 'date'], keep='first', inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_rps_data_from_sql(code, beg):
    sql = "select * from stock_rps where code ='{0}'  and date > '{1}' order by date desc".format(code, beg)
    df = pd.read_sql(sql, engine)
    return df


def get_stock_minute_data_from_sql(code, date):
    sql = "select * from stock_minute_data where code='{0}' and date>='{1} 00:00:00' and date <= '{1} 15:00:00' " \
          "order by date asc".format(code, date)
    df = pd.read_sql(sql, engine)
    return df


def get_stock_minute_data_batch_from_sql(codes, date):
    sql = "select * from stock_minute_data where code in ('{1}') and date>='{0} 00:00:00' and date <= '{0} 15:00:00' order by code,date asc".format(
        date, "','".join(codes))
    df = pd.read_sql(sql, engine)
    return df


def get_stocks_by_product_object(product):
    sql = "select * from stock_product where object='{}'".format(product)
    df = pd.read_sql(sql, engine)
    return df['subject']


def get_all_products():
    sql = "select * from stock_product where predict='产品名称'"
    df = pd.read_sql(sql, engine)
    return df


def get_all_industry():
    sql = "select * from stock_industry"
    df = pd.read_sql(sql, engine)
    return df


def get_same_industry_codes_by_code(code):
    sql = "select * from stock_industry where code = '{}'".format(code)
    df = pd.read_sql(sql, engine)
    industry = df['industry'].values[0]
    sql = "select * from stock_industry where industry = '{}'".format(industry)
    df = pd.read_sql(sql, engine)
    return df['code'].values.tolist()


def get_all_concept():
    sql = "select * from stock_concept"
    df = pd.read_sql(sql, engine)
    return df


def get_up_limt_codes(date):
    sql = "select * from stock_data where pct_chg>9.95 and date='{}'".format(date)
    df = pd.read_sql(sql, engine)
    return df['code']


def get_stocks_by_product(pro):
    sql = "select * from stock_product  where object like '%%{}%%'".format(pro)
    df = pd.read_sql(sql, engine)
    return df


def get_top_up_codes(date):
    sql = "select * from stock_data where StaticsData='{}' order by pct_chg desc limit 30".format(date)
    df = pd.read_sql(sql, engine)
    return df


def get_stock_concepts(codes):
    sql = "select * from stock_concept where code in ('{}')".format("','".join(codes))
    df = pd.read_sql(sql, engine)
    cc = Counter(df['concept'].values)
    result = [x for x in list(sorted(dict(cc).items(), key=lambda x: x[1], reverse=True)) if x[1] > 1]
    return result


def get_stock_industry(codes):
    sql = "select * from stock_industry where code in ('{}')".format("','".join(codes))
    df = pd.read_sql(sql, engine)
    cc = Counter(df['industry'].values)
    result = [x for x in list(sorted(dict(cc).items(), key=lambda x: x[1], reverse=True)) if x[1] > 1]
    top_stock = {}
    for res in result:
        tmp = df[df['industry'] == res[0]]
        codes = list(tmp[['code', 'name']].apply(lambda x: ':'.join(x), axis=1).values)
        top_stock[res[0]] = codes
    return result, top_stock


def get_stocks_by_concept(con):
    sql = "select * from stock_concept where concept='{}'".format(con)
    df = pd.read_sql(sql, engine)
    return df['code'].values.tolist()


def get_stock_name_code_data():
    sql = "select distinct(code,name) from stock_data"
    df = pd.read_sql(sql, engine)
    return df


def get_stock_code_by_name(names):
    sql = "select *  from stock_info where name in ('{}')".format("','".join(names))
    df = pd.read_sql(sql, engine)
    return df['code'].values


def get_top_concepts_df_by_sql(date):
    sql = "select * from stock_top_concept where date='{}'".format(date)
    df = pd.read_sql(sql, engine)
    return df


def get_all_cls_symble_df():
    sql = "select distinct (active_concepts.symbol_code, active_concepts.symbol_name)  from active_concepts"
    df = pd.read_sql(sql, engine)
    return df


def get_active_concepts():
    sql = "select * from active_concepts"
    df = pd.read_sql(sql, engine)
    return df


def get_codes_by_cls_symbol(cls_symbol):
    sql = "select * from cls_codes where cls_code='{}'".format(cls_symbol)
    df = pd.read_sql(sql, engine)
    return df


def get_stock_exchange_days(ndays):
    sql = "select * from stock_data where code='000001' order by date desc limit {}".format(ndays)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(inplace=True)
    df.index = range(df.shape[0])
    return df['date'].values.tolist()


def get_stock_data_lbg():
    sql = "select * from stock_lbg order by date"
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(inplace=True)
    df.index = range(df.shape[0])
    return df


def get_zt_stock_data_by_date(date):
    sql = "select * from stock_zt_data where date='{}'".format(date)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(inplace=True)
    df.index = range(df.shape[0])
    return df


def get_all_zt_stock_data():
    sql = "select * from stock_zt_data"
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(inplace=True)
    return df


def get_exist_zt_dates():
    sql = "select * from stock_zt_data"
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['date'], inplace=True)
    df.sort_values(by=['date'], ascending=True, inplace=True)
    df.index = range(df.shape[0])
    return df['date'].values.tolist()


def get_zt_dapan_feature(date=None):
    if date:
        sql = "select * from dapan_zt_features where date='{}'".format(date)
    else:
        sql = "select * from dapan_zt_features order by date asc"
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(inplace=True)
    df.index = range(df.shape[0])
    return df


def get_latest_day_lbg():
    sql = "select * from stock_lbg order by date desc limit 1"
    df = pd.read_sql(sql)
    return df['date'].values[0]


def get_stock_lhb_data(code, date):
    sql = "select * from stock_lhb where code = '{}' and date = '{}'".format(code, date)
    df = pd.read_sql(sql, engine)
    return df


def get_train_data_from_sql():
    sql = "select * from train_data"
    df = pd.read_sql(sql, engine)
    return df


def get_feature_dapan_data_from_sql():
    sql = "select * from feature_dapan"
    df = pd.read_sql(sql, engine)
    return df


def get_all_jinjia_data_from_sql():
    sql = "select * from stock_jj_data"
    df = pd.read_sql(sql, engine)
    return df


def get_stock_jinjia_data_from_sql(code):
    sql = "select * from stock_jj_data where code='{}'".format(code)
    df = pd.read_sql(sql, engine)
    df.sort_values(by=['date'], inplace=True, ascending=True)
    return df


def get_single_jinjia_data_from_sql(code):
    sql = "select * from stock_jj_data where code ='{}'".format(code)
    df = pd.read_sql(sql, engine)
    return df


def get_feature_industry_data_from_sql():
    sql = "select * from feature_industry"
    df = pd.read_sql(sql, engine)
    return df


def get_feature_day_data_from_sql():
    sql = "select * from feature_day"
    df = pd.read_sql(sql, engine)
    return df


def get_label_data_from_sql():
    sql = "select * from stock_label"
    df = pd.read_sql(sql, engine)
    return df


def update_table_with_new_df(tabel, new_df):
    conn = psycopg2.connect(database="astock", user="postgres", password="admin", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    sql = "drop table {};".format(tabel)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    insert_sql(new_df, tabel)


def drop_stock_data_with_code(code):
    conn = psycopg2.connect(database="astock", user="postgres", password="admin", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    sql = "delete from stock_data where code='{}'".format(code)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


def drop_stock_basic_info_with_code(code):
    conn = psycopg2.connect(database="astock", user="postgres", password="admin", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    sql = "delete from stock_basic_info where code = '{}';".format(code)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


def get_stock_basic_info(code):
    sql = "select * from stock_basic_info where code = '{}';".format(code)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['trade_date'], keep='last', inplace=True)
    df.rename(columns={"trade_date": "date"}, inplace=True)
    df.index = range(df.shape[0])
    return df


def get_all_codes_by_basic_info():
    sql = "select distinct(code) from stock_basic_info"
    df = pd.read_sql(sql, engine)
    df.rename(columns={"trade_date": "date"}, inplace=True)
    return df['code'].values.tolist()


def get_batch_stock_data_basic_info(codes, date):
    sql = "select * from stock_basic_info where code in ('{}') and trade_date ='{}';".format("','".join(codes), date)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['trade_date', 'code'], keep='last', inplace=True)
    df.rename(columns={"trade_date": "date"}, inplace=True)
    df.index = range(df.shape[0])
    return df


def get_stock_data_basic_info(code, date):
    sql = "select * from stock_basic_info where code = '{}' and date='{}';".format(code, date)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['trade_date'], keep='last', inplace=True)
    df.rename(columns={"trade_date": "date"}, inplace=True)
    df.index = range(df.shape[0])
    return df


def get_dapan_tech_indicators(code, date):
    sql = "select * from dapan_tech_indicators where date='{}' and code = '{}';".format(date, code)
    df = pd.read_sql(sql, engine)
    df.drop_duplicates(subset=['code'], keep='last', inplace=True)
    df.index = range(df.shape[0])
    return df


if __name__ == '__main__':
    codes = ['000001', '000002']
    get_lasted_date_by_codes(codes)
