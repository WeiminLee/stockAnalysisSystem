# -*- encoding: utf-8 -*-
# user:LWM

class Const():
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "Cookie": "v=AyWHFOPfqqYs7s76V2jPLZJjNOpaYtn0Ixa9SCcK4dxrPksU77LpxLNmzRm0; Hm_lvt_fb36a023098c718e4cae93708339c4d7=1669513427; Hm_lpvt_fb36a023098c718e4cae93708339c4d7=1669513427"
    }
    news_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        'Referer': 'https://www.cls.cn/telegraph'
    }

    lbg_header = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        "cookie": "typography=Nunito; version=light; layout=vertical; primary=color_1; headerBg=color_1; navheaderBg=color_1; sidebarBg=color_1; sidebarStyle=full; themeBg=theme_1; sidebarPosition=fixed; headerPosition=fixed; containerLayout=full; navigationBarImg=; session=.eJxNjcEKwjAQRP9lz0E0MemSkx8SKKFdMJBNS9yliPjvVuzB2_DmDfOCcaXOuVETiNKVDBDnUiFCLRsVLs0i4u1iw2laGAyUGWK4ojOwyJ16y0y7vBdC9Rf0QX2U5_rlSf10zklxdj7p4C0mDdYNh3as_6_g_QHWTS5x.ZDLBHQ.IHvJYYEPguyiIDAs5Vibsq5sFGI"
    }

    gb_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        'Cookie': 'qgqp_b_id=a01d24bb874d50cc4e76f783ccb7c764; HAList=ty-1-600785-%u65B0%u534E%u767E%u8D27%2Cty-1-600020-%u4E2D%u539F%u9AD8%u901F%2Cty-0-000721-%u897F%u5B89%u996E%u98DF%2Cty-0-002244-%u6EE8%u6C5F%u96C6%u56E2; st_si=86361283827727; _adsame_fullscreen_18009=1; st_asi=delete; st_pvi=72206493183740; st_sp=2022-12-14%2021%3A35%3A01; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=21; st_psi=20230105233927619-117001313005-9473905017'
    }

    xueqiu_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        'Cookie': 'device_id=119a1b8a56f825a420bac7aa7e00ad0a; s=cs1ap333wn; bid=20b6c5269b7fd5361a50b687a4382d9b_lca6lesr; xq_a_token=b3e93eb617b10a047045618b41d775f22c4b780a; xqat=b3e93eb617b10a047045618b41d775f22c4b780a; xq_r_token=da6bb975548730b980e8ad8e04ae4e88e5dd1080; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjg1MjQzNjczMTUsImlzcyI6InVjIiwiZXhwIjoxNjc0OTc2NDYxLCJjdG0iOjE2NzI0MDgyMTQzMzksImNpZCI6ImQ5ZDBuNEFadXAifQ.K1vSoCKeDTkxqqp-2d_GsMa7TNe6y1K6gvlOa6bgmDkqzajELg8kSRHB6ocs8_SnbvYTwqFRKIq8PIyhsFEm57ohyLrKxNfnBMT-X5p-7GeIJAU315OESCBiLUiqWVngzZUG6JioVYnqWrMu98kvDKg_QeqVX7aI8ro57_7J4ZyxxI7rlaosDzPfgJANT0u61IJOsTkG6yCX5Jr0525orK5NZJ57Rlkucp4fVXVyqHD4v3GyHc-FprmCle8CqkmWCBaP92WuRLR_ytHZhxefc28jxajnvkKG2vRd-bW97Es26VIJZWcvv77-qQWJXhvW_SNd0RcjIX5-VMxMBG7l-A; xq_is_login=1; u=8524367315; acw_tc=276077b516730118225543550e23a8cb42136430b59da716a6325e44673e45; Hm_lvt_1db88642e346389874251b5a1eded6e3=1671264952,1672384067,1672668594,1673011823; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1673012438; is_overseas=0'
    }

    rename_dict = {
        '股票名称': 'name',
        '股票代码': "code",
        '日期': 'date',
        '开盘': 'open',
        '收盘': "close",
        '最高': "high",
        '最低': "low",
        '成交量': 'vol',
        '成交额': 'amount',
        '振幅': 'amp',
        '涨跌幅': 'pct_chg',
        '涨跌额': 'change',
        '换手率': 'pct_vol'
    }

    rename_rps = {
        "代码": "code",
        "名称": "name",
        "涨幅%": "pct_chg",
        "收盘": "close",
        "总金额": "amount",
    }

    rename_news = {
        '标题1': "title",
        "标题链接1": "",
        "字段1": "time",
        "字段2": "context"
    }

    rename_jingjia = {
        "股票代码": "code",
        "交易日期": "date",
        "交易时间": "time",
        "成交价": "price",
        "成交量": "jj_vol",
        "成交额": "jj_amount"
    }

    rename_zt = {
        '序号': "index",
        '代码': "code",
        '名称': "name",
        '涨跌幅': "pct_chg",
        '最新价': "last_price",
        '成交额': "amount",
        '流通市值': "liutong_sz",
        '总市值': "zong_sz",
        '换手率': "turnover",
        '封板资金': "fb_money",
        '首次封板时间': "fst_fb",
        '最后封板时间': "lst_fb",
        '炸板次数': "zb_count",
        '涨停统计': "zt_count",
        '连板数': 'lb_count',
        '所属行业': 'industry'
    }

    rename_lhb = {
        '股票代码': "code",
        '股票名称': "name",
        '上榜日期': "date",
        '解读': "reason",
        '收盘价': "close",
        '涨跌幅': "pct_chg",
        '换手率': "pct_vol",
        '龙虎榜净买额': "buy_net",
        '龙虎榜买入额': "buy",
        '龙虎榜卖出额': "sell",
        '龙虎榜成交额': "lhb_amount",
        '市场总成交额': "total_amount",
        '净买额占总成交比': "buy_ratio",
        '成交额占总成交比': "amount_ratio",
        '流通市值': "mv_vol",
        '上榜原因': "type"
    }

    dapan_zt_features_filepath = 'StaticsData/dapan_zt_features.csv'
    jigou_youzi_filepath = 'StaticsData/jigou_youzi_ration.csv'
    sacj_path = 'StaticsData/sacs_old.json'
    stock_concept_path = 'StaticsData/stock_concepts.csv'
    stock_controller_path = 'StaticsData/stock_controller.csv'
    stock_product_path = 'StaticsData/stock_products.csv'


# stock_basic_features
stock_basic_features = ["name", "code", "date", "open", "close", "high", "low", "vol", "amount", "amp", "pct_chg",
                        "change", "pct_vol", "close_ma5", "close_ma10", "close_ma20", "close_ma30", "close_ma60",
                        "high_ma5", "high_ma10", "high_ma20", "high_ma30", "high_ma60", "low_ma5", "low_ma10",
                        "low_ma20", "low_ma30", "low_ma60", "vol_ma5", "vol_ma10", "vol_ma20", "vol_ma30", "vol_ma60",
                        "close_ma5t", "close_ma10t", "close_ma20t", "close_ma30t", "close_ma60t", "high_ma5t",
                        "high_ma10t", "high_ma20t", "high_ma30t", "high_ma60t", "low_ma5t", "low_ma10t", "low_ma20t",
                        "low_ma30t", "low_ma60t", "time", "price", "jj_vol", "jj_amount", "price_ma5", "price_ma10",
                        "jj_vol_ma5", "jj_vol_ma10", "jj_amount_ma5", "jj_amount_ma10", "price_close_ma5_r",
                        "open_close_ma5_r", "price_high_ma5_r", "open_high_ma5_r", "price_low_ma5_r", "open_low_ma5_r",
                        "price_close_ma10_r", "open_close_ma10_r", "price_high_ma10_r", "open_high_ma10_r",
                        "price_low_ma10_r", "open_low_ma10_r", "price_close_ma20_r", "open_close_ma20_r",
                        "price_high_ma20_r", "open_high_ma20_r", "price_low_ma20_r", "open_low_ma20_r",
                        "price_close_ma30_r", "open_close_ma30_r", "price_high_ma30_r", "open_high_ma30_r",
                        "price_low_ma30_r", "open_low_ma30_r", "price_close_ma60_r", "open_close_ma60_r",
                        "price_high_ma60_r", "open_high_ma60_r", "price_low_ma60_r", "open_low_ma60_r"]
# close_ma5t : 5日均线的趋势价
# jj_vol： 竞价相关指标
# open_high_ma5_r ： 开盘价与 最高价5日趋势线的占比
# price_low_ma10_r: 竞价价格与最低价10日趋势线偏离程度

stock_tech_data = ["date", "code", "ma5", "ma10", "ma20", "ma30", "ma60", "open_hhv5", "open_hhv10", "open_hhv30",
                   "open_hhv60", "open_ll5", "open_llv10", "open_llv30", "open_llv60", "close_hhv5", "close_hhv10",
                   "close_hhv30", "close_hhv60", "close_ll5", "close_llv10", "close_llv30", "close_llv60", "low_hhv5",
                   "low_hhv10", "low_hhv30", "low_hhv60", "low_ll5", "low_llv10", "low_llv30", "low_llv60", "high_hhv5",
                   "high_hhv10", "high_hhv30", "high_hhv60", "high_ll5", "high_llv10", "high_llv30", "high_llv60",
                   "DIFF", "DEA", "MACD", "KDJ_K", "KDJ_D", "KDJ_J", "SKDJ_K", "SKDJ_D", "BBI", "BOLL", "UB", "LB",
                   "ROC", "MAROC", "MTM", "MTMMA"]

invalid_concepts = ['连板股', '公告', '其他', '次新', '上市新股', '低价']

lhb_rename = {
    "code": "代码",
    "date": "日期",
    "reason": "解读",
    "buy_net": "净买额",
    "buy": "买入额",
    "sell": "卖出额",
    "lhb_amount": "成交额",
    "total_amount": "市场总成交",
    "buy_ratio": "净买占比",
    "amount_ratio": "成交占比",
    "pct_vol": "换手率",
    "mv_vol": "流通值",
    "type": "原因"
}
lhb_cols = ['buy_net', 'buy', 'sell', 'lhb_amount','total_amount','mv_vol']

fillna_alg = {
    "dv_ttm": "avg",
    "pe_ttm": "avg",
    "has_buy_jigou": 0,
    "buy_jigou_r": 0,
    "buy_jigou_buy_r": 0,
    "has_sell_jigou": 0,
    "": ""
}

valid_features = ['open_ma20',
                  'high_ma10',
                  'open_close_llv_30_deg',
                  'high_close_llv_10_deg',
                  'low_close_ma20_deg',
                  'dapan_open_ma10_trend',
                  'dapan_high_ma60_trend',
                  'dapan_low_ma60_trend',
                  'dapan_high_ma120',
                  'dapan_low_ma5',
                  'dapan_low_llv_10',
                  'dapan_open_close_ma10_trend_deg',
                  'dapan_open_high_ma30_trend_deg',
                  'dapan_open_high_ma60_trend_deg',
                  'dapan_open_close_hhv_10_deg',
                  'dapan_open_high_hhv_10_deg',
                  'dapan_open_high_llv_5_deg',
                  'dapan_high_open_ma10_trend_deg',
                  'dapan_high_open_ma60_trend_deg',
                  'dapan_high_close_ma5_trend_deg',
                  'dapan_high_high_ma5_trend_deg',
                  'dapan_high_low_ma60_trend_deg',
                  'dapan_high_open_hhv_60_deg',
                  'dapan_high_open_llv_60_deg',
                  'dapan_high_close_hhv_60_deg',
                  'dapan_high_low_llv_60_deg',
                  'dapan_high_high_llv_10_deg',
                  'dapan_close_open_ma20_trend_deg',
                  'dapan_close_high_llv_5_deg',
                  'dapan_low_open_ma10_trend_deg',
                  'dapan_low_open_ma30_trend_deg',
                  'dapan_low_close_ma10_trend_deg',
                  'dapan_low_close_ma20_trend_deg',
                  'dapan_low_close_ma60_trend_deg',
                  'dapan_low_low_ma20_trend_deg',
                  'dapan_low_close_hhv_10_deg',
                  'three_five_down_r',
                  'sanban_n',
                  'BILLBOARD_DEAL_AMT',
                  'buy1_buy_up_cnt',
                  'buy3_sell_n_r']
