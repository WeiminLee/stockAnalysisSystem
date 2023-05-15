# -*- encoding: utf-8 -*-
# user:LWM
import datetime


def get_today():
    return str(datetime.datetime.now()).split(' ')[0]


def get_n_days_before(n):
    today = datetime.datetime.today()
    before = today - datetime.timedelta(days=n)
    return str(before).split(" ")[0]


def get_n_days_after(beg, n):
    today = datetime.datetime.strptime(beg, "%Y-%m-%d")
    before = today + datetime.timedelta(days=n)
    return str(before).split(" ")[0]


def get_n_days_before_from_beg(beg, n):
    today = datetime.datetime.strptime(beg, "%Y-%m-%d")
    before = today - datetime.timedelta(days=n)
    return str(before).split(" ")[0]
