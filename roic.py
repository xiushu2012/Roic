# -*- coding: utf-8 -*-

import akshare as ak
import numpy as np  
import pandas as pd  
import math
import datetime
import os
import matplotlib.pyplot as plt
import openpyxl
import time, datetime
import xlsxwriter
from matplotlib.pyplot import MultipleLocator


def get_akshare_stock_financial(xlsfile,stock):
    try:
        shname='financial'
        isExist = os.path.exists(xlsfile)
        if not isExist:
            stock_financial_abstract_df = ak.stock_financial_abstract(stock)
            stock_financial_abstract_df.to_excel(xlsfile,sheet_name=shname)
            print("xfsfile:%s create" % (xlsfile))
        else:
            print("xfsfile:%s exist" % (xlsfile))
            #print(stock_financial_abstract_df)
    except IOError:
        print("Error get stock financial:%s" % stock )
    else:
        return xlsfile, shname

def get_akshare_stock_trade(xlsfile,stock):
    try:
        shname='trade'
        isExist = os.path.exists(xlsfile)
        if not isExist:
            stock_a_indicator_df = ak.stock_a_lg_indicator(stock)
            stock_a_indicator_df.to_excel(xlsfile,sheet_name=shname)
            print("xfsfile:%s create" % (xlsfile))
        else:
            print("xfsfile:%s exist" % (xlsfile))
    except IOError:
        print("Error get stock trade:%s" % stock )
    else:
        return xlsfile, shname


def get_fin_number(strcounts):
    if strcounts is np.nan:
        return 0
    else:
        counts = float(strcounts[0:-1].replace(',',''))
        return counts

def get_fin_date(time):
    return time+" 00:00:00"

def get_roic_value(capital, profits,cost):
    if (capital is np.nan):
        return 0
    floatcapital = float(capital[0:-1].replace(',',''))

    floatprofits = 0.0
    if (profits is not np.nan):
        floatprofits = float(profits[0:-1].replace(',',''))
    floatcost = 0.0
    if (cost is not np.nan):
        floatcost = float(cost[0:-1].replace(',',''))

    return 100*(floatcost+floatprofits)/floatcapital


def calc_latest_roic_mean(row,beg,end):
    latestlist = list(row[beg:end:-1])
    nozerocnt =  len(latestlist)- latestlist.count(0)
    #print(latestlist,nozerocnt)
    if nozerocnt == 0:
        return 0
    else:
        return np.sum(latestlist)/nozerocnt

def calc_all_roic_std(row):
    latestlist = list(row[:-1])
    nozerolist = [roic for roic in latestlist if roic !=0 ]
    nozerocnt =  len(nozerolist)
    print(nozerolist,nozerocnt)
    if nozerocnt == 0:
        return 0
    else:
        return np.std(nozerolist)


def get_time_stamp(date):
    time1 = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    secondsFrom1970 = time.mktime(time1.timetuple())
    return secondsFrom1970


def get_latest30_pbvalue(tradedf,datecolumn,pbcolumn):
    count = 0
    value = 0
    days = 30
    latest = ''
    for tup in zip(tradedf[datecolumn], tradedf[pbcolumn]):
        value += float(tup[1])
        count += 1

        if count == 1:
            latest = tup[0]
        if count >= days:
            break
    return ('30pb',value/days)


def calc_stock_roic_df(stock,name):
    roic_stock_df = pd.DataFrame()
    bget = False
    try:
        filefolder = r'./data'
        isExist = os.path.exists(filefolder)
        if not isExist:
            os.makedirs(filefolder)
            print("AkShareFile:%s create" % (filefolder))
        else:
            print("AkShareFile:%s exist" % (filefolder))

        fininpath = "%s/%s%s" % (filefolder, stock, '_fin_in.xlsx')
        tradeinpath = "%s/%s%s" % (filefolder, stock, '_trade_in.xlsx')

        # 总资产22,493,600,000.00元
        finpath, finsheet = get_akshare_stock_financial(fininpath, stock)
        #print("data of path:" + finpath + "sheetname:" + finsheet)
        tradepath, tradesheet = get_akshare_stock_trade(tradeinpath, stock)
        #print("data of path:" + tradepath + "sheetname:" + tradesheet)

        stock_a_indicator_df = pd.read_excel(tradepath, tradesheet, converters={'trade_date': str, 'pb': str})[['trade_date', 'pb']]
        stock_financial_abstract_df = pd.read_excel(finpath, finsheet, converters={'截止日期': str, '资产总计': str,'净利润': str,'财务费用': str})[['截止日期', '资产总计', '净利润', '财务费用']]

        if stock_financial_abstract_df.empty:
            bget = False;
        else:
            findatecol =  stock  +  'date'
            finroiccol =  stock  +   name

            stock_financial_abstract_df[findatecol] = stock_financial_abstract_df.apply(lambda row: get_fin_date(row['截止日期']),axis=1)
            stock_financial_abstract_df[finroiccol] = stock_financial_abstract_df.apply(lambda row: get_roic_value(row['资产总计'], row['净利润'],row['财务费用']), axis=1)

            roic_stock_df = stock_financial_abstract_df[stock_financial_abstract_df['净利润'] != 0][[findatecol,finroiccol]]
            latestpb = get_latest30_pbvalue(stock_a_indicator_df, 'trade_date', 'pb')
            pbdataframe = pd.DataFrame([[latestpb[0],latestpb[1]]],columns=roic_stock_df.columns)
            roic_stock_df = roic_stock_df.append(pbdataframe)

            bget = True;
    except IOError:
        print("read error file:%s" % stock)
    finally:
        return bget, roic_stock_df



def init_global_time_df(timepath):

    isExist = os.path.exists(timepath)
    if not isExist:
        print("time path not exist:%s" % (timepath))
        return pd.DataFrame()
    else:
        print("time path exist:%s" % (timepath))

    time_list = pd.read_excel(timepath, "analy")['date'].values.tolist()
    time_df = pd.DataFrame(index=time_list)
    return time_df



def get_laststock_set(hs300,datadir):
    allset = set()
    if os.path.exists(hs300):
        input = open(hs300,'r')
        allset = set([stock.rstrip() for stock in input.readlines()])
    else:
        index_stock_cons_df = ak.index_stock_cons(index="000300") #沪深300
        allset = set(index_stock_cons_df['品种代码'].values.tolist()[0::])

    print(len(allset),allset)

    existset = set()
    if os.path.exists(datadir):
        filelist = os.listdir(datadir)
        existset = set([stock.split('_')[0] for stock in filelist])

    lastset = allset - existset

    return allset,lastset


if __name__=='__main__':
    #print(get_time_stamp('2021-02-24 00:00:00'))

    from sys import argv
    hsstocks = ""
    if len(argv) > 1:
        hsstocks = argv[1]
    else:
        print("please run like 'python roic.py [*|002230]'")
        exit(1)


    index_stock_cons_df = pd.DataFrame()
    if hsstocks == '*':
        hs300 = './hs300';datadir = './data'
        stockset,lastset = get_laststock_set(hs300, datadir)
        if len(lastset) >0 :
            print("stock data is not complete",lastset)

        index_stock_cons_df['code'] = [stock for stock in stockset]
        index_stock_cons_df['name'] = ['' for stock in stockset]    
    else:
        index_stock_cons_df['code'] = [stock for stock in argv[1:]]
        index_stock_cons_df['name'] = ['' for stock in argv[1:]]

    timepath = r'./time.xlsx'
    roic_global_df = init_global_time_df(timepath)

    for item in index_stock_cons_df.itertuples():
        stock = item[1].rjust(6,'0')#品种代码 code
        name  = item[2]							#品种名称  name

        bget,roic_stock_df = calc_stock_roic_df(stock,name)
        if bget is False:
            print("get empty DataFrame:%s" % stock)
            continue

        col_name = roic_stock_df.columns.tolist()
        for tup in roic_stock_df.itertuples():
            try:
                if tup[1] in roic_global_df.index.values:
                    roic_global_df.loc[tup[1], col_name[1]] = tup[2]
            except KeyError:
                print("stock:%s,time:%s,location error" % (stock,tup[1]))
    roic_global_df = roic_global_df.T
    roic_global_df[np.isnan(roic_global_df)] = 0.;
    globalstddf = roic_global_df.apply(lambda row: calc_all_roic_std(row), axis=1)
    nearmeandf = roic_global_df.apply(lambda row: calc_latest_roic_mean(row,-2,-5),axis=1)
    farmeandf  = roic_global_df.apply(lambda row: calc_latest_roic_mean(row, -5, -8), axis=1)

    roic_global_df['全局标差'] = globalstddf
    roic_global_df['近期均值'] = nearmeandf
    roic_global_df['远期均值'] = farmeandf
    roic_global_df['价值品质'] = roic_global_df.apply(lambda row: row['近期均值']/5-row['30pb'], axis=1)
    roic_global_df = roic_global_df.sort_values('近期均值', ascending=False)

    #bond_selected_df = roic_global_df[(roic_global_df['近期均值'] >= 8.0) & (roic_global_df['远期均值'] >= 8.0)]
    bond_selected_df = roic_global_df[roic_global_df['近期均值'] >= 8.0]
    bond_selected_df = bond_selected_df.sort_values('价值品质',ascending=False)

    fileout =  './roic' + datetime.datetime.now().strftime('%Y%m') + '.xlsx'
    writer = pd.ExcelWriter(fileout)
    roic_global_df.to_excel(writer,'all')
    bond_selected_df.to_excel(writer,'selected')
    writer.save()
    print("roic value out in:" + fileout)

    # outanalypath = r'./roic.xlsx'
    # workbook = xlsxwriter.Workbook(outanalypath)
    # worksheet = workbook.add_worksheet()
    # bold = workbook.add_format({'bold': True})
    # headRows = 1
    # headCols = 1
    #
    # dfindex = roic_global_df.index.values.tolist()
    # for rowNum in range(len(dfindex)):
    #     worksheet.write_string(rowNum + headRows, 0, str(dfindex[rowNum]))
    #
    #
    # for colNum in range(len(roic_global_df.columns)):
    #     xlColCont = roic_global_df[roic_global_df.columns[colNum]].tolist()
    #     worksheet.write_string(0, colNum+headCols, str(roic_global_df.columns[colNum]), bold)
    #     for rowNum in range(len(xlColCont)):
    #         if np.isnan(xlColCont[rowNum]):
    #             worksheet.write_number(rowNum + headRows, colNum + headCols, 0)
    #         else:
    #             worksheet.write_number(rowNum + headRows, colNum+headCols, xlColCont[rowNum])
    # workbook.close()
    #
    # print("roic value out in :" + outanalypath)
