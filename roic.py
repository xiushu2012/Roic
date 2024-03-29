﻿# -*- coding: utf-8 -*-

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

def get_akshare_stock_financial_analysis(xlsfile,stock):
    try:
        shname='financial'
        isExist = os.path.exists(xlsfile)
        if not isExist:
            stock_financial_analysis_indicator_df = ak.stock_financial_analysis_indicator(symbol=stock)
            stock_financial_analysis_indicator_df.to_excel(xlsfile,sheet_name=shname)
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
            #stock_a_indicator_df = ak.stock_a_lg_indicator(stock)
            stock_a_indicator_df = ak.stock_a_indicator_lg(stock)
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
    

def get_roic_value_ex(capital,profitrate,shareholder,debtratio):
    #print(capital,profitrate,shareholder,longterm)
    floatcapital = float(capital)
    
    floatprofit = float(profitrate)/100
    floatholder = float(shareholder)/100
    floatdebt = float(debtratio)/100

    return 100*floatprofit/(floatholder+floatdebt)



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


def get_latest30_tobinqc(tradedf,datecolumn,pbcolumn,debt,capital):
    count = 0
    value = 0
    days = 30
    for tup in zip(tradedf[datecolumn], tradedf[pbcolumn]):
        #print(tup[0],tup[1])
        value += float(tup[1])*10000
        count += 1
        if count >= days:
            break
    qcvalue = (value/count+debt)/capital
    print('qc30:',qcvalue)
    return qcvalue


def calc_stock_roic_df(stock,name,qcname):
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
        #finpath, finsheet = get_akshare_stock_financial(fininpath, stock)
        finpath, finsheet = get_akshare_stock_financial_analysis(fininpath, stock)
        #print("data of path:" + finpath + "sheetname:" + finsheet)
        tradepath, tradesheet = get_akshare_stock_trade(tradeinpath, stock)
        #print("data of path:" + tradepath + "sheetname:" + tradesheet)

        stock_a_indicator_df = pd.read_excel(tradepath, tradesheet, converters={'trade_date': str, 'total_mv': str})[['trade_date', 'total_mv']]
        stock_a_indicator_df = stock_a_indicator_df.sort_values('trade_date', ascending=False)
        
        #stock_financial_abstract_df = pd.read_excel(finpath, finsheet, converters={'截止日期': str, '资产总计': str,'净利润': str,'财务费用': str,'长期负债合计':str})[['截止日期', '资产总计', '净利润', '财务费用','长期负债合计']]
        #stock_financial_abstract_df = stock_financial_abstract_df.sort_values('截止日期', ascending=False)

        stock_financial_abstract_df = pd.read_excel(finpath, finsheet, converters={'日期': str, '总资产(元)': str,'总资产净利润率(%)': str,'股东权益比率(%)': str,'资产负债率(%)':str})[['日期', '总资产(元)', '总资产净利润率(%)', '股东权益比率(%)','资产负债率(%)']]
        stock_financial_abstract_df = stock_financial_abstract_df.sort_values('日期', ascending=False)
        stock_financial_abstract_df = stock_financial_abstract_df.replace('--','0')
        strcapital  = stock_financial_abstract_df['总资产(元)'][0]
        strdebt = stock_financial_abstract_df['资产负债率(%)'][0]
        
        print("资产负债率",strdebt);print("资产总计",strcapital);
        if stock_financial_abstract_df.empty or (strdebt is np.nan) or (strcapital is np.nan):
            bget = False;
        else:

            findatecol =  stock  +  'date'
            finroiccol =  stock  +   name


            #stock_financial_abstract_df[findatecol] = stock_financial_abstract_df.apply(lambda row: get_fin_date(row['截止日期']),axis=1)
            #stock_financial_abstract_df[finroiccol] = stock_financial_abstract_df.apply(lambda row: get_roic_value(row['资产总计'], row['净利润'],row['财务费用']), axis=1)
            
            stock_financial_abstract_df = stock_financial_abstract_df[(stock_financial_abstract_df['总资产净利润率(%)'] != '0') & (stock_financial_abstract_df['总资产(元)'] != '0')]
            stock_financial_abstract_df[findatecol] = stock_financial_abstract_df.apply(lambda row: get_fin_date(row['日期']),axis=1)
            stock_financial_abstract_df[finroiccol] = stock_financial_abstract_df.apply(lambda row: get_roic_value_ex(row['总资产(元)'], row['总资产净利润率(%)'],row['股东权益比率(%)'],row['资产负债率(%)']), axis=1)
            #print(stock_financial_abstract_df)
            
            roic_stock_df = stock_financial_abstract_df[[findatecol,finroiccol]]
            #print(roic_stock_df)

            capital = float(strcapital)
            debt = capital*float(strdebt)/100.0
            #print("长期债务",debt);print("资产总计",capital);

            qcvalue = get_latest30_tobinqc(stock_a_indicator_df, 'trade_date', 'total_mv',debt,capital)
            qcdataframe = pd.DataFrame([[qcname,qcvalue]],columns=roic_stock_df.columns)
            #roic_stock_df = roic_stock_df.append(qcdataframe)
            roic_stock_df = pd.concat([roic_stock_df,qcdataframe],ignore_index=True)

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

def get_stockname_dict(hs300):
    stockdict ={}
    if os.path.exists(hs300):
        input = open(hs300,'r')
        for stock in input.readlines():
          line = stock.rstrip().split('\t')
          stockcode = line[0];stockname = line[1]
          stockdict[stockcode]=stockname
    return stockdict


def get_laststock_set(hs300,datadir):
    allset = set()
    if os.path.exists(hs300):
        input = open(hs300,'r')
        allset = set([(stock.rstrip().split())[0] for stock in input.readlines()])
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

        stockdict = get_stockname_dict(hs300)
        index_stock_cons_df['code'] = [stock for stock in stockset]
        index_stock_cons_df['name'] = [stockdict[stock] for stock in stockset if stock in stockdict.keys()]    
    else:
        index_stock_cons_df['code'] = [stock for stock in argv[1:]]
        index_stock_cons_df['name'] = ['' for stock in argv[1:]]

    timepath = r'./time.xlsx'
    roic_global_df = init_global_time_df(timepath)
    qcname = roic_global_df.index.values.tolist()[-1]
    print(qcname)

    for item in index_stock_cons_df.itertuples():
        stock = item[1].rjust(6,'0')#品种代码 code
        name  = item[2]							#品种名称  name

        bget,roic_stock_df = calc_stock_roic_df(stock,name,qcname)
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
    roic_global_df['价值溢价'] = roic_global_df.apply(lambda row: (row[qcname]-row['近期均值']/5)/(row['近期均值']/5), axis=1)
    roic_global_df = roic_global_df.sort_values('近期均值', ascending=False)

    bond_selected_df = roic_global_df[(roic_global_df['近期均值'] >= 8.0) & (roic_global_df['远期均值'] >= 8.0)]
    #bond_selected_df = roic_global_df[(roic_global_df['近期均值'] >= 8.0) & (roic_global_df['价值溢价'] <= 0.0)]
    bond_selected_df = bond_selected_df.sort_values('价值溢价',ascending=True)

    fileout =  './roic' + datetime.datetime.now().strftime('%Y%m') + '.xlsx'
    writer = pd.ExcelWriter(fileout)
    roic_global_df.to_excel(writer,'all')
    bond_selected_df.to_excel(writer,'selected')
    #writer.save()
    writer.close()
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
