#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author:haojian
# File Name: EricssonLteCmParser.py
# Created Time: 2022/6/1 10:50:57
#文件路径/data/esbftp/cm/4G/ERICSSON/OMC1/CM/202*/CM_4G*.tar.gz


import os,sys
import logging
from logging.handlers import RotatingFileHandler
import xml.etree.ElementTree as ET
from xml.parsers import expat
import math
import glob
import tarfile
import gzip
import datetime

#assert ('linux' in sys.platform), '该代码只能在 Linux 下执行'


os.chdir(sys.path[0])
#assert ('linux' in sys.platform), '该代码只能在 Linux 下执行'
if 'linux' in sys.platform:
    inpath='/data/esbftp/cm/4G/ERICSSON/OMC1/CM/%s/CM_4G*.tar.gz'%datetime.datetime.now().strftime('%Y%m%d')
    outpath='/data/output/cm/ericsson/4g/'
    logpath='../log/'
else:
    inpath='./CM_4G*.tar.gz'
    outpath='./'
    logpath='./'


#handler = RotatingFileHandler('EricssonLteCmParser.log',maxBytes = 100*1024*1024,backupCount = 3)
handler = logging.FileHandler(logpath+'EricssonLteCmParser_%s.log'%datetime.datetime.now().strftime('%Y%m%d'))
#handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
console = logging.StreamHandler()
#console.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
logger.addHandler(handler)
if 'linux' not in sys.platform:
    logger.addHandler(console)

def fcn2feq(fcn):
    fcn=int(fcn)
    if fcn<600:	
        ful=1920+0.1*(fcn-0);fdl=ful+190
    elif fcn<1200:	
        ful=1850+0.1*(fcn-600);fdl=ful+80
    elif fcn<1950:	
        ful=1710+0.1*(fcn-1200);fdl=ful+95
    elif fcn<2400:	
        ful=1710+0.1*(fcn-1950);fdl=ful+400
    elif fcn<2650:	
        ful=824+0.1*(fcn-2400);fdl=ful+45
    elif fcn<2750:	
        ful=830+0.1*(fcn-2650);fdl=ful+45
    elif fcn<3450:	
        ful=2500+0.1*(fcn-2750);fdl=ful+120
    elif fcn<3800:	
        ful=880+0.1*(fcn-3450);fdl=ful+45
    elif fcn<4150:	
        ful=1749.9+0.1*(fcn-3800);fdl=ful+95
    elif fcn<4750:	
        ful=1710+0.1*(fcn-4150);fdl=ful+400
    elif fcn<5000:	
        ful=1427.9+0.1*(fcn-4750);fdl=ful+48
    elif fcn<5180:	
        ful=698+0.1*(fcn-5000);fdl=ful+30
    elif fcn<5280:	
        ful=777+0.1*(fcn-5180);fdl=ful+-31
    elif fcn<5380:	
        ful=788+0.1*(fcn-5280);fdl=ful+-30
    elif fcn<5850:	
        ful=704+0.1*(fcn-5730);fdl=ful+30
    elif fcn<6000:	
        ful=815+0.1*(fcn-5850);fdl=ful+45
    elif fcn<6150:	
        ful=830+0.1*(fcn-6000);fdl=ful+45
    elif fcn<6450:	
        ful=832+0.1*(fcn-6150);fdl=ful+-41
    elif fcn<6600:	
        ful=1447.9+0.1*(fcn-6450);fdl=ful+48
    elif fcn<7400:	
        ful=3410+0.1*(fcn-6600);fdl=ful+100
    elif fcn<7700:	
        ful=2000+0.1*(fcn-7500);fdl=ful+180
    elif fcn<8040:	
        ful=1626.5+0.1*(fcn-7700);fdl=ful+-101.5
    elif fcn<8690:	
        ful=1850+0.1*(fcn-8040);fdl=ful+80
    elif fcn<9040:	
        ful=814+0.1*(fcn-8690);fdl=ful+45
    elif fcn<9210:	
        ful=807+0.1*(fcn-9040);fdl=ful+45
    elif fcn<9660:	
        ful=703+0.1*(fcn-9210);fdl=ful+55
    else:
        ful=0;fdl=0
    return ful,fdl

def deal_with_file(xmltext,csvList):
    csvList_=[]
    #tree = ET.parse(fname)
    #root = tree.getroot()
    root=ET.fromstring(xmltext)
    for t in list(root):
        if t.tag=='{configData.xsd}configData':
            #configData=root.find('{configData.xsd}configData')
            configData=t
            SubNetwork=configData.find('{genericNrm.xsd}SubNetwork')
            MeContext=SubNetwork.find('{genericNrm.xsd}MeContext')
            ManagedElement=MeContext.find('{genericNrm.xsd}ManagedElement')
            VsDataContainer=[i for i in list(ManagedElement) if i.tag=='{genericNrm.xsd}VsDataContainer' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataType').text=='vsDataENodeBFunction' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataFormatVersion').text=='EricssonSpecificAttributes'][0]
            attributes=VsDataContainer.find('{genericNrm.xsd}attributes')
            vsDataENodeBFunction=attributes.find('{EricssonSpecificAttributes.xsd}vsDataENodeBFunction')
            eNBId=vsDataENodeBFunction.find('{EricssonSpecificAttributes.xsd}eNBId').text
            logger.info(eNBId)
            for VsDataContainer in [i for i in list(VsDataContainer) if i.tag=='{genericNrm.xsd}VsDataContainer' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataType').text=='vsDataEUtranCellFDD' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataFormatVersion').text=='EricssonSpecificAttributes']:
                cellname=VsDataContainer.attrib['id']
                attributes=VsDataContainer.find('{genericNrm.xsd}attributes')
                vsDataEUtranCellFDD=attributes.find('{EricssonSpecificAttributes.xsd}vsDataEUtranCellFDD')
                cellId=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}cellId').text
                mncList=[i.find('{EricssonSpecificAttributes.xsd}mnc').text for i in list(vsDataEUtranCellFDD) if i.tag=='{EricssonSpecificAttributes.xsd}additionalPlmnList']
                share='是' if '11' in mncList else '否' #是否共享是/否
                sdate=''    #分析时间文本格式为：2022-04-12 15:00:00
                isalive='1'    #是否在网统一写1
                islock='0' if vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}administrativeState').text=='1' else '1'    #是否闭锁闭锁写1，不闭锁写0
                vendor='爱立信'    #厂家华为/中兴/诺基亚/爱立信
                eci='127.'+eNBId+'.'+cellId    #对象编号文本格式：127.131271.18
                #cellname=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}eUtranCellFDDId').text    #小区名不含（,|）
                logger.info(eNBId)
                eNodebId=eNBId    #基站号十进制整数
                lcrid=cellId    #小区号十进制整数
                isp='联通'    #承建运营商电信/联通
                #share=''    #是否共享是/否
                earfcn=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}earfcndl').text    #频点(下行)十进制整数
                ful,fdl=fcn2feq(int(earfcn))
                ful=str(ful)    #上行中心频率(MHz)十进制小数
                fdl=str(fdl)    #下行中心频率(MHz)十进制小数（例如951.6）
                pci=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}physicalLayerCellId').text    #物理小区id十进制整数
                tac=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}tac').text    #tac十进制整数
                Bandwidth=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}dlChannelBandwidth').text[:-3]    #载波宽度(MHz)1.4/3/5/10/15/20
                Pa=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}pa').text if vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}pa')!=None else ''    #Pa十进制小数 0-OMC值*0.01
                Pb=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}pb').text if vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}pb')!=None else ''    #Pb
                rspwr=''    #参考信号功率（dBm）十进制小数
                configuredMaxTxPower=''  #扇区总功率configuredMaxTxPower，单位毫瓦
                csvList_.append([isalive,islock,vendor,eci,cellname,eNodebId,lcrid,isp,share,earfcn,ful,fdl,pci,tac,Bandwidth,Pa,Pb,rspwr])
            for VsDataContainer in [i for i in list(VsDataContainer) if i.tag=='{genericNrm.xsd}VsDataContainer' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataType').text=='vsDataSectorCarrier' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataFormatVersion').text=='EricssonSpecificAttributes']:
                vsDataSectorCarrier=i.find('{genericNrm.xsd}attributes').find('{EricssonSpecificAttributes.xsd}vsDataSectorCarrier')
                print(VsDataContainer)
        elif t.tag=='{configData.xsd}fileFooter':
            sdate=t.attrib['dateTime']
            sdate=sdate[0:10]+' '+sdate[11:19]
    for i in csvList_:
        csvList.append([sdate,*i])


def deal_with_tar(tarName,csvList):
    #with tarfile.open(tarName,'r') as tar:
    #    for gz in tar.getmembers()[:]:
    #        logger.info(gz.name)
            #deal_with_file(gzip.decompress(tar.extractfile(gz).read()),out)
    #        deal_with_file(tar.extractfile(gz).read(),csvList)
            #open(gz.name,'w').write(tar.extractfile(gz).read().decode())
    #deal_with_file(open('CM_4GNSA_BDBG_DongMaYingErZhan2100MERR-share_A20220628010000.xml').read(),csvList)
    deal_with_file(open('CM_4GNSA_CZ_QX_liuquetun2100M5ERR-share_A20220714010000.xml').read(),csvList)

if __name__ == '__main__':
    os.chdir(sys.path[0])
    csvList=[['sdate','isalive','islock','vendor','eci','cellname','eNodebId','lcrid','isp','share','earfcn','ful','fdl','pci','tac','Bandwidth','Pa','Pb','rspwr']]
    tarName=glob.glob(inpath)[-1]
    logger.info(tarName)
    deal_with_tar(tarName,csvList)
    csvName=outpath+'EricssonLteCm_%s.csv'%(datetime.datetime.now().strftime("%Y%m%d_%H"))
    if os.path.isfile(csvName):os.remove(csvName)
    with open(csvName+'.temp','w') as f:
        f.write('\n'.join([','.join(i) for i in csvList]))
    os.rename(csvName+'.temp',csvName)

