import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
import collections
import numpy as np
from collections import Counter
from datetime import datetime
from brilws import api
import time
import array
import re

dbtimefm = 'MM/DD/YY HH24:MI:SS.ff6'
pydatetimefm = '%m/%d/%y %H:%M:%S.%f'

def datatagList(connection,datatagnameid,schemaname=None,runmin=None,runmax=None,fillmin=None,fillmax=None,beamstatus=None,amodetag=None,targetegev=None):
    '''
    output:{(fillnum,runnum,lsnum):datatagid}
    select fillnum,runnum,lsnum,DATATAGID from <schemaname>.IDS_DATATAG [where ]
    '''
    q = '''select FILLNUM as fillnum, RUNNUM as runnum, LSNUM as lsnum, max(DATATAGID) as datatagid from IDS_DATATAG where DATATAGNAMEID<=:datatagnameid'''
    qCondition = ''
    qPieces = []
    binddict = {'datatagnameid':datatagnameid}
    if runmin:
        qPieces.append('RUNNUM>=:runmin')
        binddict['runmin'] = runmin
    if runmax:
        qPieces.append('RUNNUM<=:runmax')
        binddict['runmax'] = runmax
    if fillmin:
        qPieces.append('FILLNUM>=:fillmin')
        binddict['fillmin'] = fillmin
    if fillmax:
        qPieces.append('FILLNUM<=:fillmax')
        binddict['fillmax'] = fillmax
    if beamstatus:
        qPieces.append('BEAMSTATUS=:beamstatus')
        binddict['beamstatus'] = beamstatus
    if amodetag:
        qPieces.append('AMODETAG=:amodetag')
        binddict['amodetag'] = amodetag
    if targetegev:
        qPieces.append('TARGETEGEV=:targetegev')
        binddict['targetegev'] = targetegev
    if qPieces:
        qCondition = ' and '.join([qCondition]+qPieces)
    q = q + qCondition +' group by RUNNUM, LSNUM'
    out = []
    with connection.begin() as trans:
        result = connection.execute(q,binddict)
        for row in result:
            fillnum = row['fillnum']
            runnum = row['runnum']
            lsnum = row['lsnum']
            datatagid = row['datatagid']
            out.append([fillnum,runnum,lsnum,datatagid])
    return out

def runListByTime(connection,datatagnameid,schemaname=None,timestampsecmin=None,timestampsecmax=None):
    '''
    
    '''
    pass

def datatagidForTag(connection,datatagnameid):
    '''
    select max(DATATAGID) from IDS_DATATAG where RUNNUM=193091 and LSNUM=1 and DATATAGNAMEID<=0;
    '''
    pass

if __name__=='__main__':
    dburl = 'sqlite:///test.db'
    dbengine = create_engine(dburl)
    dbconnection = dbengine.connect()
    runlist = datatagList(dbconnection,0,runmin=193091,runmax=193091)
    print runlist
