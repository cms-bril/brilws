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
from prettytable import PrettyTable

#dbtimefm = 'MM/DD/YY HH24:MI:SS.ff6'
#pydatetimefm = '%m/%d/%y %H:%M:%S.%f'

def datatagIter(engine,datatagnameid,schemaname=None,runmin=None,runmax=None,fillmin=None,fillmax=None,beamstatus=None,amodetag=None,targetegev=None,chunksize=9999):
    '''
    output: iterator
    select fillnum,runnum,lsnum,DATATAGID from <schemaname>.IDS_DATATAG [where ]
    '''
    q = '''select FILLNUM as fillnum, RUNNUM as runnum, LSNUM as lsnum, BEAMSTATUS as beamstatus, AMODETAG as amodetag, TARGETEGEV as targetegev, max(DATATAGID) as datatagid from IDS_DATATAG where DATATAGNAMEID<=:datatagnameid'''
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
    result = pd.read_sql_query(q,engine,chunksize=chunksize,params=binddict,index_col='datatagid')   
    return result
 
def beamInfoIter(engine,datatagidmin,datatagidmax,schemaname=None,tablename=None,chunksize=9999,withBX=False):
    '''
    output: 
    query: select egev,intensity1,intensity2 from BEAM_RUN1 where DATATAGID>=:datatagidmin and DATATAGID<=:datatagidmax 

    withbxquery: select b.DATATAGID as datatagid, b.EGEV as egev, b.INTENSITY1 as intensity1, b.INTENSITY2 as intensity2, bx.BXIDX as bxidx, bx.BXINTENSITY1 as bxintensity1, bx.BXINTENSITY2 as bxintensity2, bx.ISCOLLIDING as iscolliding from BEAM_RUN1 b, BX_BEAM_RUN1 bx where b.DATATAGID=bx.DATATAGID and b.DATATAGID>=:datatagidmin and b.DATATAGID<=:datatagidmax 

    '''    
    q = '''select DATATAGID as datatagid, EGEV as egev, INTENSITY1 as intensity1, INTENSITY2 as intensity2 from BEAM_RUN1 where DATATAGID>=:datatagidmin and DATATAGID<=:datatagidmax'''
    if withBX:
        q = '''select b.DATATAGID as datatagid, b.EGEV as egev, b.INTENSITY1 as intensity1, b.INTENSITY2 as intensity2, bx.BXIDX as bxidx, bx.BXINTENSITY1 as bxintensity1, bx.BXINTENSITY2 as bxintensity2, bx.ISCOLLIDING as iscolliding from BEAM_RUN1 b, BX_BEAM_RUN1 bx where b.DATATAGID=bx.DATATAGID and b.DATATAGID>=:datatagidmin and b.DATATAGID<=:datatagidmax'''
        
    result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'datatagidmin':datatagidmin,'datatagidmax':datatagidmax},index_col='datatagid')
    return result


if __name__=='__main__':
    dburl = 'sqlite:///test.db'
    dbengine = create_engine(dburl)
    pd.options.display.float_format = '{0:.2f}'.format
    for idchunk in datatagIter(dbengine,0,runmin=193091,runmax=193091,chunksize=10):
        dataids = idchunk.index
        for beaminfochunk in beamInfoIter(dbengine,dataids.min(),dataids.max(),chunksize=4000,withBX=False):
            finalchunk = idchunk.join(beaminfochunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)            
            print finalchunk
            #x = PrettyTable(finalchunk.columns)
            #for row in finalchunk.iterrows():
            #    x.add_row(row)
            #    x.align = 'r'
            #print(x)
            del beaminfochunk
        del idchunk

