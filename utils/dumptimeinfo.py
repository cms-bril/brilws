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

dbtimefm = 'MM/DD/YY HH24:MI:SS.ff6'
pydatetimefm = '%m/%d/%y %H:%M:%S.%f'

def transfertimeinfo(connection,destconnection,runnum):
    '''
    query timeinformation of a given run
    '''
    q = """select lhcfill,runnumber,lumisection,to_char(starttime,'%s') as starttime from CMS_RUNTIME_LOGGER.LUMI_SECTIONS where runnumber=:runnum"""%(dbtimefm)
    i = """insert into TIMEINDEX(FILLNUM,RUNNUM,LSNUM,TIMESTAMPSEC,TIMESTAMPMSEC,TIMESTAMPSTR,WEEKOFYEAR,DAYOFYEAR,DAYOFWEEK,YEAR,MONTH) values(:fillnum, :runnum, :lsnum, :timestampsec, :timestampmsec, :timestampstr, :weekofyear, :dayofyear, :dayofweek, :year, :month)"""
    with connection.begin() as trans:
        result = connection.execute(q,{'runnum':runnum})        
        allrows = []
        for r in result:
            irow = {'fillnum':0,'runnum':runnum,'lsnum':0,'timestampsec':0,'timestampmsec':0,'timestampstr':'','weekofyear':0,'dayofyear':0,'dayofweek':0,'year':0,'month':0}
            irow['fillnum'] = r['lhcfill']
            irow['lsnum'] = r['lumisection']
            starttimestr = r['starttime']
            irow['timestampstr'] = starttimestr
            #stoptimestr = r['stoptime']
            starttime = datetime.strptime(starttimestr,pydatetimefm)
            irow['timestasec'] = time.mktime(starttime.timetuple())
            irow['timestamsec'] = starttime.microsecond/1e3
            irow['weekofyear'] = starttime.date().isocalendar()[1]
            irow['dayofyear'] = starttime.timetuple().tm_yday
            irow['dayofweek'] = starttime.date().isoweekday()
            irow['year'] = starttime.date().year
            irow['month'] = starttime.date().month
            allrows.append(irow)
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

import collections

        
def transfer_ids_datatag(connection,destconnection,runnum,lumidataid):
    '''
    '''
    qrunsummary = """select r.fillnum as fillnum,r.egev as targetegev, r.amodetag as amodetag, s.lumilsnum as lsnum,s.beamstatus as beamstatus from CMS_LUMI_PROD.CMSRUNSUMMARY r, CMS_LUMI_PROD.lumisummaryv2 s where r.runnum=s.runnum and r.runnum=:runnum and s.data_id=:lumidataid"""
    datatagnameid = 0    
    allrows = []
    datatagnameid = 0
    allkeys = []
    with connection.begin() as trans:
        result = connection.execute(qrunsummary,{'runnum':runnum,'lumidataid':lumidataid})
        for r in result:
            #irow = {'datatagnameid':datatagnameid, 'datatagid':next(api.nonsequential_key(7)), 'fillnum':0,'runnum':runnum,'lsnum':0,'targetegev':0,'beamstatus':'','amodetag':''}
            k = next(api.nonsequential_key(3))
            allkeys.append(k)
            #irow = {'datatagnameid':datatagnameid, 'datatagid':0, 'fillnum':0,'runnum':runnum,'lsnum':0,'targetegev':0,'beamstatus':'','amodetag':''}
            #irow['fillnum'] = r['fillnum']
            #irow['lsnum'] = r['lsnum']
            #irow['datatagid'] = irow['lsnum']
            #irow['targetegev'] = r['targetegev']
            #irow['beamstatus'] = r['beamstatus']
            #irow['amodetag'] = r['amodetag']
            #allrows.append(irow)
            #print datatagnameid,runnum,irow['lsnum']
    #print allrows
    print allkeys
    print [x for x, y in collections.Counter(allkeys).items() if y > 1]
    #i = """insert into IDS_DATATAG(DATATAGNAMEID,DATATAGID,FILLNUM,RUNNUM,LSNUM,TARGETEGEV,BEAMSTATUS,AMODETAG) values(:datatagnameid, :datatagid, :fillnum, :runnum, :lsnum, :targetegev, :beamstatus, :amodetag)"""
        
    #with destconnection.begin() as trans:
    #    r = destconnection.execute(i,allrows)
        
if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    connection = engine.connect().execution_options(stream_results=True)
    desturl = 'sqlite:///test.db'
    destengine = create_engine(desturl)
    destconnection = destengine.connect()
    
    #transfertimeinfo(connection,destconnection,193091)
    transfer_ids_datatag(connection,destconnection,193091,1649)
