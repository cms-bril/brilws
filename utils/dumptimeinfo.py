import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
import collections
import numpy as np
from collections import Counter
from datetime import datetime

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
    print allrows
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)
        
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
    
    #querytimeinfo(connection,162713)
    transfertimeinfo(connection,destconnection,193091)
