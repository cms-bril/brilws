import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
import collections
import numpy as np
from collections import Counter

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    q = """select tag.TAGID as tagid,r.RUNNUM,tag.lumidataid as lumidataid ,tag.trgdataid as trgdataid,tag.hltdataid as hltdataid from CMS_LUMI_PROD.CMSRUNSUMMARY r, CMS_LUMI_PROD.TAGRUNS tag where r.RUNNUM=tag.RUNNUM and tag.TAGID<=13 and lumidataid!=0 and trgdataid!=0 and hltdataid!=0"""
    result = pd.read_sql_query(q,engine)
    
    # dups = [item for item,count in Counter(result['runnum']).iteritems() if count>1]
    #print result.loc[result['runnum'].isin(dups)]
    r=result.groupby('runnum',group_keys=False).apply(lambda x: x.ix[x.tagid.idxmax()])
    print 'checking'
    print r.loc[r['runnum'].isin([205620,205526])]
    print 'to csv'
    r.to_csv('run1tags.csv')
