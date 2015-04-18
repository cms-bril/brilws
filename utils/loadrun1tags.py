import sys,logging
import pandas as pd
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser

def queryrunsummary(engine,runnum):
    q="""select runnum,hltkey,fillnum,starttime,l1key,amodetag,egev,fillscheme,ncollidingbunches from CMS_LUMI_PROD.CMSRUNSUMMARY where runnum=:runnum"""
    result=pd.read_sql_query(q,engine,params={'runnum':runnum})
    return result

def querytrgdata(engine,tagid):
    q=""""""
if __name__=='__main__':    
    infilename = 'run1tags.csv'
    fromfile = pd.read_csv(infilename,index_col=False)
    #print fromfile
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    runsummary=pd.DataFrame(columns=['runnum','hltkey','fillnum','starttime','l1key','amodetag','egev','fillscheme','ncollidingbunches'])
    for runnum in fromfile['runnum']:
        r = queryrunsummary(engine,runnum)
        r.columns=['runnum','hltkey','fillnum','starttime','l1key','amodetag','egev','fillscheme','ncollidingbunches']
        runsummary=pd.concat([runsummary,r])
    #print runsummary
    runsummary.to_csv('cmsrunsummary.csv')
    
    
    
