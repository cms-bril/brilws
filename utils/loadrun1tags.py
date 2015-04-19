import sys,logging
import pandas as pd
import numpy as np
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import array

def unpackblobstr(iblobstr,itemtypecode):
    if itemtypecode not in ['c','b','B','u','h','H','i','I','l','L','f','d']:
        raise RuntimeError('unsupported typecode '+itemtypecode)
    result=array.array(itemtypecode)
    #blobstr=iblob.readline()
    if not iblobstr :
        return None
    result.fromstring(iblobstr)
    return np.array(result)

def queryrunsummary(engine,runnum):
    '''
    output: df(hltkey,fillnum,starttime,l1key,amodetag,egev,fillscheme,ncollidingbunches), index=runnum
    '''
    q="""select runnum,hltkey,fillnum,starttime,l1key,amodetag,egev,fillscheme,ncollidingbunches from CMS_LUMI_PROD.CMSRUNSUMMARY where runnum=:runnum"""
    result=pd.read_sql_query(q,engine,params={'runnum':runnum})
    return result

def querytrgdata(engine,dataid):
    '''
    output: [cmslsnum,df(presc,trgcount)]
    '''
    q="""select runnum,cmslsnum,deadfrac,prescaleblob,trgcountblob from CMS_LUMI_PROD.LSTRG where data_id=:dataid"""
    result=pd.read_sql_query(q,engine,params={'dataid':dataid},index_col='cmslsnum')
    r = []
    for cmslsnum,row in result.iterrows():
        runnum = row['runnum']
        if runnum<150008:
            presc =  unpackblobstr(row['prescaleblob'],'l')
            trgcount = unpackblobstr(row['trgcountblob'],'l')
        else:
            presc =  unpackblobstr(row['prescaleblob'],'I')
            trgcount = unpackblobstr(row['trgcountblob'],'I')
        b = pd.DataFrame({'presc':presc,'trgcount':trgcount})
        r.append([cmslsnum,b])
    return r

def queryhltdata(engine,dataid):
    '''
    output: [cmslsnum,df(presc,hltcount,hltaccept)]
    '''
    q="""select runnum,cmslsnum,prescaleblob,hltcountblob,hltacceptblob from CMS_LUMI_PROD.LSHLT where data_id=:dataid"""
    result=pd.read_sql_query(q,engine,params={'dataid':dataid},index_col='cmslsnum')
    r = []
    for cmslsnum,row in result.iterrows():
       runnum = row['runnum']
       if runnum<150008:
           presc = unpackblobstr(row['prescaleblob'],'l')
           hltcount = unpackblobstr(row['hltcountblob'],'l')
           hltaccept = unpackblobstr(row['hltacceptblob'],'l')
       else:
           presc = unpackblobstr(row['prescaleblob'],'I')
           hltcount = unpackblobstr(row['hltcountblob'],'I')
           hltaccept = unpackblobstr(row['hltacceptblob'],'I')

       b = pd.DataFrame({'presc':presc,'hltcount':hltcount,'hltaccept':hltaccept})
       r.append([cmslsnum,b]) 
    return r

def querybeamdata(engine,dataid):
    '''
    output: [lumilsnum,cmslsnum,beamstatus,beamenergy,df(bxidx,beam1intensity,beam2intensity)]
    '''
    q="""select lumilsnum,cmslsnum,beamstatus,beamenergy,cmsbxindexblob,beamintensityblob_1,beamintensityblob_2 from CMS_LUMI_PROD.lumisummaryv2 where data_id=:dataid"""
    result=pd.read_sql_query(q,engine,params={'dataid':dataid},index_col='lumilsnum')
    r = []
    for lumilsnum,row in result.iterrows():
        bxindex=unpackblobstr(row['cmsbxindexblob'],'h')
        beam1intensity=unpackblobstr(row['beamintensityblob_1'],'f')
        beam2intensity=unpackblobstr(row['beamintensityblob_2'],'f')
        b = pd.DataFrame({'bxidx':bxindex,'beam1intensity':beam1intensity,'beam2intensity':beam2intensity})
        r.append([lumilsnum,row['cmslsnum'],row['beamstatus'],row['beamenergy'],b])
    return r

def querylumidata(engine,dataid):
    '''
    output: [lumilsnum,instlumi,df(bxidx,bxlumi)]
    '''
    q="""select lumilsnum,instlumi,bxlumivalue_occ1 from CMS_LUMI_PROD.lumisummaryv2 where data_id=:dataid"""    
    result=pd.read_sql_query(q,engine,params={'dataid':dataid},index_col='lumilsnum')
    r = []
    for lumilsnum,row in result.iterrows():
        instlumi = row['instlumi']
        bxlumi = unpackblobstr(row['bxlumivalue_occ1'],'f')
        bxidx = np.nonzero(bxlumi)[0]
        bxlumi = bxlumi[np.nonzero(bxlumi)]
        b = pd.DataFrame({'bxidx':bxidx,'bxlumi':bxlumi})
        r.append([lumilsnum,instlumi,b])
    return r

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
    #runsummary=pd.DataFrame(columns=['runnum','hltkey','fillnum','starttime','l1key','amodetag','egev','fillscheme','ncollidingbunches'])
    #for runnum in fromfile['runnum']:
    #    r = queryrunsummary(engine,runnum)
    #    r.columns=['runnum','hltkey','fillnum','starttime','l1key','amodetag','egev','fillscheme','ncollidingbunches']
    #    runsummary=pd.concat([runsummary,r])
    #print runsummary
    #runsummary.to_csv('cmsrunsummary.csv')    

    #beamdata = querybeamdata(engine,2651)
    #print beamdata
    
    #lumidata = querylumidata(engine,2651)
    #print lumidata
    
    trgdata = querytrgdata(engine,2423)
    print trgdata
    
    #hltdata = queryhltdata(engine,2229)
    #print hltdata

    
