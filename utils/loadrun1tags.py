import sys,logging,base64
import pandas as pd
import numpy as np
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import array
import os

def unpackblobstr(iblobstr,itemtypecode):
    if itemtypecode not in ['c','b','B','u','h','H','i','I','l','L','f','d']:
        raise RuntimeError('unsupported typecode '+itemtypecode)
    result=array.array(itemtypecode)
    #blobstr=iblob.readline()
    if not iblobstr :
        return result
    result.fromstring(iblobstr)
    return result

def queryrunsummary(engine,runnum,dirname):
    '''
    file: runsummary_<run>.csv
    desttables: 
            runinfo(datatagid,hltkey,gt_rs_key)
            ids_datatag(datatagnameid,datatagid,fillnum,runnum,)
    '''
    q="""select runnum,hltkey,fillnum,starttime,l1key,amodetag,egev,fillscheme,ncollidingbunches from CMS_LUMI_PROD.CMSRUNSUMMARY where runnum=:runnum"""
    result=pd.read_sql_query(q,engine,params={'runnum':runnum})
    result.to_csv('%s/runsummary_%d.csv'%(dirname,runnum),index=False,float_format='%.3f')
    del result
    return 

def querytrgdata(engine,dataid,runnum,dirname):
    '''
    files: trgdeadfrac_<run>.csv, trgbit_<run>_<cmslsnum>.csv
    '''
    qdeadfrac = """select cmslsnum,deadfrac from CMS_LUMI_PROD.LSTRG where data_id=:dataid"""
    qbits = """select cmslsnum,prescaleblob,trgcountblob from CMS_LUMI_PROD.LSTRG where data_id=:dataid"""
    deadfracresult = pd.read_sql_query(qdeadfrac,engine,params={'dataid':dataid},index_col='cmslsnum')
    deadfracresult.to_csv('%s/trgdeadfrac_%d.csv'%(dirname,runnum),index=True,float_format='%.3f')
    del deadfracresult
    bitresult = pd.read_sql_query(qbits,engine,params={'dataid':dataid},index_col='cmslsnum')
    cols = ['prescale','trgcount']
    dt = {'prescale':'object','trgcount':'object'}
    for cmslsnum,row in bitresult.iterrows():
        if runnum<150008:
            try:
               prescblob = unpackblobstr(row['prescaleblob'],'l')
               trgcountblob = unpackblobstr(row['trgcountblob'],'l')
               if not prescblob or not trgcountblob:
                   continue
            except ValueError:
               prescblob = unpackblobstr(row['prescaleblob'],'I')
               trgcountblob = unpackblobstr(row['trgcountblob'],'I')
               if not prescblob or not trgcountblob:
                   continue
        else:
            prescblob = unpackblobstr(row['prescaleblob'],'I')
            trgcountblob = unpackblobstr(row['trgcountblob'],'I')
            if not prescblob or not trgcountblob:
               continue
        b = pd.DataFrame({'prescale':pd.Series(prescblob),'trgcount':pd.Series(trgcountblob)})
        for c in b.columns:
            b[c] = b[c].astype(dt[c])
        b.to_csv('%s/trgbit_%d_%d.csv'%(dirname,runnum,cmslsnum),index=True)
        del b
    del bitresult
    return 

def queryhltdata(engine,dataid,runnum,dirname):
    '''
    files: hltpath_<run>_<cmslsnum>.csv
    '''
    q="""select cmslsnum,prescaleblob,hltcountblob,hltacceptblob from CMS_LUMI_PROD.LSHLT where data_id=:dataid"""
    result=pd.read_sql_query(q,engine,params={'dataid':dataid},index_col='cmslsnum')
    cols = ['prescale','hltcount','hltaccept']
    dt = {'prescale':'object','hltcount':'object','hltaccept':'object'}   
    for cmslsnum,row in result.iterrows():       
       if runnum<150008:
           try:
               prescblob = unpackblobstr(row['prescaleblob'],'l')
               hltcountblob = unpackblobstr(row['hltcountblob'],'l')
               hltacceptblob = unpackblobstr(row['hltacceptblob'],'l')
               if not prescblob or not hltcountblob or not hltacceptblob:
                   continue
           except ValueError:
               prescblob = unpackblobstr(row['prescaleblob'],'I')
               hltcountblob = unpackblobstr(row['hltcountblob'],'I')
               hltacceptblob = unpackblobstr(row['hltacceptblob'],'I')
               if not prescblob or not hltcountblob or not hltacceptblob:
                   continue
       else:            
           prescblob = unpackblobstr(row['prescaleblob'],'I')
           hltcountblob = unpackblobstr(row['hltcountblob'],'I')
           hltacceptblob = unpackblobstr(row['hltacceptblob'],'I')
           if not prescblob or not hltcountblob or not hltacceptblob:
               continue
       b = pd.DataFrame({'prescale':pd.Series(prescblob),'hltcount':pd.Series(hltcountblob),'hltaccept':pd.Series(hltacceptblob)})
       for c in b.columns:
            b[c] = b[c].astype(dt[c])
       b.to_csv('%s/hltpath_%d_%d.csv'%(dirname,runnum,cmslsnum),index=True)
       del b
    del result
    return 

def querybeamdata(engine,dataid,runnum,dirname):
    '''    
    files: beam_<run>.csv, bxbeam_<run>_<lsnum>.csv
    '''
    qsummary="""select lumilsnum,beamstatus,beamenergy from CMS_LUMI_PROD.lumisummaryv2 where data_id=:dataid"""
    qbx="""select lumilsnum,cmsbxindexblob,beamintensityblob_1,beamintensityblob_2 from CMS_LUMI_PROD.lumisummaryv2 where data_id=:dataid"""
    summaryresult=pd.read_sql_query(qsummary,engine,params={'dataid':dataid},index_col='lumilsnum')
    bxresult=pd.read_sql_query(qbx,engine,params={'dataid':dataid},index_col='lumilsnum')
    summarycols = ['beamstatus','beamenergy']
    bxcols = ['bxidx','beam1intensity','beam2intensity']
    summarydt = {'beamstatus':'str','beamenergy':'float32'}
    bxdt = {'bxidx':'object','beam1intensity':'object','beam2intensity':'object'}

    summaryresult.to_csv('%s/beam_%d.csv'%(dirname,runnum),index=True,float_format='%.3f')
    del summaryresult
    for lumilsnum,row in bxresult.iterrows():
        bxindexblob = unpackblobstr(row['cmsbxindexblob'],'h')
        beam1intensityblob = unpackblobstr(row['beamintensityblob_1'],'f')
        beam2intensityblob = unpackblobstr(row['beamintensityblob_2'],'f')
        if not bxindexblob or not beam1intensityblob or not beam2intensityblob:
            continue
        bxindex = pd.Series(bxindexblob, dtype=np.dtype("object"))
        beam1intensity = pd.Series(beam1intensityblob, dtype=np.dtype("object"))
        beam2intensity = pd.Series(beam2intensityblob, dtype=np.dtype("object"))
        b = pd.DataFrame({'bxidx':bxindex,'beam1intensity':beam1intensity,'beam2intensity':beam2intensity},columns=bxcols)
        for c in b.columns:
            b[c] = b[c].astype(bxdt[c])
        b.to_csv('%s/bxbeam_%d_%d.csv'%(dirname,runnum,lumilsnum),index=False,float_format='%.3f')
        del b
    del bxresult
    return 

def querylumidata(engine,dataid,runnum,dirname):
    '''
     files: lumi_<run>.csv, bxlumi_<run>_<lsnum>.csv
    '''
    qlumi = """select lumilsnum,instlumi from CMS_LUMI_PROD.lumisummaryv2 where data_id=:dataid"""    
    qbxlumi = """select lumilsnum, bxlumivalue_occ1 from CMS_LUMI_PROD.lumisummaryv2 where data_id=:dataid"""    
    lumiresult = pd.read_sql_query(qlumi,engine,params={'dataid':dataid},index_col='lumilsnum')
    lumiresult.to_csv('%s/lumi_%d.csv'%(dirname,runnum),index=True,float_format='%.5f')
    del lumiresult
    
    bxlumiresult =  pd.read_sql_query(qbxlumi,engine,params={'dataid':dataid},index_col='lumilsnum')
    cols = ['bxidx','bxrawlumi']
    dt = {'bxidx':'uint16','bxrawlumi':'object'}
    for lumilsnum,row in bxlumiresult.iterrows():
        bxrawlumiblob = unpackblobstr(row['bxlumivalue_occ1'],'f')
        if not bxrawlumiblob: continue
        bxrawlumi = pd.Series(bxrawlumiblob, dtype=np.dtype("object"))        
        bxidx = np.nonzero(bxrawlumi)[0]
        bxrawlumi = bxrawlumi[bxidx]
        b = pd.DataFrame({'bxidx':bxidx,'bxrawlumi':bxrawlumi})
        for c in b.columns:
            b[c] = b[c].astype(dt[c])
        b.to_csv('%s/bxlumi_%d_%d.csv'%(dirname,runnum,lumilsnum),index=False,float_format='%.3f')
        del b
    del bxlumiresult
    
    return 

if __name__=='__main__':
    Usage="""Usage: python loadrun1tags.py <sourcedbconnect> [startindex] [endindex]"""
    if len(sys.argv)<2:
        print Usage
        exit(-1)
    infilename = 'run1tags.csv'
    fromfile = pd.read_csv(infilename,index_col=False)
    startindex = fromfile.index[0]
    endindex = fromfile.index[-1]

    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger('logrun1tags')    
    connectstr = sys.argv[1]
    if len(sys.argv)>2:
        startindex = int(sys.argv[2])
    if len(sys.argv)>3:
        endindex = int(sys.argv[3])
    log.info("process row range [%d,%d]"%(startindex,endindex))
    
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    pcode = base64.b64decode(passwd).decode('UTF-8')
    connecturl = connectstr[:idx]+':'+pcode+connectstr[idx:]
    engine = create_engine(connecturl)
    
    for index,row in fromfile.iterrows():
        if index<startindex or index>endindex: continue
        runnum = row['runnum']
        lumidataid = row['lumidataid']
        trgdataid = row['trgdataid']
        hltdataid = row['hltdataid']
        log.info("idx %d runnum %d lumidataid %d trgdataid %d hltdataid %d"%(index,runnum,lumidataid,trgdataid,hltdataid))
        dirname = 'run1data_%d_%d'%(index,runnum)
        log.info('creating dir %s'%dirname)
        if not os.path.exists(dirname): os.makedirs(dirname)
        queryrunsummary(engine,runnum,dirname)
        querybeamdata(engine,lumidataid,runnum,dirname)        
        querylumidata(engine,lumidataid,runnum,dirname)
        querytrgdata(engine,trgdataid,runnum,dirname)
        queryhltdata(engine,lumidataid,runnum,dirname)

