import sys,csv,base64
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
    
streamwhitelist = ["'A'"]
if __name__=='__main__':
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    pcode = base64.b64decode(passwd).decode('UTF-8')
    connecturl = connectstr[:idx]+':'+pcode+connectstr[idx:]
    engine = create_engine(connecturl)
    
    hltpathq = """select PATHID as HLTPATHID,NAME as HLTPATHNAME from CMS_HLT.PATHS where ISENDPATH=0 and NAME like 'HLT_%'"""
    hltpathresultcolumns = ['HLTPATHID','HLTPATHNAME']
    hltpathresult = pd.read_sql_query(hltpathq,engine)
    hltpathresult.columns = hltpathresultcolumns
    outfilename = 'hltpaths.csv'
    hltpathresult.to_csv(outfilename,header=True,index=False)

    datasetq = """select DATASETID as DATASETID, DATASETLABEL as DATASETNAME from CMS_HLT.PRIMARYDATASETS where DATASETLABEL!='Unassigned path'"""
    datasetresultcolumns = ['DATASETID','DATASETLABEL']
    datasetresult = pd.read_sql_query(datasetq,engine)
    datasetresult.columns = datasetresultcolumns
    outfilename = 'datasets.csv'
    datasetresult.to_csv(outfilename,header=True,index=False)
    
    selectedstreams = ','.join(streamwhitelist)
    linksq = """select p.PATHID as HLTPATHID,s.STREAMID as STREAMID,s.STREAMLABEL as STREAMLABEL,d.DATASETID as DATASETID from CMS_HLT.PATHSTREAMDATASETASSOC link,CMS_HLT.STREAMS s, CMS_HLT.PATHS p, CMS_HLT.PRIMARYDATASETS d where p.PATHID=link.PATHID and link.DATASETID=d.DATASETID and link.STREAMID=s.STREAMID and d.DATASETLABEL!='Unassigned path' and s.FRACTODISK>0 and s.STREAMLABEL in ({0}) and p.ISENDPATH=0 and p.NAME like 'HLT_%'""".format(selectedstreams)
    linkresultcolumns = ['HLTPATHID','STREAMID','STREAMLABEL','DATASETID']
    linkresult = pd.read_sql_query(linksq,engine)
    linkresult.columns = linkresultcolumns

    #by chunk
    for chunkresult in pd.read_sql_query(linksq,engine,chunksize=1000):
        chunkresult.columns = linkresultcolumns
        print chunkresult
        
    #outfilename = 'streamdatasetpaths.csv'
    #linkresult.to_csv(outfilename,header=True,index=False)
    #destdbstr = 'sqlite:///test.db'
    #destengine = create_engine(destdbstr)
    #result.to_sql('HLTPATHMAP',destengine,if_exists='replace',index=False)
