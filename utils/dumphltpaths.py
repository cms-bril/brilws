import sys,csv
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser

# select PATHS.PATHID, STRINGPARAMVALUES.VALUE from CMS_HLT.PATHS, CMS_HLT.STRINGPARAMVALUES, CMS_HLT.PARAMETERS, CMS_HLT.SUPERIDPARAMETERASSOC, CMS_HLT.MODULES, CMS_HLT.MODULETEMPLATES, CMS_HLT.PATHMODULEASSOC, CMS_HLT.CONFIGURATIONPATHASSOC, CMS_HLT.CONFIGURATIONS where  PARAMETERS.PARAMID=STRINGPARAMVALUES.PARAMID and SUPERIDPARAMETERASSOC.PARAMID=PARAMETERS.PARAMID and MODULES.SUPERID=SUPERIDPARAMETERASSOC.SUPERID and MODULETEMPLATES.SUPERID=MODULES.TEMPLATEID and PATHMODULEASSOC.MODULEID=MODULES.SUPERID and PATHS.PATHID=PATHMODULEASSOC.PATHID and CONFIGURATIONPATHASSOC.PATHID=PATHS.PATHID and CONFIGURATIONS.CONFIGID=CONFIGURATIONPATHASSOC.CONFIGID and MODULETEMPLATES.NAME ='HLTLevel1GTSeed' and PARAMETERS.NAME='L1SeedsLogicalExpression' and CONFIGURATIONS.PROCESSNAME='HLT' and PATHS.ISENDPATH=0 and CONFIGURATIONS.CONFIGID=:configid and CONFIGURATIONS.CONFIGDESCRIPTOR=:hltkey

#hltseed='HLTLevel1GTSeed'
#l1seedexpr='L1SeedsLogicalExpression'
#hltkey =:hltkey

#select PATHID,NAME from CMS_HLT.PATHS where ISENDPATH=0 and NAME like "HLT%" order by PATHID;
#result [[hltpathid,hltpathname]]

if __name__=='__main__':
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    connection = engine.connect()
    q = """select PATHID as HLTPATHID, NAME as PATHNAME from CMS_HLT.PATHS where ISENDPATH=0 and NAME LIKE 'HLT%' order by PATHID"""
    resultcolumns = ['HLTPATHID','HLTPATHNAME']
    import pandas as pd
    result = pd.read_sql_query(q,engine)
    result.columns = resultcolumns
    outfilename = 'hltpaths.csv'
    result.to_csv(outfilename,header=True,index=False)
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    result.to_sql('HLTPATHMAP',destengine,if_exists='replace',index=False)
