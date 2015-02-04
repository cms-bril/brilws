import sys,csv
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser

# select distinct ALGO_INDEX,ALIAS from CMS_GT.GT_RUN_ALGO_VIEW order by ALGO_INDEX
# select distinct TECHTRIG_INDEX, NAME from CMS_GT.GT_RUN_TECH_VIEW order by TECHTRIG_INDEX
#result: [[bitid,bitname,isalgo],[0, L1_TripleJet_68_48_32_VBF,0,isalgo],[127, L1_SingleMuBeamHalo,0]...]

if __name__=='__main__':
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    qAlgo = """select distinct ALGO_INDEX as BITID,ALIAS as BITNAME from CMS_GT.GT_RUN_ALGO_VIEW order by ALGO_INDEX"""
    qTech = """select distinct TECHTRIG_INDEX as BITID,NAME as BITNAME from CMS_GT.GT_RUN_TECH_VIEW order by TECHTRIG_INDEX"""
    resultcolumns = ['BITID','BITNAME','ISALGO']
    import pandas as pd
    dfalgo = pd.read_sql_query(qAlgo,engine)
    dfalgo['ISALGO'] = [1]*len(dfalgo)
    dftech = pd.read_sql_query(qTech,engine)
    dftech['ISALGO'] = [0]*len(dftech)
    result = dfalgo.append(dftech)
    result.index=range(len(result))
    result.columns = resultcolumns
    outfilename = 'trgbits.csv'
    result.to_csv(outfilename,header=True,index_label='BITNAMEID')
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    result.to_sql('TRGBITMAP',destengine,if_exists='replace',index_label='BITNAMEID')
    fromfile = pd.read_csv(outfilename,index_col=False)
    print fromfile.to_string(index=False)

