import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
from brilws import api

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('/home/zhen/authentication.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    outfilename = 'trghltmap.csv'
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    trghltseedmap = api.TrgHltSeedMap()
    hltconfigid = 1905
    data = trghltseedmap.from_hltdb(engine,hltconfigid)
    print data
    trghltseedmap.to_brildb(destengine,data)
    #trgmapdf = trgmap.from_trgdb(engine)
    #trgmap.to_brildb(destengine,trgmapdf)
    #trgmap.to_csv(outfilename,trgmapdf,chunksize=100)

    
