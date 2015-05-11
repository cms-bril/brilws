import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
#import pandas as pd
from brilws import api

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)                    

    outfilename = 'trghltseedmap.csv'
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    trghltseedmap = api.TrgHltSeedMap()
    trghltseedmapdf = trghltseedmap.from_sourcedb(engine)
    trghltseedmap.to_brildb(destengine,trghltseedmapdf)
    #print trghltseedmapdf
    #trghltseedmap.to_csv(outfilename,trghltseedmapdf)
    
    
