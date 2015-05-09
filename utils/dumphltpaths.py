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
    
    '''
    outfilename = 'trgbits.csv'
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    trgmap = api.TrgBitMap()
    trgmapdf = trgmap.from_sourcedb(engine)
    #trgmap.to_brildb(destengine,trgmapdf)
    #print trgmapdf
    trgmap.to_csv(outfilename,trgmapdf)
    '''

    outfilename = 'hltpaths.csv'
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    hltmap = api.HLTPathMap()
    hltmapdf = hltmap.from_sourcedb(engine)
    hltmap.to_brildb(destengine,hltmapdf)
    #print hltmapdf
    #hltmap.to_csv(outfilename,hltmapdf)
    
    
