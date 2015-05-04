import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
import collections
import numpy as np
from collections import Counter
from datetime import datetime
from brilws import api
import time
import array
import re
from prettytable import PrettyTable

if __name__=='__main__':
    dburl = 'sqlite:///test.db'
    dbengine = create_engine(dburl)
    pd.options.display.float_format = '{0:.2f}'.format
    
    for idchunk in api.datatagIter(dbengine,0,runmin=193091,runmax=193091,chunksize=10):
        dataids = idchunk.index
        for beaminfochunk in api.beamInfoIter(dbengine,dataids.min(),dataids.max(),chunksize=4000,withBX=False):
            finalchunk = idchunk.join(beaminfochunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)            
            print finalchunk
            #x = PrettyTable(finalchunk.columns)
            #for row in finalchunk.iterrows():
            #    x.add_row(row)
            #    x.align = 'r'
            #print(x)
            del beaminfochunk
        del idchunk

