import pandas as pd
from sqlalchemy import *
from sqlalchemy import exc
if __name__=='__main__':    
    resultcolumns = ['BEAMSTATUSID','BEAMSTATUS']
    infilename = '../data/datatables.csv'
    fromfile = pd.read_csv(infilename,index_col=False)
    fromfile.columns = resultcolumns
    print fromfile.to_string(index=False)
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    fromfile.to_sql('BEAMSTATUSMAP',destengine,if_exists='replace')
    fromfile = pd.read_csv(infilename,index_col=False)

