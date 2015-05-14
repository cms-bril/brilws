from sqlalchemy import *
import pandas as pd
if __name__=='__main__':    
    resultcolumns = ['id','minrun','maxrun']
    infilename = '../data/tableshards.csv'
    fromfile = pd.read_csv(infilename,index_col=False)
    fromfile.columns = resultcolumns
    print fromfile.to_string(index=False)
    destdbstr = 'sqlite:///test.db'
    destengine = create_engine(destdbstr)
    fromfile.to_sql('TABLESHARDS',destengine,if_exists='replace')
    fromfile = pd.read_csv(infilename,index_col=False)
