import pandas as pd
import numpy as np

_floatformatter='{:,.3f}'.format

def listdf(df,npp=30, columns=None, formatters=None, index=False):
    '''
    Inputs:
        df:         dataframe
        npp:        number of records per page (default=50)
        formatters: field formatter (default=None)
        index:      display row id (default=False)
    '''
    nrows, ncols = df.shape
    total_pages = nrows/npp + 1 
    rec_last_pg = nrows % npp # number of records in last page
    ptr = 0
    for i in xrange(total_pages):
        fromrow = ptr
        if i==(total_pages-1):
           torow = fromrow + rec_last_pg -1
        else:
           torow = fromrow + npp -1
        ptr = torow +1
        print df.ix[fromrow:torow,:].to_string(columns=columns,formatters=formatters,index=index)
        if i!=(total_pages-1):
            try: 
               raw_input("Press a key to continue or '^C' to break...")
            except KeyboardInterrupt:
               break

if __name__=='__main__':
    chunksize = 200
    #rowdef = np.dtype([('tagid',np.uint64),('tagname',object),('value',np.float32)])
    rowdef = np.dtype([('tagid','u8'),('tagname','O'),('value','f4')]) 
    mydf = np.empty( (chunksize,), dtype=rowdef )
    myformatter = {'value':_floatformatter}
    for i in xrange(chunksize):
        mydf['tagname'][i]='ab'
        mydf['tagid'][i]=i
        mydf['value'][i]=0.5*i
    df = pd.DataFrame.from_records(mydf,index=None,columns=rowdef.names)
    print rowdef.names
    listdf(df,npp=30,formatters=myformatter,columns=['tagid','tagname'])




       
