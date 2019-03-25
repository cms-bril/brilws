import tables as tb
import numpy as np
#import multiprocessing as mp
#import pandas as pd
import os
import logging

from time import time

log = logging.getLogger('brilws')

def _is_subset(me,target):
    '''
    Check me collection is a subset of the target collection
    '''
    return np.in1d(me,target).all()

def _open_validfile(filename, tables):
    '''
    Check if the file has all the required table    
    Input: f: filename, tables: required table names
    Output: filehandle if input is valid, None if input is invalid 
    '''    
    f = tb.open_file(filename)
    nodes = f.root._v_children.keys()
    if not len(nodes):
        f.close()
        return None
    if not _is_subset(tables,nodes):
        f.close()
        return None
    tablelist = map(lambda x: f.get_node('/'+x),tables) #get selected tables by name
    filledtables = filter(lambda x: x.nrows>0, tablelist)  #filledtables
    if len(tablelist) != len(filledtables):
        return None
    return f

def open_validfiles(filenames,requiredtables):
    '''
    Input: [filename]
    Output: [filehandle]
    '''    
    return filter(lambda x: x is not None,[_open_validfile(f,requiredtables) for f in filenames])
    
def dataRangeIter(filehandles,datatable,runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None):
    '''
    Generate data coordinate
    '''
    conditions = []
    if runmin and runmax and runmin==runmax:
        conditions.append('(runnum == %d)'%runmin)   
    elif runmin: 
        conditions.append('(runnum >= %d)'%runmin)
    elif runmax: 
        conditions.append('(runnum <= %d)'%runmax)
    if fillmin and fillmax and fillmin==fillmax:
        conditions.append('(fillnum == %d)'%fillmin)
    elif fillmin:
        conditions.append('(fillnum >= %d)'%fillmin)
    elif fillmax: 
        conditions.append('(fillnum <= %d)'%fillmax)
    if tssecmin and tssecmax and tssecmin==tssecmax:
        conditions.append('(tssec == %d)'%tssecmin)
    elif tssecmin: 
        conditions.append('(tssec >= %d)'%tssecmin)
    elif tssecmax: 
        conditions.append('(tssec <= %d)'%tssecmax)
    conditionStr = '&'.join(conditions)       

    for f in filehandles:
        log.debug('Processing '+f.filename)
        n = f.get_node('/'+datatable)        
        coordinates = n.get_where_list(conditionStr,sort=False)
        if len(coordinates)==0:
            continue
        yield (n,coordinates) 

def dataFilter(tablehandle,coordinates,field_dtypes,runlsselectSeries=None):
    '''
    Filter input table nodes on fields, runls select, bx select conditions
    Input:  
            tablehandle: table handle
            coordinates: row coordinates
            fields: output fields dtype
            runlsselect Series: runls selection matrix
    Output: 
            numpy record array 
    '''
    result = None
    myrows = tablehandle.read_coordinates(coordinates)
    lsnums = np.array(myrows['lsnum'])
    if runlsselectSeries is not None:
        selectedruns = runlsselectSeries.index.unique()
        thisrun = np.array(myrows['runnum'])[0]
        if thisrun not in selectedruns: #run is not selected
            return result
        thisselectedls = runlsselect[thisrun].values
        masks = np.logical_or.reduce([np.logical_and(lsnums>=xmin, lsnums<=xmax) for [xmin,xmax] in thisselectedls]) #find masks
    else:
        masks = np.full(lsnums.shape, True, dtype=bool) #default to select all
    nrows = masks[np.nonzero(masks)].size
    log.debug('selected rows %d'%nrows)
    all_fields = []
    for fieldname in field_dtypes.names:
        value = myrows[fieldname][masks]
        all_fields.append(value)
    mytype = np.dtype(field_dtypes)
    result = np.core.records.fromarrays(all_fields,names=field_dtypes.names,dtype=mytype)
    return result

if __name__=='__main__':
    tables = ['beam','hfetlumi']
    filenames = ['/home/zhen/data/7491/7491_327554_1812020507_1812020558.hd5','/home/zhen/data/7491/7491_327559_1812020558_1812020731.hd5','/home/zhen/data/7491/7491_327560_1812020731_1812021237.hd5']
    filehandles = open_validfiles(filenames,tables)
    #print [x.filename for x in filehandles]
    dataIter = dataRangeIter(filehandles,'hfetlumi',fillmin=7491,fillmax=7491)
    then = time()
    #runlsselect = pd.Series([[1340,1345],[1400,1500]], index=[327554,327554]) #pd.Series , index=runnum, value=[[lsmin,lsmax]]
    runlsselect = None
    result_dtype = np.dtype( [('lsnum','uint32'),('timestampsec','uint32'),('avg','float32'),('avgraw','float32'),('bx','float32',(3564,)),('bxraw','float32',(3564,))] ) 
    for (tablehandle,coordinates ) in dataIter: 
        print coordinates
        result = dataFilter(tablehandle,coordinates,field_dtypes=result_dtype,runlsselectSeries=runlsselect)
        print result.shape
        #print result[0]['lsnum'],result[0]['bx']
    print time()-then
    [f.close() for f in filehandles]
