import tables as tb
import numpy as np
#import multiprocessing as mp
import pandas as pd
import os,sys
import logging

from time import time

log = logging.getLogger('brilws')

def _is_element_tuple(me):
    '''
    Check if element of me is tuple 
    Input: collection
    '''
    return np.all([isinstance(x,tuple) for x in me])

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
    Iterator generating (tablehandle,data coordinate in files) based on range selection parameters
    Input:
        filehandles: input file handles
        datatable: data table name without '/'
        range selection parameters: runmin,runmax,fillmin,tssecmin,tssecmax,fillmax. None means select all        
    Output: 
       (tablehandle,data coordinate)
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
    

def dataFilter(tablehandle,coordinates,field_dtypes,runlsRangeSelectSeries=None,runlsnbPointFilterSeries=None):
    '''
    Filter input table nodes on fields based on runls range selection and runlsnb point selection conditions
    Input:  
            tablehandle: table handle
            coordinates: row coordinates
            fields: output fields dtype
            runlsRangeSelectSeries: run,ls range selection matrix. Note: if None, select all
            runlsnbPointSelectSeries: [run,(ls,nb)]. Note: if None, select all
    Output: 
            numpy record array of type field_dtypes
    Note: this filter does not produce None. Empty result means reject all
    '''   
    myrows = tablehandle.read_coordinates(coordinates)
    thisrun = np.array(myrows['runnum'])[0]
    lsnums = np.array(myrows['lsnum'])
    masks = np.full(lsnums.shape, True, dtype=bool) #default to select all

    if runlsRangeSelectSeries is not None:
        selectedruns = runlsRangeSelectSeries.index
        if thisrun not in selectedruns: #run is not selected
            masks = np.full(lsnums.shape, False, dtype=bool) #mask out all ls
        else: 
            thisselectedls = runlsselect[thisrun].values
            masks = np.logical_or.reduce([np.logical_and(lsnums>=xmin, lsnums<=xmax) for [xmin,xmax] in thisselectedls]) #find masks   

    if runlsnbPointFilterSeries is not None:
        if thisrun not in runlsnbPointFilterSeries.index: #run is not selected
            masks = np.full(lsnums[masks].shape, False, dtype=bool) #mask out all the rest ls                
        else:
            lsnbselect = runlsnbPointFilterSeries[thisrun].values.tolist() #this run values : [(lsnum,nbnum)] 
            mylsnbs = [tuple(x) for x in np.column_stack((myrows['lsnum'][masks],myrows['nbnum'][masks]))]
            for i,lsnb in enumerate(mylsnbs):
                if lsnb not in lsnbselect:
                    masks[i] = False

    all_fields = []
    for fieldname in field_dtypes.names:
        value = myrows[fieldname][masks]
        all_fields.append(value)
    nrows = masks[np.nonzero(masks)].size
    log.debug('selected rows %d'%nrows)
    result = np.core.records.fromarrays(all_fields,names=field_dtypes.names,dtype=field_dtypes)
    return result

def _make_runlsnb_Series(runlsnbarray):
    '''
    Make pandas Series from [[run,ls,nb]] structure
    Input: 2-D structure [[run,ls,nb]]
    Output: pandas Series [(ls,nb), index=runs]
    '''
    runs = runlsnbarray[:,:1]
    lsnb = [tuple(x) for x in runlsnbarray[:,1:3]]
    return pd.Series(lsnb,index=runs.T[0])

if __name__=='__main__':
    tables = ['beam','tcds','hfetlumi']
    filenames = ['/home/zhen/data/7491/7491_327554_1812020507_1812020558.hd5','/home/zhen/data/7491/7491_327559_1812020558_1812020731.hd5','/home/zhen/data/7491/7491_327560_1812020731_1812021237.hd5']
    filehandles = open_validfiles(filenames,tables)
    print [x.filename for x in filehandles]

    tcdsIter = dataRangeIter(filehandles,'tcds',fillmin=7491,fillmax=7491)
    
    #runlsselect = pd.Series([[1340,1345],[1400,1500]], index=[327554,327554]) #pd.Series , index=runnum, value=[[lsmin,lsmax]]
    runlsselect = pd.Series([[1,10],[14,15]], index=[327560,327560]) #pd.Series , index=runnum, value=[[lsmin,lsmax]]
    #runlsselect = None

    tcds_result_dtype = np.dtype( [('runnum','uint32'),('lsnum','uint32'),('nbnum','uint32'),('timestampsec','uint32'),('cmson','bool8'),('deadfrac','float32'),('ncollidingbx','uint32')] )
    tcds_result = None      
    then = time()
    for (tablehandle,coordinates ) in tcdsIter:         
        tcds_result = dataFilter(tablehandle,coordinates,field_dtypes=tcds_result_dtype,runlsRangeSelectSeries=runlsselect)
    print 'Time processing tcds: ',time()-then
    tcdsrunlsnbSeries = None
    if tcds_result.size :
      selected_time = np.column_stack((tcds_result['runnum'],tcds_result['lsnum'],tcds_result['nbnum']))       
      tcdsrunlsnbSeries = _make_runlsnb_Series( selected_time )
      #selected_time = np.vstack(set(tuple(row) for row in selected_time))
  
    if tcdsrunlsnbSeries is None:
        print 'no tcds selected'
        sys.exit(0)

    print 'tcds result ',tcds_result.shape
    dataIter = dataRangeIter(filehandles,'hfetlumi',fillmin=7491,fillmax=7491)
    hfetlumi_result_dtype = np.dtype( [('runnum','uint32'),('lsnum','uint32'),('nbnum','uint32'),('timestampsec','uint32'),('avg','float32'),('avgraw','float32'),('bx','float32',(3564,)),('bxraw','float32',(3564,))] ) 
    hfetlumi_result = None
    then = time()
    for (tablehandle,coordinates) in dataIter: 
        hfetlumi_result = dataFilter(tablehandle,coordinates,field_dtypes=hfetlumi_result_dtype,runlsRangeSelectSeries=None,runlsnbPointFilterSeries=tcdsrunlsnbSeries)
        #print hfetlumi_result

    print 'hfetlumi result ',hfetlumi_result.shape
    print 'Time processing hfetlumi: ',time()-then
    
    #beam_result_dtype = np.dtype( [('lsnum','uint32'),('timestampsec','uint32'),('status','bool8')] )
    #beam_result = None   
    [f.close() for f in filehandles]
