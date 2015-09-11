import numpy as np
from brilws import api
import pandas as pd

def expandrange(element):
    '''
    expand [x,y] to range[x,y+1]
    output: np array
    '''
    return np.arange(element[0],element[1]+1)

def consecutive(npdata, stepsize=1):
    '''
    split input array into chunks of consecutive numbers
    np.diff(a,n=1,axis=-1)
    Calculate the n-th order discrete difference along given axis.
    output: list of ndarrays
    '''
    return np.split(npdata, np.where(np.diff(npdata) != stepsize )[0]+1)

def mergerangeseries(x,y):
    '''
    merge two range type series
    x [[x1min,x1max],[x2min,x2max],...]
    y [[y1min,y1max],[y2min,y2max],...]
    into
    z [[z1min,z1max],[z2min,z2max],...]
    '''
    a = pd.Series(x).apply(expandrange)
    ai = np.hstack(a.values)
    b = pd.Series(y).apply(expandrange)
    bi = np.hstack(b.values)
    i = np.intersect1d(np.unique(ai),np.unique(bi),assume_unique=True)
    scatter = consecutive(i)
    return scatter

def merge_two_dicts(x,y):
    z = x.copy()
    z.update(y)
    return z

def mergeiovrunls(iovselect,cmsselect):
    '''
    merge iovselect list and cms runls select dict
    input:
        iovselect: pd.Series from dict {run:[[]],}
        cmsselect:  [[iovtag,pd.Series],...]  pd.Series from dict {run:[[]],}
        
    '''
    cmsselect_runs = cmsselect.index
    final = []#[[iovtag,{}],[iovtag,{}]]
    previoustag = ''
    for [iovtag,iovtagrunls] in iovselect:
        iovtagruns = iovtagrunls.index
        runlsdict = {}       
        selectedruns = np.intersect1d(cmsselect_runs,iovtagruns)
        if selectedruns.size == 0: continue
        for runnum in selectedruns:
            scatter = mergerangeseries(iovtagrunls[runnum],cmsselect[runnum])
            for c in scatter:
                if len(c)==0: continue
                runlsdict.setdefault(runnum,[]).append([np.min(c),np.max(c)])                
        if iovtag!=previoustag:
            if runlsdict:
                final.append([iovtag,runlsdict])
                previoustag = iovtag
        else:
            x = final[-1][1]                
            y = runlsdict
            final[-1][1] = merge_two_dicts(x,y)
    return final

if __name__=='__main__':
    cmsselect="{237890:[[1,22],[35,48]],247259:[[1,15],[19,20],[21,21],[95,97]],267939:[[2,25]],287259:[[11,25]]}"
    iovtagselect='''[
    [a,{237890:[[1,12],[14,21],[27,98]]}],
    [a,{247259:[[12,46],[95,95]]}],
    [b,267938,267939],
    [a,{287259:[[12,46]]}],
    ]'''

    cmsselectresult = api.parsecmsselectJSON(cmsselect,numpy=True)
    iovresult = api.parseiovtagselectionJSON(iovtagselect)
    result = mergeiovrunls(iovresult,cmsselectresult)
    print 'result ',result

    cmsselectfile = '/home/zhen/cmsselect.json'
    iovtagselectfile = '/home/zhen/iovtagselect.json'
    cmsselectresult = api.parsecmsselectJSON(cmsselectfile,numpy=False)
    iovresult = api.parseiovtagselectionJSON(iovtagselectfile)
    result = mergeiovrunls(iovresult,cmsselectresult)
    print 'result ',result

    iovtagselect = 'hello'
    iovresult =  api.parseiovtagselectionJSON(iovtagselect)
    print 'result ',iovresult
