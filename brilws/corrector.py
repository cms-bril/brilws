import ast
import numpy as np
import collections
from numpy.polynomial import polynomial as P

class FunctionRoot(object):
    def __init__(self,*args):
        self.root = []
        for a in args:
            self.root.append(a)
        
class FunctionFactory(object):

    def hcaldrift(self,ivalue,rawavglumi,intglumi,nbx,a0=1.0,a1=0.0,a2=0.0,drift=0.0,c1=0.0,afterglowthresholds=''):
        '''
        this is a pain . just dump here. make it usable again only if later maybe used.        
        '''
        avglumi = 0.
        if c1 and nbx>0:
            avglumi = c1*rawavglumi/nbx
        driftterm = 1.0
        if drift and intglumi: driftterm=1.0+drift*intglumi
        resultfactor = a0 * driftterm * self.inversepoly1d(avglumi,np.array([a2,a1,1])) 
        result = ivalue*resultfactor
        if afterglowthresholds:
            result = self.afterglow(result,nbx,afterglowthresholds)
        return result
    
    def poly1d(self,functionroot,ncollidingbx,**kwds):
        '''
        root0: avglumi or bxlumi        
        '''
        ivalue = functionroot.root[0]
        coefsStr = kwds['coefs']
        coefs = np.fromstring(coefsStr, dtype=np.float, sep=',')
        if not isinstance(ivalue,collections.Iterable) : #is totallumi, in this case, need to use term totallumi/nbx
            with np.errstate(divide='ignore',invalid='ignore'):
                ivalue = np.true_divide(ivalue,ncollidingbx)
                if ivalue == np.inf:
                    ivalue = 0
                ivalue = np.nan_to_num(ivalue)
        if len(coefs)>1:
            coefs = coefs[::-1] #reverse the order because polyval coefs order is reverse of np.poly1d
        if isinstance(ivalue,collections.Iterable) :
            return P.polyval(ivalue,coefs)
        else:
            return ncollidingbx*P.polyval(ivalue,coefs)
                    
    def poly2dlL(self,functionroot,ncollidingbx,**kwds):
        '''
        poly2d of l=bxlumi, L=totallumi
        c00 + c10*x + c01*y (n=1)
        c00 + c10*x + c01*y + c20*x**2 + c02*y**2 + c11*x*y (n=2)
        if l is scalar, scale l to y/NBX, then the final result is NBX*singleresult
        otherwise, make l,L  the same shape
        '''
        l = functionroot.root[0]  #per bunch lumi or totallumi
        L = functionroot.root[1] #total lumi
        totlumi = 0
        bxlumi = 0

        if isinstance(l,collections.Iterable):
            bxlumi = np.array(l)
            totlumi = np.full_like(bxlumi, L)
        else:
            if l!=L:
                raise ValueError('l and L are of different value ')
            with np.errstate(divide='ignore',invalid='ignore'):
                bxlumi = np.true_divide(L,ncollidingbx)
                if bxlumi == np.inf:
                    bxlumi = 0
                bxlumi = np.nan_to_num(bxlumi)                    
            totlumi = L

        coefsStr = kwds['coefs']
        coefs = np.array(ast.literal_eval(coefsStr), dtype=np.float)
        if isinstance(l,collections.Iterable):
            return P.polyval2d(bxlumi,totlumi,coefs)
        else:
            return ncollidingbx*P.polyval2d(bxlumi,totlumi,coefs)

    def inversepoly1d(self,functionroot,ncollidingbx,**kwds):
        return 1./self.poly1d(functionroot,ncollidingbx,**kwds)
    
    def afterglow(self,functionroot,ncollidingbx,**kwds):
        ivalue = functionroot.root[0]
        afterglowthresholds = kwds['afterglowthresholds']
        afterglow = 1.

        if afterglowthresholds[-1]!=',':
            afterglowthresholds+=','
        afterglowthresholds = np.array(ast.literal_eval(afterglowthresholds))
        for (bxthreshold,c) in afterglowthresholds:
            if ncollidingbx >= bxthreshold:
                afterglow = c
        return ivalue*afterglow
    
    #def afterglow_tostring(self,functionroot,ncollidingbx,**kwds):
    #    return 'afterglow: %s'%(kwds['afterglowthresholds'])

    def poly1dWafterglow(self,functionroot,ncollidingbx,**kwds):
        '''
        poly1d*afterglow
        '''
        poly1dresult = self.poly1d(functionroot,ncollidingbx**kwds)
        fr = FunctionRoot(polu1dresult)
        result = self.afterglow(fr,ncollidingbx,**kwds)
        return result
 
    def inversepoly1dWafterglow(self,functionroot,ncollidingbx,**kwds):
        '''
        poly1d*afterglow
        '''
        inverseresult = self.inversepoly1d(functionroot,ncollidingbx,**kwds)
        fr = FunctionRoot(inverseresult)
        result = self.afterglow(fr,ncollidingbx,**kwds)
        return result

    def afterglowWpoly2dlL(self,functionroot,ncollidingbx,**kwds):
        '''
        afterglow*poly2dlL
        '''
        l = functionroot.root[0]
        L = functionroot.root[1]
        afterglowl = self.afterglow(FunctionRoot(l),ncollidingbx,**kwds)
        afterglowL = self.afterglow(FunctionRoot(L),ncollidingbx,**kwds)
        fr = FunctionRoot(afterglowl,afterglowL)
        result = self.poly2dlL(fr,ncollidingbx,**kwds)
        return result
    
    def poly2dlLWafterglow(self,functionroot,ncollidingbx,**kwds):
        '''
        poly2dlL*afterglow
        '''
        polyresult = self.poly2dlL(functionroot,ncollidingbx,**kwds)
        fr = FunctionRoot(polyresult)
        result = self.afterglow(fr,ncollidingbx,**kwds)
        return result
    
def FunctionCaller(funcName,functionroot,ncollidingbx,**kwds):
    fac = FunctionFactory()
    try:
        myfunc = getattr(fac,funcName,None)
    except AttributeError:
        print '[ERROR] unknown correction function '+funcName
        raise
    if callable(myfunc):
        return myfunc(functionroot,ncollidingbx,**kwds)
    else:
        raise ValueError('uncallable function '+funcName)
     
if __name__=='__main__':
    ivalue = np.array([2.,2.,2.])
    nbx=3
    fr = FunctionRoot(ivalue)
    result = FunctionCaller('poly1d',fr,nbx,coefs='2.0,1.0,0.')
    print 'result perbunch ',result

    ivalue = 6
    fr = FunctionRoot(ivalue)
    result = FunctionCaller('poly1d',fr,nbx,coefs='2.0,1.0,0.')
    print 'result  total ',result
    
    result = FunctionCaller('afterglow',fr,nbx,afterglowthresholds='(1,2),(2,5),')
    print result
 
    result = FunctionCaller('afterglow',fr,nbx,afterglowthresholds='(1,2),')
    print result

    fr = FunctionRoot([1,2,3],2)
    print FunctionCaller('poly2dlL',fr,nbx,coefs='[[3,2,1],[-10,4,0],[2,0,0]]')

    fr = FunctionRoot(3,3)
    print FunctionCaller('poly2dlL',fr,nbx,coefs='[[3,2,1],[-10,4,0],[2,0,0]]')
