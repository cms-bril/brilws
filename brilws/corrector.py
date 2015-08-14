import ast
import numpy as np


class FunctionFactory(object):

    
    def hcaldrift(self,ivalue,rawavglumi,intglumi,nbx,a0=1.0,a1=0.0,a2=0.0,drift=0.0,c1=0.0,afterglowthresholds=''):
        '''
        pain . later maybe
        
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
        
    def poly1d(self,ivalue,icoefs=[0.]):
        coefs = icoefs
        if isinstance(icoefs,str):
            coefs = np.fromstring(coefsstr, dtype=np.float, sep=',')
        f = np.poly1d(coefs)
        return f(ivalue)
    
    def poly1d_tostring(self,icoefs=[1.,0.]):
        coefs = icoefs
        if isinstance(icoefs,str):
            coefs = np.fromstring(coefsstr, dtype=np.float, sep=',')
        f = np.poly1d(coefs)
        return 'poly1d: %s'%f
    
    def inversepoly1d(self,ivalue,icoefs=[1.,0.]):        
        return 1./self.poly1d(ivalue,icoefs)

    def inversepoly1d_tostring(self,icoefs=[1.,0.]):
        return 'inversepoly1d: 1/%s'%(self.poly1d_tostring(icoefs=icoefs))
    
    def afterglow(self,ivalue,ncollidingbx,iafterglowthresholds=[(1,1.)]):
        afterglowthresholds = iafterglowthresholds
        afterglow = 1.
        if isinstance(iafterglowthresholds,str):
            afterglowthresholds = np.array(ast.literal_eval(iafterglowthresholds))
        for (bxthreshold,c) in afterglowthresholds:
            if ncollidingbx >= bxthreshold: afterglow = c
        return ivalue*afterglow
    
    def afterglow_tostring(self,iafterglowthresholds=[(1,1.)]):
        return 'afterglow: %s'%(iafterglowthresholds)
    
    def chooser(self,ivalue):        
        return ivalue

    def chooser_tostring(self,ivalue):
        return 'chooser: %s'%(ivalue)
        
def FunctionCaller(funcName,*args,**kwds):
    fac = FunctionFactory()
    try:
        myfunc = getattr(fac,funcName,None)
    except AttributeError:
        print '[ERROR] unknown correction function '+funcName
        raise
    if callable(myfunc):
        return myfunc(*args,**kwds)
    else:
        raise ValueError('uncallable function '+funcName)

    
if __name__=='__main__':
    ivalue = np.array([1.,2.,35.])
    result = FunctionCaller('poly1d',ivalue,icoefs=np.array([2.,0.]))
    print result
    
    result = FunctionCaller('inversepoly1d',ivalue,icoefs=np.array([2.,0.]))
    print result

    result = FunctionCaller('chooser','aa')
    print result

    result = FunctionCaller('afterglow',ivalue,3,iafterglowthresholds=[(1,2),(2,5)])
    print result
 
    result = FunctionCaller('afterglow',ivalue,3,iafterglowthresholds='(1,2),(2,5)')
    print result

    print FunctionCaller('afterglow_tostring',iafterglowthresholds='(1,2),(2,5)')

    print FunctionCaller('poly1d_tostring',icoefs=np.array([2.,0.]))

    ##accumulative function sequence
    fs = [ ['poly1d',[ivalue],{'icoefs':np.array([2.,0.])}], ['afterglow',[ivalue,3], {'iafterglowthresholds':'(1,2),(2,5)'}] ]

    result = None
    for f in fs:
        f_name = f[0]        
        f_args = f[1]
        if result is not None:
            f_args[0] = result
        f_kwds = f[2]
        result = FunctionCaller(f_name,*f_args,**f_kwds)
        print 'applying function: %s'%(FunctionCaller(f_name+'_tostring',**f_kwds))
    print 'final result ',result
    
