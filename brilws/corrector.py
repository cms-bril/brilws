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
    
    def poly1d(self,*args,**kwds):
        ivalue = args[0]                #avglumi or bxlumi
        ncollidingbx = args[1]
        coefsStr = kwds['coefs']
        bxcoefs = np.fromstring(coefsStr, dtype=np.float, sep=',')            
        if isinstance(ivalue,collections.Iterable) or len(bxcoefs)==1: #is bx lumi or only a const term 
            f = np.poly1d(bxcoefs)
            return f(ivalue)
            
        coefs = copy.deepcopy(bxcoefs)
        maxpower = len(bxcoefs)-1 
        for i,c in enumerate(bxcoefs):    #recalculate coefs for avg lumi
            currentpower = maxpower-i     #current term power=maxpower-i
            if currentpower>1:            #the only term having no need of change is power=1
                coefs[i] = np.divide(c,ncollidingbx**(currentpower-1))
            elif currentpower==0:
                coefs[-1] = ncollidingbx*c #const term is ncollidingbx*const
        f = np.poly1d(coefs)
        return f(ivalue)
    
    def poly1d_tostring(self,*args,**kwds):
        coefs = kwds['coefs']
        if isinstance(coefs,str):
            coefs = np.fromstring(coefs, dtype=np.float, sep=',')
        f = np.poly1d(coefs)
        return 'poly1d: %s'%f
    
    def inversepoly1d(self,*args,**kwds):
        return 1./self.poly1d(*args,**kwds)

    def inversepoly1d_tostring(self,*args,**kwds):
        return 'inversepoly1d: 1/%s'%(self.poly1d_tostring(*args,**kwds))
    
    def afterglow(self,*args,**kwds):
        ivalue = args[0]
        ncollidingbx = args[1]
        afterglowthresholds = kwds['afterglowthresholds']
        afterglow = 1.
        if isinstance(afterglowthresholds,str):
            if afterglowthresholds[-1]!=',': afterglowthresholds+=','
            afterglowthresholds = np.array(ast.literal_eval(afterglowthresholds))
        for (bxthreshold,c) in afterglowthresholds:
            if ncollidingbx >= bxthreshold:
                afterglow = c
        return ivalue*afterglow
    
    def afterglow_tostring(self,*args,**kwds):
        return 'afterglow: %s'%(kwds['afterglowthresholds'])

    def poly1dWafterglow(self,*args,**kwds):
        '''
        poly1d*afterglow
        '''
        newargs = [ self.poly1d(*args,**kwds) ]
        if len(args)>1: newargs = newargs+list(args[1:])
        result = self.afterglow(*tuple(newargs),**kwds)
        return result

    def poly1dWafterglow_tostring(self,*args,**kwds):
        return '('+poly1d_tostring(**kwds)+')*('+afterglow_tostring(**kwds)+')'

    def inversepoly1dWafterglow(self,*args,**kwds):
        '''
        poly1d*afterglow
        '''
        result = self.inversepoly1d(*args,**kwds)
        args[0] = result
        result = self.afterglow(*args,**kwds)
        return result

    def inversepoly1dWafterglow_tostring(self,*args,**kwds):
        return '('+inversepoly1d_tostring(**kwds)+')*('+afterglow_tostring(**kwds)+')'
    
    def chooser(self,*args,**kwds):        
        return args[0]

    def chooser_tostring(self,*args,**kwds):
        return 'chooser: %s'%(args[0])
        
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
    result = FunctionCaller('poly1d',ivalue,coefs=np.array([2.,0.]))
    print result
    print FunctionCaller('poly1d_tostring',coefs=np.array([2.,0.]))    
    result = FunctionCaller('inversepoly1d',ivalue,coefs=np.array([2.,0.]))
    print result
    

    result = FunctionCaller('chooser','aa')
    print result

    result = FunctionCaller('afterglow',ivalue,3,afterglowthresholds=[(1,2),(2,5)])
    print result
 
    result = FunctionCaller('afterglow',ivalue,3,afterglowthresholds='(1,2),')
    print result

    print FunctionCaller('afterglow_tostring',afterglowthresholds='(1,2),(2,5)')



    ##accumulative function sequence
    fs = [ ['poly1d',[ivalue],{'coefs':np.array([2.,0.])}], ['afterglow',[ivalue,3], {'afterglowthresholds':'(1,2),(2,5)'}] ]

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
    
