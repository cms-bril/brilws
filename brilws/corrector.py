from functools import wraps

import ast
import copy
import inspect
import collections

from numpy.polynomial import polynomial as P

import numpy as np


class FunctionRoot(object):
    def __init__(self,*args):
        self.root = []
        for a in args:
            self.root.append(a)


class CorrectionFunction(object):
    def __init__(self,fname,fparams):
        self.fname = fname
        self.fparams = fparams
    def name(self):
        return self.fname
    def params(self):
        return self.fparams


def collect_sanitizers(cls):
    cls.required_params = {}
    cls.validators = {}
    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        if hasattr(method, '_required_params') and hasattr(method, '_validators'):
            cls.required_params[name] = method._required_params
            cls.validators[name] = method._validators
    return cls

def argument_sanitizer(required_params, validators=None):
    def decorator(func):
        func._required_params = required_params
        func._validators = validators if validators else []
        @wraps(func)
        def wrapper(instance, arg1, kwds):
            cls = instance.__class__
            cls.sanitize_arguments(kwds, func.__name__)
            return func(instance, arg1, kwds)
        return wrapper
    return decorator

def calculate_lumisection_sbil(ivalue, ncollidingbx):
    with np.errstate(divide='ignore', invalid='ignore'):
        ivalue = np.true_divide(ivalue, ncollidingbx)
        ivalue = np.nan_to_num(ivalue, nan=0.0, posinf=0.0, neginf=0.0)
    return ivalue

def applyCorrectionStr(funcstrs , paramstrs, funcroot ):
        funcs_list = [ funcstrs.strip() ]
        params_list = [ paramstrs.strip() ]
        if funcstrs[0]=='[':
            funcs_list = ast.literal_eval(funcstrs)
        if paramstrs[0]=='[':
            params_list = ast.literal_eval(paramstrs)
        funcs = []       
        for f,p in zip(funcs_list,params_list):
            paramdict = p
            if isinstance(p,str):
                paramdict = ast.literal_eval(paramdict)
            funcs.append( CorrectionFunction(f, paramdict) )
        return applyCorrection( funcs, funcroot )                        
        
def applyCorrection(funcs, funcroot):
        '''
        input:
            funcroot:  FunctionRoot object (bxoravglumi,avglumi,ncollidingbx)
            funcs:  list of CorrectionFunction objects
        output: 
            (totlumi,bxlumi|None)
        '''
        fr = copy.deepcopy(funcroot)
        toproot = fr.root[0]
        withBX = True
        totlumi = fr.root[1]
        bxlumi = None
        if not isinstance(toproot,collections.Iterable) :
            withBX = False
        else:
            bxlumi = toproot

        for f in funcs:
            totlumi = FunctionCaller( f, FunctionRoot(totlumi,totlumi,fr.root[2]) )
            fr.root[0] = totlumi
            fr.root[1] = totlumi
            if withBX:
                bxlumi = FunctionCaller( f, FunctionRoot(bxlumi,totlumi,fr.root[2]) )
                fr.root[0] = bxlumi
        return (totlumi,bxlumi)

@collect_sanitizers
class FunctionFactory(object):
    @staticmethod
    def validate_required_arguments(params, func_name):
        #TODO: Make this functions return a string option. Makes it easier to report all errors. If that is something desirable. May not be.
        if not hasattr(FunctionFactory, func_name):
            raise ValueError(f"Function '{func_name}' does not exist in class '{FunctionFactory.__name__}'.")
        if func_name not in FunctionFactory.required_params:
            raise ValueError(f"Function '{func_name}' cannot be used for corrections.")

        # At this stage params will only be None if The iovtag
        # provided no arguments under 'payload' This may be intended,
        # if the func_name actually does not receive arguments.
        # Otherwise it will be an improper iovtag.
        if params is None:
            params = {}

        param_desc_list = FunctionFactory.required_params[func_name]
        missing_params = []
        for param, desc in param_desc_list:
            if param not in params:
                missing_params.append(f"- {param}: {desc}")
        if missing_params:
            err_str = (
                f"The configuration for '{func_name}' is missing required arguments:\n"
                + "\n".join(missing_params)
            )
            raise ValueError(err_str)

        validators_list = FunctionFactory.validators[func_name]
        validator_errs = []
        for validator, err_msg in validators_list:
            if not validator(params):
                validator_errs.append(f"- {err_msg}")
        if validator_errs:
            err_str = (
                f"The configuration for '{func_name}' has invalid parameters:\n"
                + "\n".join(validator_errs)
            )
            raise ValueError(err_str)

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

    @argument_sanitizer(
        required_params=(
            ("coefs", "Comma-separated polynomial coefficients in decreasing order."),
        )
    )
    def poly1d(self,functionroot,kwds):
        '''
        root0: avglumi or bxlumi        
        '''
        ivalue = functionroot.root[0]
        ncollidingbx =  functionroot.root[2]
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

    @argument_sanitizer(
        required_params=(
            ("sigvis", "Luminometer visible cross-section."),
            ("eff", "Efficiency factor to be applied to 'sigvis'."),
            ("slope", "Slope extracted from linear fit to sbil_1 / sbil_2 = a * sbil_2 + b."),
            ("frev", "LHC's revolution frequency."),
        )
    )
    def slopeApproximation(self, functionroot, kwds):
        ivalue = functionroot.root[0]
        ncollidingbx =  functionroot.root[2]

        if isinstance(ivalue, collections.Iterable):
            lumi = (kwds["frev"] * ivalue) / (kwds["sigvis"] * kwds["eff"])
            return lumi - kwds["slope"] * lumi**2
        else:
            ivalue = calculate_lumisection_sbil(ivalue, ncollidingbx)
            lumi = (kwds["frev"] * ivalue) / (kwds["sigvis"] * kwds["eff"])
            return ncollidingbx * (lumi - kwds["slope"] * lumi**2)

    @argument_sanitizer(
        required_params=(
            ("sigvis", "Luminometer visible cross-section."),
            ("eff", "Efficiency factor to be applied to 'sigvis'."),
            ("slope", "Slope extracted from linear fit to sbil_1 / sbil_2 = a * sbil_2 + b."),
            ("intercept", "Intercept extracted from linear fit to sbil_1 / sbil_2 = a * sbil_2 + b."),
            ("frev", "LHC's revolution frequency."),
        ),
        validators=(
            (lambda kwds: not np.isclose(kwds["slope"], 0.0), "'slope' parameter can't be 0."),
        )
    )
    def preciseCorrection(self, functionroot, kwds):
        ivalue = functionroot.root[0]
        ncollidingbx =  functionroot.root[2]

        alpha = kwds["slope"] / kwds["intercept"] ** 2
        if isinstance(ivalue, collections.Iterable):
            lumi = (kwds["frev"] * ivalue) / (kwds["sigvis"] * kwds["eff"])
            return (np.sqrt(4 * alpha * lumi + 1) - 1) / (2*alpha)
        else:
            ivalue = calculate_lumisection_sbil(ivalue, ncollidingbx)
            lumi = (kwds["frev"] * ivalue) / (kwds["sigvis"] * kwds["eff"])
            return ncollidingbx * (np.sqrt(4 * alpha * lumi + 1) - 1) / (2*alpha)

    @argument_sanitizer(
        required_params= (
            ("coefs", "Literal python list containing the c_{i,j} coefs of the 2D polynomial"),
        )
    )
    def poly2dlL(self,functionroot,kwds):
        '''
        poly2d of l=bxlumi, L=totallumi
        c00 + c10*x + c01*y (n=1)
        c00 + c10*x + c01*y + c20*x**2 + c02*y**2 + c11*x*y (n=2)
        if l is scalar, scale l to y/NBX, then the final result is NBX*singleresult
        otherwise, make l,L  the same shape
        '''        
        l = functionroot.root[0]  #per bunch lumi or totallumi
        L = functionroot.root[1] #total lumi
        ncollidingbx = functionroot.root[2] 
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

    @argument_sanitizer(
        required_params=(
            ("coefs", "Comma-separated polynomial coefficients in increasing order."),
        )
    )
    def inversepoly1d(self,functionroot,kwds):
        return 1./self.poly1d(functionroot,kwds)
    
    @argument_sanitizer(
        required_params=(
            ("afterglowthresholds", "Pair or comma-separated list of pairslike (number_bxs, afterglow_factor)."),
        )
    )
    def afterglow(self,functionroot,kwds):
        ivalue = functionroot.root[0]
        ncollidingbx = functionroot.root[2]
        afterglowthresholds = kwds['afterglowthresholds']
        afterglow = 1.

        if afterglowthresholds[-1]!=',':
            afterglowthresholds+=','
        afterglowthresholds = np.array(ast.literal_eval(afterglowthresholds))
        for (bxthreshold,c) in afterglowthresholds:
            if ncollidingbx >= bxthreshold:
                afterglow = c

        return ivalue*afterglow 

    @argument_sanitizer(
        required_params=(
            ("coefs", "Comma-separated polynomial coefficients in decreasing order."),
            ("afterglowthresholds", "Pair or comma-separated list of pairslike (number_bxs, afterglow_factor).")
        )
    )
    def poly1dWafterglow( self,functionroot,kwds ):
        poly1dresult = self.poly1d(functionroot,kwds)
        fr = FunctionRoot(poly1dresult,functionroot.root[1],functionroot.root[2])
        result = self.afterglow(fr,kwds)
        return result

    @argument_sanitizer(
        required_params=(
            ("coefs", "Literal python list containing the c_{i,j} coefs of the 2D polynomial"),
            ("afterglowthresholds", "Pair or comma-separated list of pairslike (number_bxs, afterglow_factor).")
        )
    )
    def afterglowWpoly2dlL(self,functionroot,kwds):
        '''
        afterglow*poly2dlL
        '''
        l = functionroot.root[0]
        L = functionroot.root[1]
        afterglowl = self.afterglow( FunctionRoot(l,functionroot.root[1],functionroot.root[2]), kwds )
        afterglowL = self.afterglow( FunctionRoot(L,functionroot.root[1],functionroot.root[2]), kwds )
        fr = FunctionRoot(afterglowl,afterglowL,functionroot.root[2])
        result = self.poly2dlL(fr,kwds)
        return result

    @argument_sanitizer(
        required_params=(
            ("coefs", "Literal python list containing the c_{i,j} coefs of the 2D polynomial"),
            ("afterglowthresholds", "Pair or comma-separated list of pairslike (number_bxs, afterglow_factor).")
        )
    )
    def poly2dlLWafterglow(self,functionroot,kwds):
        '''
        poly2dlL*afterglow
        '''
        polyresult = self.poly2dlL(functionroot,kwds)
        fr = FunctionRoot(polyresult,functionroot.root[1],functionroot.root[2])
        result = self.afterglow(fr,kwds)
        return result


def FunctionCaller(func,functionroot):
    fac = FunctionFactory()
    try:
        myfunc = getattr(fac,func.fname,None)
    except AttributeError:
        print ('[ERROR] unknown correction function '+ func.fname)
        raise
    if callable(myfunc):
        return myfunc(functionroot, func.fparams)
    else:
        raise ValueError('uncallable function '+func.fname)
     
if __name__=='__main__':
    # test CorrectionFunction class
    f = CorrectionFunction('poly1d',{'coefs':'2.,1.,0.'})
    print (f.name(), f.params())
    nbx = 3
    ivalue =  np.array([2.,2.,2.])
    funcroot = FunctionRoot( ivalue, None, nbx)
    
    funcs = [ CorrectionFunction('poly1d',{'coefs':'2.,1.,0.'}) ]
    result = applyCorrection(funcs,funcroot)
    print (result)

    ivalue = 6
    fr = FunctionRoot( ivalue,ivalue,nbx)
    funcs = [ CorrectionFunction('poly1d',{'coefs':'2.,1.,0.'}) ]
    result =  applyCorrection(funcs,fr)
    print ('result  total ',result)

    funcs =  [ CorrectionFunction('afterglow',{'afterglowthresholds':'(1,2),'}) ]
    result = applyCorrection(funcs,fr)
    print ('afterglow result ',result)

    fr =  FunctionRoot([1,2,3],2,nbx)
    funcs =  [ CorrectionFunction('poly2dlL',{'coefs':'[[3,2,1],[-10,4,0],[2,0,0]]'}) ]
    result = applyCorrection(funcs,fr)
    print ('poly2dlL result ',result)

    fr = FunctionRoot(3,3,nbx)
    funcs =  [ CorrectionFunction('poly2dlL',{'coefs':'[[3,2,1],[-10,4,0],[2,0,0]]'}) ]
    result = applyCorrection(funcs,fr)
    print ('poly2dlL result ',result)
    
    result = applyCorrectionStr('poly2dlL' , "{'coefs':'[[3,2,1],[-10,4,0],[2,0,0]]'}", fr)
    print ('poly2dlL result from string ',result)

    result = applyCorrectionStr("['poly2dlL','afterglow']" , "[{'coefs':'[[3,2,1],[-10,4,0],[2,0,0]]'},{'afterglowthresholds':'(1,2),'}]", fr)
    print ('poly2dlL result from string ',result)
