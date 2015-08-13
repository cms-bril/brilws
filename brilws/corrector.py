import ast
import numpy as np
from numpy.polynomial import Polynomial as P

def apply_poly_str(ivalue,coefsstr,isreverse=False):
    '''
    apply polynomial function on ivalue.
    where the input coef is in string format. space separated float array or a single float 
    '''
    coefs = np.fromstring(coefsstr, dtype=np.float, sep=',')
    return apply_poly(ivalue,coefs=coefs,isreverse=isreverse)

def apply_poly(ivalue,coefs=[0,1],isreverse=False):
    '''
    apply polynomial function to input value or np array
    '''
    f = P(coefs)
    result = f(ivalue)
    if isreverse:
        return 1./result
    return result

def apply_afterglow_str(ivalue,nbxs,afterglowthresholdsstr):
    '''
    apply afterglow correction on ivalue
    where the input afterglow thresholds is in string format. space separated pair array
    '''
    afterglowthresholds = np.array(ast.literal_eval(afterglowthresholdsstr))
    return apply_afterglow(ivalue,nbxs,afterglowthresholds)
    
def apply_afterglow(ivalue,nbxs,afterglowthresholds):
    afterglow = 1.0
    for (bxthreshold,correction) in afterglowthresholds:
        if nbxs >= bxthreshold:
            afterglow = correction
    return ivalue*afterglow

if __name__=='__main__':
    x = apply_poly(np.array([1.,2.,35.]),coefs=[0.,2.],isreverse=False)
    print x
    y = apply_poly(np.array([1.,2.,35.]),coefs=[0.,2.],isreverse=True)
    print y
    xstr = apply_poly_str(np.array([1.,2.,35.]),"0.,2.",isreverse=False)
    print xstr
    xstr = apply_poly_str(2.5,"0.,2.5",isreverse=False)
    print xstr
    astr = '(213,0.992),(321,0.99),(423,0.988)'
    print apply_afterglow_str(1.,300,astr)
    
        
