"""    
Usage:
  briltag listiov [options]


Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to DB.
      -p AUTHPATH              Path to authentication.xml
      --name TAGNAME           IOV tag name filter
      --type DATASOURCE        Data source type. bcmf,plt,pxl,hfoc,hfet
      --applyto APPLYTO        Type of correction applied. lumi,bkg,daq
      --isdefault              Show only default tags [default: False]

"""

from docopt import docopt
from schema import Schema
from brilws import clicommonargs
import os

def validate(optdict,sources,applyto,ostyles):
    result = {}
    myvalidables = ['-c','-p','--name','--applyto','--type',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    result = schema.validate(argdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


