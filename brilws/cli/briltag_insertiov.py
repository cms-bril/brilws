"""    
Usage:
  briltag insertiov [options]


Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to DB.
      -p AUTHPATH              Path to authentication.xml
      -i INPUTFILE             Input yaml file
      --name TAGNAME           IOV tag name
      --applyto APPLYTO        Type of correction applied. lumi,bkg,daq
      --type DATASOURCETYPE    Data source type. bcmf,plt,pxl,hfoc,hfet        
      --isdefault              Tag is default of datasource, applyto 
      --comments COMMENTS      Comments on the tag
      --debug                  Debug mode
      --nowarning              Switch off warnings
"""

from docopt import docopt
from schema import Schema
from brilws import clicommonargs
import os

def validate(optdict):
    result = {}
    myvalidables = ['-c','-p','--name','--applyto','--type',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result = schema.validate(argdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


