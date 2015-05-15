"""    
Usage:
  briltag insertdata [options]

Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to lumiDB [default: frontier://LumiCalc/CMS_LUMI_PROD]
      -p AUTHPATH              Path to authentication.xml 
      --name TAGNAME           Name of the data tag
      --debug                  Debug mode
      --nowarning              Switch off warnings
"""

import os
from docopt import docopt
from schema import Schema
from brilws import clicommonargs

def validate(optdict):
    result = {}
    myvalidables = ['-c','-p','--name',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


