"""    
Usage:
  briltag listdata [options]


Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to DB.
      -p AUTHPATH              Path to authentication.xml 
      -o OUTPUTFILE            Output csv file. Special file '-' for stdout.
      --name TAGNAME           Tag name filter
      --output-style OSTYLE    Screen output style. tab, html, csv [default: tab]
      --debug                  Debug mode
      --nowarning              Switch off warnings
"""

import os
from docopt import docopt
from schema import Schema
from brilws import clicommonargs

def validate(optdict,sources,applyto,ostyles):
    result = {}
    myvalidables = ['-c','-p','--name','--output-style',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


