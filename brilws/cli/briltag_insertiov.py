"""    
Usage:
  briltag insert [options]


Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to DB.
      -p AUTHPATH              Path to authentication.xml file [default: .]
      -i INPUTFILE             Input yaml file
      --setdefault TAGNAME     Set tag as default of datasource, applyto 
      --unsetdefault TAGNAME   Unset tag as default of datasource, applyto
      --debug                  Debug mode
      --nowarning              Switch off warnings
"""

from docopt import docopt
from schema import Schema, And, Or, Use
import os

def validate(optdict):
    result = {}
    s = Schema({
      '-c': And(str,error='-c CONNECT is required'),
      '-p': And(os.path.exists, error='AUTHPATH should exist'),
      '-i': Or(None, And(os.path.exists, error='INPUTFILE should exist')), 
      '--setdefault': Or(None,str),
      '--unsetdefault': Or(None,str),
      str:object # catch all
    })
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


