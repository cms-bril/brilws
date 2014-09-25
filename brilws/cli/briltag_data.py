"""    
Usage:
  briltag data [options]


Options:
      -h --help           Show this screen.
      -c CONNECT          Connect string to DB.
      -p AUTHPATH         Path to authentication.xml file [default: .]
      --name TAGNAME      Tag name 
      --default-only      Show only default tags [default: False]
"""

from docopt import docopt
from schema import Schema, And, Or, Use
import os

def validate(optdict):
    result = {}
    s = Schema({
      '-c': And(str,error='-c CONNECT must exist'),
      '-p': And(os.path.exists, error='AUTHPATH should exist'),
      '--name': Or(None,str),
      str:object # catch all
    })
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


