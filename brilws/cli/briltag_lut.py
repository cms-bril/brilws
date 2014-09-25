"""    
Usage:
  briltag lut [options]


Options:
      -h --help           Show this screen.
      -c CONNECT          Connect string to DB.
      -p AUTHPATH         Path to authentication.xml file [default: .]
      --name TAGNAME      Tag name 
      --source SOURCE     Data source (bhm|bcm1f|plt|hfoc|pixel)   
      --default-only      Show only default tags [default: False]
"""

from docopt import docopt
from schema import Schema, And, Or, Use
import os
def validate(optdict,sources):
    result = {}
    s = Schema({
      '-c': And(str,error='-c CONNECT must exist'),
      '-p': And(os.path.exists, error='AUTHPATH should exist'),
      '--name': Or(None,str),
      '--source': Or(None,And(str,lambda s: s.upper() in sources), error='--source choice must be in '+str(sources) ),
      str:object # catch all
    })
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


