"""    
Usage:
  briltag norm [options]


Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to DB.
      -p AUTHPATH              Path to authentication.xml file [default: .]
      --name TAGNAME           Tag name 
      --datasource DATASOURCE  Datasource filter on bhm,bcm1f,plt,hfoc,pixel
      --applyto APPLYTO        Apply to which type of result lumi,bkg
      --output-style OSTYLE    Output style tab,csv,html [default: tab]
      --default-only           Show only default tags [default: False]
"""

from docopt import docopt
from schema import Schema, And, Or, Use
import os

def validate(optdict,sources,applyto,ostyles):
    result = {}
    s = Schema({
      '-c': And(str,error='-c CONNECT must exist'),
      '-p': And(os.path.exists, error='AUTHPATH should exist'),
      '--name': Or(None,str),
      '--datasource': Or(None,And(str,lambda s: s.lower() in sources), error='--source choice must be in '+str(sources) ),
      '--applyto': Or(None,And(str,lambda s: s.lower() in applyto), error='--applyto choice must be in '+str(applyto) ),
      '--output-style': Or(None,And(str,lambda s: s.lower() in ostyles), error='--output-style choice must be in '+','.join(ostyles) ),
      str:object # catch all
    })
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


