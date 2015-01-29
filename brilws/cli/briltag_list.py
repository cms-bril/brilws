"""    
Usage:
  briltag list [options]


Options:
      -h --help                Show this screen.
      -c CONNECT               Connect string to DB.
      -p AUTHPATH              Path to authentication.xml file [default: .]
      -o OUTPUTFILE            Output csv file. Special file '-' for stdout.
      --name TAGNAME           Tag name filter
      --datasource DATASOURCE  Datasource filter: bhm,bcm1f,plt,hfoc,pixel
      --applyto APPLYTO        Apply to which type of daq: lumi,bkg,daq
      --output-style OSTYLE    Screen output style. tab, html, csv [default: tab]
      --default-only           Show only default tags [default: False]

"""

from docopt import docopt
from schema import Schema, And, Or, Use
import os

sources = ['hfoc','bcm1f','bhm','plt','pixel']
applyto_choices = ['lumi','bkg','daq']
outstyle = ['tab','html','csv']
def validate(optdict,sources,applyto,ostyles):
    result = {}
    s = Schema({
      '-c': And(str,error='-c CONNECT is required'),
      '-p': And(os.path.exists, error='AUTHPATH should exist'),
      '-o': Or(None,str), 
      '--name': Or(None,str),
      '--datasource': Or(None,And(str,lambda s: s.lower() in sources), error='--source choice must be in '+str(sources) ),
      '--applyto': Or(None,And(str,Use(str.lower), lambda s: s in applyto_choices), error='--applyto choice must be in '+str(applyto_choices) ),
      '--output-style': And(str,Use(str.lower), lambda s: s in outstyle, error='--output-style choice must be in '+str(outstyle) ),
      str:object # catch all
    })
    result = s.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


