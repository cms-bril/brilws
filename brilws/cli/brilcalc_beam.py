"""Usage: brilcalc beam [options] 

options:
  -h,  --help                  Show this screen.
  -c CONNECT                   Connect string to DB [default: frontier://LumiCalc/CMS_LUMI_PROD]
  -p AUTHPATH                  Path to authentication.xml file [default: .]
  -n SCALEFACTOR               Scale factor on results [default: 1.0]
  -f FILL                      Fill number
  -r RUN                       Run number
  -i INPUTFILE                 Input selection file
  -o OUTPUTFILE                Output csv file. Special file '-' for stdout.
  --siteconfpath SITECONFPATH  Path to SITECONF/local/JobConfig/site-local-config.xml [default: $CMS_PATH] 
  --amodetag AMODETAG          Accelerator mod choices 
  --beamstatus BEAMSTATUS      Beam mode choices
  --egev EGEV                  Target single beam energy in GeV  
  --datatag DATATAG            Data tag name
  --begin BEGIN                Min start time 
  --end END                    Max start time
  --output-style OSTYLE        Screen output style. tab, html, csv [default: tab]
  --chunk-size CHUNKSIZE       Main data chunk size [default: 100]
  --xing                       Show result in bx granularity
  --nowarning                  Suppress warning messages 
  --debug                      Debug
 
"""

import os
from docopt import docopt
from schema import Schema
from brilws import clicommonargs

def validate(optdict):
    result={}
    argdict = clicommonargs.argvalidators
    #extract sub argdict here
    myvalidables = ['-n','-f','-r','-i','-o','--amodetag','--beamstatus','--egev','--datatag','--begin','--end','--output-style','--chunk-size','--siteconfpath',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result = schema.validate(optdict)
    return result

if __name__ == '__main__':
    print(docopt(__doc__))
