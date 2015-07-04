"""
Usage:
  brilcalc lumi [options] 

Options:
  -h, --help                    Show this screen
  -c CONNECT                    Service name [default: offline]
  -p AUTHPATH                   Path to authentication file [default: data/readdb3.ini]
  -n SCALEFACTOR                Scale factor to results [default: 1.0]
  -f FILLNUM                    Fill number
  -r RUNNUMBER                  Run number
  -i INPUTFILE                  Input selection json file or string
  -o OUTPUTFILE                 Output csv file. Special file '-' for stdout.
  -b BEAMSTATUS                 Beam mode. FLAT TOP,SQUEEZE,ADJUST,STABLE BEAMS
  --amodetag AMODETAG           Accelerator mode 
  --beamenergy BEAMENERGY       Target single beam energy in GeV
  --minBiasXsec MINBIASXSEC     Minbias cross-section in ub [default: 78400.0]
  --datatag DATATAG             Data tag name
  --normtag NORMTAG             correction/calibration tag
  --begin BEGIN                 Min start time/fill/run 
  --end END                     Max start time/fill/run
  --output-style OSTYLE         Screen output style. tab, html, csv [default: tab]
  --type LUMITYPE               Luminosity type. hfoc,pxl,bcm1f,plt,pltzero,hfet.  
  --byls                        Show result in ls granularity
  --xing                        Show result in bx granularity
  --without-correction          Show raw data without calibration
  --cerntime                    Show time in CERN local time

"""

import os
from docopt import docopt
from schema import Schema
from brilws.cli import clicommonargs

def validate(optdict):
    result={}
    #argdict = clicommonargs.argvalidators
    #extract sub argdict here
    myvalidables = ['-c','-n','-f','-r','-i','-o','--amodetag','-b','--beamenergy','--datatag','--begin','--end','--output-style','--type',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result = schema.validate(optdict)
    return result

if __name__ == '__main__':
    args = docopt(__doc__,options_first=True)
    print args

