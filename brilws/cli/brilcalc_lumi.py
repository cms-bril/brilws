"""
Usage:
  brilcalc lumi [options] 

Options:
  -h, --help                    Show this screen
  -c CONNECT                    Connect string to lumiDB [default: frontier://LumiCalc/CMS_LUMI_PROD]
  -p AUTHPATH                   Path to authentication.xml
  -n SCALEFACTOR                Scale factor to results [default: 1.0]
  -f FILLNUM                    Fill number
  -r RUNNUMBER                  Run number
  -i INPUTFILE                  Input selection json file or string
  -o OUTPUTFILE                 Output csv file. Special file '-' for stdout.
  -b BEAMSTATUS                 Beam mode. FLAT TOP,SQUEEZE,ADJUST,STABLE BEAMS
  --siteconfpath SITECONFPATH   Path to SITECONF/local/JobConfig/site-local-config.xml [default: $CMS_PATH]                    
  --amodetag AMODETAG           Accelerator mode 
  --beamenergy BEAMENERGY       Target single beam energy in GeV
  --minBiasXsec MINBIASXSEC     Minbias cross-section in ub [default: 78400.0]
  --datatag DATATAG             Data tag name 
  --normtag NORMTAG             correction/calibration tag
  --begin BEGIN                 Min start time/fill/run 
  --end END                     Max start time/fill/run
  --output-style OSTYLE         Screen output style. tab, html, csv [default: tab]
  --chunk-size CHUNKSIZE        Main data chunk size [default: 100]
  --type LUMITYPE               Luminosity type or fallback order [default: PXL|BCMF|PLT|HFOC] 
  --hltpath HLTPATH             HLT path name/pattern for effective luminosity
  --byls                        Show result in ls granularity
  --xing                        Show result in bx granularity
  --nowarning                   Supress warning messages
  --debug                       Debug

"""

import os
from docopt import docopt
from schema import Schema
from brilws import clicommonargs

def validate(optdict):
    result={}
    argdict = clicommonargs.argvalidators
    #extract sub argdict here
    myvalidables = ['-c','-n','-f','-r','-i','-o','--amodetag','-b','--beamenergy','--datatag','--begin','--end','--output-style','--chunk-size','--siteconfpath',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    args = docopt(__doc__,options_first=True)
    print args

