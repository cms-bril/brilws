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
  --beamegev BEAMEGEV          Target single beam energy in GeV  
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
from schema import Schema, And, Or, Use
from brilws import RegexValidator,params

def validate(optdict):
    result={}
    schema = Schema({
     '--amodetag':  Or(None,And(str,lambda s: s.upper() in params._amodetagChoices), error='--amodetag must be in '+str(params._amodetagChoices) ),
     '--beamegev': Or(None,And(Use(int), lambda n: n>0), error='--beamegev should be integer >0'),
     '--beamstatus': Or(None, And(str, lambda s: s.upper() in params._beamstatusChoices), error='--beamstatus must be in '+str(params._beamstatusChoices) ),
     '--begin': Or(None, And(str,Use(RegexValidator.RegexValidator(params._timeopt_pattern))), error='wrong format'),
     '--end': Or(None, And(str,Use(RegexValidator.RegexValidator(params._timeopt_pattern))), error='wrong format'),
     '--output-style': And(str,Use(str.lower), lambda s: s in params._outstyle, error='--output-style choice must be in '+str(params._outstyle) ),
     '--chunk-size':  And(Use(int), lambda n: n>0, error='--chunk-size should be integer >0'),
     '--siteconfpath': Or(None, str, error='--siteconfpath should be string'),
     '-c': str,
     '-p': And(os.path.exists, error='AUTHPATH should exist'),
     '-i': Or(None,str),
     '-o': Or(None,str),    
     '-f': Or(None, And(Use(RegexValidator.RegexValidator(params._fillnum_pattern)),Use(int)), error='-f FILL has wrong format'), 
     '-n': And(Use(float), lambda f: f>0, error='-n SCALEFACTOR should be float >0'),      
     '-r': Or(None, And(Use(RegexValidator.RegexValidator(params._runnum_pattern)),Use(int)), error='-r RUN has wrong format'),
     str:object # catch all
    })
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print(docopt(__doc__))
