"""Usage: brilcalc beam (byls | bylsXing) [options] [--] 

options:
  -h,  --help        Show this screen.
  -c CONNECT         Connect string to lumiDB 
                     [default: frontier://LumiCalc/CMS_LUMI_PROD]
  -p AUTHPATH        Path to authentication.xml 
  -n SCALEFACTOR     Scale factor to results 
                     [default: 1.0]
  -f FILLNUM         Fill number
  -r RUNNUMBER       Run number
  -i INPUTFILE       Input selection file
  -o OUTPUTFILE      Output file
  --siteconfpath=SITECONFPATH 
                     Path to SITECONF/local/JobConfig/site-local-config.xml
                     [default: $CMS_PATH] 
  --amodetag=AMODETAG 
                     Accelerator mod choices 
  --beamstatus=BEAMSTATUS
                     Beam mode choices
  --beamenergy=BEAMENERGY
                     Single beam energy in GeV
  --beamfluctuation=BEAMFLUCTUATION 
                     Fluctuation in fraction allowed to beam energy 
                     [default: 0.2]
  --datatag=DATATAG  Version of data
  --begin=BEGIN      Min start time 
  --end=END          Max start time
  --with-beamintensity  
                     With beam intensity
                     [default: False]
  --minintensity=MININTENSITY 
                     Min filter on beam intensity 
                     [default: 0.1]
  --nowarning        Suppress warning messages
  --debug            Debug

"""

from docopt import docopt
from schema import Schema, And, Or, Use

def validate(optdict,amodetagChoices):
    result={}
    schema = Schema({
     '--amodetag':  Or(None,And(str,lambda s: s.upper() in amodetagChoices), error='--amodetag choice must be in '+str(amodetagChoices) ),
     '--beamenergy': Or(None,And(Use(int), lambda n: n>0), error='--beamenergy should be integer >0'),
     '--beamfluctuation': And(Use(float), lambda f: f>0 and f<1.0, error='--beamfluctuation should be float >0'),
     '--beamstatus': Or(None, And(Use(str), lambda s: s.upper()), error='--beamstatus should be string'),
     '--begin': Or(None, Use(str), error='--begin should be string'),
     '--end': Or(None, Use(str), error='--end should be string'),
     '--siteconfpath': Or(None, str, error='--siteconfpath should be string'),
     '-c': str,
     '-f': Or(None, And(Use(int), lambda n: n>1000), error='-f FILLNUMBER should be integer >1000'), 
     '-n': And(Use(float), lambda f: f>0, error='-n SCALEFACTOR should be float >0'),      
     '-r': Or(None, And(Use(int), lambda n: n>100000), error='-r RUNNUMBER should be integer >100000'),
     str:object # catch all
    })
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print(docopt(__doc__))
