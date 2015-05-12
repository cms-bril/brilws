"""Usage: brilcalc.py hlt [options] 

Options:
  -h, --help                   Show this screen
  -c CONNECT                   Connect string to DB [default: frontier://LumiCalc/CMS_LUMI_PROD]
  -p AUTHPATH                  Path to authentication.xml 
  -f FILL                      Fill number
  -r RUN                       Run number
  -i INPUTFILE                 Input selection file
  -o OUTPUTFILE                Output file
  -b BEAMSTATUS                Beam mode. FLAT TOP,SQUEEZE,ADJUST,STABLE BEAMS
  --siteconfpath SITECONFPATH  Path to SITECONF/local/JobConfig/site-local-config.xml [default: $CMS_PATH] 
  --amodetag AMODETAG          Accelerator mode 
  --beamenergy BEAMENERGY      Target single beam energy in GeV  
  --datatag DATATAG            Data tag name
  --begin BEGIN                Min start time/fill/run 
  --end END                    Max start time/fill/run
  --output-style OSTYLE        Screen output style. tab, html, csv [default: tab]
  --chunk-size CHUNKSIZE       Main data chunk size [default: 100]
  --name PATHNAME              HLT path name/pattern
  --pathinfo                   Show hltpath prescale, counts info
  --nowarning                  Suppress warning messages
  --debug                      Debug

"""
from docopt import docopt
from schema import Schema
from brilws import clicommonargs

def validate(optdict):
    result={}
    argdict = clicommonargs.argvalidators
    #extract sub argdict here
    myvalidables = ['-c','-f','-r','-i','-o','--amodetag','-b','--beamenergy','--datatag','--begin','--end','--output-style','--name','--chunk-size','--siteconfpath',str]
    argdict = dict((k,v) for k,v in clicommonargs.argvalidators.iteritems() if k in myvalidables)
    schema = Schema(argdict)
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print(docopt(__doc__))
