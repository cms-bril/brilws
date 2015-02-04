"""
Usage:
  brilschema create [options] 

Options:
  -h, --help       Show this screen
  -i INPUTFILE     Input schema definition file
  -f DBFLAVOR      Database flavor [default: sqlite]
  --schema SCHEMA  Oracle schema name [default: CMS_LUMI_PROD]
  --suffix SUFFIX  Table suffix [default: RUN1]
"""

dbflavors=['oracle','sqlite']

import os
from docopt import docopt
from schema import Schema, And, Or, Use

def validate(optdict):
    result={}
    schema = Schema({ 
     '-i': Use(open, error='-i INPUTFILE should be readable'),
     '-f': And(str,lambda s: s.lower() in dbflavors, error='-f must be in '+str(dbflavors)),
     '--schema': USE(str.upper),
     '--suffix': Use(str.upper),
     str:object # catch all
    })
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)
