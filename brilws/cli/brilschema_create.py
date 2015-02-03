"""
Usage:
  brilschema create [options] 

Options:
  -h, --help       Show this screen
  -i INPUTFILE     Input schema definition file
  -f DBFLAVOR      Database flavor [default: sqlite]
  -w WRITERACCOUNT Writer oracle account name
  --suffix SUFFIX  Table suffix
"""

dbflavors=['oracle','sqlite']

import os
from docopt import docopt
from schema import Schema, And, Or, Use

def validate(optdict):
    result={}
    schema = Schema({ 
     '-i': Use(open, error='-i INPUTFILE should be readable'),
     '-f': Or(None,And(str,lambda s: s.lower() in dbflavors), error='-f must be in '+str(dbflavors) ),
     '-w': Or(None,str),
     '--suffix': Or(None,str),
     str:object # catch all
    })
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)
