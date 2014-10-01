"""
Usage:
  brilschema load [options] 

Options:
  -h, --help      Show this screen
  -i INPUTFILE    Input data file
  -c CONNECT      Connect string
  -p AUTHPATH     Path to authentication.xml [default: .]
  
"""

import os
from docopt import docopt
from schema import Schema, And, Or, Use

def validate(optdict):
    result={}
    schema = Schema({
     '-i': Use(open, error='INPUTFILE should exist and be readable'),
     '-c': str,
     '-p': And(os.path.exists, error='AUTHPATH should exist'),
     str:object # catch all
    })
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


