"""
Usage:
  brilschema fetch [options] 

Options:
  -h, --help     Show this screen
  -c SOURCE      Source database connection string 
  -d DEST        Dest database connect string [default: sqlite_file:brilcond.db]
  -p AUTHPATH    Path to authentication.xml [default: .]
  -t TAG         Tag(s) to download to download. [default: all]

"""

import os
from docopt import docopt
from schema import Schema, And, Or, Use

def validate(optdict):
    result={}
    schema = Schema({   
     '-c': And(str, error='option -c SOURCE is required'),
     '-d': str,
     '-p': And(os.path.exists, error='AUTHPATH directory should exist'),
     '-t': Use(lambda x: filter(None,x.split(','))),
     str:object # catch all
    })
    result=schema.validate(optdict)
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)


