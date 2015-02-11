"""
Usage:
  brilschema loadmap [options] 

Options:
  -h, --help       Show this screen
  --name NAME      Result: hfoclumi,hfoclumionline,pixellumi
  -i INSELECT      Input run, ls selection json file or string
  -o OUTPUT        Output csv file or Database
  --SOURCE         Source database or csv file
  -p AUTH          Path to authentication.ini 

"""


import os,re
from docopt import docopt
from schema import Schema, And, Or, Use
choices_resultdata = ['hfoclumi','hfoclumionline','pixellumi']


def validate(optdict):
    result={}
    schema = Schema({ 
     '-i': And(str,lambda s: os.path.isfile(os.path.expanduser(s)) or s.find('sqlite:')!=-1 or s.find('oracle:')!=-1 ,error='-i SOURCE should be a csv file or database connect string'),
     '-o': And(str,lambda s: os.path.isfile(os.path.expanduser(s)) or s.find('sqlite:')!=-1 or s.find('oracle:')!=-1 ,error='-o DEST should be a csv file or database connect string'),     
     '--name': And(str,lambda s: s.lower() in choices_mapdata, error='--name must be in '+str(choices_mapdata)),
     '--source': 
     '-p': Or(None,And(str,lambda s: os.path.isfile(os.path.join(os.path.expanduser(s),'authentication.ini'))) ,error='-p AUTH should contain authentication.ini'),   
     str:object # catch all
    })
    result=schema.validate(optdict)    
    return result

if __name__ == '__main__':
    print docopt(__doc__,options_first=True)
