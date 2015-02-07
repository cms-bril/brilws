from docopt import docopt
from sqlalchemy import *
from schema import Schema, And, Or, Use
from brilws import api
import os
from ConfigParser import SafeConfigParser
choices_map = ['datatable','beamstatus','trgbit','hltpath','dataset','hltstreamdataset']

def brildb_frommap():
    docstr = """

    Usage:
      brildb_frommap.py MAP BRILDB SOURCE [options]

    Arguments:
      MAP              Map to upload
      BRILDB           Connection string of brildb
      SOURCE           Connection string of sourcedb or csv file

    Options:
      -h --help        Show this screen.
      -p AUTHPATH      Path to authentication file [default: .]
      --debug          Debug

    """
    parseresult = docopt(docstr)
    schema = Schema({
        'MAP': And(str,lambda s: s.lower() in choices_map,error='MAP must be in '+str(choices_map)),
        'BRILDB': str,
        '-p': And(os.path.exists, error='AUTHPATH should exist'),
        str:object # catch all
    })
    
    parseresult = schema.validate(parseresult)
    mapname = parseresult['MAP']
    if mapname.lower() == 'datatable':
        d = api.DatatableMap()        
    elif mapname.lower() == 'beamstatus':
        d = api.BeamStatusMap()
    elif mapname.lower() == 'trgbit':
        d = api.TrgBitMap()
    elif mapname.lower() == 'hltpath':
        d = api.HLTPathMap()        
    elif mapname.lower() == 'dataset':
        d = api.DatasetMap()
    elif mapname.lower() == 'hltstreamdataset':
        d = api.HLTStreamDatasetMap()        

    dest = parseresult['BRILDB']
    parser = SafeConfigParser()
    if dest.find('oracle') != -1:
        parser.read(os.path.join(parseresult['-p'],'Authentication.ini'))
        destpasswd = parser.get(dest,'pwd')
        idx = dest.find('@')
        desturl = dest[:idx]+':'+destpasswd.decode('base64')+dest[idx:]
    else:
        desturl = dest
    destengine = create_engine(desturl)
    
    source = parseresult['SOURCE']
    if os.path.isfile(source):
        print 'load from csv file %s'%source
        data = d.from_csv(source)
        d.to_brildb(destengine,data)
    else:
        print 'load from db %s'%source        
        if d.name in ['datatable','beamstatus']:
            print '%s has no external db source, do nothing'%d.name
        else:
            parser.read(os.path.join(parseresult['-p'],'Authentication.ini'))
            sourcepasswd =  parser.get(source,'pwd')
            idx = source.find('@')
            sourceurl = source[:idx]+':'+sourcepasswd.decode('base64')+source[idx:]
            sourceengine = create_engine(sourceurl)
            data = d.from_sourcedb(sourceengine)
            d.to_brildb(destengine,data)
if __name__ == '__main__':    
    brildb_frommap()

