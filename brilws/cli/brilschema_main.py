import sys,logging
import docopt
import schema
import brilws
from brilws import api
import pandas as pd

log = logging.getLogger('brilschema')
logformatter = logging.Formatter('%(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#fh = logging.FileHandler('/tmp/brilschema.log')
ch.setFormatter(logformatter)
#fh.setFormatter(logformatter)
log.addHandler(ch)
#log.addHandler(fh)

g_condregtablename = 'CONDTAGREGISTRY'
g_condnextidtablename = 'CONDTAGNEXTID'

def brilschema_main():
    docstr='''


    usage:
      brilschema (-h|--help) 
      brilschema --version
      brilschema --checkforupdate
      brilschema [--debug|--nowarning] <command> [<args>...]

    commands:
      create  create tables
      fetch   download 
      load    upload 


    See 'brilschema <command> --help' for more information on a specific command.

    '''
    args = {}
    argv = sys.argv[1:]
    args = docopt.docopt(docstr,argv,help=True,version=brilws.__version__,options_first=True)
    
    if '--debug' in sys.argv:
       log.setLevel(logging.DEBUG)
       ch.setLevel(logging.DEBUG)
    if args['--version'] : print brilws.__version__
    log.debug('global arguments: %s',args)
    cmmdargv = [args['<command>']] + args['<args>']

    log.debug('command arguments: %s',cmmdargv)
    parseresult = {}

    try:
      if args['<command>'] == 'create':
         import os,yaml
         import brilschema_create
         parseresult = docopt.docopt(brilschema_create.__doc__,argv=cmmdargv)
         parseresult = brilschema_create.validate(parseresult)
         columntypemap={}
         schemadatadef={}
         infile = parseresult['-i']
         infilenamebase = os.path.basename(infile.name).split('.')[0]
         suffix = parseresult['--suffix']             
         writeraccount = ''
         if parseresult['-f']=='oracle':
              columntypemap = api.oracletypemap
              writeraccount = parseresult['-w']
         else:
              columntypemap = api.sqlitetypemap
         schemadatadef = yaml.load(parseresult['-i'])
         print 'Creating drop sql file for: %s'%infilenamebase
         api.drop_tables_sql(infilenamebase,schemadatadef,suffix=suffix,dbflavor=parseresult['-f'])
         print 'Creating create sql file for: %s'%infilenamebase
         api.create_tables_sql(infilenamebase,schemadatadef,suffix=suffix,dbflavor=parseresult['-f'],writeraccount=writeraccount)
         print 'Done'

      elif args['<command>'] == 'fetch':
         import brilschema_fetch
         import coral
         parseresult = docopt.docopt(brilschema_fetch.__doc__,argv=cmmdargv)
         parseresult = brilschema_fetch.validate(parseresult)
         sourceconnect = parseresult['-c']
         destconnect = parseresult['-d']
         if api.db_connect_protocol(sourceconnect)=='oracle' or api.dbconnect_protocol(destconnect)=='oracle':
             os.environ['CORAL_AUTH_PATH'] = parseresult['-p'] 
         svc = coral.ConnectionService()
         sourcesession = svc.connect(sourceconnect, accessMode = coral.access_ReadOnly)
         destsession = svc.connect(destconnect, accessMode = coral.access_Update)
         tagnames = []

         if parseresult['-t']=='all':
             sourcesession.transaction().start(True)
             qHandle = sourcesession.nominalSchema().newQuery()
             tablist = [(g_condregtablename,'')]
             qOutputRowDef = {'TAGNAME':('string','tagname')}
             g = api.db_query_generator(qHandle,tablist,qOutputRowDef)
             tagnames = [x[1] for x in g]                 
             del qHandle
             sourcesession.transaction().commit()                
         else:
             tagnames = parseresult['-t']

         for tagname in tagnames:
            sourcesession.transaction().start(True)
            tagdata = []
            qHandle = sourcesession.nominalSchema().newQuery()
            tablist = [(g_condregtablename,'')]
            qConditionStr = 'TAGNAME=:tagname'
            qCondition = coral.AttributeList()
            qCondition.extend('tagname','string')
            qCondition['tagname'].setData(tagname)
            g = api.db_query_generator(qHandle,tablist,{},qConditionStr,qCondition)
            sourcedf = pd.DataFrame.from_records(g)
            sourcesession.transaction().commit()
            print sourcedf.values
           
         del sourcesession
         del destsession
         del svc
      elif args['<command>'] == 'load':
         import brilschema_load
         import coral,yaml
         import os
         from datetime import datetime
         parseresult = docopt.docopt(brilschema_load.__doc__,argv=cmmdargv)
         parseresult = brilschema_load.validate(parseresult)
         infile = parseresult['-i']
         fpath = os.path.dirname(os.path.abspath(infile.name))
         connectStr = parseresult['-c']
         protocol = api.db_connect_protocol(connectStr)
         if protocol=='oracle':
            os.environ['CORAL_AUTH_PATH'] = parseresult['-p']
         datamap = yaml.safe_load(infile)
         infile.close()
         log.debug('datamap: %s'%str(datamap))
         
         tags = datamap.keys()
         d = str(datetime.utcnow())

         svc = coral.ConnectionService()         
         session = svc.connect(connectStr, accessMode = coral.access_Update)  
         session.transaction().start(False) 
         dbschema = session.nominalSchema() 
         tagregrowdef = api.db_getrowdef(dbschema,g_condregtablename)
         for tagname in tags:
             tagproperties = [x for x in datamap[tagname].keys() if isinstance(x,str)]
             timesequence = [x for x in datamap[tagname].keys() if isinstance(x,int)]
             tagdatatable = datamap[tagname]['tagdatatable']
             tagdatarowdef = api.db_getrowdef(dbschema,tagdatatable)
             paramtable = datamap[tagname]['paramtable']
             if paramtable:
                paramrowdef = api.db_getrowdef(dbschema,paramtable)
             tagid = api.db_getnextid(dbschema,g_condnextidtablename)
             tagregdata = {}
             tagregdata['TAGID'] = tagid
             tagregdata['TAGNAME'] = tagname
             tagregdata['CREATIONUTC'] = d
             for tagproperty in tagproperties:
                 colval = datamap[tagname][tagproperty]
                 tagregdata[tagproperty.upper()]=colval
             api.db_singleInsert(dbschema,g_condregtablename,tagregrowdef,tagregdata)
             tagpayload = [] 
             params = []
             for sincerun in timesequence:
                 thispayload = {}
                 thispayload['TAGID'] = tagid                 
                 thispayload['SINCERUN'] = sincerun
                 thispayloadproperties = datamap[tagname][sincerun]
                 for k,v in thispayloadproperties.iteritems():             
                     if paramtable and type(v) is not list:     
                         thispayload[k.upper()] = v 
                     else:
                         for idx,[paramname,paramtype,paramval] in enumerate(v):
                            thisparam = {}
                            thisparam['TAGID'] = tagid
                            thisparam['SINCERUN'] = sincerun 
                            thisparam['PARAMIDX'] = idx
                            thisparam['PARAMNAME'] = paramname
                            thisparam[paramtype.upper()] = paramval
                            params.append(thisparam)                 
                 tagpayload.append(thispayload)
             api.db_bulkInsert(dbschema,tagdatatable,tagdatarowdef,tagpayload)
             api.db_bulkInsert(dbschema,paramtable,paramrowdef,params)
             session.transaction().commit()
         del session
         del svc
      else:
         exit("%r is not a brilschema command. See 'brilschema --help'."%args['<command>'])
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])
    except schema.SchemaError as e:
      exit(e)

    if not parseresult.has_key('--debug'):
       if parseresult.has_key('--nowarning'):
          log.setLevel(logging.ERROR)
          ch.setLevel(logging.ERROR)
    else:
        log.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    log.debug('create arguments: %s',parseresult)

    
if __name__ == '__main__':
    brilschema_main()
