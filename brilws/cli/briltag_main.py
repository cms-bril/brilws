import sys,logging,os
import docopt
import schema
import coral
import numpy as np
import pandas as pd
import brilws
from brilws import api,display

log = logging.getLogger('briltag')
logformatter = logging.Formatter('%(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#fh = logging.FileHandler('/tmp/briltag.log')
ch.setFormatter(logformatter)
#fh.setFormatter(logformatter)
log.addHandler(ch)
#log.addHandler(fh)

choice_sources = ['BHM','BCM1F','PLT','HFOC','PIXEL']
choice_applyto = ['LUMI','BKG']

def combine_params(groups):
    '''
    paramname:combine paramname and not null param into one row of [('paramname',paramval)]
    '''
    result = pd.concat([groups['f4'].dropna(),groups['s'].dropna(),groups['u4'].dropna()])
    result = zip( groups['paramname'].values,result.values)
    groups['params']= ''.join(map(display.formatter_tuple,result))
    return groups
    
def briltag_main():
    docstr='''

    usage:
      briltag (-h|--help) 
      briltag --version
      briltag --checkforupdate
      briltag [--debug|--nowarning] <command> [<args>...]

    commands:
      norm Norm tag 
      data Data tag
      lut  Lut tag
      
    See 'briltag <command> --help' for more information on a specific command.

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
    svc = coral.ConnectionService()    
    try:      
      if args['<command>'] == 'norm':
         import briltag_norm
         parseresult = docopt.docopt(briltag_norm.__doc__,argv=cmmdargv)
         parseresult = briltag_norm.validate(parseresult,sources=choice_sources,applyto=choice_applyto)
         dbconnect = parseresult['-c']

         dbsession = svc.connect(dbconnect, accessMode = coral.access_ReadOnly)
         dbsession.transaction().start(True)
         qHandle = dbsession.nominalSchema().newQuery()
         qTablelist = [('CONDTAGREGISTRY','registry')]
         qOutputRowDef = {'TAGID':('unsigned long long','tagid'),'TAGNAME':('string','tagname'),'ISDEFAULT':('unsigned char','isdefault'),'CREATIONUTC':('string','creation time'),'TAGDATATABLE':('string','tagdatatable'),'PARAMTABLE':('string','paramtable'),'DATASOURCE':('string','datasource'),'APPLYTO':('string','applyto'),'COMMENT':('string','comment')}
         qConditionStrs = []
         qCondition = coral.AttributeList()
         if parseresult['--applyto']:
             qConditionStrs.append('APPLYTO=:applyto')
             qCondition.extend('applyto','string')
             qCondition['applyto'].setData(parseresult['--applyto'])
         if parseresult['--source']:
             qConditionStrs.append('DATASOURCE=:datasource')
             qCondition.extend('datasource','string')
             qCondition['datasource'].setData(parseresult['--source'])
         if parseresult['--default-only']:    
             qConditionStrs.append('ISDEFAULT=1')
         qConditionStr = ' AND '.join(qConditionStrs)
         regdf = pd.DataFrame.from_records(api.db_query_generator(qHandle,qTablelist,qOutputRowDef,qConditionStr,qCondition))
         del qHandle

         if parseresult['--name']:
             tagname = parseresult['--name']
             selectedtag = regdf.loc[(regdf['tagname']==tagname),['tagid','tagdatatable','paramtable']].values
             for tagid,tagtable,paramtable in selectedtag:
                qHandle = dbsession.nominalSchema().newQuery()
                #select n.comment,n.amodetag,n.egev,n.minbiasxsec,n.funcname, p.* from $paramtable p where p.tagid=:tagid 
                qOutputRowDef = {'n.SINCERUN':('unsigned int','sincerun'),'n.COMMENT':('string','comment'),'n.AMODETAG':('string','amodetag'),'n.EGEV':('unsigned int','egev'),'n.MINBIASXSEC':('unsigned int','minbiasxsec'),'n.FUNCNAME':('string','funcname'),'p.PARAMIDX':('unsigned int','paramidx'),'p.PARAMNAME':('string','paramname'),'p.U1':('unsigned char','u1'),'p.I1':('char','i1'),'p.U2':('unsigned short','u2'),'p.I2':('short','i2'),'p.F4':('float','f4'),'p.U4':('unsigned int','u4'),'p.I4':('int','i4'),'p.U8':('unsigned long long','u8'),'p.I8':('long long','i8'),'p.S':('string','s')}
                qTablelist = [(tagtable,'n'),(paramtable,'p')]
                
                qConditionStr = 'n.TAGID=p.TAGID AND n.SINCERUN=p.SINCERUN AND n.TAGID=:tagid'
                qCondition = coral.AttributeList()
                qCondition.extend('tagid','unsigned long long')
                qCondition['tagid'].setData(tagid)
               
                paramdf = pd.DataFrame.from_records(api.db_query_generator(qHandle,qTablelist,qOutputRowDef,qConditionStr,qCondition))
                grouped = paramdf.groupby(['sincerun'])
                result = grouped.apply(combine_params)                                               
                display.listdf(result,columns=['sincerun','amodetag','egev','minbiasxsec','funcname','params','comment'])
                del qHandle
         dbsession.transaction().commit()
         del dbsession
         if regdf.empty:
             print 'No result found'
         else:
             display.listdf(regdf,columns=['tagname','datasource','applyto','creation time','isdefault','comment'])

      elif args['<command>'] == 'lut':
         import briltag_lut
         parseresult = docopt.docopt(briltag_lut.__doc__,argv=cmmdargv)
         parseresult = briltag_lut.validate(parseresult,sources=choice_sources)

      elif args['<command>'] == 'data':
         import briltag_data
         parseresult = docopt.docopt(briltag_data.__doc__,argv=cmmdargv)
         parseresult = briltag_data.validate(parseresult)

      else:
          exit("%r is not a briltag command. See 'briltag --help'."%args['<command>']) 
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)
    del svc


    if not parseresult.has_key('--debug'):
       if parseresult.has_key('--nowarning'):
          log.setLevel(logging.ERROR)
          ch.setLevel(logging.ERROR)
    else:
       log.setLevel(logging.DEBUG)
       ch.setLevel(logging.DEBUG)
       log.debug('create arguments: %s',parseresult)
    log.debug('create arguments: %s',parseresult)

if __name__ == '__main__':
    briltag_main()
