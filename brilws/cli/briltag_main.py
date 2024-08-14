import sys
import os
sys.path.insert(0,os.path.dirname(sys.executable)+'/../lib/python2.7/site-packages/') #ignore other PYTHONPATH

import logging
import docopt
import schema
import csv
import numpy as np
import pandas as pd
import brilws
import prettytable
from sqlalchemy import *
from brilws import api,params,display
from brilws.cli import clicommonargs
from brilws.corrector import FunctionFactory

log = logging.getLogger('brilws')
#logformatter = logging.Formatter('%(levelname)s %(message)s')
log.setLevel(logging.ERROR)
#logging.StreamHandler().setFormatter(logformatter)
#ch = logging.StreamHandler()
#ch.setFormatter(logformatter)
#log.addHandler(ch)

def query_creationutc():
    return '''select to_char(sys_extract_utc(systimestamp), 'YY/MM/DD HH24:MI:SS') from dual'''

def validate_iovdata(iovdata):
    for entry in iovdata:
        for run, data in entry.items():
            func_name = data["func"]
            payload = data["payload"]
            try:
                FunctionFactory.validate_required_arguments(payload, func_name)
            except ValueError as err:
                raise ValueError(f"Invalid config at run {run}.") from err

def briltag_main(progname=sys.argv[0]):
    
    docstr='''

    usage:
      briltag (-h|--help|--version) 
      briltag [--debug|--warn] <command> [<args>...]

    commands:
      listdata    list data tags
      listiov     list norm tags 
      insertiov   insert norm tag
      insertdata  insert data tag
    See 'briltag <command> --help' for more information on a specific command.

    '''
    argv = sys.argv[1:]
    args = docopt.docopt(docstr,argv,help=True,version=brilws.__version__,options_first=True)
    if args['--debug']:
        log.setLevel(logging.DEBUG)
    elif args['--warn']:
        log.setLevel(logging.WARNING)    
    log.debug('global arguments: %s',args)   
    cmmdargv = [args['<command>']] + args['<args>']
    
    log.debug('command arguments: %s',cmmdargv)
    parseresult = {}

    try:      
      if args['<command>'] == 'listdata':
         from . import briltag_listdata
         parseresult = docopt.docopt(briltag_listdata.__doc__,argv=cmmdargv)
         parseresult = briltag_listdata.validate(parseresult)

         ##parse selection params
         pargs = clicommonargs.parser(parseresult)
         dbschema = ''
         if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
         if sys.version_info > (3,):
             dbengine = create_engine(pargs.connecturl,max_identifier_length=128)
         else:
             dbengine = create_engine(pargs.connecturl)     
         tags = api.data_gettags(dbengine,schemaname=dbschema)
         header = ['datatagnameid','datatagname','creationutc','comments']
         ptable = display.create_table(header,header=True)
         for tid,tval in tags.items():
             display.add_row( [str(tid),tval[0],tval[1],tval[2]], ptable=ptable )
         display.show_table(ptable,'tab')

      elif args['<command>'] == 'listiov':
         from . import briltag_listiov
         parseresult = docopt.docopt(briltag_listiov.__doc__,argv=cmmdargv)
         parseresult = briltag_listiov.validate(parseresult)
         pargs = clicommonargs.parser(parseresult)
         dbschema = ''
         if not pargs.dbconnect.find('oracle')!=-1: 
             dbschema = 'cms_lumi_prod'
         if sys.version_info > (3,):
             dbengine = create_engine(pargs.connecturl,max_identifier_length=128)
         else:
             dbengine = create_engine(pargs.connecturl)
         istypedefault = False
         if '--isdefault' in parseresult:
             istypedefault = True
         if not pargs.name:            
             iovtaglist = api.iov_gettags(dbengine,datasource=pargs.lumitype,applyto=pargs.applyto,isdefault=istypedefault,schemaname=dbschema)
             header = ['tagname','tagid','creationutc','applyto','datasource','isdefault','comments']
             ptable = display.create_table(header,header=True)
             for tagname,tagcontents in iovtaglist.items():
                 display.add_row( [tagname,str(tagcontents[0]),tagcontents[1],tagcontents[2],tagcontents[3],tagcontents[4],tagcontents[5]], ptable=ptable )
             display.show_table(ptable,'tab')
         else:
             iovtagdata = api.iov_gettagdata(dbengine,pargs.name,schemaname=dbschema)
             header = ['since','func','payload','comments']
             ptable = display.create_table(header,header=True)
             for sincedata in iovtagdata:
                 since = sincedata[0]
                 func = sincedata[1]                 
                 payload = sincedata[2]
                 comments = sincedata[3]                
                 display.add_row( [ '%d'%since, func, payload, comments], ptable=ptable )
             display.show_table(ptable,'tab')
             
      elif args['<command>'] == 'insertiov':
         from . import briltag_insertiov
         parseresult = docopt.docopt(briltag_insertiov.__doc__,argv=cmmdargv)
         parseresult = briltag_insertiov.validate(parseresult)
         pargs = clicommonargs.parser(parseresult)
         dbschema = ''
         if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'         
         if sys.version_info > (3,):
             dbengine = create_engine(pargs.connecturl,max_identifier_length=128)
         else:
             dbengine = create_engine(pargs.connecturl)
         istypedefault=False
         if 'istypedefault' in pargs.yamlobj:
             istypedefault =  pargs.yamlobj['istypedefault']
         iovtagname = ''
         if 'name' in pargs.yamlobj:
             iovtagname = pargs.yamlobj['name']
         else:
             ValueError('name cannot be empty')
         applyto = 'lumi'
         if 'applyto' in pargs.yamlobj:
             applyto = pargs.yamlobj['applyto']
         datasource = None
         if 'datasource' in pargs.yamlobj:
             datasource = pargs.yamlobj['datasource']
         else:
             raise ValueError('datasource cannot be empty')
         comments = ''
         if 'comments' in pargs.yamlobj:
             comments = pargs.yamlobj['comments']
         istypedefault = False
         if 'istypedefault' in pargs.yamlobj:
             istypedefault = True
         iovdata = None
         if 'since' in pargs.yamlobj:
             iovdata = pargs.yamlobj['since']
         else:
             raise ValueError('since cannot be empty')
         validate_iovdata(iovdata)
         iovtagid = api.iov_insertdata(dbengine,iovtagname,datasource,iovdata,applyto=applyto,isdefault=istypedefault,comments=comments,schemaname=dbschema)
                  
      elif args['<command>'] == 'insertdata':
          from . import briltag_insertdata
          parseresult = docopt.docopt(briltag_insertdata.__doc__,argv=cmmdargv)
          parseresult = briltag_insertdata.validate(parseresult)
          pargs = clicommonargs.parser(parseresult)
          dbschema = ''
          if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
          if sys.version_info > (3,):
             dbengine = create_engine(pargs.connecturl,max_identifier_length=128)
          else:
             dbengine = create_engine(pargs.connecturl)
          name = pargs.name
          if not name: raise NameError('--name cannot be empty')
          datatagnameid = api.data_createtag(dbengine,datatagname=name,comments=pargs.comments,schemaname=dbschema)
          print ('created datatag %s , datatagnameid %ul , comments %s'%(name,datatagnameid,pargs.comments)     )
      else:
          exit("%r is not a briltag command. See 'briltag --help'."%args['<command>']) 
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)

if __name__ == '__main__':
    briltag_main()
