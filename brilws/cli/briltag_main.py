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

log = logging.getLogger('brilws')
#logformatter = logging.Formatter('%(levelname)s %(message)s')
log.setLevel(logging.ERROR)
#logging.StreamHandler().setFormatter(logformatter)
#ch = logging.StreamHandler()
#ch.setFormatter(logformatter)
#log.addHandler(ch)

def query_creationutc():
    return '''select to_char(sys_extract_utc(systimestamp), 'YY/MM/DD HH24:MI:SS') from dual'''

def briltag_main(progname=sys.argv[0]):
    
    docstr='''

    usage:
      briltag (-h|--help|--version) 
      briltag [--debug|--warn] <command> [<args>...]

    commands:
      listdata    list data tags
      listiov     list norm tags 
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
         import briltag_listdata
         parseresult = docopt.docopt(briltag_listdata.__doc__,argv=cmmdargv)
         parseresult = briltag_listdata.validate(parseresult)

         ##parse selection params
         pargs = clicommonargs.parser(parseresult)
         dbschema = ''
         if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
         dbengine = create_engine(pargs.connecturl)          
         tags = api.data_gettags(dbengine,schemaname=dbschema)
         header = ['datatagnameid','datatagname','creationutc','comments']
         ptable = display.create_table(header,header=True)
         for tid,tval in tags.items():
             display.add_row( [str(tid),tval[0],tval[1],tval[2]], ptable=ptable )
         display.show_table(ptable,'tab')
    
    
         #ofile = '-'
         #if parseresult['--name']:
         #    if not tags: return
         #    tagid = tags.keys()[0]
         #    sinces = sorted([x for x in tags[tagid].keys() if isinstance(x,int)])
         #    payloadids = [tags[tagid][since]['payloadid'] for since in sinces]
         #    payloadcomments = [tags[tagid][since]['payloadcomment'] or '' for since in sinces]
         #    payloaddict = api.iov_parsepayloaddatadict(tags[tagid]['datadict'])
         #    fieldalias = [field['alias']or'v_'+str(field_idx) for field_idx,field in enumerate(payloaddict)]
         #    datahead = fieldalias
         #    maxnitems = tags[tagid]['maxnitems']
         #    header = ['since', 'comment']+datahead
         #    ofile = parseresult['-o']             
         #    results = []
         #    for payloadidx,payloadid in enumerate(payloadids):
         #        tagdetails = api.iov_getpayload(connection,payloadid,payloaddict,maxnitems=maxnitems)
         #        results.append([sinces[payloadidx],payloadcomments[payloadidx],tagdetails])
         #    if parseresult['-o'] or parseresult['--output-style']=='csv':
         #        with api.smart_open(ofile) as fh:
         #            print >> fh, '#'+','.join(header)
         #            csvwriter = csv.writer(fh)
         #            for row in results:                         
         #                csvwriter.writerow(row)
         #    else:
         #        ptable = prettytable.PrettyTable(header)                 
         #        ptable.align = 'l'
         #        ptable.header_style = 'cap'
         #        ptable.max_width['params']=80
         #        for [s,c,d] in results:
         #            dataitems = []
         #            for item in d:
         #                for field in item:
         #                   if field is None: val = ''
         #                   if isinstance(field,list):
         #                       if len(field)==1:
         #                           val = str(field[0])
         #                       else:
         #                           val = '['+','.join([str(f) for f in field if f is not None])+']'
         #                   dataitems.append( val )
         #            ptable.add_row([s,c]+dataitems)
         #        if parseresult['--output-style']=='tab':
         #            print(ptable)
         #        elif parseresult['--output-style']=='html' :
         #            print(ptable.get_html_string())
         #        else:
         #            raise RuntimeError('Unsupported output style %s'%parseresult['--output-style'])
         #else:
         #    header = ['name','creation','default','datasource','applyto','payload','items','iov','comment']
         #    if parseresult['-o'] or parseresult['--output-style']=='csv':
         #        ofile = parseresult['-o']
         #        with api.smart_open(ofile) as fh:
         #            print >> fh, '#'+','.join(header)
         #            csvwriter = csv.writer(fh)
         #            for tagid,tag in tags.items():
         #                sinces = [x for x in sorted(tag.keys()) if isinstance(x,int)]
         #                firstsincestr = str(sinces[0])
         #                sinceStr = firstsincestr+',...'
         #                if len(sinces)>1:
         #                    sinceStr += ','+str(sinces[-1])
         #                csvwriter.writerow([tag['tagname'],tag['creationutc'],tag['isdefault'],tag['datasource'],tag['applyto'],tag['datadict'],tag['maxnitems'],sinceStr,tag['tagcomment']])                     
         #    else:
         #        ptable = prettytable.PrettyTable(header)
         #        ptable.sortby = 'applyto'
         #        ptable.align = 'l'
         #        ptable.header_style = 'cap'
         #        ptable.max_width['params']=60
         #        for tagid,tag in tags.items():
         #            #sinceStr = '\n'.join([str(x) for x in sorted(tag.keys()) if isinstance(x,int)])
         #            sinces = [x for x in sorted(tag.keys()) if isinstance(x,int)]
         #            firstsincestr = str(sinces[0])
         #            sinceStr = firstsincestr+',...'
         #            if len(sinces)>1:
         #                sinceStr += ','+str(sinces[-1])
         #            ptable.add_row([tag['tagname'],tag['creationutc'],tag['isdefault'],tag['datasource'],tag['applyto'],tag['datadict'].replace(' ','\n'),tag['maxnitems'],sinceStr,tag['tagcomment'] or ''])
         #        if parseresult['--output-style']=='tab':
         #            print(ptable)
         #        elif parseresult['--output-style']=='html' :
         #            print(ptable.get_html_string())
         #        else:
         #            raise RuntimeError('Unsupported output style %s'%parseresult['--output-style'])
      elif args['<command>'] == 'insertiov':
         import briltag_insertiov
         parseresult = docopt.docopt(briltag_insertiov.__doc__,argv=cmmdargv)
         parseresult = briltag_insertiov.validate(parseresult)
         pargs = clicommonargs.parser(parseresult)
         dbschema = ''
         if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'         
         dbengine = create_engine(pargs.connecturl)
         istypedefault=False
         if pargs.yamlobj.has_key('istypedefault'):
             istypedefault =  pargs.yamlobj['istypedefault']
         iovtagname = ''
         if pargs.yamlobj.has_key('name'):
             iovtagname = pargs.yamlobj['name']
         else:
             ValueError('name cannot be empty')
         applyto = 'lumi'
         if pargs.yamlobj.has_key('applyto'):
             applyto = pargs.yamlobj['applyto']
         datasource = None
         if pargs.yamlobj.has_key('datasource'):
             datasource = pargs.yamlobj['datasource']
         else:
             raise ValueError('datasource cannot be empty')
         comments = ''
         if pargs.yamlobj.has_key('comments'):
             comments = pargs.yamlobj['comments']
         istypedefault = False
         if pargs.yamlobj.has_key('istypedefault'):
             istypedefault = True
         iovdata = None
         if pargs.yamlobj.has_key('since'):
             iovdata = pargs.yamlobj['since']
         else:
             raise ValueError('since cannot be empty')
         print iovtagname,applyto,datasource,comments,istypedefault
         iovtagid = api.iov_insertdata(dbengine,iovtagname,datasource,iovdata,applyto=applyto,isdefault=istypedefault,comments=comments,schemaname=dbschema)
         
         #iovtagid = api.createIOVTag(dbengine,name,datasource,applyto=applyto,isdefault=isdefault,comments=comments,schemaname=dbschema)
         #print iovtagid
         #if iovfile:
         #    iovdata = api.read_yaml(parseresult['-i'])
         #    tagname = iovdata['tagname']
         #    mytag = api.iov_listtags(connection,tagname=tagname)
         #    if not mytag:
         #        tagid = api.iov_createtag(connection,iovdata)
         #    else:    
         #        log.warn('tag %s exists, switch to append mode'%tagname)
         #        mytagid = mytag.keys()[0]
         #        oldsinces = [k for k in mytag[mytagid].keys() if isinstance(k,int) ]
         #        newsinces = [k for k in iovdata.keys() if isinstance(k,int) ]
         #        if oldsinces>=newsinces:
         #            exit('No new since to append, exit')
         #        deltasince = api.seqdiff(newsinces,oldsinces)
         #        deltasince = sorted(deltasince)
         #        for since in deltasince:
         #            c = ''
         #            if iovdata[since].has_key('comment'):
         #               c = iovdata[since]['comment']                
         #            api.iov_appendtotag(connection,mytagid,since,iovdata[since]['payload'],mytag[mytagid]['datadict'],c)         
         #if updatetagname is not None and isdefault is not None:
         #    api.iov_updatedefault(connection,updatetagname,defaultval=isdefault)
      elif args['<command>'] == 'insertdata':
          import briltag_insertdata
          parseresult = docopt.docopt(briltag_insertdata.__doc__,argv=cmmdargv)
          parseresult = briltag_insertdata.validate(parseresult)
          pargs = clicommonargs.parser(parseresult)
          dbschema = ''
          if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
          dbengine = create_engine(pargs.connecturl)
          name = pargs.name
          if not name: raise NameError('--name cannot be empty')
          datatagnameid = api.data_createtag(dbengine,datatagname=name,comments=pargs.comments,schemaname=dbschema)
          print 'created datatag %s , datatagnameid %ul , comments %s'%(name,datatagnameid,pargs.comments)     
      else:
          exit("%r is not a briltag command. See 'briltag --help'."%args['<command>']) 
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)

if __name__ == '__main__':
    briltag_main()
