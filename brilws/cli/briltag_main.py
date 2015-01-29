import sys,logging,os,gc
import docopt,schema
import coral
import csv
import numpy as np
import pandas as pd
import brilws
from sqlalchemy import * 
from brilws import api,display,prettytable

log = logging.getLogger('briltag')
logformatter = logging.Formatter('%(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#fh = logging.FileHandler('/tmp/briltag.log')
ch.setFormatter(logformatter)
#fh.setFormatter(logformatter)
log.addHandler(ch)
#log.addHandler(fh)

choice_sources = ['bhm','bcm1f','plt','hfoc','pixel']
choice_applyto = ['lumi','bkg','daq']
choices_ostyles = ['tab','csv','html']

def briltag_main():
    docstr='''

    usage:
      briltag (-h|--help) 
      briltag --version
      briltag --checkforupdate
      briltag [--debug|--nowarning] <command> [<args>...]

    commands:
      list  
      insert 
      
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
    try:      
      if args['<command>'] == 'list':
         import briltag_list
         parseresult = docopt.docopt(briltag_list.__doc__,argv=cmmdargv)
         parseresult = briltag_list.validate(parseresult,sources=choice_sources,applyto=choice_applyto,ostyles=choices_ostyles)
         engine = create_engine(parseresult['-c'])
         connection = engine.connect().execution_options(stream_results=True)
         tags = api.iov_listtags(connection,tagname=parseresult['--name'],datasource=parseresult['--datasource'],applyto=parseresult['--applyto'],isdefault=parseresult['--default-only'])
         ofile = '-'
         if parseresult['--name']:
             tagid = tags.keys()[0]
             sinces = [x for x in tags[tagid].keys() if isinstance(x,int)]
             payloadids = [tags[tagid][since]['payloadid'] for since in sinces]
             payloadcomments = [tags[tagid][since]['payloadcomment'] for since in sinces]
             payloaddict = api.iov_parsepayloaddatadict(tags[tagid]['datadict'])
             fieldalias = [field['alias']or'v_'+str(field_idx) for field_idx,field in enumerate(payloaddict)]
             datahead = ' '.join(fieldalias)
             maxnitems = tags[tagid]['maxnitems']
             header = ['since', 'comment',datahead]
             ofile = parseresult['-o']             
             results = []
             for payloadidx,payloadid in enumerate(payloadids):
                 tagdetails = api.iov_getpayload(connection,payloadid,payloaddict,maxnitems=maxnitems)
                 results.append([sinces[payloadidx],payloadcomments[payloadidx],tagdetails])

             if parseresult['-o'] or parseresult['--output-style']=='csv':
                 with api.smart_open(ofile) as fh:
                     print >> fh, '#'+','.join(header)
                     csvwriter = csv.writer(fh)
                     for row in results:
                         csvwriter.writerow(row)
             else:
                 ptable = prettytable.PrettyTable(header)                 
                 ptable.align = 'l'
                 ptable.header_style = 'cap'
                 ptable.max_width['params']=60
                 for [s,c,d] in results:
                     dataitems = []
                     for item in d:
                         fieldstr = []
                         for field in item:
                            if isinstance(field,list):
                                val = ','.join([str(f) for f in field])
                            if len(field) >1:
                                val = '['+val+']'
                            else:
                                val = str(field)
                         fieldstr.append(val)
                     dataitems.append( ' '.join(fieldstr) )
                     ptable.add_row([s,c,'\n'.join(dataitems)])
                 if parseresult['--output-style']=='tab':
                     print(ptable)
                 elif parseresult['--output-style']=='html' :
                     print(ptable.get_html_string())
                 else:
                     raise RuntimeError('Unsupported output style %s'%parseresult['--output-style'])
         else:
             header = ['name','creation','default','datasource','applyto','payload','items','iov','comment']
             if parseresult['-o'] or parseresult['--output-style']=='csv':
                 ofile = parseresult['-o']
                 with api.smart_open(ofile) as fh:
                     print >> fh, '#'+','.join(header)
                     csvwriter = csv.writer(fh)
                     for tagid,tag in tags.items():
                         sinceStr = str([x for x in tag.keys() if isinstance(x,int)])
                         csvwriter.writerow([tag['tagname'],tag['creationutc'],tag['isdefault'],tag['datasource'],tag['applyto'],tag['datadict'],tag['maxnitems'],sinceStr,tag['tagcomment']])                     
             else:
                 ptable = prettytable.PrettyTable(header)
                 ptable.sortby = 'applyto'
                 ptable.align = 'l'
                 ptable.header_style = 'cap'
                 ptable.max_width['params']=60
                 for tagid,tag in tags.items():
                     sinceStr = '\n'.join([str(x) for x in tag.keys() if isinstance(x,int)])
                     ptable.add_row([tag['tagname'],tag['creationutc'],tag['isdefault'],tag['datasource'],tag['applyto'],tag['datadict'],tag['maxnitems'],sinceStr,tag['tagcomment']])
                 if parseresult['--output-style']=='tab':
                     print(ptable)
                 elif parseresult['--output-style']=='html' :
                     print(ptable.get_html_string())
                 else:
                     raise RuntimeError('Unsupported output style %s'%parseresult['--output-style'])
      
      else:
          exit("%r is not a briltag command. See 'briltag --help'."%args['<command>']) 
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
    log.debug('create arguments: %s',parseresult)

if __name__ == '__main__':
    briltag_main()
