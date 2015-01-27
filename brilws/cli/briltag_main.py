import sys,logging,os,gc
import docopt,schema
import coral
import numpy as np
import pandas as pd
import brilws
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

def combine_params(groups):
    '''
    paramname:combine paramname and not null param into one row of [('paramname',paramval)]
    '''
    result = pd.concat([groups['f4'].dropna(),groups['s'].dropna(),groups['u4'].dropna()])
    groups.drop('f4',axis=1,inplace=True)
    groups.drop('s',axis=1,inplace=True)
    groups.drop('u4',axis=1,inplace=True)
    groups.drop('i4',axis=1,inplace=True)
    groups.drop('u8',axis=1,inplace=True)
    groups.drop('i8',axis=1,inplace=True)
    groups.drop('u2',axis=1,inplace=True)
    groups.drop('i2',axis=1,inplace=True)
    groups.drop('u1',axis=1,inplace=True)
    groups.drop('i1',axis=1,inplace=True)
    result = zip( groups['paramname'].values,result.values)
    groups['params']= ' '.join(map(display.formatter_tuple,result))
    groups.drop('paramname',axis=1,inplace=True)
    return groups
    
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
    svc = coral.ConnectionService()    
    try:      
      if args['<command>'] == 'list':
        
         import briltag_list
         from sqlalchemy import * 
         parseresult = docopt.docopt(briltag_list.__doc__,argv=cmmdargv)
         parseresult = briltag_list.validate(parseresult,sources=choice_sources,applyto=choice_applyto,ostyles=choices_ostyles)
         engine = create_engine(parseresult['-c'])
         connection = engine.connect().execution_options(stream_results=True)
         tags = api.iov_listtags(connection,tagname=parseresult['--name'],datasource=parseresult['--datasource'],applyto=parseresult['--applyto'],isdefault=parseresult['--default-only'])
         ptable =  prettytable.PrettyTable(['name','creation','default','datasource','applyto','payload','items','iov','comment'])
         ptable.sortby = 'applyto'
         ptable.align = 'l'
         ptable.header_style = 'cap'
         ptable.max_width['params']=60
         for tagid,tag in tags.items():
             sinceStr = '\n'.join([str(x) for x in tag.keys() if isinstance(x,int)])
             ptable.add_row([tag['tagname'],tag['creationutc'],tag['isdefault'],tag['datasource'],tag['applyto'],tag['datadict'],tag['maxnitems'],sinceStr,tag['tagcomment']])
         print(ptable)         
         """
         grouped =  taginfo_df.groupby(['since'])
         result = grouped.apply(combine_params).loc[:,['since','amodetag','egev','minbiasxsec','func','params','comment']]
         del taginfo_df
         gc.collect()
                
         if parseresult['--output-style'].lower() in ['tab','html']:           
             x = prettytable.PrettyTable(['since','amodetag','egev','minbiasxsec','func','params','comment'])
             x.sortby = 'since'
             x.align = 'l'
             x.header_style = 'cap'
             x.max_width['params']=60
             map(x.add_row,result.values) 
             if parseresult['--output-style'].lower()=='tab':
                 print '\n%s'%tagname
                 print(x)
             else:
                 print tagname
                 print(x.get_html_string())
         else:
             print '#%s'%tagname
             print result.to_csv(header=['#since','amodetag','egev','minbiasxsec','func','params','comment'],index=False)
         else:
             if regdf.empty:
                print 'No result found'
             else:
                t = regdf.loc[:,['tagname','datasource','applyto','isdefault','creation time','comment']]
                if parseresult['--output-style'].lower() in ['tab','html']:   
                    x = prettytable.PrettyTable(['tagname','datasource','applyto','isdefault','creation time','comment'])
                    x.sortby = 'applyto'
                    x.align = 'l'
                    x.header_style = 'cap'
                    map(x.add_row,t.values)
                    if parseresult['--output-style'].lower()=='tab':
                        print(x)
                    else:
                        print(x.get_html_string())
                else:
                    print t.to_csv(header=['#tagname','datasource','applyto','isdefault','creation time','comment'],index=False)

"""
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
