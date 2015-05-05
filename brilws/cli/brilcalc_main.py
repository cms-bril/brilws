import sys,logging
import docopt
import schema
import brilws
import prettytable
from brilws import api,params
import re,time
from datetime import datetime
log = logging.getLogger('brilcalc')
logformatter = logging.Formatter('%(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#fh = logging.FileHandler('/tmp/brilcalc.log')
ch.setFormatter(logformatter)
#fh.setFormatter(logformatter)
log.addHandler(ch)
#log.addHandler(fh)

def brilcalc_main():
    docstr='''

    usage:
      brilcalc (-h|--help) 
      brilcalc --version
      brilcalc --checkforupdate
      brilcalc [--debug|--nowarning] <command> [<args>...]

    commands:
      lumi Luminosity
      beam Beam       
      trg  L1 trigger
      hlt  HLT
      bkg  Background
      
    See 'brilcalc <command> --help' for more information on a specific command.

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
      if args['<command>'] == 'lumi':
          import brilcalc_lumi

          parseresult = docopt.docopt(brilcalc_lumi.__doc__,argv=cmmdargv)
          parseresult = brilcalc_lumi.validate(parseresult)

      elif args['<command>'] == 'beam':
          import brilcalc_beam
          import csv
          from sqlalchemy import *

          parseresult = docopt.docopt(brilcalc_beam.__doc__,argv=cmmdargv)
          parseresult = brilcalc_beam.validate(parseresult)

          ##db params
          dbengine = create_engine(parseresult['-c'])
          authpath = parseresult['-p']
          
          ##selection params
          s_bstatus = parseresult['--beamstatus']
          if s_bstatus: s_bstatus = s_bstatus.upper()
          s_egev = parseresult['--beamegev']
          s_amodetag = parseresult['--amodetag']
          if s_amodetag: s_amodetag = s_amodetag.upper()
          s_datatagname = parseresult['--datatag']
          s_fillmin = None
          s_fillmax = None
          if parseresult['-f'] :
              s_fillmin = parseresult['-f']
              s_fillmax = parseresult['-f']
          s_runmin = None
          s_runmax = None
          if parseresult['-r']:
              s_runmin = parseresult['-r']
              s_runmax = parseresult['-r']
              
          s_beg = None
          s_end = None
          if not parseresult['-f'] and not parseresult['-r']:
              if parseresult['--begin']:
                  s_beg = parseresult['--begin']                  
                  for style,pattern in {'fill':params._fillnum_pattern,'run':params._runnum_pattern, 'time':params._time_pattern}.items():
                      if re.match(pattern,s_beg):
                          if style=='fill':
                              s_fillmin = int(s_beg)
                          elif style=='run':
                              s_runmin = int(s_beg)
                          elif style=='time':
                              s_tssecmin = int(time.mktime(datetime.strptime(s_beg,params._datetimefm).timetuple()))
              if parseresult['--end']:
                  s_end = parseresult['--end']                  
                  for style,pattern in {'fill':params._fillnum_pattern,'run':params._runnum_pattern, 'time':params._time_pattern}.items():
                      if re.match(pattern,s_end):
                          if style=='fill':
                              s_fillmax = int(s_end)
                          elif style=='run':
                              s_runmax = int(s_end)
                          elif style=='time':
                              s_tssecmax = int(time.mktime(datetime.strptime(s_end,params._datetimefm).timetuple()))

          csize = parseresult['--chunk-size']
          bxcsize = csize
          withBX = False
          
          header = ['fill','run','ls','time','beamstatus','amodetag','beamegev','intensity1','intensity2']
          if parseresult['--xing']:
              withBX = True
              header = ['fill','run','ls','time','bx','bxintensity1','bxintensity2','iscolliding']
              bxcsize = csize*3564
              
          ofile = '-'
          fh = None
          totable = False
          ptable = None
          csvwriter = None
          if parseresult['-o'] or parseresult['--output-style']=='csv':
              if parseresult['-o']:
                  ofile = parseresult['-o']
                  fh = open(ofile,'w')
              else:
                  fh = sys.stdout
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              totable = True
          
          nchunk = 0
          it = api.datatagIter(dbengine,0,fillmin=s_fillmin,fillmax=s_fillmax,runmin=s_runmin,runmax=s_runmax,amodetag=s_amodetag,targetegev=s_egev,beamstatus=s_bstatus,tssecmin=s_tssecmin,tssecmax=s_tssecmax,chunksize=csize)
          if not it: exit(1)
          for idchunk in it:              
              dataids = idchunk.index              
              for beaminfochunk in api.beamInfoIter(dbengine,dataids.min(),dataids.max(),chunksize=bxcsize,withBX=withBX):
                  finalchunk = idchunk.join(beaminfochunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                  if totable:
                      ptable = prettytable.PrettyTable(header)
                      if not nchunk:
                          ptable.header = True
                      else:
                          ptable.header = False
                      ptable.align = 'l'
                      ptable.max_width['params']=80 
                  for datatagid,row in finalchunk.iterrows():
                      timestampsec = row['timestampsec']
                      dtime = datetime.fromtimestamp(int(timestampsec)).strftime(params._datetimefm)
                      if fh:
                          if not withBX:
                              csvwriter.writerow([row['fillnum'],row['runnum'],row['lsnum'],dtime,row['beamstatus'],row['amodetag'],'%.2f'%(row['egev']),'%.6e'%(row['intensity1']),'%.6e'%(row['intensity2'])])
                          else:
                              csvwriter.writerow([ row['fillnum'],row['runnum'],row['lsnum'],dtime,row['bxidx'],'%.6e'%(row['bxintensity1']),'%.6e'%(row['bxintensity2']),row['iscolliding'] ])
                      else:
                          if not withBX:
                              ptable.add_row([row['fillnum'],row['runnum'],row['lsnum'],dtime,row['beamstatus'],row['amodetag'],'%.2f'%(row['egev']),'%.6e'%(row['intensity1']),'%.6e'%(row['intensity2'])])
                          else:
                              ptable.add_row([row['fillnum'],row['runnum'],row['lsnum'],dtime,row['bxidx'],'%.6e'%(row['bxintensity1']),'%.6e'%(row['bxintensity2']),row['iscolliding'] ])

                  if parseresult['--output-style']=='tab':
                      print(ptable)
                      del ptable
                  elif parseresult['--output-style']=='html' :
                      print(ptable.get_html_string())
                      del ptable

                  del finalchunk  
                  nchunk = nchunk + 1                  
                  del beaminfochunk
              del idchunk
          if fh and fh is not sys.stdout: fh.close()  

        
      elif args['<command>'] == 'trg':
          import brilcalc_trg
          parseresult = docopt.docopt(brilcalc_trg.__doc__,argv=cmmdargv)
          parseresult = brilcalc_trg.validate(parseresult)
          
      elif args['<command>'] == 'hlt':
          import brilcalc_hlt
          parseresult = docopt.docopt(brilcalc_hlt.__doc__,argv=cmmdargv)
          parseresult = brilcalc_hlt.validate(parseresult)

      elif args['<command>'] == 'bkg':
          exit("bkg is not implemented")
      else:
          exit("%r is not a brilcalc command. See 'brilcalc --help'."%args['<command>'])
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)

    if not parseresult['--debug'] :
       if parseresult['--nowarning']:
          log.setLevel(logging.ERROR)
          ch.setLevel(logging.ERROR)
    else:
       log.setLevel(logging.DEBUG)
       ch.setLevel(logging.DEBUG)
       log.debug('create arguments: %s',parseresult)

if __name__ == '__main__':
    brilcalc_main()
