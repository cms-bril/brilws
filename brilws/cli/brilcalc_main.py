import sys,logging
import docopt
import schema
import brilws
import prettytable
import pandas as pd
import numpy as np
from brilws import api,params,clicommonargs,display
import re,time, csv
from datetime import datetime
from sqlalchemy import *

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
          
          ##parse selection params
          lumiargs = clicommonargs.parser(parseresult)

          ##db params
          dbengine = create_engine(lumiargs.dbconnect)
          authpath = lumiargs.authpath          

          ##display params
          csize = lumiargs.chunksize
          withBX = lumiargs.withBX          
          byls = lumiargs.byls
          bxcsize = csize
          totable = lumiargs.totable
          fh = None
          ptable = None
          csvwriter = None

          header = summaryheader = ['fill','run','time','nls','ncms','delivered','recorded']
          footer = ['nfill','nrun','nls','ncms','delivered','recorded']
          bylsheader = ['fill','run','ls','time','cms','delivered','recorded','avgpu','source']
          xingheader = ['fill','run','ls','bx','delivered','recorded']

          if withBX:
              bxcsize = csize*3564
              header = xingheader
          elif byls:
              header = bylsheader

          if not totable:
              fh = lumiargs.ofilehandle
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)

          datatagname = lumiargs.datatagname
          datatagnameid = 0
          if not datatagname:
              r = api.max_datatagname(dbengine)
              if not r:
                  raise 'no tag found'
              datatagname = r[0]
              datatagnameid = r[1]
          else:
              datatagnameid = api.datatagnameid(dbengine,datatagname=datatagname)
            
          print '#Data tag : ',datatagname
          nchunk = 0
          it = api.datatagIter(dbengine,datatagnameid,fillmin=lumiargs.fillmin,fillmax=lumiargs.fillmax,runmin=lumiargs.runmin,runmax=lumiargs.runmax,amodetag=lumiargs.amodetag,targetegev=lumiargs.egev,beamstatus=lumiargs.beamstatus,tssecmin=lumiargs.tssecmin,tssecmax=lumiargs.tssecmax,runlsselect=lumiargs.runlsSeries,chunksize=csize)
          if not it: exit(1)

          tot_nfill = 0
          tot_nrun = 0
          tot_nls = 0
          tot_ncms = 0
          tot_delivered = 0
          tot_recorded = 0
          runtot = {} #{run:{'fill':fill,'time':time,'nls':nls,'':ncms,'delivered':delivered,'recorded':recorded}}
          allfills = []
          
          for idchunk in it:              
              dataids = idchunk.index
              idstrings = ','.join([str(d) for d in dataids])
              print len(dataids)
              
              #for lumichunk in api.lumiInfoIter(dbengine,dataids.min(),dataids.max(),'HFOC','RUN1',chunksize=9999,withBX=withBX):
              for lumichunk in api.lumiInfoIter(dbengine,dataids,'HFOC','RUN1',chunksize=9999,withBX=withBX):
                  finalchunk = idchunk.join(lumichunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)

                  if byls or withBX:
                      if totable:
                          if not nchunk:
                              ptable = display.create_table(header,header=True)
                          else:
                              ptable = display.create_table(header,header=False)
                      for datatagid,row in finalchunk.iterrows():
                          timestampsec = row['timestampsec']
                          dtime = datetime.fromtimestamp(int(timestampsec)).strftime(params._datetimefm)
                          cms = 1
                          
                          if fh:
                              if byls:
                                  csvwriter.writerow( [row['fillnum'],row['runnum'],row['lsnum'],dtime,cms,'%.4e'%(row['avgrawlumi']),'%.4e'%(row['avgrawlumi']),'%.4e'%(row['avgrawlumi']*0.5),'HFOC'] )
                              else:
                                  csvwriter.writerow( [row['fillnum'],row['runnum'],row['lsnum'],row['bxidx'],'%.6e'%(row['bxrawlumi']),'%.6e'%(row['bxrawlumi'])] )
                          else:
                              if byls:
                                  ptable.add_row( [row['fillnum'],row['runnum'],row['lsnum'],dtime,cms,'%.4e'%(row['avgrawlumi']),'%.4e'%(row['avgrawlumi']),'%.4e'%(row['avgrawlumi']*0.5),'HFOC'] )
                              else:
                                  ptable.add_row( [row['fillnum'],row['runnum'],row['lsnum'],row['bxidx'],'%.6e'%(row['bxrawlumi']),'%.6e'%(row['bxrawlumi'])] )
                      if lumiargs.outputstyle=='tab':
                          print(ptable)
                      elif lumiargs.outputstyle=='htlm':
                          print(ptable.get_html_string())
                      del ptable
                      ptable = None
                      
                  if not withBX:   
                      finalchunk.reset_index()
                      rungrouped = finalchunk.groupby('runnum', as_index=False)    
                      for run, items in rungrouped:
                          if not runtot.has_key(run):
                              runtot[run] = {}
                              fillnum = items['fillnum'].iloc[0]
                              runtot[run]['fill'] = fillnum
                              timestampsec_run = items['timestampsec'].iloc[0]
                              dtime = datetime.fromtimestamp(int(timestampsec_run)).strftime(params._datetimefm)
                              runtot[run]['time'] = dtime
                              runtot[run]['nls'] = 0
                              runtot[run]['ncms'] = 0
                              runtot[run]['delivered'] = 0.
                              runtot[run]['recorded'] = 0.
                              allfills.append(fillnum)                           
                          runtot_countls =  items['lsnum'].count()
                          runtot_avgrawlumi = items['avgrawlumi'].sum()
                          runtot[run]['nls'] = runtot[run]['nls']+runtot_countls
                          tot_nls = tot_nls +  runtot_countls
                          runtot[run]['ncms'] = runtot[run]['ncms']+runtot_countls
                          tot_ncms = tot_ncms +  runtot_countls
                          runtot[run]['delivered'] = runtot[run]['delivered']+runtot_avgrawlumi
                          tot_delivered = tot_delivered + runtot_avgrawlumi
                          runtot[run]['recorded'] = runtot[run]['recorded']+runtot_avgrawlumi
                          tot_recorded = tot_recorded + runtot_avgrawlumi
                      
                  del finalchunk
                  del lumichunk
              del idchunk
          np_allfills = np.array(allfills)
          tot_nfill = len(np.unique(np_allfills))
          tot_nrun = len(runtot.keys())
          
          if not byls and not withBX: #run table
              if totable:
                  ptable = display.create_table(header)
              for run in sorted(runtot):
                  if fh:
                      csvwriter.writerow( [runtot[run]['fill'],run,runtot[run]['time'],runtot[run]['nls'],runtot[run]['ncms'],'%.3e'%(runtot[run]['delivered']),'%.3e'%(runtot[run]['recorded'])] )
                  else:
                      ptable.add_row( [runtot[run]['fill'],run,runtot[run]['time'],runtot[run]['nls'],runtot[run]['ncms'],'%.3e'%(runtot[run]['delivered']),'%.3e'%(runtot[run]['recorded'])] )
              if lumiargs.outputstyle=='tab':
                  print(ptable)
              elif lumiargs.outputstyle=='html' :
                  print(ptable.get_html_string())

          # common footer
          if not withBX:
              if totable:
                  ftable = display.create_table(footer)
                  if lumiargs.outputstyle=='tab':
                      ftable.add_row( [ tot_nfill,tot_nrun,tot_nls,tot_ncms,'%.3e'%(tot_delivered),'%.3e'%(tot_recorded) ] )
                      print "#Total: "
                      print(ftable)
                  elif lumiargs.outputstyle=='html' :
                      ftable.add_row( [ tot_nfill,tot_nrun,tot_nls,tot_ncms,'%.3e'%(tot_delivered),'%.3e'%(tot_recorded) ] )
                      print "Total: "
                      print(ftable)
              else:
                  print >> fh, '#Total:'                  
                  print >> fh, '#'+','.join(footer)
                  print >> fh, '#'+','.join( [ '%d'%tot_nfill,'%d'%tot_nrun,'%d'%tot_nls,'%d'%tot_ncms,'%.3e'%(tot_delivered),'%.3e'%(tot_recorded) ] )
          if fh and fh is not sys.stdout: fh.close()
          
      elif args['<command>'] == 'beam':
          import brilcalc_beam

          parseresult = docopt.docopt(brilcalc_beam.__doc__,argv=cmmdargv)
          parseresult = brilcalc_beam.validate(parseresult)
          
          ##parse selection params
          beamargs = clicommonargs.parser(parseresult)

          ##db params
          dbengine = create_engine(beamargs.dbconnect)
          authpath = beamargs.authpath
                    
          ##display params
          csize = beamargs.chunksize
          withBX = beamargs.withBX
          bxcsize = csize
          totable = beamargs.totable
          fh = None
          ptable = None
          csvwriter = None

          header = ['fill','run','ls','time','beamstatus','amodetag','egev','intensity1','intensity2']
          bxheader = ['fill','run','ls','time','bx','bxintensity1','bxintensity2','iscolliding']
          if withBX:
              bxcsize = csize*3564
              header = bxheader
              
          if not totable:
              fh = beamargs.ofilehandle
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)

          datatagname = beamargs.datatagname
          datatagnameid = 0
          if not datatagname:
              r = api.max_datatagname(dbengine)
              if not r:
                  raise 'no tag found'
              datatagname = r[0]
              datatagnameid = r[1]
          else:
              datatagnameid = api.datatagnameid(dbengine,datatagname=datatagname)
          print 'data tag : ',datatagname          
          
          nchunk = 0
          it = api.datatagIter(dbengine,datatagnameid,fillmin=beamargs.fillmin,fillmax=beamargs.fillmax,runmin=beamargs.runmin,runmax=beamargs.runmax,amodetag=beamargs.amodetag,targetegev=beamargs.egev,beamstatus=beamargs.beamstatus,tssecmin=beamargs.tssecmin,tssecmax=beamargs.tssecmax,runlsselect=beamargs.runlsSeries ,chunksize=csize)
          if not it: exit(1)
          for idchunk in it:              
              dataids = idchunk.index              
              for beaminfochunk in api.beamInfoIter(dbengine,dataids,'RUN1',chunksize=bxcsize,withBX=withBX):
                  finalchunk = idchunk.join(beaminfochunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                  if totable:
                      if not nchunk:
                          ptable = display.create_table(header,header=True)
                      else:
                          ptable = display.create_table(header,header=False)
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

                  if beamargs.outputstyle=='tab':
                      print(ptable)
                      del ptable
                  elif beamargs.outputstyle=='html' :
                      print(ptable.get_html_string())
                      del ptable
                  del finalchunk  
                  del beaminfochunk
              del idchunk
              nchunk = nchunk + 1                  
          if fh and fh is not sys.stdout: fh.close()  
        
      elif args['<command>'] == 'trg':
          import brilcalc_trg
          parseresult = docopt.docopt(brilcalc_trg.__doc__,argv=cmmdargv)
          parseresult = brilcalc_trg.validate(parseresult)
          
          ##parse selection params
          trgargs = clicommonargs.parser(parseresult)

          ##db params
          dbengine = create_engine(trgargs.dbconnect)
          authpath = trgargs.authpath

          ##display params
          csize = trgargs.chunksize
          bybit = trgargs.bybit
          totable = trgargs.totable
          fh = None
          ptable = None
          csvwriter = None

          header = ['fill','run','ls','time','deadfrac']          
          bybitheader = ['fill','run','ls','id','name','prescidx','presc','counts','mask']
          if bybit:
              csize = csize*200
              header = bybitheader
          if not totable:
              fh = trgargs.ofilehandle
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)

          datatagname = trgargs.datatagname
          datatagnameid = 0
          if not datatagname:
              r = api.max_datatagname(dbengine)
              if not r:
                  raise 'no tag found'
              datatagname = r[0]
              datatagnameid = r[1]
          else:
              datatagnameid = api.datatagnameid(dbengine,datatagname=datatagname)
          print 'data tag : ',datatagname
          
          nchunk  = 0
          it = api.datatagIter(dbengine,datatagnameid,fillmin=trgargs.fillmin,fillmax=trgargs.fillmax,runmin=trgargs.runmin,runmax=trgargs.runmax,amodetag=trgargs.amodetag,targetegev=trgargs.egev,beamstatus=trgargs.beamstatus,tssecmin=trgargs.tssecmin,tssecmax=trgargs.tssecmax,runlsselect=trgargs.runlsSeries ,chunksize=csize)
          if not it: exit(1)
          for idchunk in it:              
              dataids = idchunk.index
              if not bybit:
                  for deadtimechunk in api.deadtimeIter(dbengine,dataids,'RUN1',chunksize=csize):
                      finalchunk = idchunk.join(deadtimechunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                      if totable:
                          if not nchunk:
                              ptable = display.create_table(header,header=True)
                          else:
                              ptable = display.create_table(header,header=False)
                      for datatagid,row in finalchunk.iterrows():
                          timestampsec = row['timestampsec']
                          dtime = datetime.fromtimestamp(int(timestampsec)).strftime(params._datetimefm)                          
                          if fh:
                              csvwriter.writerow( [row['fillnum'],row['runnum'],row['lsnum'],dtime,'%.4f'%(row['deadtimefrac']) ] )
                          else:
                              ptable.add_row( [ row['fillnum'], row['runnum'], row['lsnum'],dtime,'%.4f'%row['deadtimefrac']] )
                      del finalchunk
                      del deadtimechunk
              else:
                  print 'blah'
              if trgargs.outputstyle=='tab':
                  print(ptable)
                  del ptable
              elif trgargs.outputstyle=='tab':
                  print (ptable.get_html_string())
                  del ptable
              del idchunk
              nchunk = nchunk + 1
              
          if fh and fh is not sys.stdout: fh.close()
          
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
