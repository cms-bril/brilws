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
          it = api.datatagIter(dbengine,datatagnameid,fillmin=lumiargs.fillmin,fillmax=lumiargs.fillmax,runmin=lumiargs.runmin,runmax=lumiargs.runmax,amodetag=lumiargs.amodetag,targetegev=lumiargs.egev,beamstatus=lumiargs.beamstatus,tssecmin=lumiargs.tssecmin,tssecmax=lumiargs.tssecmax,runlsselect=lumiargs.runlsSeries,chunksize=csize,slim=True)
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
              
              for lumichunk in api.lumiInfoIter(dbengine,dataids,'HFOC','RUN1',chunksize=csize,withBX=withBX):
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
                          if byls:
                              print row['fillnum']
                              display.add_row( [row['fillnum'],row['runnum'],row['lsnum'],dtime,cms,'%.4e'%(row['avgrawlumi']),'%.4e'%(row['avgrawlumi']),'%.4e'%(row['avgrawlumi']*0.5),'HFOC'] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                          else:
                              display.add_row( [row['fillnum'],row['runnum'],row['lsnum'],row['bxidx'],'%.6e'%(row['bxrawlumi']),'%.6e'%(row['bxrawlumi'])] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                      display.show_table(ptable,lumiargs.outputstyle)                              
                      if ptable: del ptable
                      
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
                  display.add_row(  [runtot[run]['fill'],run,runtot[run]['time'],runtot[run]['nls'],runtot[run]['ncms'],'%.3e'%(runtot[run]['delivered']),'%.3e'%(runtot[run]['recorded'])] , fh=fh, csvwriter=csvwriter, ptable=ptable)
              display.show_table(ptable,lumiargs.outputstyle)    

          # common footer
          if not withBX:
              if totable:
                  ftable = display.create_table(footer)
                  display.add_row( [ tot_nfill,tot_nrun,tot_nls,tot_ncms,'%.3e'%(tot_delivered),'%.3e'%(tot_recorded) ], fh=None, csvwriter=None, ptable=ftable)
                  print "#Total: "
                  display.show_table(ftable,lumiargs.outputstyle);                  
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
          bxheader = ['fill','run','ls','bx','bxintensity1','bxintensity2','iscolliding']
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

          it = api.datatagIter(dbengine,datatagnameid,fillmin=beamargs.fillmin,fillmax=beamargs.fillmax,runmin=beamargs.runmin,runmax=beamargs.runmax,amodetag=beamargs.amodetag,targetegev=beamargs.egev,beamstatus=beamargs.beamstatus,tssecmin=beamargs.tssecmin,tssecmax=beamargs.tssecmax,runlsselect=beamargs.runlsSeries ,chunksize=csize,slim=withBX )
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
                      if not withBX:
                          timestampsec = row['timestampsec']
                          dtime = datetime.fromtimestamp(int(timestampsec)).strftime(params._datetimefm)
                          display.add_row( [row['fillnum'],row['runnum'],row['lsnum'],dtime,row['beamstatus'],row['amodetag'],'%.2f'%(row['egev']),'%.6e'%(row['intensity1']),'%.6e'%(row['intensity2'])] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                      else:
                          display.add_row( [ row['fillnum'],row['runnum'],row['lsnum'],row['bxidx'],'%.6e'%(row['bxintensity1']),'%.6e'%(row['bxintensity2']),row['iscolliding'] ], fh=fh, csvwriter=csvwriter, ptable=ptable)
                      
                  display.show_table(ptable,beamargs.outputstyle)
                  if ptable: del ptable                  
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
          bybitheader = ['fill','run','ls','id','name','prescidx','presc','counts']
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
          it = api.datatagIter(dbengine,datatagnameid,fillmin=trgargs.fillmin,fillmax=trgargs.fillmax,runmin=trgargs.runmin,runmax=trgargs.runmax,amodetag=trgargs.amodetag,targetegev=trgargs.egev,beamstatus=trgargs.beamstatus,tssecmin=trgargs.tssecmin,tssecmax=trgargs.tssecmax,runlsselect=trgargs.runlsSeries ,chunksize=csize, slim=True)
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
                          display.add_row( ['%d'%row['fillnum'],'%d'%row['runnum'],'%d'%row['lsnum'],dtime,'%.4f'%(row['deadtimefrac']) ] , fh=fh, csvwriter=csvwriter, ptable=ptable )
                      del finalchunk
                      del deadtimechunk
              else:
                  for trginfochunk in api.trgInfoIter(dbengine,dataids,'RUN1',schemaname='',bitnamepattern=trgargs.name,chunksize=csize*192):
                      finalchunk = idchunk.join(trginfochunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                      if totable:
                          if not nchunk:
                              ptable = display.create_table(header,header=True)
                          else:
                              ptable = display.create_table(header,header=False)
                      for datatagid,row in finalchunk.iterrows():
                          display.add_row( [ '%d'%row['fillnum'],'%d'%row['runnum'],'%d'%row['lsnum'],'%d'%row['bitid'],row['bitname'],'%d'%row['prescidx'],'%d'%row['presc'],'%d'%row['counts'] ], fh=fh, csvwriter=csvwriter, ptable=ptable )
                      del finalchunk
                      del trginfochunk
                      
              display.show_table(ptable,trgargs.outputstyle)
              if ptable: del ptable             
              del idchunk
              nchunk = nchunk + 1
              
          if fh and fh is not sys.stdout: fh.close()
          
      elif args['<command>'] == 'hlt':
          import brilcalc_hlt
          parseresult = docopt.docopt(brilcalc_hlt.__doc__,argv=cmmdargv)
          parseresult = brilcalc_hlt.validate(parseresult)
          ##parse selection params
          hltargs = clicommonargs.parser(parseresult)

          ##db params
          dbengine = create_engine(hltargs.dbconnect)
          authpath = hltargs.authpath

          ##display params
          csize = hltargs.chunksize
          bybit = hltargs.bybit
          totable = hltargs.totable
          fh = None
          ptable = None
          csvwriter = None

          header = ['fill','run','hltkey','hltpath','l1seed']

          if not totable:
              fh = hltargs.ofilehandle
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
              
          datatagname = hltargs.datatagname
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

          it = api.rundatatagIter(dbengine,datatagnameid,fillmin=hltargs.fillmin,fillmax=hltargs.fillmax,runmin=hltargs.runmin,runmax=hltargs.runmax,amodetag=hltargs.amodetag,targetegev=hltargs.egev,tssecmin=hltargs.tssecmin,tssecmax=hltargs.tssecmax,runlsselect=hltargs.runlsSeries ,chunksize=csize)
          if not it: exit(1)
          
          for idchunk in it:
              rundataids = idchunk.index
              
              for runchunk in api.runinfoIter(dbengine,rundataids,chunksize=csize,fields=['hltconfigid','hltkey']):
                  finalchunk = idchunk.join(runchunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                  if totable:
                      if not nchunk:
                          ptable = display.create_table(header,header=True)
                      else:
                          ptable = display.create_table(header,header=False)
                  for rundatatagid,row in finalchunk.iterrows():
                      fill = row['fillnum']
                      run = row['runnum']
                      hltkey = row['hltkey']
                      hltconfigid = row['hltconfigid']
                      for hltpathchunk in api.hltl1seedinfoIter(dbengine,hltconfigid,hltpathnameorpattern=hltargs.name,):
                          for idx,hltpathinfo in hltpathchunk.iterrows():                              
                              hltpathname = hltpathinfo['hltpath']                              
                              l1seedexp = hltpathinfo['l1seed']
                              l1bits = api.findUniqueSeed(hltpathname,l1seedexp)
                              if l1bits is not None:
                                  l1logic = str(l1bits[1])
                                  if not l1bits[0]:
                                      l1logic = l1bits[1][0]
                                  else:
                                      l1logic = l1bits[0]+' '+' '.join(l1bits[1])
                                  display.add_row( [fill,run,hltkey,hltpathname,l1logic], fh=fh, csvwriter=csvwriter, ptable=ptable )
                          del hltpathchunk
                  nchunk = nchunk+1
                  del finalchunk
                  if ptable:
                      ptable.max_width['hltkey']=20
                      ptable.max_width['hltpath']=60
                      ptable.max_width['l1seed']=20
                      ptable.align='l'
                      display.show_table(ptable,hltargs.outputstyle)

                      
          if fh and fh is not sys.stdout: fh.close()
          
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
