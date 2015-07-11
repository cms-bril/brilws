import sys
import os
sys.path.insert(0,os.path.dirname(sys.executable)+'/../lib/python2.7/site-packages/') #ignore other PYTHONPATH

import logging
import docopt
import schema
import brilws
import prettytable
import pandas as pd
import numpy as np
from brilws import api,params,display,formatter,lumiParameters
from brilws.cli import clicommonargs
import re,time, csv
from datetime import datetime
from sqlalchemy import *
import math
from dateutil import tz
import pytz
log = logging.getLogger('brilws')
logformatter = logging.Formatter('%(levelname)s %(name)s %(message)s')
log.setLevel(logging.ERROR)
ch = logging.StreamHandler()
ch.setFormatter(logformatter)
log.addHandler(ch)

class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream
    def write(self, data):
        self.stream.write(data)
        self.stream.flush()
    def __getattr__(self, attr):
        return getattr(self.stream.attr)
sys.stdout = Unbuffered(sys.stdout)

lumip = lumiParameters.ParametersObject()
lslengthsec= lumip.lslengthsec()

utctmzone = tz.gettz('UTC')
cerntmzone = tz.gettz('CEST')


def findtagname(dbengine,datatagname,dbschema):
    '''
    output: (datatagname,datatagnameid)
    '''
    datatagnameid=0
    if datatagname:
        datatagnameid = api.datatagnameid(dbengine,datatagname=datatagname,schemaname=dbschema)
    else:
        r = api.max_datatagname(dbengine,schemaname=dbschema)
        if not r:
            raise RuntimeError('no tag found')
        datatagname = r[0]
        datatagnameid = r[1]
    return (datatagname,datatagnameid)

          
def brilcalc_main(progname=sys.argv[0]):

    docstr='''

    usage:
      brilcalc (-h|--help|--version) 
      brilcalc [--debug|--warn] <command> [<args>...]

    commands:
      lumi Luminosity
      beam Beam       
    See 'brilcalc <command> --help' for more information on a specific command.

    '''
    args = {}
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
      if args['<command>'] == 'lumi':
          import brilcalc_lumi          
          parseresult = docopt.docopt(brilcalc_lumi.__doc__,argv=cmmdargv)
          parseresult = brilcalc_lumi.validate(parseresult)
          ##parse selection params
          pargs = clicommonargs.parser(parseresult)
          dbschema = ''
          if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
          dbengine = create_engine(pargs.connecturl)          
          totz=utctmzone
          if pargs.cerntime: totz=cerntmzone
          if pargs.tssec: totz=None
          
          fh = None
          ptable = None
          ftable = None
          csvwriter = None
          vfunc_lumiunit = np.vectorize(formatter.lumiunit)
          header = ['run:fill','time','nls','ncms','delivered(/ub)','recorded(/ub)']
          footer = ['nfill','nrun','nls','ncms','totdelivered(/ub)','totrecorded(/ub)']
          bylsheader = ['run:fill','ls','time','beamstatus','E(GeV)','delivered(/ub)','recorded(/ub)','avgpu','source']
          runtot = {}#{run:{'fill':,'time':,'nls':,'ncms':,'delivered':,'recorded':}}
          if pargs.withBX:
              header = bylsheader+['[bxidx bxdelivered(/ub) bxrecorded(/ub)]']
          elif pargs.byls:
              header = bylsheader
          header = vfunc_lumiunit(header,pargs.scalefactor).tolist()
          footer = vfunc_lumiunit(footer,pargs.scalefactor).tolist()
          
          shards = [3]
          #print pargs.datatagname
          (datatagname,datatagnameid) = findtagname(dbengine,pargs.datatagname,dbschema)
          #print datatagname,datatagnameid
          if not pargs.totable:
              fh = pargs.ofilehandle
              print >> fh, '#Data tag : ',datatagname
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              ptable = display.create_table(header,header=True)
              ftable = display.create_table(footer)
                         
          if datatagnameid==1:
              basetablename = 'online_result'
              source = 'best'
              if pargs.lumitype:
                  source=pargs.lumitype                  
              for shard in shards:
                  tablename = basetablename+'_'+str(shard)
                  shardexists = api.table_exists(dbengine,tablename,schemaname=dbschema)
                  if not shardexists: continue
                  onlineit = None
                  if source == 'best':
                      rfields = ['fillnum','runnum','lsnum','timestampsec','cmson','beamstatusid','targetegev','delivered','recorded','avgpu','datasource']
                      if pargs.withBX: rfields = rfields+['bxdeliveredblob'] 
                      onlineit = api.online_resultIter(dbengine,tablename,schemaname=dbschema,fillmin=pargs.fillmin,fillmax=pargs.fillmax,runmin=pargs.runmin,runmax=pargs.runmax,amodetagid=pargs.amodetagid,targetegev=pargs.egev,beamstatusid=pargs.beamstatusid,tssecmin=pargs.tssecmin,tssecmax=pargs.tssecmax,runlsselect=pargs.runlsSeries,chunksize=None,fields=rfields,sorted=True)
                  else:
                      rfields = ['avglumi']
                      idfields = ['fillnum','runnum','lsnum','timestampsec','beamstatusid','cmson','deadtimefrac','targetegev']
                      if pargs.withBX: rfields = rfields+['bxlumiblob']
                      onlineit = api.resultDataIter(dbengine,source,shard,datafields=rfields,idfields=idfields,schemaname=dbschema,fillmin=pargs.fillmin,fillmax=pargs.fillmax,runmin=pargs.runmin,runmax=pargs.runmax,amodetagid=pargs.amodetagid,targetegev=pargs.egev,beamstatusid=pargs.beamstatusid,tssecmin=pargs.tssecmin,tssecmax=pargs.tssecmax,runlsselect=pargs.runlsSeries,sorted=True)
                  if not onlineit: continue
                  for row in onlineit:
                      fillnum = row['fillnum']
                      runnum = row['runnum']
                      lsnum = row['lsnum']
                      cmslsnum = lsnum
                      timestampsec = row['timestampsec']
                      #print timestampsec
                      cmson = row['cmson']
                      if not cmson:
                          cmslsnum = 0
                      beamstatusid = row['beamstatusid']
                      beamstatus = params._idtobeamstatus[beamstatusid]
                      tegev = row['targetegev']
                      dtime = str(timestampsec)
                      if totz is not None:
                          d = datetime.fromtimestamp(int(timestampsec),tz=pytz.utc)
                          dtime = d.astimezone(totz).strftime(params._datetimefm)

                      delivered = recorded = avgpu = 0.
                      if source == 'best':
                          delivered = 0.
                          if row.has_key('delivered') and row['delivered']:
                              delivered = row['delivered']*lslengthsec/pargs.scalefactor
                          recorded = 0.
                          if delivered>0 and row.has_key('recorded') and row['recorded']:
                              recorded = row['recorded']*lslengthsec/pargs.scalefactor
                          avgpu = 0.
                          if delivered>0 and row.has_key('avgpu') and row['avgpu']:
                              avgpu = row['avgpu']
                          datasource = 'UNKNOWN'
                          if row.has_key('datasource') and row['datasource']:
                              datasource = row['datasource']
                          livefrac = 0.                          
                          if delivered: livefrac = np.divide(recorded,delivered)
                          if pargs.withBX:
                              bxlumi = None
                              bxlumistr = '[]'
                              if row.has_key('bxdeliveredblob'):
                                  bxdeliveredarray = np.array(api.unpackBlobtoArray(row['bxdeliveredblob'],'f'))
                                  bxidx = np.nonzero(bxdeliveredarray)
                                  if bxidx[0].size>0:
                                      bxdelivered = bxdeliveredarray[bxidx]*lslengthsec/pargs.scalefactor
                                      bxlumi = np.transpose( np.array([bxidx[0],bxdelivered,bxdelivered*livefrac]) )
                                  del bxdeliveredarray
                                  del bxidx
                              if bxlumi is not None:
                                  a = map(formatter.bxlumi,bxlumi)  
                                  bxlumistr = '['+' '.join(a)+']'                              
                              display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),datasource,'%s'%bxlumistr] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                              del bxlumi
                          elif pargs.byls:
                              display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),datasource] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                      else:  #with lumi source
                          datasource = source.upper()
                          livefrac = 0. 
                          if beamstatus not in ['FLAT TOP','STABLE BEAMS','SQUEEZE','ADJUST']: continue
                          if row.has_key('deadtimefrac') and row['deadtimefrac'] is not None:
                              livefrac = 1.-row['deadtimefrac']
                          delivered = row['avglumi']*lslengthsec/pargs.scalefactor
                          recorded = delivered*livefrac  
                          if pargs.withBX:
                              bxlumi = None
                              bxlumistr = '[]'
                              if row.has_key('bxlumiblob'):                                  
                                  bxdeliveredarray = np.array(api.unpackBlobtoArray(row['bxlumiblob'],'f'))
                                  bxidx = np.nonzero(bxdeliveredarray)
                                  if bxidx[0].size>0:
                                      bxdelivered = bxdeliveredarray[bxidx]*lslengthsec/pargs.scalefactor
                                      bxlumi = np.transpose( np.array([bxidx[0],bxdelivered,bxdelivered*livefrac]) )
                                  del bxdeliveredarray
                                  del bxidx
                              if bxlumi is not None:
                                  a = map(formatter.bxlumi,bxlumi)  
                                  bxlumistr = '['+' '.join(a)+']'                              
                              display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),datasource,'%s'%bxlumistr] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                              del bxlumi
                          elif pargs.byls:
                              display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),datasource] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                      if runtot.has_key(runnum):#accumulate                          
                          runtot[runnum]['nls'] += 1
                          if cmson:
                              runtot[runnum]['ncms'] += 1
                          runtot[runnum]['delivered'] += delivered
                          runtot[runnum]['recorded'] += recorded
                      else:
                          runtot[runnum] = {'fill':fillnum,'time':dtime,'nls':1,'ncms':int(cmson),'delivered':delivered,'recorded':recorded}
                      
          if runtot:              
              df_runtot = pd.DataFrame.from_dict(runtot,orient='index')
              nruns = len(df_runtot.index)
              nfills = df_runtot['fill'].nunique()
              nls = df_runtot['nls'].sum()
              ncmsls = df_runtot['ncms'].sum()
              totdelivered = df_runtot['delivered'].sum()
              totrecorded = df_runtot['recorded'].sum()
              display.add_row( [ nfills,nruns,nls,ncmsls,'%.3f'%(totdelivered),'%.3f'%(totrecorded)], fh=None, csvwriter=None, ptable=ftable)
              del df_runtot
              if not pargs.byls and not pargs.withBX: #run table
                  for run in sorted(runtot):
                      display.add_row( ['%d:%d'%(run,runtot[run]['fill']),runtot[run]['time'],runtot[run]['nls'],runtot[run]['ncms'],'%.3f'%(runtot[run]['delivered']),'%.3f'%(runtot[run]['recorded'])] , fh=fh, csvwriter=csvwriter, ptable=ptable)
        
              if pargs.totable:              
                  print '#Data tag : ',datatagname
                  display.show_table(ptable,pargs.outputstyle)
                  print "#Summary: "
                  display.show_table(ftable,pargs.outputstyle)
                  del ptable
                  del ftable
              else:              
                  print >> fh, '#Summary:'                  
                  print >> fh, '#'+','.join(footer)
                  print >> fh, '#'+','.join( [ '%d'%nfills,'%d'%nruns,'%d'%nls,'%d'%ncmsls,'%.3f'%(totdelivered),'%.3f'%(totrecorded)] )
          
              if fh and fh is not sys.stdout: fh.close()
          sys.exit(0)
      elif args['<command>'] == 'beam':
          import brilcalc_beam

          parseresult = docopt.docopt(brilcalc_beam.__doc__,argv=cmmdargv)
          parseresult = brilcalc_beam.validate(parseresult)
          ##parse selection params
          pargs = clicommonargs.parser(parseresult)

          ##db params
          dbschema = ''
          if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
          dbengine = create_engine(pargs.connecturl)
          totz=utctmzone
          if pargs.cerntime: totz=cerntmzone
          if pargs.tssec: totz=None
          ##display params          
          fh = None
          ptable = None
          csvwriter = None

          (datatagname,datatagnameid) = findtagname(dbengine,pargs.datatagname,dbschema)
          log.debug('datatagname: %s, datatagnameid: %d'%(datatagname,datatagnameid))    
          header = ['fill','run','ls','time','egev','intensity1','intensity2']
          if pargs.withBX:
              header = ['fill','run','ls','time','[bxidx intensity1 intensity2]']
          if not pargs.totable:
              fh = pargs.ofilehandle
              print >> fh, '#Data tag : ',datatagname
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              ptable = display.create_table(header,header=True,maxwidth=80)                        

          idfields = ['fillnum','runnum','lsnum','timestampsec','beamstatusid']    
          fields = ['egev','intensity1','intensity2']
          if pargs.withBX:
              fields = ['bxidxblob','bxintensity1blob','bxintensity2blob']
          beamIt = api.beamInfoIter(dbengine,3,datafields=fields,idfields=idfields,schemaname=dbschema,fillmin=pargs.fillmin,fillmax=pargs.fillmax,runmin=pargs.runmin,runmax=pargs.runmax,amodetagid=pargs.amodetagid,targetegev=pargs.egev,beamstatusid=pargs.beamstatusid,tssecmin=pargs.tssecmin,tssecmax=pargs.tssecmax,runlsselect=pargs.runlsSeries,sorted=True)
          if not beamIt: sys.exit(0)
          for row in beamIt:
              fillnum = row['fillnum']
              runnum = row['runnum']
              lsnum = row['lsnum']                          
              timestampsec = row['timestampsec']
              dtime = str(timestampsec)
              if totz is not None:
                  d = datetime.fromtimestamp(int(timestampsec),tz=pytz.utc)
                  dtime = d.astimezone(totz).strftime(params._datetimefm) 
              if pargs.withBX:
                  bxintensity = None
                  bxintensitystr = '[]'
                  if row.has_key('bxidxblob') and row['bxidxblob'] is not None:
                      bxidxarray = np.array(api.unpackBlobtoArray(row['bxidxblob'],'H'))                            
                      bxidxarray = bxidxarray[bxidxarray!=np.array(None)]
                      if bxidxarray is not None and bxidxarray.size>0:
                          bxintensity1array =  np.array(api.unpackBlobtoArray(row['bxintensity1blob'],'f'))
                          bxintensity2array =  np.array(api.unpackBlobtoArray(row['bxintensity2blob'],'f'))
                          bxintensity = np.transpose( np.array([bxidxarray,bxintensity1array,bxintensity2array]) )
                          a = map(formatter.bxintensity,bxintensity)                          
                          bxintensitystr = '['+' '.join(a)+']'
                          del bxintensity1array
                          del bxintensity2array
                      del bxidxarray
                      display.add_row( ['%d'%fillnum,'%d'%runnum,'%d'%lsnum,dtime,'%s'%bxintensitystr], fh=fh, csvwriter=csvwriter, ptable=ptable )
              else:
                  egev = row['egev']
                  intensity1 = row['intensity1']/pargs.scalefactor
                  intensity2 = row['intensity2']/pargs.scalefactor
                  display.add_row( ['%d'%fillnum,'%d'%runnum,'%d'%lsnum,dtime,'%.1f'%egev,'%.4e'%intensity1,'%.4e'%intensity2], fh=fh, csvwriter=csvwriter, ptable=ptable)

          if pargs.totable:
              print '#Data tag : ',datatagname
              display.show_table(ptable,pargs.outputstyle)
              del ptable
          if fh and fh is not sys.stdout: fh.close()    
          sys.exit(0)    
      elif args['<command>'] == 'trg':
          raise NotImplementedError           
      """
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
          shards = [1,2]
          header = ['fill','run','ls','time','deadfrac']          
          if bybit:
              header = ['fill','run','ls','id','name','pidx','pval','counts']
          if not totable:
              fh = trgargs.ofilehandle
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              ptable = display.create_table(header,header=True)
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
          
          it = api.datatagIter(dbengine,datatagnameid,fillmin=trgargs.fillmin,fillmax=trgargs.fillmax,runmin=trgargs.runmin,runmax=trgargs.runmax,amodetag=trgargs.amodetag,targetegev=trgargs.egev,beamstatus=trgargs.beamstatus,tssecmin=trgargs.tssecmin,tssecmax=trgargs.tssecmax,runlsselect=trgargs.runlsSeries ,chunksize=csize,fields=['fillnum','runnum','lsnum','timestampsec'])
          if not it: exit(1)
          for idchunk in it:
              dataids = idchunk.index
              for shardid in shards:
                  if not bybit:
                      for deadtimechunk in api.deadtimeIter(dbengine,dataids,str(shardid),chunksize=csize):
                          finalchunk = idchunk.join(deadtimechunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False) 
                          for datatagid,row in finalchunk.iterrows():
                              timestampsec = row['timestampsec']
                              dtime = datetime.fromtimestamp(int(timestampsec)).strftime(params._datetimefm)                   
                              display.add_row( ['%d'%row['fillnum'],'%d'%row['runnum'],'%d'%row['lsnum'],dtime,'%.4f'%(row['deadtimefrac']) ] , fh=fh, csvwriter=csvwriter, ptable=ptable )
                          del finalchunk
                          del deadtimechunk
                  else:
                      for trginfochunk in api.trgInfoIter(dbengine,dataids,str(shardid),schemaname='',bitnamepattern=trgargs.name,chunksize=csize*192):
                          finalchunk = idchunk.join(trginfochunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                          for datatagid,row in finalchunk.iterrows():
                              display.add_row( [ '%d'%row['fillnum'],'%d'%row['runnum'],'%d'%row['lsnum'],'%d'%row['bitid'],row['bitname'],'%d'%row['prescidx'],'%d'%row['presc'],'%d'%row['counts'] ], fh=fh, csvwriter=csvwriter, ptable=ptable )
                          del finalchunk
                          del trginfochunk
                      ptable.max_width['bitname']=20
                      ptable.align='l'
              del idchunk
              
          if ptable:
              display.show_table(ptable,trgargs.outputstyle)
              del ptable     
          if fh and fh is not sys.stdout: fh.close()
      """     
      #elif args['<command>'] == 'hlt':
      #    raise NotImplementedError
      """
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
          if  hltargs.pathinfo:
              header = ['fill','run','ls','hltpath','pidx','pval','l1pass','accept']
          shards = [1,2]
          if not totable:
              fh = hltargs.ofilehandle
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              ptable = display.create_table(header,header=True)
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
          
          it = None
          if hltargs.pathinfo:
              it = api.datatagIter(dbengine,datatagnameid,fillmin=hltargs.fillmin,fillmax=hltargs.fillmax,runmin=hltargs.runmin,runmax=hltargs.runmax,amodetag=hltargs.amodetag,targetegev=hltargs.egev,beamstatus=hltargs.beamstatus,tssecmin=hltargs.tssecmin,tssecmax=hltargs.tssecmax,runlsselect=hltargs.runlsSeries,chunksize=csize,fields=['fillnum','runnum','lsnum','timestampsec'])
          else:
              it = api.rundatatagIter(dbengine,datatagnameid,fillmin=hltargs.fillmin,fillmax=hltargs.fillmax,runmin=hltargs.runmin,runmax=hltargs.runmax,amodetag=hltargs.amodetag,targetegev=hltargs.egev,tssecmin=hltargs.tssecmin,tssecmax=hltargs.tssecmax,runlsselect=hltargs.runlsSeries ,chunksize=csize)
          if not it: exit(1)
          
          for idchunk in it:
              if hltargs.pathinfo:
                  dataids = idchunk.index
                  for shardid in shards:
                      for hltchunk in api.hltInfoIter(dbengine,dataids,str(shardid),schemaname='',hltpathnamepattern=hltargs.name,chunksize=csize):
                          finalchunk = idchunk.join(hltchunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                          for datatagid, row in finalchunk.iterrows():
                              display.add_row( [row['fillnum'],row['runnum'],row['lsnum'],row['hltpathname'],row['prescidx'],row['prescval'],row['l1pass'],row['hltaccept'] ],fh=fh, csvwriter=csvwriter, ptable=ptable )                      
                          del finalchunk
                          del hltchunk
                  if ptable:
                      ptable.max_width = 80
                      ptable.max_width['hltpath']=40
                      ptable.align='l'

              else:
                  rundataids = idchunk.index              
                  for runchunk in api.runinfoIter(dbengine,rundataids,chunksize=csize,fields=['hltconfigid','hltkey']):
                      finalchunk = idchunk.join(runchunk,how='inner',on=None,lsuffix='l',rsuffix='r',sort=False)
                      for rundatatagid,row in finalchunk.iterrows():
                          fill = row['fillnum']
                          run = row['runnum']
                          hltkey = row['hltkey']
                          hltconfigid = row['hltconfigid']
                          for hltpathchunk in api.hltl1seedinfoIter(dbengine,hltconfigid,hltpathnameorpattern=hltargs.name):
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
                      del finalchunk
                      if ptable:                          
                          ptable.max_width['hltkey']=20
                          ptable.max_width['hltpath']=60
                          ptable.max_width['l1seed']=20
                          ptable.align='l'

          if ptable:
              display.show_table(ptable,hltargs.outputstyle)
              del ptable                 
          if fh and fh is not sys.stdout: fh.close()
          
      elif args['<command>'] == 'bkg':
          raise NotImplementedError
      else:
          exit("%r is not a brilcalc command. See 'brilcalc --help'."%args['<command>'])
          """
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)    
    return

if __name__ == '__main__':
    brilcalc_main()
    exit(0)
