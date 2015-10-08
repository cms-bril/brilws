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
import ast
from brilws import api,params,display,formatter,lumiParameters,corrector
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

lumip = lumiParameters.ParametersObject()
lslengthsec= lumip.lslengthsec()
utctmzone = tz.gettz('UTC')
cerntmzone = tz.gettz('CEST')

class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream
    def write(self, data):
        self.stream.write(data)
        self.stream.flush()
    def __getattr__(self, attr):
        return getattr(self.stream.attr)
sys.stdout = Unbuffered(sys.stdout)

np.seterr(divide='ignore', invalid='ignore')


def lumi_per_normtag(shards,lumiquerytype,dbengine,dbschema,datasource=None,normtag=None,withBX=False,byls=None,fh=None,csvwriter=None,ptable=None,scalefactor=1,totz=utctmzone,fillmin=None,fillmax=None,runmin=None,runmax=None,amodetagid=None,egev=None,beamstatusid=None,tssecmin=None,tssecmax=None,runlsSeries=None,hltpath=None):   
    runtot_df = pd.DataFrame(columns=['run','fill','time','nls','ncms','delivered','recorded','hltpath'])
    
    validitychecker = None
    lastvalidity = None
    if normtag and normtag is not 'withoutcorrection':
        normdata = api.iov_gettagdata(dbengine, normtag,schemaname=dbschema)
        if not normdata: raise ValueError('normtag %s does not exist'%normtag)
        validitychecker = ValidityChecker(normdata)
        
    for shard in shards:              
        lumiiter = None
        if lumiquerytype == 'bestresultonline':
            tablename = 'online_result_'+str(shard)
            shardexists = api.table_exists(dbengine,tablename,schemaname=dbschema)
            if not shardexists: continue
            rfields = ['fillnum','runnum','lsnum','timestampsec','cmson','beamstatusid','targetegev','delivered','recorded','avgpu','datasource']
            if withBX: rfields = rfields+['bxdeliveredblob']
            lumiiter = api.online_resultIter(dbengine,tablename,schemaname=dbschema,fields=rfields,fillmin=fillmin,fillmax=fillmax,runmin=runmin,runmax=runmax,amodetagid=amodetagid,targetegev=egev,beamstatusid=beamstatusid,tssecmin=tssecmin,tssecmax=tssecmax,runlsselect=runlsSeries,sorted=True)            
            
        elif lumiquerytype == 'detresultonline':
            tablename = datasource.lower()+'_result_'+str(shard)
            shardexists = api.table_exists(dbengine,tablename,schemaname=dbschema)
            if not shardexists: continue
            rfields = ['avglumi']
            idfields = ['fillnum','runnum','lsnum','timestampsec','beamstatusid','cmson','deadtimefrac','targetegev']
            if withBX: rfields = rfields+['bxlumiblob']
            lumiiter = api.det_resultDataIter(dbengine,datasource,shard,datafields=rfields,idfields=idfields,schemaname=dbschema,fillmin=fillmin,fillmax=fillmax,runmin=runmin,runmax=runmax,amodetagid=amodetagid,targetegev=egev,beamstatusid=beamstatusid,tssecmin=tssecmin,tssecmax=tssecmax,runlsselect=runlsSeries,sorted=True)
            
        elif lumiquerytype =='detraw':
            tablename = datasource+'_raw_'+str(shard)
            shardexists = api.table_exists(dbengine,tablename,schemaname=dbschema)
            if not shardexists: continue
            rfields = ['avglumi']
            idfields = ['fillnum','runnum','lsnum','timestampsec','beamstatusid','cmson','deadtimefrac','targetegev']
            if withBX: rfields = rfields+['bxlumiblob']
            lumiiter = api.det_rawDataIter(dbengine,datasource,shard,datafields=rfields,idfields=idfields,schemaname=dbschema,fillmin=fillmin,fillmax=fillmax,runmin=runmin,runmax=runmax,amodetagid=amodetagid,targetegev=egev,beamstatusid=beamstatusid,tssecmin=tssecmin,tssecmax=tssecmax,runlsselect=runlsSeries,sorted=True)
                  
        if not lumiiter: continue

        g_run_old = 0
        g_ls_trglastscaled_old = 0
        g_hltconfigid_old = None
        prescale_map = {} # for global scope
        for row in lumiiter:            
            fillnum = row['fillnum']
            runnum = row['runnum']
            lsnum = row['lsnum']            
            cmslsnum = lsnum
            timestampsec = row['timestampsec']
            cmson = row['cmson']
            if not cmson: cmslsnum = 0            
            if hltpath is not None :
                totpresc = 0
                if cmslsnum==0: continue #cms is not running, skip.                
                ls_trglastscaled = api.get_ls_trglastscaled(dbengine,runnum,cmslsnum,schemaname=dbschema)
                if g_run_old!=runnum: #on each new run, get hltconfigid
                    hltrunconfig_df = api.get_hltrunconfig(dbengine,runnum=runnum,schemaname=dbschema) #['runnum','hltconfigid','hltkey']
                    hltconfigid = int(np.unique(hltrunconfig_df['hltconfigid'].values)[0])
                    if hltconfigid != g_hltconfigid_old:
                        hltl1map_df = api.get_hlttrgl1seedmap(dbengine,hltpath=hltpath,hltconfigids=hltconfigid,schemaname=dbschema)
                        #['hltconfigid','hltpathid','hltpathname','seedtype','seedvalue']
                        pathids = np.unique( hltl1map_df['hltpathid'].values )
                        g_hltconfigid_old = hltconfigid
                        #print hltl1map_df
                    presc_df = api.get_hltconfig_trglastscaled(dbengine,hltconfigids=hltconfigid,runnums=runnum,schemaname=dbschema)
                    #['hltconfigid','hltkey','runnum','lslastscaler','prescidx']                    
                    if presc_df is None: continue
                    #print presc_df
                    #del presc_df
                    #del hltrunconfig_df
                    g_run_old = runnum
                if g_ls_trglastscaled_old != ls_trglastscaled:
                    prescale_map = {} #clear
                    tmp_df = presc_df.merge(hltl1map_df, on='hltconfigid', how='inner',copy=False)
                    if tmp_df is None: continue
                    grouped = tmp_df.groupby(['hltconfigid','runnum','lslastscaler','hltpathid'])
                    for name,group in grouped:
                        l1candidates = np.hstack( group['seedvalue'].values )
                        l1seedlogic = group['seedtype'].values[0]
                        r = api.get_trgprescale(dbengine,runnum,ls_trglastscaled,pathids=pathids,l1candidates=l1candidates,ignorel1mask=True,schemaname=dbschema)
                                        #['prescidx','hltpathid','hltprescval','bitname','trgprescval','bitmask']
                        #print r
                        if r is None: continue
                        hltprescval = r['hltprescval'].values[0]                        
                        if np.all(r['trgprescval'].values==1):
                            totpresc = hltprescval
                        elif l1seedlogic=='ONE':
                            totpresc = hltprescval*r['trgprescval'].values[0]
                        elif l1seedlogic=='OR':
                            totpresc = hltprescval*np.min(r['trgprescval'].values)
                        elif l1seedlogic=='AND':
                            totpresc = hltprescval*np.max(r['trgprescval'].values)
                        hltpathname = str( group['hltpathname'].values[0] )
                        prescale_map[hltpathname] = totpresc                        
                        del r                        
                    g_ls_trglastscaled_old = ls_trglastscaled

            beamstatusid = row['beamstatusid']
            beamstatus = params._idtobeamstatus[beamstatusid]
            if beamstatus not in ['FLAT TOP','STABLE BEAMS','SQUEEZE','ADJUST']: continue
            tegev = row['targetegev']
            dtime = str(timestampsec)
            if totz is not None:
                d = datetime.fromtimestamp(int(timestampsec),tz=pytz.utc)
                dtime = d.astimezone(totz).strftime(params._datetimefm)

            delivered = recorded = avgpu = livefrac = 0.
            
            if not (runtot_df['run']==runnum).any(): #new run
                if hltpath is not None:
                    for p in prescale_map.keys():
                        runtot_df.loc[ len(runtot_df)+1 ] = [runnum,fillnum,dtime,0,0,0.,0.,p]
                else:
                    runtot_df.loc[ len(runtot_df)+1 ] = [runnum,fillnum,dtime,0,0,0.,0.,None]
                    
            if lumiquerytype == 'bestresultonline':
                if row.has_key('delivered') and row['delivered']:
                    delivered = np.divide(row['delivered']*lslengthsec,scalefactor)
                if delivered>0 and row.has_key('recorded') and row['recorded']:
                    recorded = np.divide(row['recorded']*lslengthsec,scalefactor)
                if delivered>0 and row.has_key('avgpu') and row['avgpu']:
                    avgpu = row['avgpu']
                ds = 'UNKNOWN' 
                if row.has_key('datasource') and row['datasource']:
                    ds = row['datasource']
                livefrac = np.divide(recorded,delivered)
                if withBX:
                    bxlumi = None
                    bxlumistr = '[]'
                    if row.has_key('bxdeliveredblob'):
                        bxdeliveredarray = np.array(api.unpackBlobtoArray(row['bxdeliveredblob'],'f'))
                        bxidx = np.nonzero(bxdeliveredarray)
                        if bxidx[0].size>0:
                            bxdelivered = np.divide( bxdeliveredarray[bxidx]*lslengthsec,scalefactor )
                            bxlumi = np.transpose( np.array([bxidx[0]+1,bxdelivered,bxdelivered*livefrac]) )
                        del bxdeliveredarray
                        del bxidx
                        
                        if hltpath is not None:
                            for pth in prescale_map.keys():
                                totpresc = prescale_map[pth]
                                if bxlumi is not None:
                                    a = map(formatter.bxlumi,bxlumi/totpresc)  
                                    bxlumistr = '['+' '.join(a)+']'
                                display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,pth,'%.3f'%(np.divide(delivered/totpresc)),'%.3f'%(recorded/totpresc),ds,'%s'%bxlumistr] , fh=fh, csvwriter=csvwriter, ptable=ptable)      
                        else:
                            if bxlumi is not None:
                                a = map(formatter.bxlumi,bxlumi)
                                bxlumistr = '['+' '.join(a)+']'
                            display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),ds,'%s'%bxlumistr] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                    del bxlumi
                elif byls:
                    if hltpath is not None:
                        for pth in prescale_map.keys():
                            totpresc = prescale_map[pth]
                            display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum), dtime, pth, '%.3f'%(np.divide(delivered,totpresc)),'%.3f'%(np.divide(recorded,totpresc)),ds] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                    else:
                        display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),ds] , fh=fh, csvwriter=csvwriter, ptable=ptable)
            
            else:  #with lumi source
                if row.has_key('deadtimefrac') and row['deadtimefrac'] is not None:
                    livefrac = 1.-row['deadtimefrac']
                avglumi = row['avglumi']  
                if validitychecker is not None:
                    if not lastvalidity or not validitychecker.isvalid(runnum,lastvalidity):
                        lastvalidity = validitychecker.getvalidity(runnum)
                    [normfunc,normparam] = validitychecker.getvaliddata(lastvalidity[0])
                    
                    ncollidingbx = 1        #fixme
                    f_args = (avglumi,ncollidingbx)
                    f_kwds = ast.literal_eval(normparam)                          
                    avglumi = corrector.FunctionCaller(normfunc,*f_args,**f_kwds)    
                delivered = np.divide(avglumi*lslengthsec,scalefactor)
                recorded = delivered*livefrac

                if withBX:
                    bxlumi = None
                    bxlumistr = '[]'
                    if row.has_key('bxlumiblob'):                                  
                        bxdeliveredarray = np.array(api.unpackBlobtoArray(row['bxlumiblob'],'f'))
                        if validitychecker is not None:              
                            f_bxargs = (bxdeliveredarray,ncollidingbx)
                            bxdeliveredarray = corrector.FunctionCaller(normfunc,*f_bxargs,**f_kwds)
                        bxidx = np.nonzero(bxdeliveredarray)
                        if bxidx[0].size>0:                                      
                            bxdelivered = np.divide( bxdeliveredarray[bxidx]*lslengthsec,scalefactor ) 
                            bxlumi = np.transpose( np.array([bxidx[0]+1,bxdelivered,bxdelivered*livefrac]) )               
                        del bxdeliveredarray
                        del bxidx
                    if hltpath is not None:
                        for pth in prescale_map.keys():
                            totpresc = prescale_map[pth]                            
                            if bxlumi is not None:                                  
                                a = map(formatter.bxlumi,bxlumi/totpresc)  
                                bxlumistr = '['+' '.join(a)+']'
                            display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,pth,'%.3f'%(np.divide(delivered/totpresc)),'%.3f'%(recorded/totpresc),ds,'%s'%bxlumistr] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                    else:
                        if bxlumi is not None:
                            a = map(formatter.bxlumi,bxlumi)
                            bxlumistr = '['+' '.join(a)+']'
                        display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),datasource.upper(),'%s'%bxlumistr] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                    del bxlumi
                elif byls:
                    if hltpath is not None:
                        for pth in prescale_map.keys():
                            totpresc = prescale_map[pth]
                            display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,pth,'%.3f'%(np.divide(delivered/totpresc)),'%.3f'%(np.divide(recorded/totpresc)),ds] , fh=fh, csvwriter=csvwriter, ptable=ptable)
                    else:
                        display.add_row( ['%d:%d'%(runnum,fillnum),'%d:%d'%(lsnum,cmslsnum),dtime,beamstatus,'%d'%tegev,'%.3f'%(delivered),'%.3f'%(recorded),'%.1f'%(avgpu),datasource.upper()] , fh=fh, csvwriter=csvwriter, ptable=ptable)
            if  hltpath is None:
                runtot_df.loc[ runtot_df.run==runnum, 'nls' ] += 1
                if cmson: runtot_df.loc[ runtot_df.run == runnum,'ncms' ] += 1
                runtot_df.loc[runtot_df.run == runnum,'delivered'] += delivered
                runtot_df.loc[runtot_df.run == runnum,'recorded'] += recorded
            else:
                #print runnum,lsnum,cmslsnum
                if not cmson: continue
                #print prescale_map
                for pth in prescale_map.keys():
                    totpresc = prescale_map[pth]
                    runtot_df.loc[ (runtot_df.run==runnum)&(runtot_df.hltpath==pth) ,'nls'] += 1
                    if cmson: runtot_df.loc[ (runtot_df.run==runnum)&(runtot_df.hltpath==pth) ,'ncms'] += 1
                    runtot_df.loc[ (runtot_df.run==runnum) & (runtot_df.hltpath==pth) ,'delivered'] += np.divide(delivered,totpresc)
                    runtot_df.loc[ (runtot_df.run==runnum) & (runtot_df.hltpath==pth) ,'recorded'] += np.divide(recorded,totpresc)
    return runtot_df
     
class ValidityChecker(object):
    def __init__(self, normdata):
        self.normdata = normdata
        self.allsince = np.array([x[0] for x in normdata])
        
    def getvalidity(self,runnum):
        if self.allsince.size == 0 : return None
        since = 1
        if self.allsince[self.allsince<=runnum].size>0:
            since = self.allsince[self.allsince<=runnum].max()
        till = 999999
        if self.allsince[self.allsince>runnum].size>0:
            till = self.allsince[self.allsince>runnum].min()
        return (since,till)
    
    def isvalid(self,runnum,validity):
        if runnum>=validity[0] and runnum<validity[1]:
            return True
        return False

    def getvaliddata(self,since):
        s = np.where(self.allsince==since)
        if s[0].size>0:
            sinceindex = s[0][0]
            func = self.normdata[sinceindex][1]
            params = self.normdata[sinceindex][2]
            return [func,params]
        return None

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
          
          selectionkwds = {}
          normtag = None          
                      
          fh = None
          ptable = None
          ftable = None
          csvwriter = None
          vfunc_lumiunit = np.vectorize(formatter.lumiunit)

          g_headers = {}
          g_headers['runheader'] = ['run:fill','time','nls','ncms','delivered(/ub)','recorded(/ub)']
          g_headers['footer'] = ['nfill','nrun','nls','ncms','totdelivered(/ub)','totrecorded(/ub)']
          g_headers['bylsheader'] = ['run:fill','ls','time','beamstatus','E(GeV)','delivered(/ub)','recorded(/ub)','avgpu','source']
          g_headers['runheader_hltpath'] = ['run:fill','time','ncms','hltpath','delivered(/ub)','recorded(/ub)']
          g_headers['footer_hltpath'] = ['hltpath','nfill','nrun','ncms','totdelivered(/ub)','totrecorded(/ub)']
          g_headers['bylsheader_hltpath'] = ['run:fill','ls','time','hltpath','delivered(/ub)','recorded(/ub)','source']
          
          scalefactor = pargs.scalefactor
          lumiunitstr = parseresult['-u']         
          if lumiunitstr not in formatter.lumiunit_to_scalefactor.keys(): raise ValueError('%s not recognised as lumi unit'%lumiunit)
          lumiunitconversion = formatter.lumiunit_to_scalefactor[lumiunitstr]
          scalefactor = scalefactor*lumiunitconversion

          if pargs.byls:
              if pargs.hltpath is None:
                  header = g_headers['bylsheader']
                  footer = g_headers['footer']
              else:
                  header = g_headers['bylsheader_hltpath']
                  footer = g_headers['footer_hltpath']
          elif pargs.withBX:
              if pargs.hltpath is None:
                  header = g_headers['bylsheader']+['[bxidx bxdelivered(/ub) bxrecorded(/ub)]']
                  footer = g_headers['footer']
              else:
                  header = g_headers['bylsheader_hltpath']+['[bxidx bxdelivered(/ub) bxrecorded(/ub)]']
                  footer = g_headers['footer_hltpath']
          else:
              if pargs.hltpath is None:
                  header = g_headers['runheader']
                  footer = g_headers['footer']
              else:
                  header = g_headers['runheader_hltpath']
                  footer = g_headers['footer_hltpath']
                  
          header = vfunc_lumiunit(header,lumiunitstr).tolist()
          footer = vfunc_lumiunit(footer,lumiunitstr).tolist()
                    
          shards = [3]
          
          (datatagname,datatagnameid) = findtagname(dbengine,pargs.datatagname,dbschema)
          
          if not pargs.totable:
              fh = pargs.ofilehandle
              print >> fh, '#Data tag : %s , Norm tag: %s'%(datatagname,normtag)
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              ptable = display.create_table(header,header=True)
              ftable = display.create_table(footer)          
              
          datasources = [] #[lumiquerytype,normtagname,datasource,runlsstr]          

          lumiquerytype = 'detraw'
          normtag = normtagname = 'withoutcorrection'
          parseerrors = []
          if not pargs.withoutcorrection:
              normtag = pargs.iovtagSelect
              if not normtag:                  
                  if pargs.lumitype:
                      lumiquerytype = 'detresultonline'
                      datasources.append( ['detresultonline',normtag,pargs.lumitype.lower(),pargs.runlsSeries] )
                      normtagname = 'onlineresult'
                  else:
                      lumiquerytype = 'bestresultonline'
                      normtagname = 'onlineresult'
                      datasources.append( ['bestresultonline',normtag,'best',pargs.runlsSeries] )
              else:
                  if isinstance(normtag,list): #normtag is list
                      if pargs.runlsSeries is None:
                          mergedselect = normtag
                      else:
                          try:
                              api.checksuperset([x[1] for x in normtag],pargs.runlsSeries)
                          except api.NotSupersetError,e:
                              parseerrors.append(e)
                              #log.error('run %d, %s is not a superset of %s'%(e.runnum,str(e.superset),str(e.subset)))
                          mergedselect = api.mergeiovrunls(normtag,pargs.runlsSeries)                          
                      normtagname = 'composite'
                      for item in mergedselect:
                          iovtag = item[0]
                          runlsdict = item[1]
                          iovtag_meta = api.iov_gettag(dbengine,iovtag,schemaname=dbschema)
                          if not iovtag_meta: raise ValueError('requested iovtags do not exist')  
                          datasource = iovtag_meta[2].lower()
                          datasources.append( [lumiquerytype,iovtag,datasource,runlsdict] )
                  else:  #normtag is string 
                      datasource = pargs.lumitype
                      normtagname = normtag
                      if datasource is None: #det lumi with correction
                          iovtag_meta = api.iov_gettag(dbengine,normtag,schemaname=dbschema)
                          if not iovtag_meta: raise ValueError('%s does not exist'%normtag)                   
                          datasource = iovtag_meta[2]
                      datasources.append( [lumiquerytype,normtag,datasource.lower(),pargs.runlsSeries ])
                     
          else:
              if not pargs.lumitype:
                  raise ValueError('--type is required with --without-correction')
              datasources.append( [lumiquerytype,None,pargs.lumitype.lower(),pargs.runlsSeries ])
              
          log.debug('lumiquerytype %s'%lumiquerytype)          
          log.debug('scalefactor: %.2f'%pargs.scalefactor)                    
          #print datasources
          #runtot = {}# {run: {'fill':fillnum,'time':dtime,'nls':1,'ncms':int(cmson),'delivered':delivered,'recorded':recorded} }          
          
          totz=utctmzone
          if pargs.cerntime:
              totz=cerntmzone
          elif pargs.tssec:
              totz=None          

          fillmin = pargs.fillmin
          fillmax = pargs.fillmax
          runmin = pargs.runmin
          runmax = pargs.runmax
          amodetagid = pargs.amodetagid
          egev = pargs.egev
          beamstatusid = pargs.beamstatusid
          tssecmin = pargs.tssecmin
          tssecmax = pargs.tssecmax
          
          runtotdfs = []        
          for [qtype,ntag,dsource,rselect] in datasources:
              #print ntag,dsource,rselect              
              r = lumi_per_normtag(shards,qtype,dbengine,dbschema,datasource=dsource,normtag=ntag,withBX=pargs.withBX,byls=pargs.byls,fh=fh,csvwriter=csvwriter,ptable=ptable,scalefactor=scalefactor,totz=totz,fillmin=fillmin,fillmax=fillmax,runmin=runmin,runmax=runmax,amodetagid=amodetagid,egev=egev,beamstatusid=beamstatusid,tssecmin=tssecmin,tssecmax=tssecmax,runlsSeries=rselect,hltpath=pargs.hltpath)
              runtotdfs.append(r)
                                
          runtot_df = pd.concat(runtotdfs)
          #if runtot_df['hltpath'].isnull().all():
          if pargs.hltpath is None:
              nruns = runtot_df['run'].nunique()
              nfills = runtot_df['fill'].nunique()
              nls = runtot_df['nls'].sum()
              ncmsls = runtot_df['ncms'].sum()
              totdelivered = runtot_df['delivered'].sum()
              totrecorded = runtot_df['recorded'].sum()
              display.add_row( [ '%d'%nfills,'%d'%nruns,'%d'%nls,'%d'%ncmsls,'%.3f'%(totdelivered),'%.3f'%(totrecorded)], fh=None, csvwriter=None, ptable=ftable)

              if not pargs.byls and not pargs.withBX: #run table
                  for idx,row in runtot_df.iterrows():
                      display.add_row( ['%d:%d'%(int(row['run']),int(row['fill'])),str(row['time']),int(row['nls']),int(row['ncms']),'%.3f'%(float(row['delivered'])),'%.3f'%(float(row['recorded'])) ] , fh=fh, csvwriter=csvwriter, ptable=ptable)
          else:
              hltpathSummary = []
              grouped = runtot_df.groupby('hltpath')
              for pathname,g in grouped:
                  nruns = int(g['run'].nunique())
                  nfills = int(g['fill'].nunique())
                  ncmsls =  int(g['ncms'].sum())
                  totdelivered = g['delivered'].sum()
                  totrecorded = g['recorded'].sum()
                  display.add_row( [ pathname, nfills,nruns,ncmsls,'%.3f'%(totdelivered),'%.3f'%(totrecorded)], fh=None, csvwriter=None, ptable=ftable)
              if not pargs.byls and not pargs.withBX: #hltpath, run table                  
                  for pname,g in grouped:
                      pnruns = int( g['run'].nunique() )
                      pnfills = int( g['fill'].nunique() )
                      pncmsls = int( g['ncms'].sum() )
                      ptotdelivered = float( g['delivered'].sum() )
                      ptotrecorded = float( g['recorded'].sum() )
                      for v in g.values:
                          trun = int(v[0])
                          tfill = int(v[1])
                          ttime = str(v[2])
                          tncmsls = int(v[4])
                          tdelivered = float(v[5])
                          trecorded = float(v[6])
                          display.add_row( [ '%d:%d'%(tfill,trun), ttime, tncmsls, pname, '%.3f'%tdelivered, '%.3f'%trecorded], fh=fh, csvwriter=None, ptable=ptable )
                      hltpathSummary.append([pname,pnfills,pnruns,pncmsls,ptotdelivered,ptotrecorded])
                          
          del runtot_df
          
          if pargs.totable:              
              print '#Data tag : %s , Norm tag: %s'%(datatagname,normtagname)
              display.show_table(ptable,pargs.outputstyle)
              print "#Summary: "
              display.show_table(ftable,pargs.outputstyle)
              del ptable
              del ftable
          else:              
              print >> fh, '#Summary:'                  
              print >> fh, '#'+','.join(footer)
              if not pargs.hltpath:
                  print >> fh, '#'+','.join( [ '%d'%nfills,'%d'%nruns,'%d'%nls,'%d'%ncmsls,'%.3f'%(totdelivered),'%.3f'%(totrecorded)] )
              else:
                  for pentry in hltpathSummary:
                      print >> fh, '#'+','.join( [ '%s'%pentry[0],'%d'%pentry[1],'%d'%pentry[2],'%d'%pentry[3],'%.3f'%pentry[4],'%.3f'%pentry[5] ] )

          if fh and fh is not sys.stdout: fh.close()

          if parseerrors:
              print '\nWarning: problems found in merging -i and --normtag selections:'
              for e in parseerrors:
                  print '  run %d, %s is not a superset of %s'%(e.runnum,str(e.superset),str(e.subset))
              print 
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
          header = ['fill','run','ls','time','egev','intensity1','intensity2','ncollidingbx']
          if pargs.withBX:
              header = ['fill','run','ls','time','[bxidx intensity1 intensity2]']
          if not pargs.totable:
              fh = pargs.ofilehandle
              print >> fh, '#Data tag : %s'%(datatagname)
              print >> fh, '#'+','.join(header)
              csvwriter = csv.writer(fh)
          else:
              ptable = display.create_table(header,header=True,maxwidth=80)                        

          idfields = ['fillnum','runnum','lsnum','timestampsec','beamstatusid']    
          fields = ['egev','intensity1','intensity2','ncollidingbx']
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
              ncollidingbx = 0
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
                          bxintensity = np.transpose( np.array([bxidxarray+1,bxintensity1array,bxintensity2array]) )
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
                  ncollidingbx = row['ncollidingbx'] 
                  display.add_row( ['%d'%fillnum,'%d'%runnum,'%d'%lsnum,dtime,'%.1f'%egev,'%.4e'%intensity1,'%.4e'%intensity2, '%d'%ncollidingbx],fh=fh, csvwriter=csvwriter, ptable=ptable)

          if pargs.totable:
              print '#Data tag : ',datatagname
              display.show_table(ptable,pargs.outputstyle)
              del ptable
          if fh and fh is not sys.stdout: fh.close()    
          sys.exit(0)    
      elif args['<command>'] == 'trg':      
          import brilcalc_trg
          parseresult = docopt.docopt(brilcalc_trg.__doc__,argv=cmmdargv)
          parseresult = brilcalc_trg.validate(parseresult)
          ##parse selection params
          pargs = clicommonargs.parser(parseresult)

          ##db params
          dbschema = ''
          if not pargs.dbconnect.find('oracle')!=-1: dbschema = 'cms_lumi_prod'
          dbengine = create_engine(pargs.connecturl)
                  
          ##display params          
          fh = None
          ptable = None
          csvwriter = None

          is_pathinfo = parseresult['--pathinfo']
          is_prescale = parseresult['--prescale']
            
          if is_pathinfo:
              header = ['hltpath','logic','l1bit']
              hltrunconfig_df = None
              hltconfigids = []
              if pargs.hltconfigid or pargs.hltkey or pargs.runmin:
                  hltrunconfig_df = api.get_hltrunconfig(dbengine,hltconfigid=pargs.hltconfigid,hltkey=pargs.hltkey,runnum=pargs.runmin,schemaname=dbschema) #pd.DataFrame(columns=['runnum','hltconfigid','hltkey'])
                  if hltrunconfig_df is None:
                      print 'hltconfig not found'
                      sys.exit(0)
                  hltconfigids = np.unique(hltrunconfig_df['hltconfigid'].values)
              seedmap_df = api.get_hlttrgl1seedmap(dbengine,hltpath=pargs.hltpath,hltconfigids=hltconfigids,schemaname=dbschema)#['hltconfigid','hltpathid','hltpathname','seedtype','seedvalue']
              if seedmap_df is None:
                  print 'hltl1seed mapping not found'
                  sys.exit(0)                  

              if not pargs.totable:
                  fh = pargs.ofilehandle
                  print >> fh, '# '+','.join(header)
                  csvwriter = csv.writer(fh)
              else:
                  ptable = display.create_table(header,header=True,maxwidth=80)
                  
              grouped = seedmap_df.groupby( ['hltconfigid','hltpathname'] )
              for name,group in grouped:
                  hltpathname = str(name[1])
                  seedtype = group['seedtype'].values[0]
                  seedvals = group['seedvalue'].values[0]
                  seedstr = ' '.join([str(i) for i in seedvals])
                  display.add_row( [ '%s'%hltpathname , '%s'%seedtype, '%s'%seedstr], fh=fh, csvwriter=csvwriter, ptable=ptable )                
              if ptable:
                  display.show_table(ptable,pargs.outputstyle)         
                  del ptable
          elif is_prescale:                    
              header = ['run','cmsls','prescidx']
              if pargs.hltpath is not None: 
                  header = header+['totprescval','hltpath/prescval','logic','l1bit/prescval']
                  
              presc_df = api.get_hltconfig_trglastscaled(dbengine,hltconfigids=pargs.hltconfigid,hltkey=pargs.hltkey,runnums=pargs.runmin,schemaname=dbschema)
              #['hltconfigid','hltkey','runnum','lslastscaler','prescidx']
              
              if presc_df is None:
                  print 'No prescale found'
                  sys.exit(0)
                  
              if not pargs.totable:
                  fh = pargs.ofilehandle
                  print >> fh, '# '+','.join(header)
                  csvwriter = csv.writer(fh)
              else:
                  ptable = display.create_table(header,header=True,maxwidth=60)
                  
              if pargs.hltpath is not None:                  
                  hltpathl1seedmap_df = api.get_hlttrgl1seedmap(dbengine,hltpath=pargs.hltpath,hltconfigids=pargs.hltconfigid,schemaname=dbschema)
                  #['hltconfigid','hltpathid','hltpathname','seedtype','seedvalue']
                  if hltpathl1seedmap_df is None:
                      print 'No hltpathl1seed mapping found'
                      sys.exit(0)
                  tmp_df = presc_df.merge(hltpathl1seedmap_df, on='hltconfigid', how='inner', copy=False)
                  grouped = tmp_df.groupby(['hltconfigid','runnum','lslastscaler','hltpathid'])
                  for name,group in grouped:
                      runnum = int(name[1])
                      lsnum = int(name[2])
                      hltpathid = int(name[3])
                      hltpathname = str( group['hltpathname'].values[0] )
                      prescidx = int( group['prescidx'].values[0] )                      
                      l1candidates = np.hstack( group['seedvalue'].values )
                      l1seedlogic = group['seedtype'].values[0]
                      r = api.get_trgprescale(dbengine,runnum,lsnum,pathids=hltpathid,l1candidates=l1candidates,ignorel1mask=parseresult['--ignore-mask'],schemaname=dbschema)
                      ##['prescidx','hltpathid','hltprescval','bitname','trgprescval','bitmask']
                      totpresc = 0
                      if r is not None:
                          hltprescval = r['hltprescval'].values[0]
                          l1bits = np.dstack([r['bitname'].values,r['trgprescval'].values])[0]
                          l1inner = map(formatter.bitprescFormatter,l1bits)
                          l1bitsStr = ' '.join(l1inner)                          
                          hltpathStr = '/'.join([hltpathname,str(hltprescval)])
                          if np.all(r['trgprescval'].values==1):
                              totpresc = hltprescval
                          elif l1seedlogic=='ONE':
                              totpresc = hltprescval*r['trgprescval'].values[0]
                          elif l1seedlogic=='OR':
                              totpresc = hltprescval*np.min(r['trgprescval'].values)
                          elif l1seedlogic=='AND':
                              totpresc = hltprescval*np.max(r['trgprescval'].values)
                          display.add_row( [ '%d'%runnum, '%d'%lsnum, '%d'%prescidx, '%d'%totpresc,'%s'%hltpathStr, '%s'%l1seedlogic, '%s'%l1bitsStr], fh=fh, csvwriter=csvwriter, ptable=ptable )        
                          del r                          
                  del tmp_df
                  del hltpathl1seedmap_df
                  del presc_df
              else:
                  for row in presc_df.values:
                      runnum = int(row[2])
                      lsnum = int(row[3])
                      prescidx = int(row[4]) 
                      display.add_row( [ '%d'%runnum, '%d'%lsnum, '%d'%prescidx ], fh=fh, csvwriter=csvwriter, ptable=ptable )        
              del presc_df              
              if ptable:
                  display.show_table(ptable,pargs.outputstyle)         
                  del ptable          
          else:
              hltrunconfig_df = api.get_hltrunconfig(dbengine,hltconfigid=pargs.hltconfigid,hltkey=pargs.hltkey,runnum=pargs.runmin,schemaname=dbschema) #pd.DataFrame(columns=['runnum','hltconfigid','hltkey'])
              if hltrunconfig_df is None:
                  print 'hltconfig not found'
                  sys.exit(0)
              header = ['hltconfigid','hltkey','run']    
              if not pargs.totable:
                  fh = pargs.ofilehandle
                  print >> fh, '# '+','.join(header)
                  csvwriter = csv.writer(fh)
              else:
                  ptable = display.create_table(header,header=True,maxwidth=80)
                  
              grouped = hltrunconfig_df.groupby(['hltconfigid','hltkey'])
              for name,group in grouped:
                  hltconfigid = int(name[0])
                  hltkey = str(name[1])
                  runsStr = ','.join([str(i) for i in group['runnum'].values])
                  display.add_row( [ '%d'%hltconfigid, '%s'%hltkey , '%s'%runsStr], fh=fh, csvwriter=csvwriter, ptable=ptable )                
              if ptable:
                  display.show_table(ptable,pargs.outputstyle)         
                  del ptable
                  
              del hltrunconfig_df
          if fh and fh is not sys.stdout: fh.close()
          sys.exit(0)
          
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)    
    return

if __name__ == '__main__':
    brilcalc_main()
    exit(0)
