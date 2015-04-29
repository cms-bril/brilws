import sys,logging
from sqlalchemy import *
from sqlalchemy import exc
from ConfigParser import SafeConfigParser
import pandas as pd
import collections
import numpy as np
from collections import Counter
from datetime import datetime
from brilws import api
import time
import array
import re
dbtimefm = 'MM/DD/YY HH24:MI:SS.ff6'
pydatetimefm = '%m/%d/%y %H:%M:%S.%f'

def unpackblobstr(iblobstr,itemtypecode):
    if itemtypecode not in ['c','b','B','u','h','H','i','I','l','L','f','d']:
        raise RuntimeError('unsupported typecode '+itemtypecode)
    result=array.array(itemtypecode)
    #blobstr=iblob.readline()
    if not iblobstr :
        return result
    result.fromstring(iblobstr)
    return result


def datatagid_of_run(connection,runnum,datatagnameid=0):
    qid = '''select datatagid as datatagid from ids_datatag where datatagnameid=:datatagnameid and runnum=:runnum and lsnum=1'''
    myid = 0
    with connection.begin() as trans:
        idresult = connection.execute(qid,{'datatagnameid':datatagnameid,'runnum':runnum})
        for r in idresult:
            myid = r['datatagid']
    return myid

def datatagid_of_ls(connection,runnum,datatagnameid=0):
    '''
    output: {lsnum:datatagid}
    '''
    qid = '''select lsnum as lsnum, datatagid as datatagid from ids_datatag where datatagnameid=:datatagnameid and runnum=:runnum'''
    result = {}
    with connection.begin() as trans:
        idresult = connection.execute(qid,{'datatagnameid':datatagnameid,'runnum':runnum})
        for r in idresult:
            result[r['lsnum'] ] = r['datatagid']
    return result

def transfertimeinfo(connection,destconnection,runnum):
    '''
    query timeinformation of a given run
    '''
    q = """select lhcfill,runnumber,lumisection,to_char(starttime,'%s') as starttime from CMS_RUNTIME_LOGGER.LUMI_SECTIONS where runnumber=:runnum"""%(dbtimefm)
    i = """insert into TIMEINDEX(FILLNUM,RUNNUM,LSNUM,TIMESTAMPSEC,TIMESTAMPMSEC,TIMESTAMPSTR,WEEKOFYEAR,DAYOFYEAR,DAYOFWEEK,YEAR,MONTH) values(:fillnum, :runnum, :lsnum, :timestampsec, :timestampmsec, :timestampstr, :weekofyear, :dayofyear, :dayofweek, :year, :month)"""
    with connection.begin() as trans:
        result = connection.execute(q,{'runnum':runnum})        
        allrows = []
        for r in result:
            irow = {'fillnum':0,'runnum':runnum,'lsnum':0,'timestampsec':0,'timestampmsec':0,'timestampstr':'','weekofyear':0,'dayofyear':0,'dayofweek':0,'year':0,'month':0}
            irow['fillnum'] = r['lhcfill']
            irow['lsnum'] = r['lumisection']
            starttimestr = r['starttime']
            irow['timestampstr'] = starttimestr
            #stoptimestr = r['stoptime']
            starttime = datetime.strptime(starttimestr,pydatetimefm)
            irow['timestasec'] = time.mktime(starttime.timetuple())
            irow['timestamsec'] = starttime.microsecond/1e3
            irow['weekofyear'] = starttime.date().isocalendar()[1]
            irow['dayofyear'] = starttime.timetuple().tm_yday
            irow['dayofweek'] = starttime.date().isoweekday()
            irow['year'] = starttime.date().year
            irow['month'] = starttime.date().month
            allrows.append(irow)
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

        
def transfer_ids_datatag(connection,destconnection,runnum,lumidataid):
    '''
    '''
    qrunsummary = """select r.fillnum as fillnum,r.egev as targetegev, r.amodetag as amodetag, s.lumilsnum as lsnum,s.beamstatus as beamstatus from CMS_LUMI_PROD.CMSRUNSUMMARY r, CMS_LUMI_PROD.lumisummaryv2 s where r.runnum=s.runnum and r.runnum=:runnum and s.data_id=:lumidataid"""
    datatagnameid = 0    
    allrows = []
    datatagnameid = 0
    with connection.begin() as trans:
        result = connection.execute(qrunsummary,{'runnum':runnum,'lumidataid':lumidataid})
        for r in result:
            lsnum = r['lsnum']
            k = next(api.generate_key(lsnum))
            irow = {'datatagnameid':datatagnameid, 'datatagid':k, 'fillnum':0,'runnum':runnum,'lsnum':0,'targetegev':0,'beamstatus':'','amodetag':''}
            irow['fillnum'] = r['fillnum']
            irow['lsnum'] = lsnum
            irow['datatagid'] = k
            irow['targetegev'] = r['targetegev']
            irow['beamstatus'] = r['beamstatus']
            irow['amodetag'] = r['amodetag']
            allrows.append(irow)
            #print datatagnameid,runnum,irow['lsnum']

    i = """insert into IDS_DATATAG(DATATAGNAMEID,DATATAGID,FILLNUM,RUNNUM,LSNUM,TARGETEGEV,BEAMSTATUS,AMODETAG) values(:datatagnameid, :datatagid, :fillnum, :runnum, :lsnum, :targetegev, :beamstatus, :amodetag)"""
        
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

def transfer_runinfo(connection,destconnection,runnum,destdatatagid):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    qruninfo = '''select hltkey,l1key,fillscheme from CMS_LUMI_PROD.CMSRUNSUMMARY where runnum=:runnum'''
    i = '''insert into RUNINFO(DATATAGID,RUNNUM,HLTKEY,GT_RS_KEY,FILLSCHEME) values(:datatagid, :runnum, :hltkey, :l1key, :fillscheme)'''
    allrows = []
    with connection.begin() as trans:
        result = connection.execute(qruninfo,{'runnum':runnum})
        for r in result:
            irow = {'datatagid':destdatatagid, 'runnum':runnum,'hltkey':'','l1key':'','fillscheme':''}
            irow['hltkey'] = r['hltkey']
            irow['l1key'] = r['l1key']
            irow['fillscheme'] = r['fillscheme']
            allrows.append(irow)
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

def transfer_trgconfig(connection,destconnection,runnum,trgdataid,destdatatagid):
    '''
    prerequisite : ids_datatag has already entries for this run    
    '''
    qmask = '''select ALGOMASK_H as algomask_high,ALGOMASK_L as algomask_low,TECHMASK as techmask from CMS_LUMI_PROD.trgdata where DATA_ID=:trgdataid'''
    i = '''insert into TRGCONFIG(DATATAGID,ALGOMASK_HIGH,ALGOMASK_LOW,TECHMASK) values(:datatagid, :algomask_high, :algomask_low, :techmask)'''
    allrows = []
    with connection.begin() as trans:
        result = connection.execute(qmask,{'trgdataid':trgdataid})
        for r in result:
            irow = {'datatagid':destdatatagid, 'algomask_high': r['algomask_high'], 'algomask_low':r['algomask_low'], 'techmask':r['techmask']}
            allrows.append(irow)
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

def transfer_beamintensity(connection,destconnection,runnum,lumidataid,destdatatagidmap):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    qbeam = '''select lumilsnum as lsnum,beamenergy as beamenergy,cmsbxindexblob as cmsbxindexblob, beamintensityblob_1 as beamintensityblob_1,beamintensityblob_2 as beamintensityblob_2 from CMS_LUMI_PROD.lumisummaryv2 where data_id=:lumidataid'''
    ibeam = '''insert into BEAM_RUN1(DATATAGID,EGEV,INTENSITY1,INTENSITY2) values(:datatagid, :egev, :intensity1, :intensity2)'''
    ibeambx = '''insert into BX_BEAM_RUN1(DATATAGID,BXIDX,BXINTENSITY1,BXINTENSITY2) values(:datatagid, :bxidx, :bxintensity1, :bxintensity2)'''
    
    allbeamrows = []
    allbxbeamrows = []
    bxcols = ['datatagid','bxidx','beam1intensity','beam2intensity']
    bxdt = {'datatagid':'int64','bxidx':'object','beam1intensity':'object','beam2intensity':'object'}
    with connection.begin() as trans:
        result = connection.execute(qbeam,{'lumidataid':lumidataid})
        for row in result:
            lsnum = row['lsnum']
            beamenergy = row['beamenergy']
            bxindexblob = unpackblobstr(row['cmsbxindexblob'],'h')
            beam1intensityblob = unpackblobstr(row['beamintensityblob_1'],'f')
            beam2intensityblob = unpackblobstr(row['beamintensityblob_2'],'f')
            if not bxindexblob or not beam1intensityblob or not beam2intensityblob:
                continue
            bxindex = pd.Series(bxindexblob, dtype=np.dtype("object"))
            beam1intensity = pd.Series(beam1intensityblob, dtype=np.dtype("object"))
            beam2intensity = pd.Series(beam2intensityblob, dtype=np.dtype("object"))
            tot_beam1intensity = beam1intensity.sum()
            tot_beam2intensity = beam2intensity.sum()
            ibeamrow = {'datatagid':destdatatagidmap[lsnum],'egev':beamenergy, 'intensity1':tot_beam1intensity, 'intensity2':tot_beam2intensity } 
            allbeamrows.append(ibeamrow)
            allbxbeamrows.append( {'datatagid':destdatatagidmap[lsnum],'bxidx':bxindex,'beam1intensity':beam1intensity,'beam2intensity':beam2intensity} )
    with destconnection.begin() as trans:
        r = destconnection.execute(ibeam,allbeamrows)
        for bxb in allbxbeamrows:
            datatagid = bxb['datatagid']
            for idx, bxidx_value in bxb['bxidx'].iteritems():
                b1 = bxb['beam1intensity'].iloc[idx]
                b2 = bxb['beam2intensity'].iloc[idx]
                if not b1 and not b2 : continue                
                destconnection.execute(ibeambx,{'datatagid':datatagid,'bxidx':bxidx_value,'bxintensity1':b1,'bxintensity2':b2})
            del bxb['bxidx']
            del bxb['beam1intensity']
            del bxb['beam2intensity']
            
def transfer_trgdata(connection,destconnection,runnum,trgdataid,destdatatagidmap):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    bitnamemap = pd.DataFrame.from_csv('trgbits.csv',index_col='BITNAMEID')
    qalgobits = '''select ALGO_INDEX as bitid, ALIAS as bitname from CMS_GT.GT_RUN_ALGO_VIEW where RUNNUMBER=:runnum order by bitid'''
    qprescidx = 'select lumi_section as lsnum, prescale_index as prescidx from CMS_GT_MON.LUMI_SECTIONS where run_number=:runnum and prescale_index!=0'
    
    qpresc = '''select cmslsnum as lsnum, prescaleblob as prescaleblob, trgcountblob as trgcountblob from CMS_LUMI_PROD.lstrg where data_id=:trgdataid'''
    i = '''insert into TRG_RUN1(DATATAGID,TRGBITID,TRGBITNAMEID,ISALGO,PRESCIDX,PRESCVAL,COUNTS) values(:datatagid, :trgbitid, :trgbitnameid, :isalgo, :prescidx, :prescval, :counts)'''

    allrows = []
    algobitalias = 128*['False']
    bitaliasmap = {}
    prescidxmap = {}
    with connection.begin() as trans:
        algoresult = connection.execute(qalgobits,{'runnum':runnum})
        algopos = 0
        for algo in algoresult:
            algobitalias[algopos] = algo['bitname']
            algopos = algopos+1
        prescidxresult = connection.execute(qprescidx,{'runnum':runnum})
        for prescidxr in prescidxresult:
            lsnum = prescidxr['lsnum']
            prescidx = prescidxr['prescidx']
            prescidxmap[lsnum] = prescidx
    #print prescidxmap
    for trgbitnameid, bitparams in bitnamemap.iterrows():
        bitname = bitparams['BITNAME']
        bitid = bitparams['BITID']
        isalgo = bitparams['ISALGO']
        bitalias = bitname
        if not isalgo:
            bitalias = str(bitid)
        bitaliasmap[bitalias] = trgbitnameid

    with connection.begin() as trans:
        result = connection.execute(qpresc,{'trgdataid':trgdataid})        
        for row in result:
            lsnum = row['lsnum']
            if runnum<150008:
                try:
                    prescblob = unpackblobstr(row['prescaleblob'],'l')
                    trgcountblob = unpackblobstr(row['trgcountblob'],'l')
                    if not prescblob or not trgcountblob:
                        continue
                except ValueError:
                    prescblob = unpackblobstr(row['prescaleblob'],'I')
                    trgcountblob = unpackblobstr(row['trgcountblob'],'I')
                    if not prescblob or not trgcountblob:
                        continue
            else:
                prescblob = unpackblobstr(row['prescaleblob'],'I')
                trgcountblob = unpackblobstr(row['trgcountblob'],'I')
                if not prescblob or not trgcountblob:
                    continue
            prescalevalues = pd.Series(prescblob)
            trgcountvalues = pd.Series(trgcountblob)

            isalgo = False
            for idx, prescval in prescalevalues.iteritems():
                if idx>127:
                    bitpos = idx-128
                    bitalias = str(bitpos)
                    bitnameid = 65535
                    isalgo = False
                else:
                    bitpos = idx
                    bitalias = algobitalias[bitpos]                    
                    if bitalias=='False': continue
                    bitnameid = bitaliasmap[bitalias]
                    isalgo = True
                counts = 0
                prescidx = 0
                try:
                    counts = trgcountvalues[idx]
                except IndexError:
                    pass
                try:
                    prescidx = prescidxmap[lsnum]
                except KeyError:
                    pass
                allrows.append({'datatagid':destdatatagidmap[lsnum], 'trgbitid':bitpos, 'trgbitnameid':bitnameid, 'isalgo':isalgo, 'prescidx':prescidx ,'prescval':prescval, 'counts':counts})
                
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

def transfer_hltdata(connection,destconnection,runnum,hltdataid,destdatatagidmap):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    p = re.compile('^HLT_+',re.IGNORECASE)
    pathnamedf = pd.DataFrame.from_csv('hltpaths.csv',index_col=False)
    pathnamemap = {}
    for i,row in pathnamedf.iterrows():
        n =row['HLTPATHNAME']
        if p.match(n) is None:
            continue
        d = row['HLTPATHID']
        pathnamemap.setdefault(n,[]).append( int(d) )
    #print pathnamemap
    #print pathnamemap.keys()
        
    qprescidx = '''select lumi_section as lsnum, prescale_index as prescidx from CMS_GT_MON.LUMI_SECTIONS where run_number=:runnum and prescale_index!=0'''

    qpathname = '''select pathnameclob from CMS_LUMI_PROD.HLTDATA where data_id=:hltdataid'''
    
    qhlt = '''select cmslsnum as lsnum, prescaleblob as prescaleblob, hltcountblob as hltcountblob, hltacceptblob as hltacceptblob from CMS_LUMI_PROD.LSHLT where data_id=:hltdataid'''    

    qpathid = '''select lsnumber as lsnum, pathid as hltpathid from cms_runinfo.hlt_supervisor_triggerpaths where runnumber=:runnum'''
    
    i = '''insert into HLT_RUN1(DATATAGID,HLTPATHID,PRESCIDX,PRESCVAL,L1PASS,HLTACCEPT) values(:datatagid, :hltpathid, :prescidx, :prescval, :l1pass, :hltaccept)'''
    
    allrows = []
    
    prescidxmap = {}
    
    lspathids = {}
    
    with connection.begin() as trans:
        prescidxresult = connection.execute(qprescidx,{'runnum':runnum})
        for prescidxr in prescidxresult:
            lsnum = prescidxr['lsnum']
            prescidx = prescidxr['prescidx']
            prescidxmap[lsnum] = prescidx

    with connection.begin() as trans:
        lspathidresult = connection.execute(qpathid,{'runnum':runnum})
        for lspathr in lspathidresult:
            lspathids.setdefault(lspathr['lsnum'],[]).append(lspathr['hltpathid'])
    
    with connection.begin() as trans:
        pathnameresult = connection.execute(qpathname,{'hltdataid':hltdataid})        
        for row in pathnameresult:
            pathnameclob = row['pathnameclob']
            pathnames = pathnameclob.split(',')

        hltresult = connection.execute(qhlt,{'hltdataid':hltdataid})
        for row in hltresult:
            lsnum = row['lsnum']
            if runnum<150008:
                try:
                    prescblob = unpackblobstr(row['prescaleblob'],'l')
                    hltcountblob = unpackblobstr(row['hltcountblob'],'l')
                    hltacceptblob = unpackblobstr(row['hltacceptblob'],'l')
                    if not prescblob or not hltcountblob or not hltacceptblob:
                        continue
                except ValueError:
                    prescblob = unpackblobstr(row['prescaleblob'],'I')
                    hltcountblob = unpackblobstr(row['hltcountblob'],'I')
                    hltacceptblob = unpackblobstr(row['hltacceptblob'],'I')
                    if not prescblob or not hltcountblob or not hltacceptblob:
                        continue
            else:
                prescblob = unpackblobstr(row['prescaleblob'],'I')
                hltcountblob = unpackblobstr(row['hltcountblob'],'I')
                hltacceptblob = unpackblobstr(row['hltacceptblob'],'I')
                if not prescblob or not hltcountblob or not hltacceptblob:
                    continue
            prescalevalues = pd.Series(prescblob)
            hltcountvalues = pd.Series(hltcountblob)
            hltacceptvalues = pd.Series(hltacceptblob)
            
            for idx, prescval in prescalevalues.iteritems():
                prescidx = 0
                hltcounts = 0
                hltaccept = 0
                hltpathname = ''
                hltpathid = 0
                try:
                    hltpathname = pathnames[idx]
                except IndexError:
                    pass
                if p.match(hltpathname) is None: continue
                if hltpathname.find('Calibration')!=-1: continue
                try:
                    hltcounts = hltcountvalues[idx]
                except IndexError:
                    pass
                try:
                    hltaccept = hltacceptvalues[idx]
                except IndexError:
                    pass
                try:
                    prescidx = prescidxmap[lsnum]
                except KeyError:
                    pass
                pathidcandidates = pathnamemap[hltpathname]
                pathidinsersection = list( set(pathidcandidates) & set(lspathids[lsnum]) )
                hltpathid = pathidinsersection[0]

                allrows.append({'datatagid':destdatatagidmap[lsnum], 'hltpathid':hltpathid, 'prescidx':prescidx, 'prescval':prescval, 'l1pass':hltcounts, 'hltaccept':hltaccept})

    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)


def transfer_trgdata(connection,destconnection,runnum,trgdataid,destdatatagidmap):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    bitnamemap = pd.DataFrame.from_csv('trgbits.csv',index_col='BITNAMEID')
    qalgobits = '''select ALGO_INDEX as bitid, ALIAS as bitname from CMS_GT.GT_RUN_ALGO_VIEW where RUNNUMBER=:runnum order by bitid'''
    qprescidx = 'select lumi_section as lsnum, prescale_index as prescidx from CMS_GT_MON.LUMI_SECTIONS where run_number=:runnum and prescale_index!=0'
    
    qpresc = '''select cmslsnum as lsnum, prescaleblob as prescaleblob, trgcountblob as trgcountblob from CMS_LUMI_PROD.lstrg where data_id=:trgdataid'''
    i = '''insert into TRG_RUN1(DATATAGID,TRGBITID,TRGBITNAMEID,ISALGO,PRESCIDX,PRESCVAL,COUNTS) values(:datatagid, :trgbitid, :trgbitnameid, :isalgo, :prescidx, :prescval, :counts)'''

    allrows = []
    algobitalias = 128*['False']
    bitaliasmap = {}
    prescidxmap = {}
    with connection.begin() as trans:
        algoresult = connection.execute(qalgobits,{'runnum':runnum})
        algopos = 0
        for algo in algoresult:
            algobitalias[algopos] = algo['bitname']
            algopos = algopos+1
        prescidxresult = connection.execute(qprescidx,{'runnum':runnum})
        for prescidxr in prescidxresult:
            lsnum = prescidxr['lsnum']
            prescidx = prescidxr['prescidx']
            prescidxmap[lsnum] = prescidx
    #print prescidxmap
    for trgbitnameid, bitparams in bitnamemap.iterrows():
        bitname = bitparams['BITNAME']
        bitid = bitparams['BITID']
        isalgo = bitparams['ISALGO']
        bitalias = bitname
        if not isalgo:
            bitalias = str(bitid)
        bitaliasmap[bitalias] = trgbitnameid

    with connection.begin() as trans:
        result = connection.execute(qpresc,{'trgdataid':trgdataid})        
        for row in result:
            lsnum = row['lsnum']
            if runnum<150008:
                try:
                    prescblob = unpackblobstr(row['prescaleblob'],'l')
                    trgcountblob = unpackblobstr(row['trgcountblob'],'l')
                    if not prescblob or not trgcountblob:
                        continue
                except ValueError:
                    prescblob = unpackblobstr(row['prescaleblob'],'I')
                    trgcountblob = unpackblobstr(row['trgcountblob'],'I')
                    if not prescblob or not trgcountblob:
                        continue
            else:
                prescblob = unpackblobstr(row['prescaleblob'],'I')
                trgcountblob = unpackblobstr(row['trgcountblob'],'I')
                if not prescblob or not trgcountblob:
                    continue
            prescalevalues = pd.Series(prescblob)
            trgcountvalues = pd.Series(trgcountblob)

            isalgo = False
            for idx, prescval in prescalevalues.iteritems():
                if idx>127:
                    bitpos = idx-128
                    bitalias = str(bitpos)
                    bitnameid = 65535
                    isalgo = False
                else:
                    bitpos = idx
                    bitalias = algobitalias[bitpos]                    
                    if bitalias=='False': continue
                    bitnameid = bitaliasmap[bitalias]
                    isalgo = True
                counts = 0
                prescidx = 0
                try:
                    counts = trgcountvalues[idx]
                except IndexError:
                    pass
                try:
                    prescidx = prescidxmap[lsnum]
                except KeyError:
                    pass
                allrows.append({'datatagid':destdatatagidmap[lsnum], 'trgbitid':bitpos, 'trgbitnameid':bitnameid, 'isalgo':isalgo, 'prescidx':prescidx ,'prescval':prescval, 'counts':counts})
                
    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)

def transfer_hltdata(connection,destconnection,runnum,hltdataid,destdatatagidmap):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    p = re.compile('^HLT_+',re.IGNORECASE)
    pathnamedf = pd.DataFrame.from_csv('hltpaths.csv',index_col=False)
    pathnamemap = {}
    for i,row in pathnamedf.iterrows():
        n =row['HLTPATHNAME']
        if p.match(n) is None:
            continue
        d = row['HLTPATHID']
        pathnamemap.setdefault(n,[]).append( int(d) )
    #print pathnamemap
    #print pathnamemap.keys()
        
    qprescidx = '''select lumi_section as lsnum, prescale_index as prescidx from CMS_GT_MON.LUMI_SECTIONS where run_number=:runnum and prescale_index!=0'''

    qpathname = '''select pathnameclob from CMS_LUMI_PROD.HLTDATA where data_id=:hltdataid'''
    
    qhlt = '''select cmslsnum as lsnum, prescaleblob as prescaleblob, hltcountblob as hltcountblob, hltacceptblob as hltacceptblob from CMS_LUMI_PROD.LSHLT where data_id=:hltdataid'''    

    qpathid = '''select lsnumber as lsnum, pathid as hltpathid from cms_runinfo.hlt_supervisor_triggerpaths where runnumber=:runnum'''
    
    i = '''insert into HLT_RUN1(DATATAGID,HLTPATHID,PRESCIDX,PRESCVAL,L1PASS,HLTACCEPT) values(:datatagid, :hltpathid, :prescidx, :prescval, :l1pass, :hltaccept)'''
    
    allrows = []
    
    prescidxmap = {}
    
    lspathids = {}
    
    with connection.begin() as trans:
        prescidxresult = connection.execute(qprescidx,{'runnum':runnum})
        for prescidxr in prescidxresult:
            lsnum = prescidxr['lsnum']
            prescidx = prescidxr['prescidx']
            prescidxmap[lsnum] = prescidx

    with connection.begin() as trans:
        lspathidresult = connection.execute(qpathid,{'runnum':runnum})
        for lspathr in lspathidresult:
            lspathids.setdefault(lspathr['lsnum'],[]).append(lspathr['hltpathid'])
    
    with connection.begin() as trans:
        pathnameresult = connection.execute(qpathname,{'hltdataid':hltdataid})        
        for row in pathnameresult:
            pathnameclob = row['pathnameclob']
            pathnames = pathnameclob.split(',')

        hltresult = connection.execute(qhlt,{'hltdataid':hltdataid})
        for row in hltresult:
            lsnum = row['lsnum']
            if runnum<150008:
                try:
                    prescblob = unpackblobstr(row['prescaleblob'],'l')
                    hltcountblob = unpackblobstr(row['hltcountblob'],'l')
                    hltacceptblob = unpackblobstr(row['hltacceptblob'],'l')
                    if not prescblob or not hltcountblob or not hltacceptblob:
                        continue
                except ValueError:
                    prescblob = unpackblobstr(row['prescaleblob'],'I')
                    hltcountblob = unpackblobstr(row['hltcountblob'],'I')
                    hltacceptblob = unpackblobstr(row['hltacceptblob'],'I')
                    if not prescblob or not hltcountblob or not hltacceptblob:
                        continue
            else:
                prescblob = unpackblobstr(row['prescaleblob'],'I')
                hltcountblob = unpackblobstr(row['hltcountblob'],'I')
                hltacceptblob = unpackblobstr(row['hltacceptblob'],'I')
                if not prescblob or not hltcountblob or not hltacceptblob:
                    continue
            prescalevalues = pd.Series(prescblob)
            hltcountvalues = pd.Series(hltcountblob)
            hltacceptvalues = pd.Series(hltacceptblob)
            
            for idx, prescval in prescalevalues.iteritems():
                prescidx = 0
                hltcounts = 0
                hltaccept = 0
                hltpathname = ''
                hltpathid = 0
                try:
                    hltpathname = pathnames[idx]
                except IndexError:
                    pass
                if p.match(hltpathname) is None: continue
                if hltpathname.find('Calibration')!=-1: continue
                try:
                    hltcounts = hltcountvalues[idx]
                except IndexError:
                    pass
                try:
                    hltaccept = hltacceptvalues[idx]
                except IndexError:
                    pass
                try:
                    prescidx = prescidxmap[lsnum]
                except KeyError:
                    pass
                pathidcandidates = pathnamemap[hltpathname]
                pathidinsersection = list( set(pathidcandidates) & set(lspathids[lsnum]) )
                hltpathid = pathidinsersection[0]

                allrows.append({'datatagid':destdatatagidmap[lsnum], 'hltpathid':hltpathid, 'prescidx':prescidx, 'prescval':prescval, 'l1pass':hltcounts, 'hltaccept':hltaccept})

    with destconnection.begin() as trans:
        r = destconnection.execute(i,allrows)


def transfer_lumidata(connection,destconnection,runnum,lumidataid,destdatatagidmap):
    '''
    prerequisite : ids_datatag has already entries for this run
    '''
    qlumioc = '''select LUMILSNUM as lsnum, INSTLUMI as avgrawlumi, BXLUMIVALUE_OCC1 as bxlumiblob from CMS_LUMI_PROD.LUMISUMMARYV2 where DATA_ID=:lumidataid'''
    
    with connection.begin() as trans:
         lumiresult = connection.execute(qlumioc,{'lumidataid':lumidataid})
         for row in lumiresult:
             lsnum = row['lsnum']
             avgrawlumi = row['avgrawlumi']
             bxrawlumiblob = unpackblobstr(row['bxlumiblob'],'f')
             if not bxrawlumiblob:
                 continue
             bxrawlumi = pd.Series(bxrawlumiblob, dtype=np.dtype("object"))
             bxrawlumi_idx = bxrawlumi.nonzero()
             bxrawlumi = bxrawlumi.loc[bxrawlumi_idx]
             print lsnum, bxrawlumi_idx,bxrawlumi
             
if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connectstr = sys.argv[1]
    parser = SafeConfigParser()
    parser.read('readdb2.ini')
    passwd = parser.get(connectstr,'pwd')
    idx = connectstr.find('@')
    connecturl = connectstr[:idx]+':'+passwd.decode('base64')+connectstr[idx:]
    engine = create_engine(connecturl)
    connection = engine.connect().execution_options(stream_results=True)
    desturl = 'sqlite:///test.db'
    destengine = create_engine(desturl)
    destconnection = destengine.connect()
    
    #transfertimeinfo(connection,destconnection,193091)
    #transfer_ids_datatag(connection,destconnection,193091,1649)
    #destdatatagid = datatagid_of_run(destconnection,193091,datatagnameid=0)
    destdatatagid_map = datatagid_of_ls(destconnection,193091,datatagnameid=0)
    #print destdatatagid_map
    #transfer_runinfo(connection,destconnection,193091,destdatatagid)
    #transfer_trgconfig(connection,destconnection,193091,1477,destdatatagid)
    #transfer_beamintensity(connection,destconnection,193091,1649,destdatatagid_map)
    #transfer_trgdata(connection,destconnection,193091,1477,destdatatagid_map)
    #transfer_hltdata(connection,destconnection,193091,1391,destdatatagid_map)
    transfer_lumidata(connection,destconnection,193091,1649,destdatatagid_map)
