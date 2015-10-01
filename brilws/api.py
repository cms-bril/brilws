import numpy as np
import pandas as pd
from sqlalchemy import exc,schema,types,Table,MetaData,Column
from datetime import datetime
import decimal
import os
import random
import time
#import yaml
import re
import contextlib
import sys
import ast
import logging
import string
from brilws import params
#from ConfigParser import SafeConfigParser

decimalcontext = decimal.getcontext().copy()
decimalcontext.prec = 3

log = logging.getLogger('brilws')

oracletypemap={
'uint8':'number(3)',
'uint16':'number(5)',
'uint32':'number(10)',
'uint64':'number(20)',
'int8':'number(3)',
'int16':'number(5)',
'int32':'number(10)',
'int64':'number(20)',
'float':'binary_float',
'double':'binary_double',
'string':'varchar2(4000)',
'blob':'blob',
'bool':'number(1)',
'timestamp':'timestamp'
}

sqlitetypemap={
'uint8':'INTEGER',
'uint16':'INTEGER',
'uint32':'INTEGER',
'uint64':'INTEGER',
'int8':'INTEGER',
'int16':'INTEGER',
'int32':'INTEGER',
'int64':'INTEGER',
'float':'REAL',
'double':'REAL',
'string':'TEXT',
'blob':'BLOB',
'bool':'INTEGER',
'timestamp':'DATETIME'
}

#def unpackBlobtoArray(iblob,itemtypecode):
#    '''
#    Inputs:
#    iblob: 
#    itemtypecode: python array type code 
#    '''
#    if itemtypecode not in ['c','b','B','u','h','H','i','I','l','L','f','d']:
#        raise RuntimeError('unsupported typecode '+itemtypecode)
#    result=array.array(itemtypecode)
#    blobstr=iblob.readline()
#    if not blobstr :
#        return None
#    result.fromstring(blobstr)
#    return result

    
####################
##    Selection API
####################

_maxls = 9999
_maxrun = 999999

class brilwsException(Exception):
    pass

class NotSupersetError(brilwsException):
    def __init__(self, message, runnum,superset,subset):
        super(brilwsException, self).__init__(message)
        self.runnum = runnum
        self.superset = superset
        self.subset = subset
    
def expandrange(element):
    '''
    expand [x,y] to range[x,y+1]
    output: np array
    '''
    return np.arange(element[0],element[1]+1)

def consecutive(npdata, stepsize=1):
    '''
    split input array into chunks of consecutive numbers
    np.diff(a,n=1,axis=-1)
    Calculate the n-th order discrete difference along given axis.
    output: list of ndarrays
    '''
    return np.split(npdata, np.where(np.diff(npdata) != stepsize )[0]+1)
    
def checksuperset(iovseries,cmsseries):
    '''
    input:
        iovseries: pd.Series 
        cmsseries: pd.Series from dict {run:[[]],}
    output:
      throw NotSupersetError exception if not superset
    '''
    iovdict = {}
    for data in iovseries:
        runnum = data.index.tolist()[0]
        v = pd.Series(data[runnum]).apply(expandrange)
        v = v.apply(np.unique)
        vi = np.hstack(v.values)
        if not iovdict.has_key(runnum):            
            iovdict[runnum] = []            
        iovdict[runnum] = iovdict[runnum]+vi.tolist()
    for runnum in sorted(iovdict.keys()):
        lsrange = iovdict[runnum]
        if runnum in cmsseries.index:
            cmslsvals = pd.Series(cmsseries[runnum]).apply(expandrange)
            cmslsvals_flat = np.unique(np.hstack(cmslsvals.values))
            if not set(lsrange).issuperset(cmslsvals_flat):
                supersetlist = [[min(x),max(x)] for x in consecutive(np.array(lsrange))]
                subsetlist = cmsseries[runnum]
                raise NotSupersetError('NotSupersetError',runnum,supersetlist,subsetlist)
        
def mergerangeseries(x,y):
    '''
    merge two range type series
    x [[x1min,x1max],[x2min,x2max],...]
    y [[y1min,y1max],[y2min,y2max],...]
    into
    z [[z1min,z1max],[z2min,z2max],...]
    '''
    a = pd.Series(x).apply(expandrange)
    ai = np.hstack(a.values)
    b = pd.Series(y).apply(expandrange)
    bi = np.hstack(b.values)    
    i = np.intersect1d(np.unique(ai),np.unique(bi),assume_unique=True)
    scatter = consecutive(i)
    return scatter

def merge_two_dicts(x,y):
    z = x.copy()
    z.update(y)
    return z

def mergeiovrunls(iovselect,cmsselect):
    '''
    merge iovselect list and cms runls select dict
    input:
        iovselect: pd.Series from dict {run:[[]],}
        cmsselect:  [[iovtag,pd.Series],...]  pd.Series from dict {run:[[]],}
        
    '''
    cmsselect_runs = cmsselect.index
    final = []#[[iovtag,{}],[iovtag,{}]]
    previoustag = ''
    for [iovtag,iovtagrunls] in iovselect:
        iovtagruns = iovtagrunls.index
        runlsdict = {}
        selectedruns = np.intersect1d(cmsselect_runs,iovtagruns)
        if selectedruns.size == 0: continue
        for runnum in selectedruns:
            scatter = mergerangeseries(iovtagrunls[runnum],cmsselect[runnum])
            for c in scatter:
                if len(c)==0: continue
                runlsdict.setdefault(runnum,[]).append([np.min(c),np.max(c)])                
        if iovtag!=previoustag:
            if runlsdict:
                final.append([iovtag,runlsdict])
                previoustag = iovtag
        else:
            x = final[-1][1]                
            y = runlsdict
            final[-1][1] = merge_two_dicts(x,y)
    return final

def parseselectionJSON(filepath_or_buffer):
    d = get_filepath_or_buffer(filepath_or_buffer)
    data = ''
    if os.path.isfile(d):
        with open(d,'r') as f:
            data = f.read().lstrip()        
    else:
        data = filepath_or_buffer.lstrip()
    if data[0]=='[':
        return parseiovtagselectionJSON(data)
    else:
        return parsecmsselectJSON(data)
        
def parseiovtagselectionJSON(filepath_or_buffer):
    """
    parse iov tag selection file
    input:
        if file, parse file
    output:
        normtag string
          or 
        list [iovtag,"{run:[[1,9999]],run:[[1,9999]]}" , [iovtag,"{run:[[lsstart,lsstop]],...}" ]                     
    """
    result = None
    d = get_filepath_or_buffer(filepath_or_buffer)    
    if os.path.isfile(filepath_or_buffer):
        result = pd.read_json(d,orient='index',convert_axes=False,typ='Series')
    elif filepath_or_buffer.find('[') == -1:
        return filepath_or_buffer
    else:
        spacer = re.compile(r'^\s+')
        d = spacer.sub('',d) #remove whitespace
        word = re.compile(r'(\w*[A-Za-z]\w*),')
        d = word.sub(r'"\1",',d) #add quotes to iovtag field
        result = pd.Series( ast.literal_eval(d) )
    final = []
    for r in result:
        iovtag = r[0]
        payload = r[1:]
        for piece in payload :
            if isinstance(piece,dict):
                s = pd.Series(piece)
                s.index = [int(k) for k in piece.keys()]
                final.append([iovtag,s])
            else:
                #p = '{"%s":[[1,%s]]}'%(piece,_maxls)
                p = {}
                p[piece] = [[1,_maxls]]
                final.append([iovtag,pd.Series(p)])
    return final

def parsecmsselectJSON(filepath_or_buffer,numpy=False):
    """
    parse cms selection json format
    input: 
        if file, parse file
        if string in dict format, eval as dict. Key is not required to be double quoted in this case
        if single string, convert to [int]
    output:
        pd.Series , index=runnum, value=[[lsmin,lsmax]]
    """
    d = get_filepath_or_buffer(filepath_or_buffer)

    try:
        result = pd.Series([int(d)])
        return result
    except ValueError:
        pass

    if os.path.isfile(d):
        result = pd.read_json(d,orient='index',convert_axes=False,typ='Series',numpy=numpy)
    else:
        d = ast.literal_eval(d)
        result = pd.Series(d)
    result.index = [int(i) for i in result.index]
    return result

@contextlib.contextmanager
def smart_open(filename=None):
    if filename and filename != '-':
        fh = open(filename, 'w')
    else:
        fh = sys.stdout
    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()
            
def seqdiff(original,exclude):
    return list(set(original)-set(exclude))

def create_table_stmt(tablename,dbflavor='sqlite'):
    """
    create table statement
    input:
       tablename
       ifnotexists: if true, add IF NOT EXISTS
    """
    if dbflavor=='sqlite':
        result='CREATE TABLE IF NOT EXISTS %s '%(tablename)
    else:
        result='CREATE TABLE %s'%(tablename)
    return result

def drop_table_stmt(tablename, dbflavor='sqlite'):
    if dbflavor=='oracle':
        result='DROP TABLE %s CASCADE CONSTRAINTS;'%(tablename)
    else:
        result='DROP TABLE IF EXISTS %s;'%(tablename)
    return result

def grant_stmt(tablename,writeraccount=None):
    result='GRANT SELECT ON "%s" TO PUBLIC;\n'%(tablename)
    if writeraccount is not None:
        result=result+'GRANT SELECT,INSERT,DELETE,UPDATE ON "%s" TO %s;'%(tablename,writeraccount)
    return result    

def create_index_stmt(tablename,indexdict):
    results=[]
    for idxname,cols in indexdict.items():
        idxcolsStr=','.join(cols)
        results.append('CREATE INDEX %s ON %s(%s) '%(idxname,tablename,idxcolsStr))
    return ';\n'.join(results)
    
def build_column_stmt(columns,typemap,notnull=[]):
    results=[]
    for cdict in columns:
        result=''
        cname,ctype = next(cdict.iteritems())
        ctype=typemap[ctype]
        result=result+'%s %s '%(cname,ctype)
        if notnull and cname in notnull:
          result=result+'NOT NULL '
        results.append(result)
    result=','.join(results)
    return result

def build_unique_stmt(tablename,uniquelist):
    results=[]
    for uniques in uniquelist:
       if type(uniques) is list:
           uniquesStr=','.join(uniques)
       else:
           uniquesStr=uniques 
       results.append('CONSTRAINT %s_UQ UNIQUE (%s)'%(tablename,uniquesStr))
    if results: return ',\n'.join(results)
    return ''

def build_pk_stmt(tablename,pklist):
    pkcolsStr=','.join(pklist)
    pkcolsStr=pkcolsStr
    result='CONSTRAINT %s_PK PRIMARY KEY(%s)'%(tablename,pkcolsStr) 
    if pkcolsStr: 
        return result
    return ''

def build_fk_stmt(tablename,fkdict):
    results=[]
    for fkname,fkcoldict in fkdict.items():
        newdict=dict(chain.from_iterable(map(methodcaller('items'),fkcoldict)))
        columndata=newdict['column']
        colsStr=','.join(columndata)
        parenttablename=newdict['parenttable']
        referencecolumns=newdict['referencecolumn']
        parentcolsStr=','.join(referencecolumns) 
        results.append('ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE CASCADE'%(tablename,fkname,colsStr,parenttablename,parentcolsStr))
    if results: return ';\n'.join(results)
    return ''

def build_sqlfilename(outfilenamebase,operationtype='create',suffix=None,dbflavor='sqlite'):
    result=''
    if suffix:
       result='%s_%s%s_%s.sql'%(outfilenamebase,dbflavor,operationtype,suffix)
    else:
       result='%s_%s%s.sql'%(outfilenamebase,dbflavor,operationtype)
    return result

def drop_tables_sql(outfilebase,schema_def,suffix=None,dbflavor='sqlite'):
    results=[]
    tables=schema_def.keys()
    outfilename=build_sqlfilename(outfilebase,operationtype='drop',suffix=suffix,dbflavor=dbflavor)
    for tname in tables:
        results.append(drop_table_stmt(tname,dbflavor=dbflavor))
    resultStr='\n'.join(results)
    if suffix:
       resultStr=resultStr.replace('&suffix',suffix)
    resultStr=resultStr.upper()

    with open(outfilename,'w') as sqlfile:
        sqlfile.write('/* tablelist: %s */\n'%(','.join([t.upper() for t in tables])))
        sqlfile.write(resultStr)
    
def create_tables_sql(outfilebase,schema_def,suffix=None,dbflavor='sqlite',writeraccount=''):
    '''
    input :
        outfilebase: output file name base
        schema_def: dictionary of dictionaries
    '''
    results=[]
    resultStr=''
    fkresults=[]
    ixresults=[]
    tables=schema_def.keys()
    outfilename=build_sqlfilename(outfilebase,operationtype='create',suffix=suffix,dbflavor=dbflavor)
    columntypemap={}  
    if dbflavor=='oracle':
        columntypemap = oracletypemap
    else:
        columntypemap = sqlitetypemap
    for tname in tables:       
        #if tname.find('NEXTID')!=-1:
        #    stmt=create_sequencetable_stmt(tname,dbflavor)
        #    results.append(create_sequencetable_stmt(tname,dbflavor))       
        #    continue      
        result=create_table_stmt(tname,dbflavor=dbflavor)
        cs=schema_def[tname]['columns']
        nnus=[]
        if schema_def[tname].has_key('notnull'):
           nnus=schema_def[tname]['notnull']
        if schema_def[tname].has_key('index'):
           idxes=schema_def[tname]['index']
           dictidxes={}
           if idxes: 
               dictidxes=dict( (k,v) for d in idxes for (k,v) in d.items() )
               ixresults.append(create_index_stmt(tname,dictidxes))        
        result=result+'('+build_column_stmt(cs,columntypemap,nnus)
        pks=schema_def[tname]['pk']
        fks={}
        for k in schema_def[tname].keys():
           if k.find('fk_')!=-1:
                fks[k]=schema_def[tname][k]
        result=result+','+build_pk_stmt(tname,pks)
        unqs=None
        if schema_def[tname].has_key('unique'):
           unqs=schema_def[tname]['unique']
           result=result+','+build_unique_stmt(tname,unqs)
        
        result=result+')'
        if dbflavor=='oracle' and tname.upper()=='IDS_DATATAG':
            result = result+' partition by range(RUNNUM) (partition r1a values less than (184000), partition r1b values less than (212000), partition r2a values less than (MAXVALUE))'
        result = result+';\n'
        if fks: fkresults.append(fk(tname,fks))
        if dbflavor=='oracle':
            result=result+grant_stmt(tname,writeraccount=writeraccount)
        results.append(result)
    resultStr='\n'.join(results)
    if resultStr.find('&suffix')!=-1:
        if suffix:
            resultStr=resultStr.replace('&suffix',suffix)
        else:
            raise Exception('--suffix is required but not specified')
    resultStr=resultStr.upper()  
    with open(outfilename,'w') as sqlfile: 
        sqlfile.write('/* tablelist: %s */\n'%(','.join([t.upper() for t in tables])))
        sqlfile.write(resultStr)
        if fkresults:
            fkresultStr=';\n'.join([t.replace('&suffix',suffix).upper() for t in fkresults])
            sqlfile.write('\n'+fkresultStr+';')
        if ixresults:
            ixresultStr=';\n'.join([t.replace('&suffix',suffix).upper() for t in ixresults])
            sqlfile.write('\n'+ixresultStr+';')

##### iovschema api
payloadtableprefix_ ='IOVP'
iovdict_typecodes_ = ['FLOAT32','UINT32','INT32','UINT64','INT64','UINT16','INT16','UINT8','INT8','STRING']

def nonsequential_key(generator_id):
    '''
    http://ericmittelhammer.com/generating-nonsequential-primary-keys/
    generator_id [1,256]
    '''
    now = int(time.time()*1000) #41 bits
    rmin = 1  
    rmax = 2**15 - 1
    rdm = random.randint(1, rmax)
    yield ((now << 22) + (generator_id << 14) + rdm )

def generate_key(n):
    '''
    generate id based on a unique number
    '''
    now = int(time.time()*1000) #41 bits
    yield ((now <<22) + n )
     
#String Folding
#The rows of result data returned by SQLAlchemy contain many repeated strings
#each one is a different Unicode object
#When these are passed to Pandas, it stores a copy of the data for each string
#on the C heap, which taking up memory.
#We want a single shared string object for any one value
#It's called folding.
#http://www.mobify.com/blog/sqlalchemy-memory-magic/
#
class StringFolder(object):
    """
    Class that will fold strings.
    This object may be safely deleted or go out of scope when strings have been folded.
    """
    def __init__(self):
        self.unicode_map = {}
    def fold_string(self,s):
        """
        Given a string (or unicode) parameter s, return a string object
        that has the same value as s (and may be s). For all objects
        with a given value, the same object will be returned. For unicode
        objects that can be coerced to a string with the same value, a
        string object will be returned.
        If s is not a string or unicode object, it is returned unchanged.
        :param s: a string or unicode object.
        :return: a string or unicode object.
        """
        # If s is not a string or unicode object, return it unchanged
        if not isinstance(s, basestring):
            return s
        
        # If s is already a string, then str() has no effect.
        # If s is Unicode, try and encode as a string and use intern.
        # If s is Unicode and can't be encoded as a string, this try
        # will raise a UnicodeEncodeError.
        try:
            return intern(str(s))
        except UnicodeEncodeError:
            # Fall through and handle s as Unicode
            pass

        # Look up the unicode value in the map and return
        # the object from the map. If there is no matching entry,
        # store this unicode object in the map and return it.
        #t = self.unicode_map.get(s, None)
        #if t is None:
        #    # Put s in the map
        #    t = self.unicode_map[s] = s
        #return t
        return self.unicode_map.setdefault(s,s)
    
def string_folding_wrapper(results):
    """
    This generator yields rows from the results as tuples,
    with all string values folded.
    """
    # Get the list of keys so that we build tuples with all
    # the values in key order.
    keys = results.keys()
    folder = StringFolder()
    for row in results:
        yield tuple( folder.fold_string(row[key]) for key in keys )    

def iov_getpayload(connection,payloadid,payloaddatadict,maxnitems=1):
    """
    input:
        connection:      db handle
        payloadid:       payloadid
        payloaddatadict: payload datadict [{'val':,'key':,'alias':,'maxnpos':}]
    output:
        payload:          [[[]]] or [[{}]] 
        fieldalias:       [alias] 
    sql:
        val : select IITEM,IPOS,VAL from %s where PAYLOADID=:payloadid and IFIELD=:ifield and ISKEY=0;
        key : select k.IITEM, k.IPOS, k.VAL, v.VAL from %s as k, %s as v where k.PAYLOADID=v.PAYLOADID and k.IITEM=v.IITEM and k.IFIELD=v.IFIELD and k.IPOS=v.IPOS and k.PAYLOADID=:payloadid and k.IFIELD=:ifield and k.ISKEY=1 and v.ISKEY=0;
    """

    nfields = len(payloaddatadict)
    payload = [None]*maxnitems            
    q = """select IITEM as iitem, IPOS as ipos, VAL as val from %s where ISKEY=0 and PAYLOADID=:payloadid and IFIELD=:ifield"""
    qq = """select k.IITEM as iitem, k.IPOS as ipos, k.VAL as key, v.VAL as val from %s as k, %s as v where k.ISKEY=1 and v.ISKEY=0 and k.PAYLOADID=v.PAYLOADID and k.IITEM=v.IITEM and k.IFIELD=v.IFIELD and k.IPOS=v.IPOS and k.PAYLOADID=:payloadid and k.IFIELD=:ifield"""
    
    with connection.begin() as trans:
        for field_idx, field_dict in enumerate(payloaddatadict):
            valtable_name = field_dict['val']            
            maxnpos = field_dict['maxnpos']
            if not valtable_name:
                raise ValueError('invalid value table name %s'%valtable_name)
            result = None
            keytable_name = field_dict['key']
            if keytable_name:
                result = connection.execute(qq%(keytable_name,valtable_name),{'payloadid':payloadid,'ifield':field_idx})
            else:
                result = connection.execute(q%(valtable_name),{'payloadid':payloadid,'ifield':field_idx})
            for r in result:
                iitem = r['iitem']
                ipos = r['ipos']                
                val = r['val']
                key = None
                if payload[iitem] is None:
                    payload[iitem] = [None]*nfields
                if payload[iitem][field_idx] is None:
                    payload[iitem][field_idx] = [None]*maxnpos
                if not r.has_key('key'):
                    payload[iitem][field_idx][ipos] = val
                else:
                    key = r['key']
                    payload[iitem][field_idx][ipos] = (key,val)
    payload = [x for x in payload if x is not None]
    
    return payload

def data_gettags(engine,schemaname=''):
    """
    inputs:
        connection: db handle
    outputs:
        result[ datatagname ] = [ datatagnameid,creationutc',comments ]
    sql: select datatagnameid, datatagname, creationutc, comments from DATATAGS
         
    """
    basetablename = tablename = 'DATATAGS'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
        
    result = {}
    q =  """select datatagnameid, datatagname, creationutc, comments from %s"""%(tablename)
    log.debug(q)
    connection = engine.connect()
    qresult = connection.execute(q,{})
    for row in qresult:
        creationutc=''
        comments=''
        if row['creationutc']: creationutc=row['creationutc']
        if row['comments']: comments=row['comments']
        result[ row['datatagnameid'] ] = [ row['datatagname'],creationutc,comments ]
    return result

def iov_gettag(engine,tagname,schemaname=''):
    '''
    inputs:
        connection:  db handle
    outputs:
        [tagid,creationutc,datasource,applyto,isdefault,comments]
    '''
    basetablename = tablename = 'iovtags'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    q =  """select tagid, creationutc, datasource, applyto, isdefault, comments from %s where tagname=:tagname"""%(tablename)    
    log.debug(q)
    connection = engine.connect()
    binddict = {'tagname':tagname}
    qresult = connection.execute(q,binddict)
    result = []
    for row in qresult:
        result = [ row['tagid'],row['creationutc'],row['datasource'],row['applyto'],row['isdefault'],row['comments'] ]
    return result

def iov_gettags(engine,datasource=None,applyto=None,isdefault=False,schemaname=''):
    """
    inputs:
        connection:  db handle
        optional query parameters: tagid, tagname,datasource,applyto,isdefault
    outputs:
       {tagname:[tagid,creationutc,datasource,applyto,isdefault,comments]}    
    sql: select tagid, tagname, creationutc, datasource, applyto, isdefault, comments from IOVTAGS where
         
    """
    basetablename = tablename = 'iovtags'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
        
    result = {}
    q =  """select tagid, tagname, creationutc, datasource, applyto, isdefault, comments from %s"""%(tablename)
    qCondition = ''
    qPieces = []
    binddict = {}

    if datasource:
        qPieces.append( "datasource=:datasource")
        binddict['datasource'] = datasource
    if applyto:
        qPieces.append( "applyto=:applyto" )
        binddict['applyto'] = applyto
    if isdefault:
        qPieces.append("isdefault=1")
    if qPieces:
        qCondition = ' and '.join(qPieces)
    if qCondition: q = q+' where '+qCondition
    log.debug(q)
    connection = engine.connect()
    qresult = connection.execute(q,binddict)
    for row in qresult:
        result[row['tagname']] = [ row['tagid'],row['creationutc'],row['datasource'],row['applyto'],row['isdefault'],row['comments'] ]
    return result

def _insert_iovtag(connection,tablename,iovtagid,iovtagname,creationutc,datasource,applyto,isdefault,comments,schemaname=None):
    t = Table(tablename, MetaData(), Column('tagid',types.BigInteger), Column('tagname',types.String),Column('creationutc',types.String), Column('applyto',types.String), Column('datasource',types.String), Column('isdefault',types.BOOLEAN), Column('comments',types.String) , schema=schemaname)
    log.debug( str( t.insert() ) )
    log.debug( 'tagid=%ul, tagname=%s, creationutc=%s, applyto=%s, datasource=%s, isdefault=%d, comments=%s'%(iovtagid,iovtagname,creationutc,applyto,datasource,int(isdefault),comments) )
    connection.execute( t.insert(), tagid=iovtagid, tagname=iovtagname, creationutc=creationutc, applyto=applyto, datasource=datasource, isdefault=isdefault, comments=comments)    
    
def _insert_iovdata(connection,tablename,iovtagid,since,payloadstr,func,comments,schemaname=None):
    t = Table(tablename, MetaData(), Column('tagid',types.BigInteger), Column('since',types.Integer),Column('payload',types.String), Column('func',types.String), Column('comments',types.String),schema=schemaname )
    try:
        log.debug( str( t.insert() ) )
        log.debug( 'tagid=%ul, since=%d, payload=%s, func=%s, comments=%s'%(iovtagid,since,payloadstr,func,comments) )
        connection.execute( t.insert(), tagid=int(iovtagid), since=since, payload=payloadstr, func=func, comments=comments)
    except exc.IntegrityError, e:
        if str(e).find('unique constraint')!=-1:
            log.debug( 'Duplicated key iovtagid %ul, since %d, skip insertion, return 0'%(iovtagid,since) )
            return 0
    return iovtagid

'''
def _insert_iovpayload(connection,payloadid,payloadfields,payloadfielddata,schemaname=None):
    tablename = 'iovp_'
        
    for fieldid,fielddata in enumerate(payloadfields):
        fieldname = fielddata[0]
        fieldtype = fielddata[1]
        maxlength = fielddata[2]
        
        ttype=''
        dbtype=None
        dataval = payloadfielddata[fieldname]
        if maxlength==1:
            if fieldtype.lower()=='float':
                ttype = 'float'
                dbtype = types.FLOAT
            if fieldtype.lower()=='bool':
                ttype = 'boolean'
                dbtype = types.BOOLEAN
            if fieldtype.lower().find('str')!=-1:
                ttype='string'
                dbtype = types.String
            if fieldtype.lower() in ['uint8','int8','uint16','int16']:
                ttype='smallint'
                dbtype = types.SMALLINT
            if fieldtype.lower() in ['uint32','int32']:
                ttype='int'
                dbtype = types.INT
        else:
            ttype = 'blob'
            typecode = ''
            if fieldtype=='float':
                typecode = 'f'
            elif fieldtype=='uint8':
                typecode = 'B'
            elif fieldtype=='int8':
                typecode = 'b'
            elif fieldtype=='uint16':
                typecode = 'H'
            elif fieldtype=='int16':
                typecode = 'h'
            elif fieldtype=='uint32':
                typecode = 'I'
            elif fieldtype=='int32':
                typecode = 'i'
            else:
                typecode = 'c'
                
            dataval = packlistoblob(typecode,dataval)
        tname = tablename+ttype
        t = Table(tname, MetaData(), Column('payloadid',types.BigInteger), Column('ifield',types.SmallInteger),Column('val',dbtype),schema=schemaname )
        log.debug( str( t.insert() ) )
        log.debug( 'payloadid=%ul, ifield=%d'%(payloadid,fieldid) )
        connection.execute( t.insert(), payloadid=payloadid, ifield=fieldid, val=dataval )
'''

"""
def _get_iovpayload(connection,payloadid,payloadfields,schemaname=None):
    '''
    output: [(fieldtype,fieldval)] 
    select val from iovp_x where payloadid=:payloadid and ifield=:fieldid
    '''
    tablename = basetablename = 'iovp_'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    result = []
    for fieldid,fielddata in enumerate(payloadfields):
        fieldname = fielddata[0]
        fieldtype = fielddata[1]
        maxlength = fielddata[2]
        if maxlength==1:
            if fieldtype.lower()=='float':
                ttype = 'float'
            elif fieldtype.lower()=='bool':
                ttype = 'boolean'
            elif fieldtype.lower().find('str')!=-1:
                ttype='string'
            elif fieldtype.lower() in ['uint8','int8','uint16','int16']:
                ttype='smallint'
            elif fieldtype.lower() in ['uint32','int32']:
                ttype='int'
        else:
            ttype = 'blob'
            typecode = ''
            if fieldtype=='float':
                typecode = 'f'
            elif fieldtype=='uint8':
                typecode = 'B'
            elif fieldtype=='int8':
                typecode = 'b'
            elif fieldtype=='uint16':
                typecode = 'H'
            elif fieldtype=='int16':
                typecode = 'h'
            elif fieldtype=='uint32':
                typecode = 'I'
            elif fieldtype=='int32':
                typecode = 'i'
            else:
                typecode = 'c'
        t = tablename+ttype    
        q = '''select val from %s where payloadid=:payloadid and ifield=:fieldid'''%t
        log.debug( q )
        log.debug( 'payloadid=%ul, ifield=%d'%(payloadid,fieldid) )
        binddict = {'payloadid':payloadid,'fieldid':fieldid}
        r = connection.execute( q, binddict )
        for row in r:
            val = row['val']
            if ttype=='blob' and val is not None:
                val = unpackBlobtoArray(val,typecode)
        result.append( [fieldname,ttype,val] )
    return result
 """
    
def iov_insertdata(engine,iovtagname,datasource,iovdata,applyto='lumi',isdefault=False,comments='',schemaname=None ):
    '''
    create a new iov tag or append to an existing one
    iovdata:[{since:{'func':,'payload':,'commments':}},]
    '''
    basetablename = 'iovtags'
    if schemaname:
       tablename = '.'.join([schemaname,basetablename])
    datatablename = 'iovtagdata'
        
    iovtagid = 0
    selectq = '''select tagid from %s where tagname=:iovtagname'''%tablename
    log.debug(selectq)
    connection = engine.connect()
    with connection.begin() as trans: 
        selectqresult = connection.execute(selectq,{'iovtagname':iovtagname})
        for row in selectqresult:
            iovtagid = row['tagid']
            log.debug( 'tag %s exists with id %ul '%(iovtagname,iovtagid) )            
        if not iovtagid:
            log.debug( 'create new tag %s'%(iovtagname) )
            iovtagid = next(nonsequential_key(78))
            utcstr = datetime.now().strftime(params._datetimefm)
            _insert_iovtag(connection,basetablename,iovtagid,iovtagname,utcstr,datasource.upper(),applyto.upper(),isdefault,comments,schemaname=schemaname)

        for sincedict in iovdata:
            sincerunnum = sincedict.keys()[0]
            payloaddata = sincedict.values()[0]
            func = sincedict[sincerunnum]['func']
            sincecomments =  sincedict[sincerunnum]['comments']
            payloadstr = str(sincedict[sincerunnum]['payload'])
            log.debug( 'append to tag %s since %d'%(iovtagname,sincerunnum) )
            inserted = _insert_iovdata(connection,datatablename,iovtagid,sincerunnum,payloadstr,func,sincecomments,schemaname=schemaname)
            print 'inserted ',inserted

"""
def parsepayloaddict(payloaddict):
    '''
    input: fieldname:fieldtype:maxlength fieldname:fieldtype:maxlength
    output : [[fieldname,fieldtype,maxlength]]
            where maxlength is optional, default to 1
    '''
    result = []
    fields = payloaddict.split(' ')
    for field in fields:
        fieldinfo = field.split(':')
        if len(fieldinfo)==2:
            fieldinfo.append(1)
        else:
            fieldinfo[2] = int(fieldinfo[2])
        result.append(fieldinfo)            
    return result
"""

def packlistoblob(typecode,data):
    dataarray = array.array(typecode,list(data))
    return buffer(dataarray.tostring())        

def iov_gettags(engine,isdefault=False,datasource='',applyto='',schemaname=''):
    '''
    output: iovtags 
    result: [[tagid,tagname,creationutc,applyto,datasource,isdefault,comments]]
    '''
    basetablename = tablename = 'iovtags'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    q = '''select tagid, tagname, creationutc, applyto, datasource, isdefault, comments from %s'''%(tablename)
    qconditions = []
    qparams = {}    
    if datasource:        
        qconditions.append('datasource=:datasource')
        qparams['datasource'] = datasource.upper()
    if applyto:
        qconditions.append('applyto=:applyto')
        qparams['applyto'] = applyto.upper()
    if isdefault:
        qconditions.append('isdefault=1')
    if qconditions:
        q = q+' where '+' AND '.join(qconditions)
    log.debug(q)
    connection = engine.connect()
    qresult = connection.execute(q,qparams)
    result = {}
    for row in qresult:
        result[row['tagname']] = [ row['tagid'],row['creationutc'],row['applyto'],row['datasource'],row['isdefault'],row['comments'] ] 
    return result
"""
def iov_getvaliddata(engine,iovtagname,runnum,schemaname=''):
    '''
    get valid data for runnum 
    result: (func,params)
    '''
    basetagstable = tagstable = 'iovtags'
    basetagdatatable = tagdatatable = 'iovtagdata'
    if schemaname:
        tagstable = '.'.join([schemaname,basetagstable])
        tagdatatable = '.'.join([schemaname,basetagdatatable])  

    q = '''select func, payloadid, payloaddict from %s where since=( select max(d.since) as since from %s d, %s t where t.tagid=d.tagid and t.tagname=:tagname and d.since<=:runnum )'''%(tagdatatable,tagdatatable,tagstable)    
    log.debug(q)
    connection = engine.connect()
    qresult = connection.execute(q,{'tagname':iovtagname,'runnum':runnum})
    result = None
    for row in qresult:
        func = row['func']
        payloaddict = row['payloaddict']
        payload = row['payload']
        #payloadfields = parsepayloaddict(payloaddict)#[[fieldname,fieldtype,maxlength]]
        #payloaddata = _get_iovpayload(connection,payloadid,payloadfields,schemaname=schemaname)    
        result = ( row['func'],payloaddata )
    return result
            
   """

def iov_gettagdata(engine,iovtagname,schemaname=''):
    '''
    result: [[since,func,payload,comments]]
    '''
    basetagstable = tagstable = 'iovtags'
    basetagdatatable = tagdatatable = 'iovtagdata'
    if schemaname:
        tagstable = '.'.join([schemaname,basetagstable])
        tagdatatable = '.'.join([schemaname,basetagdatatable])
        
    q='''select d.since as since, d.payload as payload, d.func as func, d.comments as comments from %s d, %s t where t.tagid=d.tagid and t.tagname=:tagname order by d.since'''%(tagdatatable,tagstable)
    log.debug(q)
    connection = engine.connect()
    qresult = connection.execute(q,{'tagname':iovtagname})
    result = []
    for row in qresult:
        payload = row['payload']
        #print 'payload ',payload
        #payloadfields = parsepayloaddict(payloaddict)#[[fieldname,fieldtype,maxlength]]
        #payloaddata = _get_iovpayload(connection,payloadid,payloadfields,schemaname=schemaname)
        #print 'payloaddata ',payloaddata
        result.append( [ row['since'],row['func'],row['payload'],row['comments'] ] )
    return result
    
def iov_updatedefault(connection,tagname,defaultval=1):
    """
    inputs:
        connection: dbhandle
        tagname:    tagname
        defaultval: value(0 or 1) of isdefault column
    sql:
        update IOVTAGS set ISDEFAULT=:defaultval
    """
    if not defaultval in [0,1]:
        raise ValueError('ISDEFAULT value must be 0 or 1')
    log.debug('api.iov_updatedefault %s isdefault %d'%(tagname,defaultval))
    ui = """update IOVTAGS set ISDEFAULT=:isdefault where TAGNAME=:tagname"""
    with connection.begin() as trans:
        log.debug('api.iov_updatedefault query %s %d'%(tagname,defaultval))
        connection.execute(ui, {'isdefault': defaultval,'tagname':tagname})

def get_filepath_or_buffer(filepath_or_buffer):
    """
    Input: 
    filepath_or_buffer: filepath or buffer
    Output:
    a filepath_or_buffer
    """
    if isinstance(filepath_or_buffer, str):
        return os.path.expanduser(filepath_or_buffer)
    return filepath_or_buffer    


#def read_yaml(path_or_buf):
#    """
#    safe_load yaml string or file 
#    """
#    filepath_or_buffer = get_filepath_or_buffer(path_or_buf)
#    if isinstance(filepath_or_buffer, str):
#        try:
#            exists = os.path.exists(filepath_or_buffer)
#        except (TypeError,ValueError):
#            exists = False
#        if exists:
#            with open(filepath_or_buffer,'r') as f:
#                obj = yaml.safe_load(f)
#        else:
#            obj = yaml.safe_load(path_or_buf)
#            if type(obj) is not dict and type(obj) is not list:
#                raise IOError('file %s does not exist'%filepath_or_buffer)
#    elif hasattr(filepath_or_buffer, 'read'):
#        obj = filepath_or_buffer.read()
#    else:
#        obj = filepath_or_buffer
#    return obj

class BrilDataSource(object):
    def __init__(self):
        self._columns = None
        self._name = type(self).__name__
    #readonly members
    @property
    def name(self):
        return self._name
    @property
    def columns(self):
        return self._columns
    
    def _from_brildb(self,engine,schema='',index_col=None,columns=None):
        log.info('%s.from_brildb'%self.name)
        sourcetab = self.name.upper()
        log.info('to %s, %s')%(engine.url,sourcetab)
        result = pd.read_sql_table(sourcetab,engine,schema=schema,index_col=index_col,columns=columns)
        result.column = self._columns
        return result
    
    def _from_brildb_iter(self,engine,schema='',suffix='RUN1',index_col=None,columns=None,chunksize=1):
        log.info('%s.from_brildb_iter'%self.name)
        sourcetab = '_'.join([self.name.upper(),suffix])
        log.info('to %s, %s')%(engine.url,sourcetab)
        result = pd.read_sql_table(sourcetab,engine,schema=schema,index_col=index_col,columns=columns,cunksize=chunksize)
        result.column = self._columns
        return result
    
    def _to_brildb(self,engine,data,schema='',if_exists='append',index=True,index_label=None,chunksize=None):
        log.info('%s.to_brildb'%self.name)
        desttab = self.name.upper()
        if schema: desttab = '.'.join([schema.upper(),desttab])
        log.info('to %s, %s'%(engine.url,desttab))
        try:
            data.to_sql(desttab,engine,if_exists=if_exists,index=index,index_label=index_label,chunksize=chunksize)
        except exc.IntegrityError as e:
            reason = e.message
            log.warn(reason)
            pass
    
    def _to_csv(self,filepath_or_buffer,data,header=True,index=False,index_label=None,chunksize=None):
        log.info('%s.to_csv'%self.name)
        log.info('to %s '%(filepath_or_buffer))
        data.to_csv(filepath_or_buffer,header=header,index=index,index_label=index_label,chunksize=chunksize)

    def _from_csv(self,filepath_or_buffer,index_col=0):
        log.info('%s.from_csv'%self.name)
        log.info('from %s '%filepath_or_buffer)
        data = pd.read_csv(filepath_or_buffer,index_col=index_col)
        return data
##### Map ######    
#class BeamStatusMap(BrilDataSource):
#    def __init__(self):
#        super(BeamStatusMap,self).__init__()
#        self._columns = ['BEAMSTATUSID','BEAMSTATUS']
#    def to_brildb(self,engine,data,schema=''):
#        super(BeamStatusMap,self)._to_brildb(engine,data,schema=schema)
#    def to_csv(self,filepath_or_buffer,data):
#        super(BeamStatusMap,self)._to_csv(filepath_or_buffer,data)
#    def from_csv(self,filepath_or_buffer):
#        return super(BeamStatusMap,self)._from_csv(filepath_or_buffer)
#    def from_brildb(self,engine,schema=''):
#        return super(BeamStatusMap,self)._from_brildb(self,engine,schema=schema)
#    def from_sourcedb(self,engine):
#        if os.path.isfile(engine):
#            return self.from_csv(engine)
#        log.info('%s.from_sourcedb'%self.name)
#        if not os.path.isfile(engine):
#            raise IOError('sourcedb must be a csv file')
#        return self.from_csv(engine)
    
class HLTPathMap(BrilDataSource):
    def __init__(self):
        super(HLTPathMap,self).__init__()
        self._columns = ['hltpathid','hltpathname']
    def to_brildb(self,engine,data,schema=''):
        super(HLTPathMap,self)._to_brildb(engine,data,schema=schema,index=False)
    def to_csv(self,filepath_or_buffer,data):
        super(HLTPathMap,self)._to_csv(filepath_or_buffer,data,index=False)
    def from_csv(self,filepath_or_buffer):
        return super(HLTPathMap,self)._from_csv(filepath_or_buffer)
    def from_sourcedb(self,engine):
        log.info('%s.from_sourcedb'%self.name)
        q = """select PATHID as hltpathid,NAME as hltpathname from CMS_HLT.PATHS where ISENDPATH=0 and NAME like 'HLT/_%' escape '/' and NAME NOT like '%Calibration%'"""
        log.info(q)
        result = pd.read_sql_query(q,engine)
        result.columns = self._columns
        return result    
    def from_brildb(self,engine,schema=''):
        return super(HLTPathMap,self)._from_brildb(self,engine,schema=schema)
    
class DatasetMap(BrilDataSource):
    def __init__(self):
        super(DatasetMap,self).__init__()
        self._columns = ['DATASETID','DATASETNAME']
    def to_brildb(self,engine,data,schema=''):
        super(DatasetMap,self)._to_brildb(engine,data,schema=schema)
    def to_csv(self,filepath_or_buffer,data):
        super(DatasetMap,self)._to_csv(filepath_or_buffer,data)
    def from_csv(self,filepath_or_buffer):
        return super(DatasetMap,self)._from_csv(filepath_or_buffer)
    def from_sourcedb(self,engine):
        if os.path.isfile(engine):
            return self.from_csv(engine)
        log.info('%s.from_sourcedb'%self.name)
        q = """select DATASETID as DATASETID, DATASETLABEL as DATASETNAME from CMS_HLT.PRIMARYDATASETS where DATASETLABEL!='Unassigned path'"""
        log.info(q)
        result = pd.read_sql_query(q,engine)
        result.columns = self._columns
        return result
    def from_brildb(self,engine,schema=''):
        return super(DatasetMap,self)._from_brildb(self,engine,schema=schema)
    
#class AmodetagMap(BrilDataSource):
#    def __init__(self):
#        super(AmodetagMap,self).__init__()
#        self._columns = ['AMODETAGID','AMODETAG']
#    def to_brildb(self,engine,data,schema=''):
#        super(AmodetagMap,self)._to_brildb(engine,data,schema=schema)
#    def to_csv(self,filepath_or_buffer,data):
#        super(AmodetagMap,self)._to_csv(filepath_or_buffer,data)
#    def from_csv(self,filepath_or_buffer):
#        return super(AmodetagMap,self)._from_csv(filepath_or_buffer)    
#    def from_brildb(self,engine,schema=''):
#        return super(AmodetagMap,self)._from_brildb(self,engine,schema=schema)
#    def from_sourcedb(self,engine):
#        if os.path.isfile(engine):
#            return self.from_csv(engine)
#        log.info('%s.from_sourcedb'%self.name)
#        if not os.path.isfile(engine):
#            raise IOError('sourcedb must be a csv file')
#        return self.from_csv(engine)
    
class HLTStreamDatasetMap(BrilDataSource):
    def __init__(self):
        super(StreamDatasetHLTPathMap,self).__init__()
        self._columns = ['HLTPATHID','STREAMID','STREAMNAME','DATASETID']
    def to_brildb(self,engine,data,schema=''):
        super(HLTStreamDatasetMap,self)._to_brildb(engine,data,schema=schema)
    def to_csv(self,filepath_or_buffer,data):
        super(HLTStreamDatasetMap,self)._to_csv(filepath_or_buffer,data)
    def from_csv(self,filepath_or_buffer):
        return super(HLTStreamDatasetMap,self)._from_csv(filepath_or_buffer)
    def from_sourcedb(self,engine):
        if os.path.isfile(engine):
            return self.from_csv(engine)
        log.info('%s.from_sourcedb'%self.name)
        streamwhitelist = ["'A'"]
        selectedstreams = ','.join(streamwhitelist)
        q = """select p.PATHID as HLTPATHID,s.STREAMID as STREAMID,s.STREAMLABEL as STREAMLABEL,d.DATASETID as DATASETID from CMS_HLT.PATHSTREAMDATASETASSOC link,CMS_HLT.STREAMS s, CMS_HLT.PATHS p, CMS_HLT.PRIMARYDATASETS d where p.PATHID=link.PATHID and link.DATASETID=d.DATASETID and link.STREAMID=s.STREAMID and d.DATASETLABEL!='Unassigned path' and s.FRACTODISK>0 and s.STREAMLABEL in ({0}) and p.ISENDPATH=0 and p.NAME like 'HLT_%'""".format(selectedstreams)
        log.info(q)
        result = pd.read_sql_query(q,engine)
        result.columns = self._columns
        return result
    def from_brildb(self,engine,schema=''):
        return super(HLTStreamDatasetMap,self)._from_brildb(self,engine,schema=schema)
    
class TableShards(BrilDataSource):
    def __init__(self):
        super(TableShards,self).__init__()
        self._columns = ['id','minrun','maxrun']
    def to_brildb(self,engine,data,schema=''):
        super(TableShards,self)._to_brildb(engine,data,schema=schema)
    def to_csv(self,filepath_or_buffer,data):
        super(TableShards,self)._to_csv(filepath_of_buffer,data)        
    def from_csv(self,filepath_or_buffer):
        return super(TableShards,self)._from_csv(filepath_or_buffer)
    def from_brildb(self,engine,schema=''):
        return super(TableShards,self)._from_brildb(self,engine,schema=schema)
    def from_sourcedb(self,engine):
        log.info('%s.from_sourcedb'%self.name)
        if not os.path.isfile(engine):
            raise IOError('sourcedb must be a csv file')
        return self.from_csv(engine)
    
class TrgBitMap(BrilDataSource):
    def __init__(self):
        super(TrgBitMap,self).__init__()
        self._columns = ['bitnameid','bitid','bitname']
    def from_brildb(self,engine,schema=''):
        return super(TrgBitMap,self)._from_brildb(engine,schema=schema,index_col='BITNAMEID')
    def from_sourcedb(self,engine):
        log.info('%s.from_sourcedb'%self.name)
        algostartid = 65
        qAlgo = """select distinct ALGO_INDEX as bitid,ALIAS as bitname from CMS_GT.GT_RUN_ALGO_VIEW"""
        log.info(qAlgo)

        techbits = [ [id,128+id,str(id)] for id in xrange(0,64) ]        
        dftech = pd.DataFrame(techbits)
        dftech.columns = self._columns
        dfalgo = pd.read_sql_query(qAlgo,engine)
        dfalgo.columns = self._columns[1:]
        dfalgo['bitnameid'] = 65+pd.Series(range(len(dfalgo)))
        dfalgo = dfalgo.reindex_axis(self._columns,axis=1) #change column order
        result = pd.concat([dftech,dfalgo])
        return result
    def to_brildb(self,engine,data,schema=''):
        return super(TrgBitMap,self)._to_brildb(engine,data,schema=schema,index=False)
    def from_csv(self,filepath_or_buffer):
        return super(TrgBitMap,self)._from_csv(filepath_or_buffer)
    def to_csv(self,filepath_or_buffer,data):
        super(TrgBitMap,self)._to_csv(filepath_or_buffer,data,index=False)

class L1SeedMap(BrilDataSource):
    def __init__(self):
        super(L1SeedMap,self).__init__()
        self._columns = ['l1seedid','l1seed']
    def from_sourcedb(self,engine):        
        log.info('%s.from_sourcedb'%self.name)
        q = """select distinct s.PARAMID as l1seedid, s.VALUE as l1seed from CMS_HLT.STRINGPARAMVALUES s,CMS_HLT.paths p, CMS_HLT.parameters, CMS_HLT.superidparameterassoc, CMS_HLT.modules, CMS_HLT.moduletemplates, CMS_HLT.pathmoduleassoc, CMS_HLT.configurationpathassoc, CMS_HLT.configurations c where parameters.paramid=s.paramid and superidparameterassoc.paramid=parameters.paramid and modules.superid=superidparameterassoc.superid and moduletemplates.superid=modules.templateid and pathmoduleassoc.moduleid=modules.superid and p.pathid=pathmoduleassoc.pathid and configurationpathassoc.pathid=p.pathid and c.configid=configurationpathassoc.configid and moduletemplates.name='HLTLevel1GTSeed' and parameters.name='L1SeedsLogicalExpression' and p.ISENDPATH=0 and p.NAME like 'HLT/_%' escape '/' and p.NAME not like '%Calibration%' and s.VALUE not like '%/_Not%' escape '/' and s.VALUE not like '\"NOT %' and s.VALUE not like '% NOT %' and s.VALUE not like '%(%'"""
        log.info(q)
        result = pd.read_sql_query(q,engine)
        result.columns = self._columns
        result['l1seed'] = result['l1seed'].apply(lambda x: x.replace('"',''))        
        return result
    def to_brildb(self,engine,data,schema=''):
        super(L1SeedMap,self)._to_brildb(engine,data,schema=schema,index=False)
    def to_csv(self,filepath_or_buffer,data):
        super(L1SeedMap,self)._to_csv(filepath_or_buffer,data)
    def from_csv(self,filepath_or_buffer):
        return super(L1SeedMap,self)._from_csv(filepath_or_buffer)
##### Data ######    
class FillInfo(BrilDataSource):
    def __init__(self):
        super(FillInfo,self).__init__()
        self._columns = ['DATAID','FILLNUM','AMODETAG','EGEV','FILLSCHEME','NCOLLIDINGBX','CROSSINGANGLE','BETASTAR','BEAMCONFIG']
        
    def from_lumidb(self,engine,fillnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        tab = '.'.join([schema,'CMSRUNSUMMARY'])
        q = """select distinct FILLNUM,AMODETAG,EGEV,FILLSCHEME,NCOLLIDINGBUNCHES as NCOLLIDINGBX from %s where AMODETAG not in ('BMSETUP','(undefined)','MDEV') and FILLNUM=:fillnum"""%(tab)
        log.info(q)
        result = pd.read_sql_query(q,engine,params={'fillnum':fillnum})
        return result
    
    def from_sourcedb(self,engine,fillnum,schema='CMS_RUNTIME_LOGGER'):
        log.info('%s.from_sourcedb'%self.name)
        tab = '.'.join([schema,'RUNTIME_SUMMARY'])
        q = """select RUNTIME_TYPE_ID, LHCFILL,ENERGY,NCOLLIDINGBUNCHES,CROSSINGANGLE,BETASTAR from %s where LHCFILL=:fillnum"""%(tab)
        log.info(q)
        result = pd.read_sql_query(q,engine,params={'fillnum':fillnum})
        return result
    
class RunInfo(BrilDataSource):
    def __init__(self):
        super(RunInfo,self).__init__()
        self._columns = ['DATAID','RUNNUM','FILLNUM','HLTKEY','GT_RS_KEY']
        
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        tab = '.'.join([schema,'CMSRUNSUMMARY'])
        q = """select RUNNUM as RUNNUM,FILLNUM as FILLNUM,HLTKEY as HLTKEY,L1KEY as GT_RS_KEY from %s where RUNNUM=:runnum"""%(tab)
        log.info(q)
        result = pd.read_sql_query(q,engine,params={'runnum':runnum})
        return result
    
    def from_sourcedb(self,engine,runnum):
        log.info('%s.from_sourcedb'%self.name)
        tab = '.'.join(['CMS_RUNINFO','RUNSESSION_PARAMETER'])
        qFillnum = """select STRING_VALUE as FILLNUM from %s where RUNNUMBER=:runnum and NAME='CMS.SCAL:FILLN order by time'"""%(tab)# take the first one
        qHltkey = """select STRING_VALUE as HLTKEY from %s where RUNNUMBER=:runnum and NAME='CMS.LVL0:HLT_KEY_DESCRIPTION'"""%(tab)
        qTrgkey = """select STRING_VALUE as GT_RS_KEY from %s where RUNNUMBER=:runnum and NAME='CMS.TRG:TSC_KEY'"""%(tab)
        log.info(qHltkey)
        log.info(qTrgkey)
        hltkeyResult = pd.read_sql_query(qHltkey,engine,params={'runnum':runnum})
        trgkeyResult = pd.read_sql_query(qTrgkey,engine,params={'runnum':runnum})
        print hltkeyResult
        print trgkeyResult
        
class BeamstatusInfo(BrilDataSource):
    def __init__(self):
        super(BeamstatusInfo,self).__init__()
        self._columns = ['DATAID','RUNNUM','LSNUM','BEAMSTATUSID']
        
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        datatab = '.'.join([schema,'LUMIDATA'])
        lsdatatab = '.'.join([schema,'LUMISUMMARYV2'])
        qDatatab = """select max(DATA_ID) from %s where RUNNUM=:runnum"""%(datatab)
        log.info(qDatatab)        
        dataids = pd.read_sql_query(qDatatab,engine,params={'runnum':runnum})
        qLSData = """select BEAMSTATUS,BEAMENERGY from %s where DATA_ID=:dataid"""
        log.info(qLSData)
        for id in dataids:
            result = pd.read_sql_query(qLSData,engine,params={'dataid':id})
            
class Beamintensity(BrilDataSource):
    def __init__(self):
        super(Beamintensity,self).__init__()
        self._columns = ['DATAID','RUNNUM','LSNUM','BXIDX','BEAM1INTENSITY','BEAM2INTENSITY']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        datatab = '.'.join([schema,'LUMIDATA'])
        lsdatatab = '.'.join([schema,'LUMISUMMARYV2'])
        qDatatab = """select max(DATA_ID) from %s where RUNNUM=:runnum"""%(datatab)
        log.info(qDatatab)        
        dataids = pd.read_sql_query(qDatatab,engine,params={'runnum':runnum})
        qLSData = """select BEAMINTENSITYBLOB_1,BEAMINTENSITYBLOB_2 from %s where DATA_ID=:dataid"""
        log.info(qLSData)
        for id in dataids:
            result = pd.read_sql_query(qLSData,engine,params={'dataid':id})
            
class HFOCLumi(BrilDataSource):
    def __init__(self):
        super(HFOCLumi,self).__init__()
        self._columns = ['DATAID','RUNNUM','LSNUM','INSTLUMI']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        datatab = '.'.join([schema,'LUMIDATA'])
        lsdatatab = '.'.join([schema,'LUMISUMMARYV2'])
        qDatatab = """select max(DATA_ID) from %s where RUNNUM=:runnum"""%(datatab)
        log.info(qDatatab)        
        dataids = pd.read_sql_query(qDatatab,engine,params={'runnum':runnum})
        qLSData = """select INSTLUMI,BXLUMIVALUE_OCC1 from %s where DATA_ID=:dataid"""
        log.info(qLSData)
        for id in dataids:
            result = pd.read_sql_query(qLSData,engine,params={'dataid':id})
            
class PixelLumi(BrilDataSource):
    def __init__(self):
        super(PixelLumi,self).__init__()
        self._columns = ['DATAID','RUNNUM','CMSLSNUM','BXIDX','INSTLUMI']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        datatab = '.'.join([schema,'PIXELLUMIDATA'])
        lsdatatab = '.'.join([schema,'PIXELLUMISUMMARYV2'])
        qDatatab = """select max(DATA_ID) from %s where RUNNUM=:runnum"""%(datatab)
        log.info(qDatatab)
        dataids = pd.read_sql_query(qDatatab,engine,params={'runnum':runnum})        
        qLSData = """select INSTLUMI from %s where DATA_ID=:dataid"""
        for id in dataids:
            result = pd.read_sql_query(qLSData,engine,params={'dataid':id})
        return result
    
class Trg(BrilDataSource):
    def __init__(self):
        super(Trg,self).__init__()
        self._columns = ['DATAID','RUNNUM','CMSLSNUM','TRGBITID','TRGBITNAMEID','PRESCIDX','PRESCVAL','COUNTS']
    
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        trgdatatab = '.'.join([schema,'TRGDATA'])
        lstrgtab = '.'.join([schema,'LSTRG'])
        qTrgid = """select max(DATA_ID) from %s where RUNNUM=:runnum"""%(trgdatatab)
        log.info(qTrgid)
        trgids = pd.read_sql_query(qTrgid,engine,params={'runnum':runnum})
        qLSTrg = """select RUNNUM, CMSLSNUM, PRESCALEBLOB, TRGCOUNTBLOB from %s where DATA_ID=:dataid"""%(lstrgtab)
        log.info(qLSTrg)
        lstrgresult = pd.read_sql_query(qLSTrg,engine,{'dataid':id})
        return lstrgresult
    
    def from_sourcedb(self,engine,runnum,minls=1,nls=1):
        log.info('%s.from_sourcedb'%self.name)
        gtschema = 'CMS_GT'
        gtmonschema = 'CMS_GT_MON'
        qTech = """select COUNT_BX as COUNTS,LUMI_SECTION as CMSLSNUM,SCALER_INDEX as TRGBITID from CMS_GT_MON.V_SCALERS_FDL_TECH where RUN_NUMBER=:runnum and LUMI_SECTION=:lsnum order by SCALER_INDEX """
        log.info(qTech)
        qAlgo = """select COUNT_BX as COUNTS,LUMI_SECTION as CMSLSNUM,SCALER_INDEX as TRGBITID from CMS_GT_MON.V_SCALERS_FDL_ALGO where RUN_NUMBER=:runnum and LUMI_SECTION=:lsnum order by SCALER_INDEX """
        log.info(qAlgo)
        qPrescIdx = """select LUMI_SECTION,PRESCALE_INDEX from CMS_GT_MON.LUMI_SECTIONS where RUN_NUMBER=:runnum and LUMI_SECTION=:lsnum """
        log.info(qPrescIdx)
        
class TrgConfig(BrilDataSource):
    def __init__(self):
        super(Trg,self).__init__()
        self._columns = ['DATAID','RUNNUM','ALGOMASK_HIGH','ALGOMASK_LOW','TECHMASK']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PRO'):
        log.info('%s.from_lumidb'%self.name)
        trgdatatab = '.'.join([schema,'TRGDATA'])
        qTrgid = """select max(DATA_ID) from %s where RUNNUM=:runnum"""%(trgdatatab)
        log.info(qTrgid)
        trgid = pd.read_sql_query(qTrgid,engine,params={'runnum':runnum})
        qTrgRun = """select RUNNUM as RUNNUM, ALGOMASK_H as ALGOMASK_HIGH,ALGOMASK_L as ALGOMASK_LOW,TECHMASK as TECHMASK from %s where DATA_ID=:dataid"""%(trgdatatab)
        log.info(qTrgRun)
        trgmaskresult = pd.read_sql_query(qTrgRun,engine,{'dataid':id})
        trgmaskresult.columns = self._columns[:1]
        return trgmaskresult
    
    def from_sourcedb(self,engine,runnum):
        """
        algomask_high, algomask_low
        127,126,...,63,62,0
        ttmask
        63,62,...0
        """
        log.info('%s.from_sourcedb'%self.name)
        qKey = """select GT_RS_KEY,RUN_NUMBER from CMS_GT_MON.GLOBAL_RUNS where RUN_NUMBER=:runnum"""
        qTechMask = """select %s from CMS_GT.GT_PARTITION_FINOR_TT tt, CMS_GT.GT_RUN_SETTINGS r where tt.ID=r.FINOR_TT_FK and r.ID=:gt_rs_key"""%(ttvars)
        qAlgoMask = """select %s from CMS_GT.GT_PARTITION_FINOR_ALGO algo, CMS_GT.GT_RUN_SETTINGS r where algo.ID=r.FINOR_ALGO_FK and r.ID=:gt_rs_key"""%(ttvars)
    
class Hlt(BrilDataSource):
    def __init__(self):
        super(Hlt,self).__init__()
        self._columns = ['DATAID','HLTPATHID','RUNNUM','CMSLSNUM','PRESCIDX','PRESCVAL','L1PASS','HLTACCEPT']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        hltdatatab = 'HLTDATA'
        tab = '.'.join([schema,hltdatatab])
        qRun="""select max(DATA_ID) as DATAID from %s where RUNNUM=:runnum"""%(tab)
        lshlttab = 'LSHLT'
        tab = '.'.join([schema,lshlttab])
        qLS="""select DATA_ID as DATAID, RUNNUM as RUNNUM, CMSLSNUM as CMSLSNUM, PRESCALEBLOB as PRESCALEBLOB, HLTACCEPTBLOB as HLTACCEPTBLOB from %s where DATA_ID=:dataid"""%(tab)
        log.info(qRun)
        log.info(qLS)
        dataid = pd.read_sql_query(qRun,engine,params={'runnum':runnum})
        for id in dataid:
            result = pd.read_sql_query(qLS,engine,params={'dataid':id})
            #unpack blobs
        return result
    def from_sourcedb(self,engine,runnum,minls=1,mls=1):
        log.info('%s.from_sourcedb'%self.name)
        gtschema = 'CMS_GT'
        gtmonschema = 'CMS_GT_MON'
        
class TrgHltSeedMap(BrilDataSource):
    def __init__(self):
        super(TrgHltSeedMap,self).__init__()
        self._columns = ['hltpathid','hltconfigid','l1seedid']
    def to_brildb(self,engine,data,schema='',chunksize=None):
        super(TrgHltSeedMap,self)._to_brildb(engine,data,schema=schema,index=False)
    def to_csv(self,filepath_or_buffer,data):
        super(TrgHltSeedMap,self)._to_csv(filepath_or_buffer,data,index=False)    
    def from_sourcedb(self,engine):
        log.info('%s.from_sourcedb'%self.name)
        q = """select p.PATHID as hltpathid, c.CONFIGID as hltconfigid, s.PARAMID as l1seedid from CMS_HLT.STRINGPARAMVALUES s,CMS_HLT.paths p, CMS_HLT.parameters, CMS_HLT.superidparameterassoc, CMS_HLT.modules, CMS_HLT.moduletemplates, CMS_HLT.pathmoduleassoc, CMS_HLT.configurationpathassoc, CMS_HLT.configurations c where parameters.paramid=s.paramid and superidparameterassoc.paramid=parameters.paramid and modules.superid=superidparameterassoc.superid and moduletemplates.superid=modules.templateid and pathmoduleassoc.moduleid=modules.superid and p.pathid=pathmoduleassoc.pathid and configurationpathassoc.pathid=p.pathid and c.configid=configurationpathassoc.configid and moduletemplates.name='HLTLevel1GTSeed' and parameters.name='L1SeedsLogicalExpression' and p.ISENDPATH=0 and p.NAME like 'HLT/_%' escape '/' and s.VALUE not like '\"NOT %' and s.VALUE not like '% NOT %' and s.VALUE not like '%(%'"""
        log.info(q)
        result = pd.read_sql_query(q,engine,params={})
        result.columns = self._columns
        return result
        
class Deadtime(BrilDataSource):
    def __init__(self):
        super(Deadtime,self).__init__()
        self._columns = ['DATAID','RUNNUM','LSNUM','DEADTIMEFRAC']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):
        log.info('%s.from_lumidb'%self.name)
        trgdatatab = 'TRGDATA'
        tab = '.'.join([schema,trgdatatab])
        qRun="""select max(DATA_ID) as DATAID from %s where RUNNUM=:runnum"""%(tab)
        lstrgtab = 'LSTRG'
        tab = '.'.join([schema,lstrgtab])
        qLS="""select DATA_ID as DATAID,RUNNUM as RUNNUM, CMSLSNUM as LSNUM, DEADFRAC as DEADFRAC from %s where DATA_ID=:dataid"""%(tab)        
        log.info(qRun)
        log.info(qLS)
        dataid = pd.read_sql_query(qRun,engine,params={'runnum':runnum})
        for id in dataid:
            result = pd.read_sql_query(qLS,engine,params={'dataid':id})
        result.columns = self._columns
        return result
    
    def from_sourcedb(self,engine,runnum,minls=1,nls=1):
        log.info('%s.from_sourcedb'%self.name)
        qDeadFrac = """select FRACTION as DEADTIMEFRAC,LUMI_SECTION as LSNUM from CMS_GT_MON.V_SCALERS_TCS_DEADTIME where RUN_NUMBER=:runnum AND and LUMI_SECTION=:lsnum and SCALER_NAME='DeadtimeBeamActive'"""
        log.info(qDeadFrac)
        for lsnum in range(minls,minls+1):
            lsdeadfrac = pd.read_sql_query(q,engine,params={'runnum':runnum,'lsnum':lsnum})
            print lsdeadfrac
            
##### Results ######
class LumiResult(BrilDataSource):
    def __init__(self):
        super(LumiResult,self).__init__(datasourcename)
        self._datasourcename = datasourcename
        self._columns = ['RUNNUM','LSNUM','CMSLSNUM','FILLNUM','BEAMSTATUS','ISONLINE','DELIVERED','RECORDED','AVGPU','DATATAGID','NORMTAGID']
    def from_lumidb(self,engine,runnum,schema='CMS_LUMI_PROD'):       
        log.info('%s.from_lumidb'%self.name)
        if not os.path.isfile(get_filepath_or_buffer(engine)):
            if self._datasourcename.lower() != 'hfoclumi':
                print 'No data in lumidb for %s '%(self._datasourcename)
                return None
            q = """select RUNNUM as RUNNUM,LS as LSNUM,CMSLSNUM as CMSLSNUM,FILLNUM as FILLNUM,BEAMSTATUS as BEAMSTATUS ENERGY as EGEV, DELIVERED as DELIVERED, RECORDED as RECORDED , AVG_PU as AVGPU from CMS_LUMI_PROD.HFLUMIRESULT """
            result = pd.read_sql_query(q,engine)
            print result
            
class PixelLumiResult(BrilDataSource):
    def __init__(self):
        super(LumiResult,self).__init__()
        self._columns = ['RUNNUM','CMSLSNUM','FILLNUM','BEAMSTATUS','ISONLINE','VALUE','AVGPU','DATATAGID','NORMTAGID']
   
import struct,array
#def packArraytoBlob(iarray,typecode):
#    '''
#    Inputs:
#    inputarray: a python array
#    '''
#    t = typecode*len(iarray)
#    buffer = struct.pack(t,*iarray)
#    return result

def unpackBlobtoArray(iblob,itemtypecode):
    '''
    Inputs:
    iblob: blob
    itemtypecode: python array type code 
    '''
    if not isinstance(iblob,buffer) and not isinstance(iblob,str):
        return None
    if itemtypecode not in ['c','b','B','u','h','H','i','I','l','L','f','d']:
        raise RuntimeError('unsupported typecode '+itemtypecode)
    result=array.array(itemtypecode)
    
    #blobstr=iblob.readline()????
    if not iblob :
        return None
    result.fromstring(iblob)
    return result

def packListstrtoCLOB(iListstr,separator=','):
    '''
    pack list of string of comma separated large string CLOB
    '''
    return separator.join(iListstr)

def unpackCLOBtoListstr(iStr,separator=','):
    '''
    unpack a large string to list of string
    '''
    return [i.strip() for i in iStr.strip().split(separator)]

##### Data tag ######    
def data_createtag(engine,datatagname,comments='',schemaname=''):
    '''
    create a new data tag, return the datatag name id
    input:
        insert into schemaname.DATATAGS(datatagname,datatagnameid,creationutc,comments) values(datatagname,datatagnameid,creationutc,comments)
    output:
        datatagnameid  
    '''
    datatagnameid = None
    if datatagname == 'online':
        datatagnameid = 1
    else:
        datatagnameid = next(nonsequential_key(1))
    basetablename = tablename = 'datatags'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    utcstr = datetime.utcnow().strftime(params._datetimefm)
    t = Table(tablename, MetaData(), Column('datatagnameid',types.BigInteger), Column('datatagname',types.String),  Column('creationutc',types.String), Column('comments',types.String) )
    q = str( t.insert() )
    log.debug(q)
    log.debug(utcstr)
    #connection = engine.connect() 
    #connection.execute( t.insert(),datatagnameid=datatagnameid,datatagname=datatagname,creationutc=utcstr,comments=comments)
    return datatagnameid

def getDatatagNameid(engine,datatagname,schemaname=''):
    '''
    select datatagnameid from DATATAGS where datatagname=%datatagname    
    '''
    datatagnameid = 0
    if datatagname=='online': return datatagnameid
    return datatagnameid

def getDatatagName(engine,schemaname='',datatagname=''):
    '''
    output: datatags dataframe
    '''
    basetablename = tablename = 'DATATAGS'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    q = '''select DATATAGNAMEID as id, DATATAGNAME as name, CREATIONUTC as creationutc, COMMENTS as comments from %s'''%(tablename)
    if datatagname:
        q = q+' where datatagname=:datatagname'        
        return pd.read_sql_query(q,engine,index_col='id',params={'datatagname':datatagname})
    return pd.read_sql_query(q,engine,index_col='id',params={})

def insertDataTagEntry(engine,idtablename,datatagnameid,runnum,lsnum,fillnum=0,schemaname=None):
    '''
    insert into IDS_DATATAG_&suffix (datatagnameid,datatagid,fillnum,runnum,lsnum) values(datatagnameid,datatagid,fillnum,runnum,lsnum);
    output:
       datatagid
    '''
    datatagid = 0
    if datatagnameid!=0:
        # generate new id
        pass
    return datatagid

####################
##    Query API
####################

def buildselect_runls(inputSeries):
    '''
    output: [conditionstring, var_runs, var_lmins, var_lmaxs]
    '''
    result = []
    bind_runindex = 0
    bind_lsindex = 0
    var_runs={}
    var_lmins={}
    var_lmaxs={}
    qstrs = []
    for run,lsrange in inputSeries.iteritems():
        runvar='r_%d'%(bind_runindex)
        var_runs[runvar] = run
        s = 'RUNNUM=:%s and '%runvar
        orss = []
        for lsmin,lsmax in lsrange:
            lminvar = 'lmin_%d'%(bind_lsindex)
            lmaxvar = 'lmax_%d'%(bind_lsindex)
            var_lmins[lminvar] = lsmin
            var_lmaxs[lmaxvar] = lsmax
            orss.append( 'LSNUM>=:%s and LSNUM<=:%s'%(lminvar,lmaxvar) )
            bind_lsindex = bind_lsindex + 1
        ss = '('+' or '.join(orss)+')'
        bind_runindex = bind_runindex + 1
        qstrs.append(s+ss)
    result.append( ' or '.join(qstrs) )
    result.append(var_runs)
    result.append(var_lmins)
    result.append(var_lmaxs)
    return result

def max_datatagname(dbengine,schemaname=''):
    '''
    get the most recent datatagname 
    output: (datatagname,datatagid)
    '''
    name = 'datatags'
    result = None    
    if schemaname:
        name = schemaname+'.'+name
    q = '''select datatagname, max(datatagnameid) as datatagnameid from %s group by datatagname'''%name
    log.debug(q)
    qresult = pd.read_sql_query(q,dbengine)
    for idx,row in qresult.iterrows():
        result = ( row['datatagname'],row['datatagnameid'] )
    return result

def datatagnameid(dbengine,datatagname,schemaname=''):
    '''
    get datatagnameid by name
    input: datatagname. 
    output: datatagnameid
    '''
    basetablename = tablename = 'datatags'
    result = None
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    result = None
    q = '''select DATATAGNAMEID as datatagnameid from %s where DATATAGNAME=:datatagname'''%tablename
    log.debug(q)
    qresult = pd.read_sql_query(q,dbengine,params={'datatagname':datatagname})
    for idx,row in qresult.iterrows():
        result = row['datatagnameid']
    return result

def max_datatagOfRun(engine,runlist,schemaname=''):
    '''
    get the most recent datatagid of runs
    output: {run:datatagid}
    '''
    result = {}
    basetablename = tablename = 'RUNINFO'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    runliststr = ','.join(runlist)
    q = '''select RUNNUM as runnum, max(DATATAGID) as datatagid from %s where RUNNUM in (%s) limit 1'''%(tablename,runliststr)
    qresult = pd.read_sql_query(q,engine,params={})
    for idx,row in qresult.iterrows():
        run = row['runnum']
        datatagid = row['datatagid']
        result[run] = datatagid
    return result

def rundatatagIter(engine,datatagnameid,schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,amodetag=None,targetegev=None,runlsselect=None,chunksize=9999):
    '''
    output: dataframe iterator, index_col='datatagid'
    '''
    q = '''select FILLNUM as fillnum, RUNNUM as runnum , TIMESTAMPSEC as timestampsec, max(DATATAGID) as datatagid from IDS_DATATAG where DATATAGNAMEID<=:datatagnameid and LSNUM=:minlsnum'''
    binddict = {'datatagnameid':datatagnameid,'minlsnum':1}

    qCondition = ''
    qPieces = []
    if fillmin:
        qPieces.append('FILLNUM>=:fillmin')
        binddict['fillmin'] = fillmin
    if fillmax:
        qPieces.append('FILLNUM<=:fillmax')
        binddict['fillmax'] = fillmax
    if tssecmin:
        qPieces.append('TIMESTAMPSEC>=:tssecmin')
        binddict['tssecmin'] = tssecmin
    if tssecmax:
        qPieces.append('TIMESTAMPSEC<=:tssecmax')
        binddict['tssecmax'] = tssecmax        
    if amodetag:
        qPieces.append('AMODETAG=:amodetag')
        binddict['amodetag'] = amodetag
    if targetegev:
        qPieces.append('TARGETEGEV=:targetegev')
        binddict['targetegev'] = targetegev
    if runlsselect is not None:
        s_runls = buildselect_runls(runlsselect)
        if s_runls:
            s_runls_str = s_runls[0]
            var_runs = s_runls[1]
            var_lmins = s_runls[2]
            var_lmaxs = s_runls[3]
            qPieces.append(s_runls_str)
            for runvarname,runvalue in var_runs.items():                
                binddict[runvarname] = runvalue
            for lminname,lmin in var_lmins.items():                
                binddict[lminname] = lmin
            for lmaxname,lmax in var_lmaxs.items():                
                binddict[lmaxname] = lmax
    else:
        if runmin:
            qPieces.append('RUNNUM>=:runmin')
            binddict['runmin'] = runmin
        if runmax:
            qPieces.append('RUNNUM<=:runmax')
            binddict['runmax'] = runmax
    if not qPieces: return None # at least one piece of selection is required
    qCondition = ' and '.join([qCondition]+qPieces)
    q = q + qCondition+' group by RUNNUM'
    return pd.read_sql_query(q,engine,chunksize=chunksize,params=binddict,index_col='datatagid')

def table_exists(engine,tablename,schemaname=None):
    return engine.dialect.has_table(engine.connect(),tablename,schema=schemaname)

def build_query_condition(runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,beamstatusid=None,amodetag=None,amodetagid=None,targetegev=None,runlsselect=None):
    qCondition = ''
    qPieces = []
    binddict = {}
    if fillmin:
        qPieces.append('fillnum>=:fillmin')
        binddict['fillmin'] = fillmin
    if fillmax:
        qPieces.append('fillnum<=:fillmax')
        binddict['fillmax'] = fillmax
    if tssecmin:
        qPieces.append('timestampsec>=:tssecmin')
        binddict['tssecmin'] = tssecmin
    if tssecmax:
        qPieces.append('timestampsec<=:tssecmax')
        binddict['tssecmax'] = tssecmax
    if beamstatusid is not None:
        qPieces.append('beamstatusid=:beamstatusid')
        binddict['beamstatusid'] = beamstatusid
    else:
        if beamstatus:
            qPieces.append('beamstatus=:beamstatus')
            binddict['beamstatus'] = beamstatus
    if amodetagid is not None:
        qPieces.append('amodetagid=:amodetagid')
        binddict['amodetagid'] = amodetagid
    else:
        if amodetag:
            qPieces.append('amodetag=:amodetag')
            binddict['amodetag'] = amodetag
    if targetegev:
        qPieces.append('targetegev=:targetegev')
        binddict['targetegev'] = targetegev
    if runlsselect is not None:
        s_runls = buildselect_runls(runlsselect)
        if s_runls:
            s_runls_str = s_runls[0]
            var_runs = s_runls[1]
            var_lmins = s_runls[2]
            var_lmaxs = s_runls[3]
            qPieces.append(s_runls_str)
            for runvarname,runvalue in var_runs.items():                
                binddict[runvarname] = runvalue
            for lminname,lmin in var_lmins.items():                
                binddict[lminname] = lmin
            for lmaxname,lmax in var_lmaxs.items():                
                binddict[lmaxname] = lmax
    
    if runmin:
        qPieces.append('runnum>=:runmin')
        binddict['runmin'] = runmin
    if runmax:
        qPieces.append('runnum<=:runmax')
        binddict['runmax'] = runmax

    if not qPieces: return ('',{})
    qCondition = ' and '.join(qPieces)
    return (qCondition,binddict)
    
def online_resultIter(engine,tablename,schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,beamstatusid=None,amodetag=None,amodetagid=None,targetegev=None,runlsselect=None,fields=[],sorted=False):
    '''
    get list of run/ls of the online tag
    '''
    t = tablename
    if schemaname: t=schemaname+'.'+t
    (qCondition,binddict) = build_query_condition(runmin=runmin,runmax=runmax,fillmin=fillmin,fillmax=fillmax,tssecmin=tssecmin,tssecmax=tssecmax,beamstatus=beamstatus,beamstatusid=beamstatusid,amodetag=amodetag,amodetagid=amodetagid,targetegev=targetegev,runlsselect=runlsselect)
    if not qCondition: return None
    if not fields:
        fields = ['runnum','lsnum']
    else:
        fields = fields
    q = '''select %s from %s'''%(','.join(fields),t) + ' where '+qCondition
    if sorted:
        q = q+' order by runnum,lsnum'
    log.debug(q)
    connection = engine.connect()
    result = connection.execution_options(stream_result=True).execute(q,binddict)
    return iter(result)
        
def datatagidIter(engine,datatagnameid,schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,amodetag=None,targetegev=None,runlsselect=None,chunksize=9999,sorted=False):
    '''
    output: dataframe iterator, index_col='datatagid'
    '''
    tablename = basetablename = 'ids_datatag'
    if schemaname: tablename = '.'.join([schemaname,basetablename])
    (qCondition,binddict) = build_query_condition(runmin=runmin,runmax=runmax,fillmin=fillmin,fillmax=fillmax,tssecmin=tssecmin,tssecmax=tssecmax,beamstatus=beamstatus,amodetag=amodetag,targetegev=targetegev,runlsselect=runlsselect)
    if not qCondition: return None
    binddict['datatagnameid'] = datatagnameid
    qCondition = 'datatagnameid<=:datatagnameid and '+qCondition
    q = '''select max(datatagid) as datatagid, runnum as run, lsnum as ls from %s where %s group by runnum,lsnum'''%(tablename,qCondition)
    if sorted:
        q = q+' order by runnum,lsnum'
    log.debug(q)
    connection = engine.connect()
    result = connection.execution_options(stream_result=True).execute(q,binddict)
    return iter(result)

def runinfoIter(engine,runs,schemaname='',chunksize=9999,fields=[]):
    '''
    output: 
    '''

    tablename = basetablename = 'runinfo'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    idstrings = ','.join([str(x) for x in datatagids])
    
    q = '''select DATATAGID as datatagid'''
    subq = []
    if fields:
        for f in fields:            
            subq.append('''%s as %s'''%(f.upper(),f.lower()))
    if subq:
        q = q+','+','.join(subq)

    result = None
    if len(datatagids)==1:
        q = q+''' from %s where DATATAGID=:datatagid'''%(tablename)
        result = pd.read_sql_query(q,engine,chunksize=1,params={'datatagid':datatagids[0]},index_col='datatagid')
    else:
        q = q+''' from %s where DATATAGID in (%s)'''%(tablename,idstrings)
        result = pd.read_sql_query(q,engine,chunksize=chunksize,params={},index_col='datatagid')
    return result

def translate_fntosql(pattern):
    '''
    translate fnmatch pattern to sql pattern 
    '''    
    sqlresult = pattern
    sqlresult = sqlresult.replace('*','%')
    sqlresult = sqlresult.replace('?','_')
    return sqlresult

def get_hlttrgl1seedmap(engine,hltpath,schemaname=''):
    '''
    input :
        hltpath : hltpath name or pattern

    output:
       hlttrgl1seedmap : pd.DataFrame(columns=['hltconfigid','hltpathid','hltpathname','l1seed'])
    '''
    tablename = name = 'hltpathl1seedmap'
    if schemaname:
        tablename = '.'.join([schemaname,name])
    hltpath_sqlexpr = hltpath
    p = re.compile(r'\?|\*|\[|\]|!')
    q = '''select hltconfigid, hltpathid, hltpathname, l1seed from %s '''%(tablename)
    if p.findall(hltpath): # is pattern
        hltpath_sqlexpr = hltpath_sqlexpr.replace('*','.*')
        hltpath_sqlexpr = hltpath_sqlexpr.replace('?','.?')
        hltpath_sqlexpr = hltpath_sqlexpr.replace('!','^')
        q = q+'''where regexp_like(hltpathname,'%s')'''%(hltpath_sqlexpr)
    else:
        q = q+'''where hltpathname=%s'''%(hltpath_sqlexpr)

    connection = engine.connect()
    resultProxy = connection.execute(q)
    hltpathl1seed = pd.DataFrame(list(resultProxy),columns=['hltconfigid','hltpathid','hltpathname','l1seed'])
    hltpathl1seed['l1seed'] = hltpathl1seed['l1seed'].apply(parseL1Seed)
    hltpathl1seed = hltpathl1seed[ hltpathl1seed['l1seed']!=('',None) ]
    return hltpathl1seed

def get_effectivescalers(engine,suffix,runnum,lsnum,hltpathid,l1seedexpr,ignorel1mask=False,schemaname=''):
    '''
    get 
    output: 
    pd.DataFrame(columns=['hltpathid','l1bitname','prescidx','trgprescval','hltprescval'])
    '''
    trgscalertable = 'trgscaler_'+str(suffix)
    hltscalertable = 'hltscaler_'+str(suffix)
    trgrunconftable = 'trgrunconfig'
    if schemaname:
        trgscalertable = '.'.join([schemaname,trgscalertable])
        hltscalertable = '.'.join([schemaname,hltscalertable])
        trgrunconftable = '.'.join([schemaname,trgrunconftable])
    q = '''select l.prescidx, l.prescval, h.prescval from %(trgscalerT)s l, %(trgrunconfT) r, %(hltscalerT)s h where l.runnum=r.runnum and l.bitid=r.bitid and r.bitname=:l1bitname and h.datatagid=l.datatagid and h.prescidx=l.prescidx and h.hltpathid=:hltpathid and l.runnum=:runnum and l.lsnum=:lsnum'''%{'trgscalerT':trgscalertable,'hltscalerT':hltscalertable,'trgrunconfT':trgrunconftable}
    if not ignorel1mask:
        q = q+' and r.bitmask!=1'
    connection = engine.connect()
    resultProxy = connection.execute(q,{'runnum':runnum,'lsnum':lsnum})    
    print list(resultProxy)
    
def parseL1Seed(l1seed):
    '''
    output: 
    '''
    sep=re.compile('(\sAND\s|\sOR\s)',re.IGNORECASE)
    result=re.split(sep,l1seed)
    cleanresult=[]
    exptype='ONE'
    notsep=re.compile('NOT\s',re.IGNORECASE)
    andsep=re.compile('\sAND\s',re.IGNORECASE)
    orsep=re.compile('\sOR\s',re.IGNORECASE)
    for r in result:
        if notsep.match(r) : #we don't know what to do with NOT
            return ('',None)
        if orsep.match(r):
            exptype='OR'
            continue
        if andsep.match(r):
            exptype='AND'
            continue
        #cleanresult.append(string.strip(r).replace('\"',''))
        cleanresult.append(string.strip(r))
    return (exptype,cleanresult)
    
def findUniqueSeed(hltPathname,l1seed):
    '''
    given a hltpath and its L1SeedExpression, find the L1 bit name
    can return None
    
    if hltPath contains the following, skip do not parse seed.
    
    FakeHLTPATH*, HLT_Physics*, HLT_*Calibration*, HLT_HFThreashold,
    HLT_MiniBias*,HLT_Random*,HLTTriggerFinalPath,HLT_PixelFED*
    parse hltpath contains at most 2 logics x OR y, x AND y, and return left val
    do not consider path containing operator NOT

    output: (expressiontype,[l1bitname])
    '''
    if re.match('HLT_Physics',hltPathname)!=None :
        return None
    if re.match('HLT_[aA-zZ]*Calibration',hltPathname)!=None :
        return None
    if re.match('HLT_[aA-zZ]*Threshold',hltPathname)!=None :
        return None
    if re.match('HLT_MiniBias',hltPathname)!=None :
        return None
    if re.match('HLT_Random',hltPathname)!=None :
        return None    
    if re.match('HLT_[aA-zZ]*FEDSize',hltPathname)!=None :
        return None
    if l1seed.find('(')!=-1 : #we don't parse expression with ()
        return None
    if re.match('FakeHLTPATH',hltPathname)!=None :
        return None
    if re.match('HLTriggerFinalPath',hltPathname)!=None :
        return None
    sep=re.compile('(\sAND\s|\sOR\s)',re.IGNORECASE)
    result=re.split(sep,l1seed)
    cleanresult=[]
    exptype=''
    notsep=re.compile('NOT\s',re.IGNORECASE)
    andsep=re.compile('\sAND\s',re.IGNORECASE)
    orsep=re.compile('\sOR\s',re.IGNORECASE)
    for r in result:
        if notsep.match(r) : #we don't know what to do with NOT
            return ('',None)
        if orsep.match(r):
            exptype='OR'
            continue
        if andsep.match(r):
            exptype='AND'
            continue
        cleanresult.append(string.strip(r).replace('\"',''))
    return (exptype,cleanresult)
    
def beamInfoIter(engine,suffix,datafields=[],idfields=[],schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,beamstatusid=None,amodetag=None,amodetagid=None,targetegev=None,runlsselect=None,sorted=False):
    '''
    output: iterator
    select b.egev as egev, a.datatagid as datatagid, a.runnum as run from cms_lumi_prod.beam_3 b,(select max(datatagid) as datatagid, runnum from cms_lumi_prod.ids_datatag group by runnum) a where a.datatagid=b.datatagid
    '''
    if not datafields: return None
    (qCondition,binddict) = build_query_condition(runmin=runmin,runmax=runmax,fillmin=fillmin,fillmax=fillmax,tssecmin=tssecmin,tssecmax=tssecmax,beamstatus=beamstatus,beamstatusid=beamstatusid,amodetag=amodetag,amodetagid=amodetagid,targetegev=targetegev,runlsselect=runlsselect)
    if not qCondition: return None
    basetablename = 'beam'
    #idfields = ['fillnum','runnum','lsnum','timestampsec','beamstatusid']
    q = build_joinwithdatatagid_query(basetablename,suffix,datafields,idfields,qCondition,schemaname=schemaname,sorted=sorted)
    log.debug(q)
    connection = engine.connect()
    result = connection.execution_options(stream_result=True).execute(q,binddict)
    return iter(result)

def build_joinwithdatatagid_query(datatablename,suffix,datafields,idfields,idcondition,schemaname='',sorted=False):
    idtablename = 'ids_datatag'
    tablename = '_'.join([datatablename,str(suffix)])
    if schemaname:
        tablename = '.'.join([schemaname,tablename])
        idtablename = '.'.join([schemaname,idtablename])
    data_fieldstr = ','.join([ '%s%s as %s'%('b.',f,f) for f in datafields ])
    id_fieldstr = ','.join([ '%s%s as %s'%('a.',f,f) for f in idfields ])
    q = '''select a.datatagid as datatagid, %s, %s from %s b,'''%(data_fieldstr,id_fieldstr,tablename)    
    id_fieldstr = groupbystr = ','.join( [str(f) for f in idfields] )
    subq = '''(select max(datatagid) as datatagid, %s from %s where %s group by %s) a where a.datatagid=b.datatagid'''%(id_fieldstr,idtablename,idcondition,groupbystr)
    q = q+subq
    if sorted:
        q = q+' order by runnum,lsnum'
    return q

def det_resultDataIter(engine,datasource,suffix,datafields=[],idfields=[],schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,beamstatusid=None,amodetag=None,amodetagid=None,targetegev=None,runlsselect=None,sorted=False):
    '''
    output: iterator
    select b.avglumi as avglumi, b.bxlumiblob as bxlumiblob, b.normtag as normtag, b.datatagid as datatagid, a.runnum as run from cms_lumi_prod._3 b,(select max(datatagid) as datatagid, runnum from cms_lumi_prod.ids_datatag group by runnum) a where a.datatagid=b.datatagid
    '''
    if not datafields: return None
    (qCondition,binddict) = build_query_condition(runmin=runmin,runmax=runmax,fillmin=fillmin,fillmax=fillmax,tssecmin=tssecmin,tssecmax=tssecmax,beamstatus=beamstatus,beamstatusid=beamstatusid,amodetag=amodetag,amodetagid=amodetagid,targetegev=targetegev,runlsselect=runlsselect)
    if not qCondition: return None
    basetablename = datasource+'_result'
    q = build_joinwithdatatagid_query(basetablename,suffix,datafields,idfields,qCondition,schemaname=schemaname,sorted=sorted)
    log.debug(q)
    connection = engine.connect()
    result = connection.execution_options(stream_result=True).execute(q,binddict)
    return iter(result)

def det_rawDataIter(engine,datasource,suffix,datafields=[],idfields=[],schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,beamstatusid=None,amodetag=None,amodetagid=None,targetegev=None,runlsselect=None,sorted=False):
    '''
    output: iterator
    select b.rawlumi as rawlumi, b.bxrawlumiblob as bxrawlumiblob, b.datatagid as datatagid, a.runnum as run from cms_lumi_prod._3 b,(select max(datatagid) as datatagid, runnum from cms_lumi_prod.ids_datatag group by runnum) a where a.datatagid=b.datatagid
    '''
    if not datafields: return None
    (qCondition,binddict) = build_query_condition(runmin=runmin,runmax=runmax,fillmin=fillmin,fillmax=fillmax,tssecmin=tssecmin,tssecmax=tssecmax,beamstatus=beamstatus,beamstatusid=beamstatusid,amodetag=amodetag,amodetagid=amodetagid,targetegev=targetegev,runlsselect=runlsselect)
    if not qCondition: return None
    basetablename = datasource+'_raw'
    q = build_joinwithdatatagid_query(basetablename,suffix,datafields,idfields,qCondition,schemaname=schemaname,sorted=sorted)
    log.debug(q)
    connection = engine.connect()
    result = connection.execution_options(stream_result=True).execute(q,binddict)
    return iter(result)
        
def trgMask(engine,datatagid):
    '''
    output: [trgmask1,...,trgmask6]
    '''
    result = 192*[0]
    q = '''select trgmask1,trgmask2,trgmask3,trgmask4,trgmask5,trgmask5 from RUNINFO where datatagid=:datatagid'''
    result = pd.read_sql_query(q,engine,params={'datatagid':datatagid})
    return result

def trgInfoIter(engine,datatagids,suffix,schemaname='',bitnamepattern='',chunksize=9999):
    '''
    input: datatagids []
    output: dataframe iterator
    [datatagid,bitid,bitname,prescidx,presc,counts]
    '''
    basetablename = 'TRG'
    tablename = '_'.join([basetablename,suffix])
    maptablename = 'TRGBITMAP'
    if schemaname:
        tablename = '.'.join([schemaname,tablename])
        maptablename = '.'.join([schemaname,maptablename])
    idstrings = ','.join([str(x) for x in datatagids])

    q = '''select t.DATATAGID as datatagid,t.BITID as bitid,m.BITNAME as bitname, t.PRESCIDX as prescidx,t.PRESCVAL as presc,t.COUNTS as counts from %s t, %s m where m.BITNAMEID=t.BITNAMEID and t.BITID=m.BITID and t.DATATAGID in (%s)'''%(tablename,maptablename,idstrings)

    if bitnamepattern:
        namefilter = ''
        if bitnamepattern.find('*')==-1:#is not pattern
            namefilter = 'm.BITNAME=:bitname'
            q = '''select t.DATATAGID as datatagid,t.BITID as bitid,m.BITNAME as bitname, t.PRESCIDX as prescidx,t.PRESCVAL as presc,t.COUNTS as counts from %s t, %s m where m.BITNAMEID=t.BITNAMEID and t.BITID=m.BITID and %s and t.DATATAGID in (%s)'''%(tablename,maptablename,namefilter,idstrings)
            result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'bitname':bitnamepattern},index_col='datatagid')
            return result
    result = pd.read_sql_query(q,engine,chunksize=chunksize,params={},index_col='datatagid')
    return result

def hltInfoIter(engine,datatagids,suffix,schemaname='',hltpathnamepattern='',chunksize=9999):
    '''
    
    '''
    basetablename = 'HLT'
    tablename = '_'.join([basetablename,suffix])
    maptablename = 'HLTPATHMAP'
    if schemaname:
        tablename = '.'.join([schemaname,tablename])
        maptablename = '.'.join([schemaname,maptablename])
    idstrings = ','.join([str(x) for x in datatagids])
    
    q = '''select h.DATATAGID as datatagid,m.HLTPATHNAME as hltpathname, h.PRESCIDX as prescidx,h.PRESCVAL as prescval,h.L1PASS as l1pass,h.HLTACCEPT as hltaccept from %s m, %s h where m.HLTPATHID=h.HLTPATHID and h.DATATAGID IN (%s)'''%(maptablename,tablename,idstrings)
    pathnamecondition = ''
    if hltpathnamepattern:
        if hltpathnamepattern.find('*')==-1 and hltpathnamepattern.find('?')==-1 and hltpathnamepattern.find('[')==-1:#is not pattern
            pathnamecondition = 'm.HLTPATHNAME=:hltpathname'
            q = q+' and '+pathnamecondition
            result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'hltpathname':hltpathnamepattern},index_col='datatagid')
            return result
        else:
            sqlpattern = translate_fntosql(hltpathnamepattern)
            q = q+" and m.HLTPATHNAME like '"+sqlpattern+"'"
            result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'hltpathname':hltpathnamepattern},index_col='datatagid')
            return result
    result = pd.read_sql_query(q,engine,chunksize=chunksize,params={},index_col='datatagid')
    return result
