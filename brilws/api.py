import numpy as np
import pandas as pd
from sqlalchemy import *
from sqlalchemy import exc, text
from datetime import datetime
import decimal
import os
import random
import time
import yaml
import re
import contextlib
import sys
import ast
import logging
import string
decimalcontext = decimal.getcontext().copy()
decimalcontext.prec = 3

log = logging.getLogger('brilws.api')

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

def iov_parsepayloaddatadict(datadictStr):
    """
    input:
        datadictstr: key-value:maxlen:alias value:maxlen:alias ...
    output:
        [{'key':keytablename, 'val':valtablename, 'alias':alias, 'maxnpos':maxnpos }]
    """
    result = []
    fields = datadictStr.split(' ')
    r = [(f.split(':')+[None]*99)[:3] for f in fields]
    for [keyval,maxnpos,alias] in r:
        [key,val] = (keyval.split('-')+[None]*99)[:2]
        if not val:
            val = key
            key = ''
        if not key:
            keytablename = ''
        elif re.search('str',key,re.IGNORECASE):
            keytablename = payloadtableprefix_+'_STRING'
        else:
            keytablename = payloadtableprefix_+'_'+key.upper()
        if not val:
            valtablename = ''
        elif re.search('str',val,re.IGNORECASE):
            valtablename = payloadtableprefix_+'_STRING'
        else:
            valtablename = payloadtableprefix_+'_'+val.upper()
        result.append({'key':keytablename,'val':valtablename,'alias':alias,'maxnpos':int(maxnpos)})
    return result

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
    
def iov_listtags(connection,tagname=None,datasource=None,applyto=None,isdefault=None):
    """
    inputs:
        connection:  db handle
        optional query parameters: tagid, tagname,datasource,applyto,isdefault
    outputs:
        {tagid: {'tagname': , 'creationutc': , 'datadict': , 'maxnitems':, 'datasource': , 'applyto': , 'isdefault': 'tagcomment': , since:{'payloadid':,'payloadcomment':} } }
    sql:
        select t.TAGID as tagid, t.TAGNAME as tagname, t.CREATIONUTC as creationutc, t.DATADICT as datadict, t.MAXNITEMS as maxnitems, t.DATASOURCE as datasource, t.APPLYTO as applyto, t.ISDEFAULT as isdefault, t.COMMENT as tagcomment, d.SINCE as since, d.PAYLOADID as payloadid, d.COMMENT as payloadcomment from IOVTAGS t, IOVTAGDATA d where t.TAGID=d.TAGID [and t.TAGNAME=:tagname and t.DATASOURCE=:datasource and t.APPLYTO=:applyto and t.isdefault=:isdefault ]; 
    """
    
    result = {}
    q =  """select t.TAGID as tagid, t.TAGNAME as tagname, t.CREATIONUTC as creationutc, t.DATADICT as datadict, t.MAXNITEMS as maxnitems, t.DATASOURCE as datasource, t.APPLYTO as applyto, t.ISDEFAULT as isdefault, t.COMMENT as tagcomment, d.SINCE as since, d.PAYLOADID as payloadid, d.COMMENT as payloadcomment from IOVTAGS t, IOVTAGDATA d where t.TAGID=d.TAGID"""
    param = {}
    if tagname:
        q += " and t.TAGNAME=:tagname"
        param['tagname'] = tagname
    if datasource:
        q += " and t.DATASOURCE=:datasource"
        param['datasource'] = datasource
    if applyto:
        q += " and t.APPLYTO=:applyto"
        param['applyto'] = applyto
    if isdefault:
        q += " and t.ISDEFAULT=:isdefault"
        param['isdefault'] = isdefault
        
    with connection.begin() as trans:
        r = connection.execute(q,param)
        for row in r:
            tagid = row['tagid']
            since = row['since']
            if not result.has_key(tagid):
                result[tagid] = {}
                result[tagid]['tagname'] = row['tagname']
                result[tagid]['creationutc'] = row['creationutc']
                result[tagid]['datadict'] = row['datadict']
                result[tagid]['maxnitems'] = row['maxnitems']
                result[tagid]['datasource'] = row['datasource']
                result[tagid]['applyto'] = row['applyto']
                result[tagid]['isdefault'] = row['isdefault']
                result[tagid]['tagcomment'] = row['tagcomment']
            result[tagid][since] = {'payloadid':row['payloadid'],'payloadcomment':row['payloadcomment']}
    return result
    
def iov_createtag(connection,iovdata):
    """
    inputs:
        connection:  db handle
        iovdata:     {'tagname':tagname, 'datadict':datadict, 'maxnitems':maxnitems, 'iovdataversion':iovdataversion, 'datasource':datasource, 'applyto':applyto, 'isdefault':isdefault, 'comment':comment, sincex:{'payload':[[dict or list,]],'comment':comment} }
    output:
        tagid
    sql:
        insert into IOVTAGS(TAGID,TAGNAME,CREATIONUTC,DATADICT,MAXNITEMS,DATASOURCE,APPLYTO,ISDEFAULT,COMMENT) VALUES(:tagid, :tagname, :creationutc, :datadict, :maxnitems, :datasource, :applyto, :isdefault, :comment)
        insert into IOVTAGDATA(TAGID,SINCE,PAYLOADID,COMMENT) VALUES(:tagid, :since, :payloadid, :comment)
        insert into %s(PAYLOADID,IITEM,IFIELD,IPOS,ISKEY,VAL) VALUES(:payloadid, :iitem, :ifield, :ipos, :iskey, :val)
    """

    tagid = next(nonsequential_key(78))
    #print "creating iovtag %s"%iovdata['tagname']
    sinces = sorted([x for x in iovdata.keys() if isinstance(x, int)])
    nowstr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    datadict = iovdata['datadict']
    payloaddatadict = iov_parsepayloaddatadict(datadict)
    maxnitems = iovdata['maxnitems']
    
    i = """insert into IOVTAGS(TAGID,TAGNAME,CREATIONUTC,DATADICT,MAXNITEMS,DATASOURCE,APPLYTO,ISDEFAULT,COMMENT) VALUES(:tagid, :tagname, :creationutc, :datadict, :maxnitems, :datasource, :applyto, :isdefault, :comment)"""
    ti = """insert into IOVTAGDATA(TAGID,SINCE,PAYLOADID,COMMENT) VALUES(:tagid, :since, :payloadid, :comment)"""
    pi = """insert into %s(PAYLOADID,IITEM,IFIELD,IPOS,ISKEY,VAL) VALUES(:payloadid, :iitem, :ifield, :ipos, :iskey, :val)"""
    with connection.begin() as trans:
        r = connection.execute(i,{'tagid':tagid, 'tagname':iovdata['tagname'], 'creationutc':nowstr, 'datadict':datadict, 'maxnitems':maxnitems, 'datasource':iovdata['datasource'], 'applyto':iovdata['applyto'], 'isdefault':iovdata['isdefault'], 'comment':iovdata['comment'] })
        
        for since in sinces:
            payloadid = next(nonsequential_key(79))
            payloaddata = iovdata[since]['payload']            
            payloadcomment = ''
            if iovdata[since].has_key('comment'):
                payloadcomment = iovdata[since]['comment']
            tr = connection.execute(ti, {'tagid':tagid, 'since':since, 'payloadid':payloadid, 'comment':payloadcomment })
            rowcache = _iov_buildpayloadcache( payloadid, payloaddata, payloaddatadict, payloadcomment)
            for ptablename, prows in rowcache.items():
                if len(prows)==0: continue
                try:
                    pr = connection.execute(pi%ptablename, prows)
                except exc.SQLAlchemyError, e:
                    raise
    return tagid
    
def iov_appendtotag(connection,tagid,since,payloaddata,datadict,payloadcomment):
    """
    inputs:
        connection: dbhandle
        tagid:      tagid        
        payloaddata: [[dict or list,]]
        datadict: str
        payloadcomment: 
    output:
        payloadid
    sql:
        insert into IOVTAGDATA(TAGID,SINCE,PAYLOADID,COMMENT) VALUES(:tagid, :since, :payloadid, :comment)
    """
    payloaddatadict = iov_parsepayloaddatadict(datadict)
    payloadid = next(nonsequential_key(79))
    ti = """insert into IOVTAGDATA(TAGID,SINCE,PAYLOADID,COMMENT) VALUES(:tagid, :since, :payloadid, :comment)"""
    pi = """insert into %s(PAYLOADID,IITEM,IFIELD,IPOS,ISKEY,VAL) VALUES(:payloadid, :iitem, :ifield, :ipos, :iskey, :val) """
    
    with connection.begin() as trans:
        tr = connection.execute(ti, {'tagid':tagid, 'since':since, 'payloadid':payloadid, 'comment':payloadcomment })
        rowcache = _iov_buildpayloadcache( payloadid, payloaddata, payloaddatadict, payloadcomment)
        for ptablename, prows in rowcache.items():
            if len(prows)==0: continue
            pr = connection.execute(pi%ptablename, prows)
    return payloadid

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

def _iov_buildpayloadcache(payloadid, payloaddata, payloaddatadict, payloadcomment):
    """
    input:
        payloadid: 
        payloaddata: [[list/dict,]]
        payloaddatadict: [{'val':tablename, 'key':tablename,'alias':alias, 'maxnpos':maxnpos } ]
    output: 
        rowcache: {tablename: [{'payloadid':, 'iitem': 'ifield':, 'ipos':, 'key':, 'val': },]}
    """

    rowcache = {}
    for item_idx, item in enumerate(payloaddata):
        for field_idx, fielddata in enumerate(item):
            if isinstance(fielddata,list):
                valtable_name = payloaddatadict[field_idx]['val']
                if not valtable_name:
                    raise ValueError('invalid value table name %s'%valtable_name)
                for ipos, val in enumerate(fielddata):
                    rowcache.setdefault(valtable_name,[]).append({'payloadid':payloadid,'iitem':item_idx,'ifield':field_idx,'ipos':ipos,'iskey':0,'val':val})
            elif isinstance(fielddata,dict):
                ipos = 0
                for key in sorted(fielddata):
                    val = fielddata[key]
                    valtable_name = payloaddatadict[field_idx]['val']
                    if not valtable_name:
                        raise ValueError('invalid value table name %s'%valtable_name)                    
                    keytable_name = payloaddatadict[field_idx]['key']
                    if not keytable_name:
                        raise ValueError('invalid key table name %s'%keytable_name)
                    rowcache.setdefault(valtable_name,[]).append({'payloadid':payloadid,'iitem':item_idx,'ifield':field_idx,'ipos':ipos,'iskey':0,'val':val})
                    rowcache.setdefault(keytable_name,[]).append({'payloadid':payloadid,'iitem':item_idx,'ifield':field_idx,'ipos':ipos,'iskey':1, 'val':key})
                    ipos += 1
            else:
                ipos = 0
                valtable_name = payloaddatadict[field_idx]['val']
                if not valtable_name:
                    raise ValueError('invalid value table name %s'%valtable_name)
                val = fielddata
                rowcache.setdefault(valtable_name,[]).append({'payloadid':payloadid,'iitem':item_idx,'ifield':field_idx,'ipos':ipos,'iskey':0,'val':val})
    return rowcache

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


def read_yaml(path_or_buf):
    """
    safe_load yaml string or file 
    """
    filepath_or_buffer = get_filepath_or_buffer(path_or_buf)
    if isinstance(filepath_or_buffer, str):
        try:
            exists = os.path.exists(filepath_or_buffer)
        except (TypeError,ValueError):
            exists = False
        if exists:
            with open(filepath_or_buffer,'r') as f:
                obj = yaml.safe_load(f)
        else:
            obj = yaml.safe_load(path_or_buf)
            if type(obj) is not dict and type(obj) is not list:
                raise IOError('file %s does not exist'%filepath_or_buffer)
    elif hasattr(filepath_or_buffer, 'read'):
        obj = filepath_or_buffer.read()
    else:
        obj = filepath_or_buffer
    return obj
    
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
def createDataTag(engine,datatagname='online',comments='',schemaname=None):
    '''
    create a new data tag, return the datatag name id
    input:
        insert into schemaname.DATATAGS(datatagname,datatagnameid,creationutc,comments) values(datatagname,datatagnameid,creationutc,comments)
    output:
        datatagnameid  
    '''
    datatagnameid = 0
    return datatagnameid

def getDatatagNameid(engine,datatagname,schemaname=None):
    '''
    select datatagnameid from DATATAGS where datatagname=%datatagname    
    '''
    datatagnameid = 0
    if datatagname=='online': return datatagnameid
    return datatagnameid

def getDatatagNames(engine,schemaname=None):
    '''
    select * from DATATAGS
    '''
    return None

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

def max_datatagname(dbengine):
    '''
    get the most recent datatagname 
    output: (datatagname,datatagid)
    '''
    result = None
    q = '''select DATATAGNAME as datatagname, max(DATATAGNAMEID) as datatagnameid from DATATAGS'''
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
    basetablename = tablename = 'DATATAGS'
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
    result = None
    q = '''select DATATAGNAMEID as datatagnameid from DATATAGS where DATATAGNAME=:datatagname limit 1'''
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
    #print q
    return pd.read_sql_query(q,engine,chunksize=chunksize,params=binddict,index_col='datatagid')
            
def datatagIter(engine,datatagnameid,schemaname='',runmin=None,runmax=None,fillmin=None,tssecmin=None,tssecmax=None,fillmax=None,beamstatus=None,amodetag=None,targetegev=None,runlsselect=None,chunksize=9999,fields=[]):
    '''
    output: dataframe iterator, index_col='datatagid'
    '''
    
    basetablename = 'IDS_DATATAG'
    tablename = basetablename
    if schemaname:
        tablename = '.'.join([schemaname,basetablename])
        
    q = '''select max(DATATAGID) as datatagid'''
    subq = []
    if fields:
        for f in fields:            
            subq.append('''%s as %s'''%(f.upper(),f.lower()))
    if subq:
        q = q+','+','.join(subq)
    q = q+''' from %s where DATATAGNAMEID<=:datatagnameid'''%(tablename)    
        
    qCondition = ''
    qPieces = []
    binddict = {'datatagnameid':datatagnameid}

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
    if beamstatus:
        qPieces.append('BEAMSTATUS=:beamstatus')
        binddict['beamstatus'] = beamstatus
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
    q = q + qCondition +' group by RUNNUM, LSNUM'
    #print q
    return pd.read_sql_query(q,engine,chunksize=chunksize,params=binddict,index_col='datatagid')

def runinfoIter(engine,datatagids,schemaname='',chunksize=9999,fields=[]):
    '''
    output: {run:datatagid}
    '''

    basetablename = 'RUNINFO'
    tablename = basetablename
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

def hltl1seedinfoIter(engine,hltconfigid,hltpathnameorpattern='',schemaname='',chunksize=9999,):
    '''
    
    '''
    seedtablename = 'L1SEEDMAP'
    hlttrgtablename = 'TRGHLTSEEDMAP'
    hltpathtablename = 'HLTPATHMAP'
    if schemaname:
        seedtablename = '.'.join([schemaname,seedtablename])
        hlttrgtablename = '.'.join([schemaname,hlttrgtablename])
        hltpathtablename = '.'.join([schemaname,hltpathtablename])
        
    q = '''select h.HLTPATHNAME as hltpath, s.L1SEED as l1seed from %s h, %s m, %s s where s.L1SEEDID=m.L1SEEDID and h.hltpathid=m.hltpathid and m.hltconfigid=:hltconfigid'''%(hltpathtablename,hlttrgtablename,seedtablename)
    pathnamecondition = ''
    result = None
    if hltpathnameorpattern:
        if hltpathnameorpattern.find('*')==-1 and hltpathnameorpattern.find('?')==-1 and hltpathnameorpattern.find('[')==-1:
            pathnamecondition = 'h.HLTPATHNAME=:hltpathname'
            q = q+' and '+pathnamecondition
            result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'hltconfigid':hltconfigid,'hltpathname':hltpathnameorpattern})
            return result
        else:
            sqlpattern = translate_fntosql(hltpathnameorpattern)
            q = q+" and h.HLTPATHNAME like '"+sqlpattern+"'"
            result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'hltconfigid':hltconfigid})
            return result
    result = pd.read_sql_query(q,engine,chunksize=chunksize,params={'hltconfigid':hltconfigid})
    return result

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
    
def beamInfoIter(engine,datatagids,suffix,schemaname='',chunksize=9999,fields=[]):
    '''
    input: datatagids []
    output: dataframe iterator
         [datatagid,] 
         #withBX=False [datatagid,fill,run,ls,timestampsec,beamstatus,amodetag,egev,intensity1,intensity2]
         #withBX=True [datatagid,fill,run,ls,bxidx,intensity1,intensity2,iscolliding]
    '''
    allfields = ['INTENSITY1','INTENSITY1','EGEV','BXIDXBLOB','BXINTENSITY1BLOB','BXINTENSITY2BLOB','BXPATTERNBLOB']
    idtablename = 'IDS_DATATAG'
    basetablename = 'BEAM'
    tablename = '_'.join([basetablename,suffix])
    if schemaname :
        idtablename = '.'.join([schemaname,idtablename])
        tablename = '.'.join([schemaname,tablename])
    idstrings = ','.join([str(x) for x in datatagids])
    
    q = '''select DATATAGID as datatagid'''
    if not fields: fields = allfields
    subq = []
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

def lumiInfoIter(engine,datatagids,datasource,suffix,schemaname='',chunksize=9999,fields=[]):
    '''
    input: datatagids []
    output: dataframe iterator            
    '''     
    basetablename = datasource.upper()
    tablename = '_'.join([basetablename,suffix]) 
    if schemaname:
        tablename = '.'.join([schemaname,tablename])
        
    idstrings = ','.join([str(x) for x in datatagids])
    q = '''select DATATAGID as datatagid'''
    subq = []
    if fields:
        for f in fields:            
            subq.append('''%s as %s'''%(f.upper(),f.lower()))
    if subq:
        q = q+','+','.join(subq)
    if len(datatagids)==1:
        q = q+''' from %s where DATATAGID=:datatagid'''%(tablename)
        result = pd.read_sql_query(q,engine,chunksize=1,params={'datatagid':datatagids[0]},index_col='datatagid')
        return result
    else:
        q = q+''' from %s where DATATAGID in (%s)'''%(tablename,idstrings)
        result = pd.read_sql_query(q,engine,chunksize=chunksize,params={},index_col='datatagid')
        return result
    
def deadtimeIter(engine,datatagids,suffix,schemaname='',chunksize=9999):
    '''
    input: datatagids []
    output: dataframe iterator
    [datatagid,deadfrac]
    '''
    basetablename = 'DEADTIME'
    tablename = '_'.join([basetablename,suffix])
    if schemaname: tablename = '.'.join([schemaname,tablename])
    idstrings = ','.join([str(x) for x in datatagids])
    q = '''select DATATAGID as datatagid,DEADTIMEFRAC as deadtimefrac from %s where  DATATAGID in (%s)'''%(tablename,idstrings)
    result = pd.read_sql_query(q,engine,chunksize=chunksize,params={},index_col='datatagid')
    return result

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
#
# operation on  data sources
# 
if __name__=='__main__':

    ## test db api , i.e. sqlalchemy sans orm
    engine = create_engine('sqlite:///test.db')
    connection = engine.connect().execution_options(stream_results=True)
    trans = connection.begin()
    try:
        connection.execute('''create table if not exists test ( a integer) ''')
        trans.commit()
    except:
        trans.rollback()
        raise
    
    with connection.begin() as trans:
        r = connection.execute("""insert into test(%s) values(%s)"""%('a',':a'),{'a':1})
        connection.execute("""insert into test(%s) values(%s)"""%('a',':a'),[{'a':2},{'a':3}])
    stmt_1 = text("select a from test where a=:x")
    stmt_2 = text("select * from test")
    with connection.begin() as trans:
        r = connection.execute(stmt_1,{'x':1})
        for i in r:
            print i
        r = connection.execute(stmt_2,{})
        df = pd.DataFrame(string_folding_wrapper(r))
        df.columns = r.keys()
        print df

    ## test iov api

    #tagname = u'bcm1fchannelmask_v1'
    #nowstr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    #isdefault = 1
    #datasource = 'bcm1f'

    #creationcomment = 'aaaa'    
    #applyto = 'daq'
    #maxnitems = 1
    #iovdataversion = '1.0.0'
    datadict = 'UINT8:48'
    payloaddatadict = iov_parsepayloaddatadict(datadict)
    print payloaddatadict
    datadict = 'STR12-STR256:40,UINT8:48'
    print iov_parsepayloaddatadict(datadict)
    iovdata = read_yaml('/home/zhen/work/brilws/data/bcm1f_channelmask_v1.yaml')
    print iovdata
    tagid = iov_createtag(connection,iovdata)
    print tagid
    alltags = iov_listtags(connection)
    print 'alltags ', alltags
    mytag = iov_listtags(connection,tagname='bcm1f_channelmask_v1')
    print 'mytag ',mytag
    mytagid = mytag.keys()[0]
    oldsinces = [k for k in mytag[mytagid].keys() if isinstance(k,int) ]
    lastsince = max(oldsinces)
    print 'lastsince ',lastsince
    newsince = lastsince+5
    payloadcomment = 'blah'
    payloaddata = [[48*[1]]]
    datadict = 'UINT8:48'
    print 'append to bcm1f_channelmask_v1'    
    newpayloadid=iov_appendtotag(connection,mytagid,lastsince+5,payloaddata,datadict,payloadcomment)
    print 'newpayloadid ',newpayloadid
            
