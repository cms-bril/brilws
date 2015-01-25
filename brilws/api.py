import numpy as np
import pandas as pd
#import coral
from sqlalchemy import *
from sqlalchemy import exc, text
from datetime import datetime
import decimal
import os
import random
import time
import yaml
decimalcontext = decimal.getcontext().copy()
decimalcontext.prec = 3

oracletypemap={
'unsigned char':'number(3)',
'unsigned short':'number(5)',
'unsigned int':'number(10)',
'unsigned long long':'number(20)',
'char':'char(1)',
'short':'number(5)',
'int':'number(10)',
'long long':'number(20)',
'float':'binary_float',
'double':'binary_double',
'string':'varchar2(4000)',
'blob':'blob',
'bool':'number(1)',
'timestamp':'timestamp'
}

sqlitetypemap={
'unsigned char':'unsignedchar',
'unsigned short':'unsignedshort',
'unsigned int':'unsignedint',
'unsigned long long':'ulonglong',
'char':'char',
'short':'short',
'int':'int',
'long long':'slonglong',
'float':'float',
'double':'double',
'string':'text',
'blob':'blob',
'bool':'boolean',
'timestamp':'timestamp'
}

ctodtypemap={
  'unsigned char':'u1',
  'unsigned short':'u2',
  'unsigned int':'u4',
  'unsigned long long':'u8',
  'char':'i1',
  'short':'i2',
  'int':'i4',
  'long long':'i8',
  'float':'f4',
  'double':'f8',
  'string':'O'
}


def _seqdiff(original,exclude):
    return list(set(original)-set(exclude))

def create_table_stmt(tablename):
    result='CREATE TABLE %s'%(tablename)
    return result

#def create_sequencetable_stmt(tablename,dbflavor='sqlite'):
#    result=''
#    if dbflavor=='sqlite':
#        result='CREATE TABLE %s(NEXTID ULONGLONG, CONSTRAINT %s_PK PRIMARY KEY (NEXTID) );\n'%(tablename,tablename)
#    else:
#        result='CREATE TABLE %s(NEXTID NUMBER(20), CONSTRAINT %s_PK PRIMARY KEY (NEXTID) );\n'%(tablename,tablename)
#    result = result + 'INSERT INTO %s(NEXTID) VALUES(1);'%(tablename)
#    return result

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

def build_sqlfilename(schema_name,operationtype='create',suffix=None,dbflavor='sqlite'):
    result=''
    if suffix:
       result='%s_%s%s_%s.sql'%(schema_name,dbflavor,operationtype,suffix)
    else:
       result='%s_%s%s.sql'%(schema_name,dbflavor,operationtype)
    return result

def drop_tables_sql(schema_name,schema_def,suffix=None,dbflavor='sqlite'):
    results=[]
    tables=schema_def.keys()
    outfilename=build_sqlfilename(schema_name,operationtype='drop',suffix=suffix,dbflavor=dbflavor)
    for tname in tables:
        results.append(drop_table_stmt(tname,dbflavor=dbflavor))
    resultStr='\n'.join(results)
    if suffix:
       resultStr=resultStr.replace('&suffix',suffix)+';'
    resultStr=resultStr.upper() 
    with open(outfilename,'w') as sqlfile:
        sqlfile.write('/* tablelist: %s */\n'%(','.join([t.replace('&suffix',suffix).upper() for t in tables])))
        sqlfile.write(resultStr)
    
def create_tables_sql(schema_name,schema_def,suffix=None,dbflavor='sqlite',writeraccount=''):
    '''
    input :
        schema_name, schema name 
        schema_def, dictionary of dictionaries
    '''
    results=[]
    resultStr=''
    fkresults=[]
    ixresults=[]
    tables=schema_def.keys()
    outfilename=build_sqlfilename(schema_name,operationtype='create',suffix=suffix,dbflavor=dbflavor)
    columntypemap={}  
    if dbflavor=='oracle':
        columntypemap = oracletypemap
    else:
        columntypemap = sqlitetypemap
    for tname in tables:       
        if tname.find('NEXTID')!=-1:
            stmt=create_sequencetable_stmt(tname,dbflavor)
            results.append(create_sequencetable_stmt(tname,dbflavor))       
            continue      
        result=create_table_stmt(tname)
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
        result=result+');\n'
        if fks: fkresults.append(fk(tname,fks))
        if dbflavor=='oracle' and writeraccount:
            result=result+grant_stmt(tname,writeraccount)
        results.append(result)
    resultStr='\n'.join(results)
    if suffix:
        resultStr=resultStr.replace('&suffix',suffix)
    resultStr=resultStr.upper()  
    with open(outfilename,'w') as sqlfile: 
        sqlfile.write('/* tablelist: %s */\n'%(','.join([t.replace('&suffix',suffix).upper() for t in tables])))
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
    result = [] #[(tablesuffix,varlen,alias),(tablesuffix,varlen,alias)]
    fields = datadictStr.split(',')
    result = [(f.split(':')+[None]*99)[:3] for f in fields]
    return result

def iov_getPtablename(typecode):
    if typecode.find('STRING')<0:
        return '_'.join([payloadtableprefix_,typecode])
    else: # ignore STR length info in case any
        return '_'.join([payloadtableprefix_,'STRING'])

def iov_getPtablenames():
    result = []
    for t in typecodes_:
        result.append(getPtablename(t))
    return result

#def db_connect_protocol(connectstr):
#    result = connectstr.split(':',1)
#    protocol = ''
#    if len(result)>1:
#       protocol = result[0]    
#       if protocol not in ['oracle','sqlite_file']:
#          raise 'unsupported technology %s'%(protocol)
#       return protocol
#    else:
#       raise 'unsupported db connection %s'%(connectstr)

#def db_getnextid(schema,tablename):
#    tablename = tablename.upper()
#    result = 0
#    try:
#       query = schema.tableHandle(tablename).newQuery()
#       query.addToOutputList('NEXTID')
#       query.setForUpdate()
#       cursor = query.execute()
#       while cursor.next():
#         result = cursor.currentRow()['NEXTID'].data()
#       dataEditor = schema.tableHandle(tablename).dataEditor()
#       inputData = coral.AttributeList()
#       dataEditor.updateRows('NEXTID = NEXTID+1','',inputData)
#       del query
#       return result
#    except Exception, e:
#       raise Exception, str(e)      


#def db_singleInsert(schema,tablename,rowdef,rowinputdict):
#    '''
#    input: 
#       tablename, string
#       rowdef,    [(colname:coltype)]
#       rowinputdict,  {colname:colval}
#    '''
#    tablename = tablename.upper()
#    try: 
#       dataEditor = schema.tableHandle(tablename).dataEditor()
#       insertdata = coral.AttributeList()
#       for (colname,coltype) in rowdef:          
#           insertdata.extend(colname,coltype)
#           if rowinputdict.has_key(colname):
#               insertdata[colname].setData(rowinputdict[colname])
#           else:
#               insertdata[colname].setData(None)
#       dataEditor.insertRow(insertdata)
#    except Exception, e:
#       raise Exception, 'api.db_singleInsert: '+str(e)



#def db_query_generator(qHandle,qTablelist,qOutputRowDef={},qConditionStr=None,qConditionVal=None):
#    '''
#    Inputs:
#      qHandle, handle of coral Query
#      qTablelist, [(table name,table alias)]
#      qOutputRowDef, {columnname: (columctype,columnalias)}
#      qConditionStr, query condition string
#      qConditionVal, coral::AttributeList of condition bind variables
#    Output:
#      yield result row dict {colname:colval}
#        
#    '''
#    resultrow = {}
#    try:
#      for t,tt in qTablelist:
#          qHandle.addToTableList(t,tt)
#      if qOutputRowDef:
#          qResult = coral.AttributeList()
#          for colname in qOutputRowDef.keys():        
#              coltype,colalias = qOutputRowDef[colname]
#              varname = colalias or colname
#              qHandle.addToOutputList(colname,colalias)
#              qResult.extend(varname,coltype) #c++ type here 
#          qHandle.defineOutput(qResult)
#      if qConditionStr:
#          qHandle.setCondition(qConditionStr,qConditionVal)
#    
#      cursor = qHandle.execute()
#      while cursor.next():
#          dbrow = cursor.currentRow()
#          if not dbrow: break
#          resultrow = {}
#          for icol in xrange(dbrow.size()):
#            varname = dbrow[icol].specification().name()
#            varval = dbrow[icol].data()
#            if type(varval) == float: 
#               varval=decimalcontext.create_decimal(varval)
#            resultrow[varname] = varval
#          yield resultrow
#    except Exception, e:
#      print 'Database Error: ',e
#      raise StopIteration

def nonsequential_key(generator_id):
    '''
    http://ericmittelhammer.com/generating-nonsequential-primary-keys/
    '''
    now = int(time.time()*1000)
    rmin = 1
    rmax = 2**8 - 1
    rdm = random.randint(1, rmax)
    yield ((now << 22) + (generator_id << 14) + rdm )

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

def iov_createtag(connection,iovdata):
    """
    inputs:
        connection:  db handle
        iovdata:     {'tagname':tagname, 'datadict':datadict, 'maxnitems':maxnitems, 'iovdataversion':iovdataversion, 'datasource':datasource, 'applyto':applyto, 'isdefault':isdefault, 'comment':comment, sincex:[[dict or list,]],{'comment':comment}] }
    """
    tagid = next(nonsequential_key(78))
    #print "creating iovtag %s"%iovdata['tagname']
    sinces = [x for x in iovdata.keys() if isinstance(x, int) ]
    nowstr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    datadict = iovdata['datadict']
    maxnitems = iovdata['maxnitems']
    
    with connection.begin() as trans:
        i = """insert into IOVTAGS(TAGID,TAGNAME,CREATIONUTC,DATADICT,MAXNITEMS,DATASOURCE,APPLYTO,ISDEFAULT,COMMENT) VALUES(:tagid, :tagname, :creationutc, :datadict, :maxnitems, :datasource, :applyto, :isdefault, :comment)"""
        r = connection.execute(i,{'tagid':tagid, 'tagname':iovdata['tagname'], 'creationutc':nowstr, 'datadict':datadict, 'maxnitems':maxnitems, 'datasource':iovdata['datasource'], 'applyto':iovdata['applyto'], 'isdefault':iovdata['isdefault'], 'comment':iovdata['comment'] })
        rowcache = {}
        payloaddatadict = iov_parsepayloaddatadict(datadict)
        for since in sinces:
            payloadid = next(nonsequential_key(79))
            ti = """insert into IOVTAGDATA(TAGID,SINCE,PAYLOADID,COMMENT) VALUES(:tagid, :since, :payloadid, :comment)"""
            payloadcomment = ''
            if len(iovdata[since])>1 and iovdata[since][-1]['comment'] is not None:
                payloadcomment = iovdata[since][-1]['comment']
            tr = connection.execute(ti, {'tagid':tagid, 'since':since, 'payloadid':payloadid, 'comment':payloadcomment })
            for item_idx, payloaddata in enumerate(iovdata[since]):
                for field_idx, fielddata in enumerate(payloaddata):
                    (tablesuffix,varlen,alias) = payloaddatadict[field_idx]
                    print 'field_idx, fielddata ',field_idx,fielddata
                    print (tablesuffix,varlen,alias)
                    payloadtable_name = iov_getPtablename(tablesuffix)
                    if isinstance(fielddata,list):
                        for ipos, val in enumerate(fielddata):
                            rowcache.setdefault(payloadtable_name,[]).append({'PAYLOADID':payloadid,'IITEM':item_idx,'IFIELD':field_idx,'IPOS':ipos,'VAL':val})
        print rowcache        
    return tagid
    
def iov_appendtotag(connection,tagid,since,payloaddata,datadict,payloadcomment):
    """
    inputs:
        connection: dbhandle
        tagid:      tagid        
        payloaddata: [[dict or list,]]
        datadict: str
        payloadcomment: 
    """
    (tablesuffix,varlen,alias) = iov_parsepayloaddatadict(datadict)
    payloadid = next(nonsequential_key(79))
    ti = """insert into IOVTAGDATA(TAGID,SINCE,PAYLOADID,COMMENT) VALUES(:tagid, :since, :payloadid, :comment)"""
    with connection.begin() as trans:
        tr = connection.execute(ti, {'tagid':tagid, 'since':since, 'payloadid':payloadid, 'comment':payloadcomment })
        for item_idx, item in enumerate(payloaddata):
                for field_idx, fielddata in enumerate(item):
                    payloadtable_name = iov_getPtablename(tablesuffix)
                    if isinstance(fielddata,list):
                        for ipos, val in enumerate(fielddata):
                            rowcache.setdefault(payloadtable_name,[]).append({'PAYLOADID':payloadid,'IITEM':item_idx,'IFIELD':field_idx,'IPOS':ipos,'VAL':val})
                            
    return payloadid

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
        print 'exists ',exists
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

if __name__=='__main__':
    #svc = coral.ConnectionService()
    #connect = 'sqlite_file:pippo.db'
    #session = svc.connect(connect, accessMode = coral.access_ReadOnly)
    #session.transaction().start(True)
    #qHandle = session.nominalSchema().newQuery()
    #qTablelist = [('CONDTAGREGISTRY','')]
    #qOutputRowDef = {'TAGID':('unsigned long long','tagid'),'TAGNAME':('string','tagname')}
    #qConditionStr = 'APPLYTO=:applyto AND DATASOURCE=:datasource'
    #qCondition = coral.AttributeList()
    #qCondition.extend('applyto','string')
    #qCondition.extend('datasource','string')
    #qCondition['applyto'].setData('lumi')
    #qCondition['datasource'].setData('hfoc')

    #qTablelist = [('NORMS','')]
    #qOutputRowDef = {}
    #df = pd.DataFrame.from_records(db_query_generator(qHandle,qTablelist,qOutputRowDef,qConditionStr,qCondition))
    #del qHandle
    #session.transaction().commit()
    #if df.empty: 
    #   print 'empty result'
    #else:
    #   print df

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
        print r
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
    #datadict = 'UINT8:48'
    #payloaddatadict = iov_parsepayloaddatadict(datadict)

    iovdata = read_yaml('/home/zhen/work/brilws/data/bcm1f_channelmask_v1.yaml')
    print iovdata
    with connection.begin() as trans:
        iov_createtag(connection,iovdata)
   
        
