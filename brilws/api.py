import numpy as np
import pandas as pd
import coral

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

def create_sequencetable_stmt(tablename,dbflavor='sqlite'):
    result=''
    if dbflavor=='sqlite':
        result='CREATE TABLE %s(NEXTID ULONGLONG, CONSTRAINT %s_PK PRIMARY KEY (NEXTID) );\n'%(tablename,tablename)
    else:
        result='CREATE TABLE %s(NEXTID NUMBER(20), CONSTRAINT %s_PK PRIMARY KEY (NEXTID) );\n'%(tablename,tablename)
    result = result + 'INSERT INTO %s(NEXTID) VALUES(1);'%(tablename)
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

def build_sqlfilename(schema_name,operationtype='create',yearsuffix='',dbflavor='sqlite'):
    result=''
    if yearsuffix:
       result='%s_%s%s_%s.sql'%(schema_name,dbflavor,operationtype,yearsuffix)
    else:
       result='%s_%s%s.sql'%(schema_name,dbflavor,operationtype)
    return result

def drop_tables_sql(schema_name,schema_def,yearsuffix,dbflavor='sqlite'):
    results=[]
    tables=schema_def.keys()
    outfilename=build_sqlfilename(schema_name,operationtype='drop',yearsuffix=yearsuffix,dbflavor=dbflavor)
    for tname in tables:
        results.append(drop_table_stmt(tname,dbflavor=dbflavor))
    resultStr='\n'.join(results)
    if yearsuffix:
       resultStr=resultStr.replace('&suffix',yearsuffix)+';'
    resultStr=resultStr.upper() 
    with open(outfilename,'w') as sqlfile:
        sqlfile.write('/* tablelist: %s */\n'%(','.join([t.replace('&suffix',yearsuffix).upper() for t in tables])))
        sqlfile.write(resultStr)
    
def create_tables_sql(schema_name,schema_def,yearsuffix,dbflavor='sqlite',writeraccount=''):
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
    outfilename=build_sqlfilename(schema_name,operationtype='create',yearsuffix=yearsuffix,dbflavor=dbflavor)
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
    if yearsuffix:
        resultStr=resultStr.replace('&suffix',yearsuffix)
    resultStr=resultStr.upper()  
    with open(outfilename,'w') as sqlfile: 
        sqlfile.write('/* tablelist: %s */\n'%(','.join([t.replace('&suffix',yearsuffix).upper() for t in tables])))
        sqlfile.write(resultStr)
        if fkresults:
            fkresultStr=';\n'.join([t.replace('&suffix',suffix).upper() for t in fkresults])
            sqlfile.write('\n'+fkresultStr+';')
        if ixresults:
            ixresultStr=';\n'.join([t.replace('&suffix',suffix).upper() for t in ixresults])
            sqlfile.write('\n'+ixresultStr+';')

def db_connect_protocol(connectstr):
    result = connectstr.split(':',1)
    protocol = ''
    if len(result)>1:
       protocol = result[0]    
       if protocol not in ['oracle','sqlite_file']:
          raise 'unsupported technology %s'%(protocol)
       return protocol
    else:
       raise 'unsupported db connection %s'%(connectstr)

def db_getnextid(schema,tablename):
    tablename = tablename.upper()
    result = 0
    try:
       query = schema.tableHandle(tablename).newQuery()
       query.addToOutputList('NEXTID')
       query.setForUpdate()
       cursor = query.execute()
       while cursor.next():
         result = cursor.currentRow()['NEXTID'].data()
       dataEditor = schema.tableHandle(tablename).dataEditor()
       inputData = coral.AttributeList()
       dataEditor.updateRows('NEXTID = NEXTID+1','',inputData)
       del query
       return result
    except Exception, e:
       raise Exception, str(e)      


def db_singleInsert(schema,tablename,rowdef,rowinputdict):
    '''
    input: 
       tablename, string
       rowdef,    [(colname:coltype)]
       rowinputdict,  {colname:colval}
    '''
    tablename = tablename.upper()
    try: 
       dataEditor = schema.tableHandle(tablename).dataEditor()
       insertdata = coral.AttributeList()
       for (colname,coltype) in rowdef:          
           insertdata.extend(colname,coltype)
           if rowinputdict.has_key(colname):
               insertdata[colname].setData(rowinputdict[colname])
           else:
               insertdata[colname].setData(None)
       dataEditor.insertRow(insertdata)
    except Exception, e:
       raise Exception, 'api.db_singleInsert: '+str(e)

def db_bulkInsert(schema,tablename,rowdef,bulkinput):
    '''
    input: 
       tablename, string
       rowdef,    [(colname:coltype)]
       bulkinput,   [{colname:colval},{colname:colval}]
    '''
    tablename = tablename.upper()
    try:
       dataEditor = schema.tableHandle(tablename).dataEditor()
       insertdata = coral.AttributeList()
       for (colname,coltype) in rowdef:
           insertdata.extend(colname,coltype)
       bulkOperation = dataEditor.bulkInsert(insertdata,len(bulkinput))
       for rowdict in bulkinput:
           for (colname,coltype) in rowdef:
               if rowdict.has_key(colname):
                   colval = rowdict[colname]
                   insertdata[colname].setData(colval)
               else:
                   insertdata[colname].setData(None)
           bulkOperation.processNextIteration()
       bulkOperation.flush()
       del bulkOperation
    except Exception, e:
       raise Exception, 'api.db_bulkInsert '+str(e)      

def db_getrowdef(schema,tablename):
    result = []
    desc = schema.tableHandle(tablename).description()
    ncols = desc.numberOfColumns()
    for icol in xrange(ncols):
       col = desc.columnDescription(icol)
       result.append((col.name(),col.type()))
    return result

def db_query_generator(qHandle,qTablelist,qOutputRowDef={},qConditionStr=None,qConditionVal=None):
    '''
    Inputs:
      qHandle, handle of coral Query
      qTablelist, [(table name,table alias)]
      qOutputRowDef, {columnname: (columctype,columnalias)}
      qConditionStr, query condition string
      qConditionVal, coral::AttributeList of condition bind variables
    Output:
      yield result row dict {colname:colval}
        
    '''
    resultrow = {}
    try:
      for t,tt in qTablelist:
          qHandle.addToTableList(t,tt)
      if qOutputRowDef:
          qResult = coral.AttributeList()
          for colname in qOutputRowDef.keys():        
              coltype,colalias = qOutputRowDef[colname]
              varname = colalias or colname
              qHandle.addToOutputList(colname,colalias)
              qResult.extend(varname,coltype) #c++ type here 
          qHandle.defineOutput(qResult)
      if qConditionStr:
          qHandle.setCondition(qConditionStr,qConditionVal)
    
      cursor = qHandle.execute()
      while cursor.next():
          dbrow = cursor.currentRow()
          if not dbrow: break
          resultrow = {}
          for icol in xrange(dbrow.size()):
            varname = dbrow[icol].specification().name()
            varval = dbrow[icol].data()
            resultrow[varname] = varval
          yield resultrow
    except Exception, e:
      print 'Database Error: ',e
      raise StopIteration

if __name__=='__main__':
    svc = coral.ConnectionService()
    connect = 'sqlite_file:pippo.db'
    session = svc.connect(connect, accessMode = coral.access_ReadOnly)
    session.transaction().start(True)
    qHandle = session.nominalSchema().newQuery()
    qTablelist = [('CONDTAGREGISTRY','')]
    qOutputRowDef = {'TAGID':('unsigned long long','tagid'),'TAGNAME':('string','tagname')}
    qConditionStr = 'APPLYTO=:applyto AND DATASOURCE=:datasource'
    qCondition = coral.AttributeList()
    qCondition.extend('applyto','string')
    qCondition.extend('datasource','string')
    qCondition['applyto'].setData('lumi')
    qCondition['datasource'].setData('hfoc')

    #qTablelist = [('NORMS','')]
    qOutputRowDef = {}
    df = pd.DataFrame.from_records(db_query_generator(qHandle,qTablelist,qOutputRowDef,qConditionStr,qCondition))
    del qHandle
    session.transaction().commit()
    if df.empty: 
       print 'empty result'
    else:
       print df
