import numpy as np
import pandas as pd
import coral

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

#def db_getrowdef(schema,tablename):
#    result = []
#    desc = schema.tableHandle(tablename).description()
#    ncols = desc.numberOfColumns()
#    for icol in xrange(ncols):
#       col = desc.columnDescription(icol)
#       result.append((col.name(),col.type()))
#    return result

def db_query_generator(qHandle,qTablelist,qOutputRowDef={},qConditionStr=None,qConditionVal=None):
    '''
    Inputs:
      qHandle, handle of coral Query
      qTablelist, [(table name,table alias)]
      qOutputRowDef, {columnname: (columctype,columnalias)}
      qConditionStr, query condition string
      qConditionVal, coral::AttributeList of condition bind variables
    Output:
      yield result row dict
        
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
