from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, types,create_engine
from sqlalchemy.orm import sessionmaker
import yaml

class TableDefinition(yaml.YAMLObject):
    ''' 
    Used to distinguish nested tables from columns.  This class is
    instantiated by YAML when an object with the ``!Table`` tag is
    visited.
    '''
    yaml_tag = u'!Table'    
    def __new__(cls, name='', columns=[], pk=[], notnull=[], unique=[]):
        obj = yaml.YAMLObject.__new__(cls)
        obj.name = name
        obj.columns = columns
        obj.pk = pk
        obj.notnull = notnull
        obj.unique = unique
        return obj
    def __str__(self):
        return "%s(name=%r, columns=%r, pk=%r, notnull=%r, unique=%r)"%(self.__class__.__name__, self.name, self.columns, self.pk, self.notnull, self.unique)

def constructColumn(name, typename, nullable=True, ):
    t = None
    if typename=='unsigned char':
        t = types.NUMERIC(3,0)
    elif typename.find('short') != -1:
        t = types.NUMERIC(5,0)
    elif typename.find('int') != -1:
        t = types.NUMERIC(10,0)
    elif typename.find('long long') != -1:
        t = types.NUMERIC(20,0)
    elif typename in ['float','double'] :
        t = types.FLOAT()
    elif typename=='string':
        t = types.String()
    elif typename=='blob':
        t = types.Binary()
    elif typename=='bool':
        t = types.Boolean()
    elif typename=='timestamp':
        t = types.DataTime()
    else:
        raise KeyError('Unknown type "%s" in column "%s"' % (typename, name))
    return Column(name,t)

def constructTable(tableDef, metadata):
    ''' 
    Adds a Table to the given MetaData, recursively adding any
    "subtables" it finds.
    '''

    # Check if this table has already been constructed.
    # This can happen if the relation graph is cyclic (not a tree).
    if tableDef.name in (table.name for table in metadata.table_iterator()):
        return

    # The primary key must be added before other columns in case this
    # table contains any cyclic references to itself.
    if not hasattr(tableDef, 'pk'):
        raise NameError('Primary Key must be defined')
    for p in tableDef.pk:
        pk = constructColumn(p, primary_key=True)
        # don't add this column again
        del tableDef.columns[p]
    
    # add a new table to the metadata
    # The 'meat' columns are added later.  This allows a recursive call to this
    # function to skip tables that have already been added here.
    table = Table(tableDef.name, metadata, pk)

    # add the columns to the metadata
    for colname,value in tableDef.columns.iteritems():
        if isinstance(value, TableDefinition):
            # recursively construct any "subtables"
            constructTable(value, metadata)
#            constructForeignKey(table, value.name, metadata)
        else: # leaf
            if colname in tableDef.notnull:
                table.append_column(constructColumn(colname, value, nullable=False))
            else:
                table.append_column(constructColumn(colname, value, nullable=True))
        # explicit/composite unique constraint.  'name' is optional.
        for ugroup in tableDef.unique:
            table.append_constraint(UniqueConstraint(tuple(ugroup)))
            
def generateMetadata(stream):
    ''' 
    Given a YAML file containing a list of table definitions, returns a
    SQLAlchemy MetaData object describing the database.
    '''

    schema = yaml.load(stream)
    metadata = MetaData()
    
    # assumes the root element is a list of table definitions
    for tableDef in schema:
        constructTable(tableDef, metadata)

    return metadata

if __name__=='__main__':
    import api
    a='''
    - !Table
      name: pippo_&suffix
      columns: 
        - tagid: long long 
        - tagname: string(32)
      pk: [tagid]

    '''
    schema = yaml.load(a)
    schemaStr = yaml.dump(schema)
    print schemaStr
    #for tableDef in schema:
    #    print tableDef.name
    #    print tableDef.pk
    #    print tableDef.columns
    #    print tableDef.unique
    engine = create_engine('sqlite:///test.db')
    session = sessionmaker(bind=engine)
    s = session()
    s.execute('''create table test ( a integer)''')
    #for i in xrange(1,10):
    api.db_insert_row(s,'test',{'a':1})
    api.db_insert_bulk(s,'test',[{'a':4},{'a':2}]);
    #s.execute("""insert into test(a) values(:a)""",[{'a':1},{'a':2}])
    s.commit()
