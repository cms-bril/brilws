import sys
from configparser import SafeConfigParser
import sqlalchemy as sql
import base64
import csv
import pandas as pd

def query_runnums(con, r):
    """Get all available runnums greater and equal(>=) than r from cms_lumi_prod.trgscaler.

    :param con:
    :param r: runnum
    :param gte: greater than or equal
    :returns: [(runnum,)]
    :rtype: list

    """
    select = 'select distinct runnum from cms_lumi_prod.trgscaler where runnum>=:runnum order by runnum'
    runnums = con.execute(select, {'runnum': r})
    
    result = []
    runnums = runnums.fetchall()
    if len(runnums) > 0:
        result = [row[0] for row in runnums]
    return result

def query_batch(con, r, gte=False):
    """Query multiple records from CMS_LUMI_PROD.TRGSCALER when runnum lesser (<) or greater and equal(>=) than r
        If gte=False just copy records from CMS_LUMI_PROD.TRGSCALER. If gte=True then copy records from CMS_UGT_MON.VIEW_UGT_RUN_PRESCALE

    :param con:
    :param r: runnum
    :param gte: greater than or equal
    :returns: [{lsnum, prescidx, bitid, trgprescval}]
    :rtype: list of dict

    """
    records_df = pd.DataFrame()
    if gte:
        runnums = query_runnums(con, r)
        for runnum in runnums:
            scaleridxs = get_prescidxchange(con, runnum)
            record_df = get_trgscaler(con, runnum, scaleridxs)
            record_df['runnum'] = runnum
            frames = [records_df, record_df]
            records_df = pd.concat(frames)
    else:
        select = 'select * from cms_lumi_prod.trgscaler where runnum<:runnum order by runnum'
        records = con.execute(select, {'runnum': r})
        records_df = pd.DataFrame(records, columns=['runnum', 'lsnum', 'prescidx', 'bitid', 'trgprescval'])
        records_df.runnum = records_df.runnum.astype(int)
        records_df.lsnum = records_df.lsnum.astype(int)
        records_df.bitid = records_df.bitid.astype(int)
        records_df.prescidx = records_df.prescidx.astype(int)
        records_df.trgprescval = records_df.trgprescval.astype(float)
    
    return records_df

def insert_batch(con, df):
    """Make a batch insertion to from cms_lumi_prod.trgscaler_new.

    :param con:
    :param df: Dataframe containing the data
    :return msg
    """
    metadata = sql.MetaData(bind=con, reflect=True)
    table = metadata.tables['trgscaler_new']
    values = []
    for index, row in df.iterrows():
        values.append({
            'runnum': int(row['runnum']),
            'lsnum': int(row['lsnum']),
            'prescidx': int(row['prescidx']),
            'bitid': int(row['bitid']),
            'trgprescval': float(row['trgprescval']),
        })

    result = con.execute(sql.insert(table), values)

    return result

def create_engine(servicemap, servicename):
    user = servicemap[servicename][1]
    passwd = base64.b64decode(servicemap[servicename][2].encode('ascii')).decode('utf-8')
    descriptor = servicemap[servicename][3]
    connurl = 'oracle+cx_oracle://{}:{}@{}'.format(user, passwd, descriptor)

    return sql.create_engine(connurl)

# function copied from bril/utils
def get_prescidxchange(con, runnum):
    """Get prescale index changes over lumisections

    :param con:
    :param runnum:
    :returns: [{lsnum, prescidx, lsnummax, missing, samplels}]
    :rtype: list of dicts

    """
    result = []
    select1 = '''select distinct lsnumber
    from cms_runinfo.hlt_supervisor_triggerpaths
    where runnumber=:runnum  order by lsnumber'''
    select2 = '''select lumi_section, prescale_index
    from CMS_UGT_MON.VIEW_LUMI_SECTIONS
    where run_number=:runnum order by lumi_section'''

    result_select1 = con.execute(select1, {'runnum': runnum})
    lumisections = [row[0] for row in result_select1]
    if len(lumisections) <= 0:
        print('NOTHING in triggerpaths')

    result_select2 = con.execute(select2, {'runnum': runnum})
    prescales = [row for row in result_select2 if row[1] is not None]

    if len(prescales) <= 0:
        print('NOTHING in CMS_UGT_MON.VIEW_LUMI_SECTIONS\n')
        return result

    prescranges = []  # [[lsmin,lsmax]]
    missing = []
    last_row = prescales[0]
    for row in prescales:
        if row == last_row or row[1] != last_row[1]:
            if prescranges:
                prescranges[-1][1] = row[0] - 1
            prescranges.append([row[0], None])
        if row[0] not in lumisections:
            missing.append(row[0])
        last_row = row
    prescranges[-1][1] = last_row[0]
    for pr in prescranges:
        record = {
            'lsnum': pr[0],
            'prescidx': [x[1] for x in prescales if x[0] == pr[0]][0],
            'lsnummax': pr[1],
            'missing': None,
            'samplels': pr[0]
        }

        result.append(record)

    if result[0]['lsnum'] != 1:
        result[0]['lsnum'] = 1

    return result

#function copied from bril/utils
def get_trgscaler(con, runnum, scaleridxs):
    """Get prescale values

    :param con:
    :param runnum:
    :param scaleridxs: [{lsnum, prescidx, lsnummax, missing, samplels}]
    :returns: pd.DataFrame(columns=['lsnum','prescidx','bitid','trgprescval']
    :rtype: pd.DataFrame

    """
    prescidx = list({scaler['prescidx'] for scaler in scaleridxs})
    if not prescidx:
        raise ValueError(
            'There must be at least one prescidx in run. {}'.format(runnum))

    qprescval_algo = '''select PRESCALE_INDEX as prescidx, ALGO_INDEX as bitid,
    PRESCALE as trgprescval from CMS_UGT_MON.VIEW_UGT_RUN_PRESCALE
    where RUN_NUMBER=:runnum and prescale_index >=:prescmin
    and prescale_index<=:prescmax'''
    binddict = {
        'runnum': runnum,
        'prescmin': int(min(prescidx)),
        'prescmax': int(max(prescidx))
    }
    with con.begin():
        prescval_algo_result = con.execute(qprescval_algo, binddict)

    prescvals_algo = pd.DataFrame(
        list(prescval_algo_result),
        columns=['prescidx', 'bitid', 'trgprescval'])
    prescvals_algo.prescidx = prescvals_algo.prescidx.astype(int)
    prescvals_algo.bitid = prescvals_algo.bitid.astype(int)
    prescvals_algo.trgprescval = prescvals_algo.trgprescval.astype(float)

    scaleridxs_df = pd.DataFrame(scaleridxs, columns=['lsnum', 'prescidx'])
    scaleridxs_df.lsnum = scaleridxs_df.lsnum.astype(int)
    scaleridxs_df.prescidx = scaleridxs_df.prescidx.astype(int)

    result = scaleridxs_df.merge(prescvals_algo, how='inner', on='prescidx')
    return result

def parseservicemap(authfile):
    """Parse service config ini file

    :param authfile: file path
    :returns: parsed service map
    :rtype: {servicealias:[protocol,user,passwd,descriptor]}

    """
    result = {}
    parser = SafeConfigParser()
    parser.read(authfile)

    for s in parser.sections():
        protocol = parser.get(s, 'protocol')
        user = parser.get(s, 'user')
        passwd = parser.get(s, 'pwd')
        descriptor = parser.get(s, 'descriptor')
        result[s] = [protocol, user, passwd, descriptor]
    return result

if __name__ == '__main__':

    user_args = sys.argv
    auth_file = 'devdb.ini'
    query_service = 'online'
    update_service = 'dev'
    r = 362759
    gte = False

    for index, user_arg in enumerate(user_args):

        if user_arg == '-p':
            auth_file = user_args[index+1]
        if user_arg == '-q':
            query_service = user_args[index+1]
        if user_arg == '-u':
            update_service = user_args[index+1]
        if user_arg == '-r':
            update_service = user_args[index+1]
        if user_arg == '--gte':
            gte = True
    
    if auth_file == '':
        raise RuntimeError('Please provide a path to a valid auth file (.ini)')
    if query_service == '':
        raise RuntimeError('Please provide the name of the service you will copy the data from')
    if update_service == '':
        raise RuntimeError('Please provide the name of the service that will receive the data')
    if r == '':
        raise RuntimeError('Please provide a valid runnum value')
    
    servicemap = parseservicemap(auth_file)

    engine = create_engine(servicemap, query_service)
    result = query_batch(engine, r, gte)

    con = create_engine(servicemap, update_service)
    insert_batch(con, result)