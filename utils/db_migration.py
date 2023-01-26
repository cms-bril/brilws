import sys
from configparser import SafeConfigParser
import sqlalchemy as sql
import base64
import csv
import pandas as pd

def update(con, r, df):
    for index, row in df.iterrows():
        
        insert = ''' insert into CMS_LUMI_PROD.TRGSCALER_NEW values ( :runnum, :lsnum, :prescidx, :bitid, :trgprescval)'''
        binddict = {
            'runnum': r,
            'lsnum': row['lsnum'],
            'prescidx': row['prescidx'],
            'bitid': row['bitid'],
            'trgprescval': row['trgprescval'],
        }
        con.execute(insert, binddict)

def create_engine(servicemap, servicename):
    user = servicemap[servicename][1]
    passwd = base64.b64decode(servicemap[servicename][2].encode('ascii')).decode('utf-8')
    descriptor = servicemap[servicename][3]
    connurl = 'oracle+cx_oracle://{}:{}@{}'.format(user, passwd, descriptor)

    return sql.create_engine(connurl)

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
    r = 362319

    for index, user_arg in enumerate(user_args):

        if user_arg == '-p':
            auth_file = user_args[index+1]
        if user_arg == '-q':
            query_service = user_args[index+1]
        if user_arg == '-u':
            update_service = user_args[index+1]
        if user_arg == '-r':
            update_service = user_args[index+1]
    
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
    scaleridxs = get_prescidxchange(engine, r)
    result = get_trgscaler(engine, r, scaleridxs)

    engine_dev = create_engine(servicemap, update_service)
    df = result.reset_index()

    update(engine_dev, r, df)