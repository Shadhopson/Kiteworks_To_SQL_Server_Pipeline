import datetime
import os
import pandas as pd
import paramiko
import pyodbc
import sys
import shutil
import logging


def get_date_from_file_name(file):
    if '-' in file:
        date_parts = file.split('-')
    else:
        date_parts = file.split('.')
    mm = ''.join([i for i in date_parts[0] if i.isdigit()]).zfill(2)
    dd = ''.join([i for i in date_parts[1] if i.isdigit()]).zfill(2)
    yy = ''.join([i for i in date_parts[2] if i.isdigit()])
    if len(yy) < 3:
        curr_date = '20'+yy+'-'+mm+'-'+dd
    else:
        curr_date = yy+'-'+mm+'-'+dd
    if int(mm) > 12:
        logging.info('Error: get_date_from_file_name, File date does not match format')
        sys.exit('File date does not match format')
    return curr_date


def get_max_lengths_of_each_columns_values(df):
    def get_length(row):
        lengths = [len(str(x)) for x in list(row)]
        return lengths
    val_lengths = df.apply(get_length,axis=0)
    return val_lengths.apply(max, axis=0)


def table_to_df(server, database, table):
    sql_conn = pyodbc.connect(DRIVER="{SQL Server Native Client 11.0};",
                                SERVER=server+";",
                                DATABASE=database+";" ,
                                Trusted_Connection="yes")
    sql = "select * from "+table
    records_df = pd.read_sql_query(sql, sql_conn)
    return records_df


def check_for_archive_folder(archive_path):
    today_str = datetime.datetime.today().strftime('%Y-%m-%d')
    archive_folders = os.listdir(archive_path)
    for folder in archive_folders:
        if folder == today_str:
            return True
    return False


def check_for_kiteworks_file(transport, kiteworks_dir):
    sftp = paramiko.SFTPClient.from_transport(transport)
    latest = 0
    for fileattr in sftp.listdir_attr(kiteworks_dir):
        if fileattr.st_mtime > latest:
            latest = fileattr.st_mtime
    if latest != 0 and datetime.datetime.fromtimestamp(latest) > (datetime.datetime.today() - datetime.timedelta(hours=24)):
        print("New Files Found. Processing now.")
        return True
    else:
        return False


def connect_to_kiteworks(config):
    hostname = config['kw_hostname']
    port = 22
    sftp_uid = config['kw_uid']
    sftp_pwd = config['kw_password']
    transport = paramiko.Transport(hostname, port)
    transport.connect(None, sftp_uid, sftp_pwd)
    return transport


def kiteworks_to_local(transport, kiteworks_source_dir, sftp_staging_dir):
    local_target_dir = sftp_staging_dir
    sftp = paramiko.SFTPClient.from_transport(transport)
    files = []
    for f in sftp.listdir_attr(kiteworks_source_dir):
        file_name = f.filename
        f_mod_date = datetime.datetime.fromtimestamp(f.st_mtime)
        if f_mod_date > (datetime.datetime.today() - datetime.timedelta(hours=24)):
            sftp.get(kiteworks_source_dir + '/' + f.filename, str(local_target_dir / file_name))
            files.append(str(local_target_dir) + '/' + file_name)
    return files

def number_of_dates_match(df,files):
    dates = set(pd.to_datetime(df.call_date).tolist())
    num_dates = len(dates)
    num_files = len(files)
    if num_dates != num_files:
        print('Number of dates and files do not match, checking for duplicates')
        return False
    return True


def truncate_table(server, database, table):
    sql_conn = pyodbc.connect(DRIVER="{SQL Server Native Client 11.0};",
                                SERVER=server+";",
                                DATABASE=database+";" ,
                                Trusted_Connection="yes")
    cursor = sql_conn.cursor()
    truncate_statement = "truncate table "+database+"."+table
    cursor.execute(truncate_statement)
    cursor.commit()
    print(database+"."+table +" truncated in "+server)
    cursor.close()


def make_insert_query(database, table, df):
    table_loc = database+'.'+table
    insert_query = '''INSERT INTO {}
            ('''.format(table_loc)
    for col in df.columns.values.tolist():
        insert_query += '[{}],'.format(col)
    insert_query = insert_query[:-1]
    insert_query += ') VALUES ('
    num_cols = len(df.columns.values.tolist()) - 1
    insert_query += '?,' * num_cols
    insert_query += '?)'
    return insert_query


def local_to_db(server, database, table, df):
    sql_conn = pyodbc.connect(DRIVER="{SQL Server Native Client 11.0};",
                                SERVER=server+";",
                                DATABASE=database+";" ,
                                Trusted_Connection="yes")
    cursor = sql_conn.cursor()
    insert_query = make_insert_query(database, table, df)
    params = [tuple(x) for x in df.values]
    cursor.executemany(insert_query, params)
    cursor.commit()
    print(str(len(params))+' rows inserted into '+server+' '+database+' '+table)
    cursor.close()

def stg_to_db(server, database, stg_table_loc, dest_table_loc):
    sql_conn = pyodbc.connect(DRIVER="{SQL Server Native Client 11.0};",
                            SERVER=server+";",
                            DATABASE=database+";" ,
                            Trusted_Connection="yes")
    cursor = sql_conn.cursor()
    insert_statement = f"INSERT INTO {dest_table_loc} SELECT *, GETDATE() AS date_created FROM {stg_table_loc}"
    cursor.execute(insert_statement)
    cursor.commit()
    print(stg_table_loc + " data inserted into " + dest_table_loc)
    cursor.close()


def archive_by_current_date(file, archive_dir):
    today_str = datetime.datetime.today().strftime('%Y-%m-%d')
    file_name = file.split(sep='/')[1]
    date_path = archive_dir / today_str
    date_path.mkdir(parents=True, exist_ok=True)
    processed_filepath = os.path.join(date_path, file_name)
    message = f'moving {file_name} to {processed_filepath}'
    # logger.info(message)
    print(message)
    shutil.move(file, processed_filepath)