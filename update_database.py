from kiteworks_to_sqlserver_utility_functions import *
import pandas as pd
import json
from pathlib import Path
import pysftp
import datetime
import os
from os import listdir
import numpy as np
import pyodbc
import sys
import shutil
import logging


def format_last_df(df,dropped_columns):
    df = df.drop(dropped_columns,axis=1)
    df['Unique'] = np.nan
    df['Last'] = np.nan
    return df


def read_therapeutics_files(files):
    df_dict = {}
    for file in files:
        df = pd.read_csv(file, encoding='cp1252')
        if 'Last' in file:
            df = format_outbound_df(df)
        df['dt'].mask(df['dt'].isnull() &
                             (df['unique_1'].notnull() |
                              df['unique_2'].notnull() |
                              df['unique_3'].notnull()), get_date_from_file_name(file), inplace=True)
        if not expected_therapeutics_columns(df):
            sys.exit(file+': Columns do not match')
        df_dict[file] = df
    return df_dict


def expected_therapeutics_columns(df,expected_cols):
    df_cols = df.columns.values.tolist()
    difference = set(df_cols) ^ set(expected_cols)
    if len(difference) == 0:
        return True
    else:
        print(difference)
        return False


def combine_dfs(df_dict):
    files = list(df_dict.keys())
    concated_df = df_dict[files[0]]
    for i in range(1, len(files)):
        concated_df =  pd.concat([concated_df,df_dict[files[i]]], axis=0 )
    return concated_df


def clean_therapeutics_data(concated_df):
    concated_df = concated_df.dropna(how='all',axis=0)
    concated_df = concated_df[~concated_df.duplicated(keep='first')]
    clean_df = concated_df.query("`unique_1` == `unique_1` |  `unique_2` == `unique_2` |  `unique_3` == `unique_3`")
    clean_df = clean_df.astype(object)
    clean_df = clean_df.fillna(np.nan).replace([np.nan], [None])
    logging.info('Files combined and cleaned into df of shape '+ str(clean_df.shape))
    return clean_df


def melt_and_clean_df(df):
    df = df.copy().reset_index()

    def only_keep_final_vals(wide_df, final_only_cols):
        for idx in range(max(wide_df.index_val.values.tolist())):
            data_per_idx = wide_df.query(f'index_val == {idx}')
            if data_per_idx.shape[0] > 1:
                last = max(data_per_idx.cn.values.tolist())
                for col in final_only_cols:
                    wide_df[col] = wide_df[col].mask((wide_df.index_val == idx) & (wide_df.cn != last),
                                                     None)
        return wide_df

    df['index_val'] = df.index.tolist()

    df_disp = df[disp_cols]
    df_other = df[other_cols]
    melted_df = pd.melt(df_disp, id_vars=['index_val'], var_name='cn', value_name='cd').dropna()
    combined_df = melted_df.join(df_other, how='left', on='index_val', lsuffix='melted')
    final_only_df = only_keep_final_vals(combined_df, final_only_col)
    final_df = final_only_df.drop(['index_valmelted', 'index_val'], axis=1)
    logging.info('Files melted and cleaned into df of shape ' + str(final_df.shape))
    return final_df


def push_data_to_tables(df, stage_table_loc, sandbox_table_loc, final_table_loc):
    stage_table = '.'.join(stage_table_loc.split('.')[1:])
    truncate_table(database_loc, database, stage_table)
    logging.info(stage_table+' truncated')
    local_to_db(database_loc, database, stage_table, df)
    logging.info('df inserted into '+stage_table)
    stg_to_db(database_loc,
                    database,
                    stage_table_loc,
                    sandbox_table_loc
                   )
    logging.info(stage_table_loc +' inserted into '+sandbox_table_loc)
    stg_to_db(D]database_loc,
                    database,
                    stage_table_loc,
                    final_table_loc
                   )
    logging.info(stage_table_loc + ' inserted into ' + final_table_loc)


def is_expected_row_difference(clean_df, melted_df):
    non_null_disp1_2 = clean_df[clean_df['unique_2'].notnull() |
                                clean_df['unique_3'].notnull()].shape[0]
    row_diff = melted_df.shape[0] - clean_df.shape[0]
    return non_null_disp1_2 == row_diff


def has_same_set_of_ids(clean_df,melted_df):
    return len(set(clean_df.id.tolist())) == len(set(melted_df.id.tolist()))


def has_reasonable_number_of_dates(df):
    num_dates = len(set(df.cd.values.tolist()))
    if num_dates < 7:
        return True
    else:
        return False

def main():
    working_dir = Path(r'.')
    input_for_db_dir = working_dir / 'add_to_db'
    archive_path = working_dir / 'archive'
    config_file = 'config.json'
    date_today = datetime.datetime.today().strftime('%Y-%m-%d')
    log_dir = 'logs'
    logging.basicConfig(filename=log_dir + '/' + date_today + '.log', format='%(asctime)s %(message)s',
                        encoding='utf-8', level=logging.DEBUG)
    logging.info(' started')

    with open(config_file, 'r') as f:
        config = json.load(f)

    transport = connect_to_kiteworks(config)
    check = check_for_kiteworks_file(transport, kiteworks_source_dir)

    if check == False:
        logging.info("No new file found. Ending program.")
        print("No new file found. Ending program.")
        transport.close()
        return

    folder_check = check_for_archive_folder(archive_path)
    if folder_check == True:
        logging.info("File already processed and archived. Ending program. ")
        print("File already processed and archived. Ending program. ")
        transport.close()
        return

    files = kiteworks_to_local(transport, kiteworks_source_dir, input_for_db_dir)
    logging.info('Pulled ' + str(files) + ' to ' + str(input_for_db_dir))
    df_dict = read_therapeutics_files(files)
    combined_dfs = combine_dfs(df_dict)
    clean_df = clean_therapeutics_data(combined_dfs)
    melted_df = melt_and_clean_df(clean_df)
    if not is_expected_row_difference(clean_df,melted_df):
        logging.info("melted_df has incorrect number of rows. Ending Program.")
        print("melted_df has incorrect number of rows")
        transport.close()
        return
    if not has_same_set_of_ids(clean_df, melted_df):
        logging.info("melted_df has incorrect number of ids. Ending Program.")
        print("melted_df has incorrect number of ids")
        transport.close()
        return
    if not has_reasonable_number_of_dates(clean_df):
        logging.info("Too many call_dates in clean_df. Ending Program.")
        print("clean_df has too many call_dates.")
        transport.close()
        return

    push_data_to_tables(clean_df,
                        stage_unformatted,
                        sandbox_unformatted,
                        final_unformatted)
    push_data_to_tables(melted_df,
                        stage,
                        sandbox,
                        final)

    for file in files:
        archive_by_current_date(file, archive_path)

    print(files)
    transport.close()
    print('finished')
    logging.info(' finished')
    return clean_df, melted_df

if __name__ == "__main__":
    main()
