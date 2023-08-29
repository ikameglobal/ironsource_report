import json
import logging
import os
import shutil
import re
import hashlib

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

LOGGER = logging.getLogger(__name__.split('.')[-1])


def default_filter_path(path):
    return path.endswith('.csv')


def rm(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


def rm_paths(paths):
    for path in paths:
        rm(path)


def clean_dir(dir_path, filter_path=default_filter_path):
    for child in os.listdir(dir_path):
        path_child = normalize_path(dir_path + '/' + child)
        if filter_path(path_child):
            rm(path_child)
    return dir_path


def clean_dirs(dir_paths, filter_path=default_filter_path):
    for path in dir_paths:
        if os.path.isdir(path):
            clean_dir(path, filter_path)


def list_child_dir_paths(dir_path):
    child_dirs = []
    if os.path.isdir(dir_path):
        for child in os.listdir(dir_path):
            path_child = normalize_path(dir_path + '/' + child)
            if os.path.isdir(path_child):
                child_dirs.append(path_child)
    return child_dirs


def list_child_file_paths(dir_path):
    child_files = []
    if os.path.isdir(dir_path):
        for child in os.listdir(dir_path):
            path_child = normalize_path(dir_path + '/' + child)
            if os.path.isfile(path_child):
                child_files.append(path_child)
    return child_files


def list_all_child_paths(path):
    child_files = []
    if os.path.isdir(path):
        for child in os.listdir(path):
            child_files.append(normalize_path(path + '/' + child))
    else:
        child_files.append(path)
    return child_files


def write_file(file_path, content):
    with open(file_path, 'w') as writer:
        writer.write(content)
        writer.flush()
    writer.close()


def normalize_path(path: str):
    return re.sub('/+', '/', path)


def md5(content):
    return hashlib.md5(content.encode()).hexdigest()


def dump_csv(file_path, rows: list, **kwargs):
    columns = kwargs.get("columns")
    index = kwargs.get("index", False)
    header = kwargs.get("header", False)
    encoding = kwargs.get("encoding", None)
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(file_path, index=index, header=header, encoding=encoding)


def load_csv(file_path, dtype=None, **kwargs):
    return pd.read_csv(file_path, dtype=dtype, **kwargs)


def load_npy(file_path):
    return pd.DataFrame(np.load(file_path, allow_pickle=True).tolist())


def dump_npy(file_path, data):
    np.save(file_path, data)


def load_jsonl(path, **kwargs):
    return pd.read_json(path_or_buf=path, lines=True, **kwargs)


def dump_jsonl(path, df: pd.DataFrame):
    df.to_json(path_or_buf=path, orient='records', lines=True)


def load_json(path):
    with open(path, 'r', encoding='utf-8') as reader:
        return json.load(reader)


def dump_json(path, data):
    with open(path, 'w', encoding='utf-8-sig') as writer:
        writer.write(json.dumps(data, indent=4))


def load_parquet(path):
    return pd.read_parquet(path)


def load_parquet_files(filepaths):
    LOGGER.info(f"There are {len(filepaths)} files to read.")
    dfs = []
    for filepath in filepaths:
        dfs.append(load_parquet(filepath))
    return pd.concat(dfs, ignore_index=True)


def dump_parquet(path, data: pd.DataFrame):
    data.to_parquet(path)


def get_filepath_dir(dir_path):
    filepaths = []
    if os.path.isdir(dir_path):
        for file in os.listdir(dir_path):
            file_path = dir_path + file if dir_path.endswith('/') else dir_path + '/' + file
            if os.path.isfile(file_path):
                filepaths.append(file_path)
            else:
                filepaths.extend(get_filepath_dir(file_path))
    elif os.path.isfile(dir_path):
        filepaths.append(dir_path)
    return filepaths


def read_csv_files(filepaths):
    LOGGER.info(f"There are {len(filepaths)} files to read: {filepaths}")
    data_csv_files = []
    for filepath in filepaths:
        csv_data = load_csv(filepath)
        data_csv_files.append(csv_data)
    if not data_csv_files:
        return pd.DataFrame([])
    return pd.concat(data_csv_files, ignore_index=True)


def dynamic_batch_iterator(iterable, batch_size_getter):
    batch = []
    batch_size = batch_size_getter()
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
            batch_size = batch_size_getter()
    if len(batch) > 0:
        yield batch


def now():
    return int(datetime.utcnow().timestamp())


def clear_dir(dir_path):
    if os.path.exists(dir_path):
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
        else:
            os.remove(dir_path)
    # make dir
    os.makedirs(dir_path)


def bq_timestamp(timestamp):
    dt = datetime.utcfromtimestamp(timestamp)
    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')


def now_timestamp(time_format='int'):
    now_time = datetime.now().timestamp()
    if time_format == 'int':
        return int(now_time)
    elif time_format == 'bq':
        return bq_timestamp(now_time)


def today_timestamp(next_day=0, pre_day=0):
    seconds_day = int(timedelta(days=1).total_seconds())
    return (int(datetime.today().timestamp()) // seconds_day + next_day - pre_day) * seconds_day


def get_size(start_path='.', unit='bytes'):
    total_size = 0
    exponents_map = {'bytes': 0, 'kb': 1, 'mb': 2, 'gb': 3}
    for dir_path, dir_names, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dir_path, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    if unit not in exponents_map:
        raise ValueError("Must select from ['bytes', 'kb', 'mb', 'gb']")
    else:
        size = total_size / 1000 ** exponents_map[unit]

    return round(size, 3)


def join_csv(csv_files: list[str], output_file: str, delete_after_done=False) -> str:
    """
    Join csv files into one file using pandas.
    Args:
        csv_files: List of csv paths.
        output_file: Output csv path.
        delete_after_done: Delete input csv files after done.
    Returns:
        Output csv path.
    """
    if not csv_files:
        raise ValueError("csv_files must not be empty")

    if len(csv_files) > 1:
        df = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files], ignore_index=True)
        df.to_csv(output_file, index=False)
    else:
        shutil.copyfile(csv_files[0], output_file)

    if delete_after_done:
        for csv_file in csv_files:
            os.remove(csv_file)
    return output_file


def join_csv_with_size_limit(csv_files: list[str] = None,
                             output_name_prefix: str = 'output',
                             limit_size: int = 100,
                             delete_after_done: bool = False) -> list[str]:
    """
    Join csv files with size limit. If limit is reached, a new file will be created.
    Args:
        csv_files: List of csv paths.
        output_name_prefix: Prefix of output csv paths.
        limit_size: Size limit in MB.
        delete_after_done: If True, delete input csv files after done.

    Returns:
        List of output csv paths.
    """

    # Group csv files by total size <= limit_size
    groups = []
    current_group = []
    current_group_size = 0
    for csv_file in csv_files:
        csv_size = os.path.getsize(csv_file) / 1024 ** 2

        if csv_size > limit_size:
            groups.append([csv_file])
        elif csv_size + current_group_size > limit_size:
            groups.append(current_group)
            current_group = [csv_file]
            current_group_size = csv_size
        else:
            current_group.append(csv_file)
            current_group_size += csv_size
    if current_group:
        groups.append(current_group)

    # Join csv files in each group
    output_files = []
    for i, group in enumerate(groups):
        output_file = f"{output_name_prefix}_{i}.csv"
        join_csv(group, output_file, delete_after_done=delete_after_done)
        output_files.append(output_file)

    return output_files
