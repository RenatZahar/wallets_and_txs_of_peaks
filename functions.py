import os
import pandas as pd
import dask.dataframe as dd
from memory_profiler import profile


pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', None)

# @profile
def find_timestamps(df, SMA_MIN):
    timestamps = df['Timestamp'].tolist()
    clear_timestamps = []
    start_timestamp = None
    end_timestamp = None
    for index, tmsp in enumerate(timestamps):
        # print(index, tmsp)
        if not start_timestamp:
            start_timestamp = tmsp

        if index + 1 < len(timestamps):
            if not tmsp + SMA_MIN * 60 == timestamps[index + 1]:
                end_timestamp = timestamps[index]
                clear_timestamps.append([start_timestamp, end_timestamp])
                start_timestamp = None
                end_timestamp = None
        else:
            # Обработка последнего интервала
            end_timestamp = tmsp
            clear_timestamps.append([start_timestamp, end_timestamp])
            break

    return clear_timestamps

# @profile
def read_tx_dfs(directory, TEST):
    txs_files =  [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
    files_paths = [os.path.join(directory, filename) for filename in txs_files]

    if TEST:
        files_paths = files_paths[:100]
    # print(files_paths)
    dataframes = [dd.read_parquet(file) for file in files_paths]
    txs_df = dd.concat(dataframes, axis=0)  # Объединяем по строкам
    return txs_df

# @profile
def find_wallets(timestamps, df, TEST):
    # Процесс обработки файлов для получения кошельков
    wallets = process_file(timestamps, df, TEST)
    # print(list(wallets))  # Вывод первых результатов для проверки
    return wallets

# @profile
def process_file(timestamps, df, TEST):
    from functools import reduce
    import operator

    # filtered_frames = []
    # for start_interval, end_interval in timestamps:
    #     current_filtered = df.loc[(df['Block_time'] >= start_interval) & (df['Block_time'] <= end_interval), ['Wallet_id']]
    #     filtered_frames.append(current_filtered)

    # if TEST:
    #     filtered_frames = filtered_frames[:20]

    # # Объединяем только если есть что объединять
    # if filtered_frames:
    #     filtered_data = dd.concat(filtered_frames)
        
        
    #     # Возвращаем Dask Series с уникальными идентификаторами кошельков
    #     return filtered_data['Wallet_id'].drop_duplicates()
    # else:
    #     # Возвращаем пустой Dask DataFrame с колонкой Wallet_id
    #     return dd.from_pandas(pd.DataFrame(columns=['Wallet_id']), npartitions=1)

    # Используем функцию reduce из functools для создания одного большого условия с использованием логического OR


    # Создаем список условий
    conditions = [
        df['Block_time'].between(start, end) for start, end in timestamps
    ]
    if TEST:
        conditions = conditions[:200]
    # Объединяем все условия в одно с помощью логического OR
    combined_condition = reduce(operator.or_, conditions)

    # Фильтруем df с использованием объединенного условия
    filtered_df = df.loc[combined_condition]

    return filtered_df

# @profile
def find_txs(wallets_to_buy, wallets_to_sell, txs):
    # # print(wallets_to_buy)
    # wallets_list = []
    # wallets_list.extend(wallets_to_buy)
    # wallets_list.extend(wallets_to_sell)
    # print('wallets_list.extend(wallets_to_sell)')
    # required_txs = txs.loc[txs['Wallet_id'].isin(wallets_list)]
    # return required_txs
    # Предполагается, что wallets_to_buy и wallets_to_sell - это Dask DataFrame с одним столбцом 'Wallet_id'
    wallets_list = dd.concat([wallets_to_buy, wallets_to_sell]).drop_duplicates().compute()
    print('wallets_list')
    print(wallets_list)
    # Мержим с основным DataFrame по 'Wallet_id'
    required_txs = txs.merge(wallets_list, on='Wallet_id', how='inner').compute()
    print('required_txs')
    print(required_txs)
    return required_txs


def create_time_intervals_df(timestamps):
    """ Создает DataFrame из временных интервалов для последующего merge. """
    intervals_df = pd.DataFrame(timestamps, columns=['start_time', 'end_time'])
    intervals_df['start_time'] = pd.to_datetime(intervals_df['start_time'])
    intervals_df['end_time'] = pd.to_datetime(intervals_df['end_time'])
    intervals_dd = dd.from_pandas(intervals_df, npartitions=1)
    return intervals_dd

def preprocess_transactions(df, intervals_dd):
    """ Объединяет транзакции с временными интервалами и фильтрует по времени. """
    df['Block_time'] = dd.to_datetime(df['Block_time'])  # Убедитесь, что время в нужном формате
    # Слияние с интервалами по условию вхождения времени блока в интервал
    result = df.map_partitions(lambda part: pd.merge_asof(part.sort_values('Block_time'), 
                                                          intervals_dd.compute().sort_values('start_time'), 
                                                          left_on='Block_time', 
                                                          right_on='start_time', 
                                                          direction='forward'))
    return result