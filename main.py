import pandas as pd
from functions import find_timestamps, read_tx_dfs
from dask.distributed import Client
import dask.dataframe as dd
from memory_profiler import profile

INTERVALS_DIR = r"I:\my_python\Traider_bot\bot, scripts, data\подбор моментов покупки и продажи (peaks)\подбор оптимальных параментров для расчета моментов покупки и продажи 0.2\btc_price_df_with_intervals.parquet"
TXS_DIR = r"I:\my_python\Traider_bot\bot, scripts, data\парсинг транзакций\parsing\data"
SMA_MIN = 10

# MEMORY_LIMIT = '9GB'
WORKERS = 4  
THREADS_PER_WORKER = 2

TEST = 1

def preprocess_transactions(df, intervals_dd):
    """Объединяет транзакции с временными интервалами и фильтрует по времени."""
    df['Block_time'] = dd.to_datetime(df['Block_time'])  # Убедитесь, что время в нужном формате
    meta = df.head().copy()  # Используем первую строку df как метаданные
    meta['start_time'] = pd.Timestamp('1970-01-01')
    meta['end_time'] = pd.Timestamp('1970-01-01')

    # Преобразуем intervals_dd в pandas DataFrame, если это необходимо
    if isinstance(intervals_dd, dd.DataFrame):
        intervals_dd = intervals_dd.compute()

    # Слияние с интервалами по условию вхождения времени блока в интервал
    result = df.map_partitions(
        lambda part: pd.merge_asof(
            part.sort_values('Block_time'),
            intervals_dd.sort_values('start_time'),
            left_on='Block_time',
            right_on='start_time',
            direction='forward'
        ),
        meta=meta
    )
    return result

# Основной скрипт
def main():
    client = Client(threads_per_worker=THREADS_PER_WORKER, n_workers=WORKERS, local_directory='I:/dask-temp')  # memory_limit=MEMORY_LIMIT
    print("Dashboard link:", client.dashboard_link)

    intervals = dd.read_parquet(INTERVALS_DIR)
    print('2')
    buy_intervals = intervals[intervals['Buy'] == 1]
    sell_intervals = intervals[intervals['Sell'] == 1]
    print('3')
    time_intervals_to_buy_df = find_timestamps(buy_intervals, SMA_MIN)
    time_intervals_to_sell_df = find_timestamps(sell_intervals, SMA_MIN)
    print('4')
    txs = read_tx_dfs(TXS_DIR, TEST)
    txs = txs.repartition(npartitions=WORKERS * THREADS_PER_WORKER * 2)  # Увеличиваем количество партий для уменьшения размера каждой
    txs = txs.persist()  # Сохраняем данные в распределенной памяти

    # Предварительное вычисление DataFrame с интервалами
    # time_intervals_to_buy_df = fu.create_time_intervals_df(time_intervals_to_buy)
    # time_intervals_to_sell_df = fu.create_time_intervals_df(time_intervals_to_sell)

    futures_buy = client.map(preprocess_transactions, [txs] * len(time_intervals_to_buy_df), [time_intervals_to_buy_df] * len(time_intervals_to_buy_df))
    futures_sell = client.map(preprocess_transactions, [txs] * len(time_intervals_to_sell_df), [time_intervals_to_sell_df] * len(time_intervals_to_sell_df))

    # Сбор результатов
    filtered_buy_transactions = client.gather(futures_buy)
    filtered_sell_transactions = client.gather(futures_sell)

    # Преобразование объектов в pandas DataFrame, если они являются объектами Dask
    # filtered_buy_transactions = [df.compute() if hasattr(df, 'compute') else df for df in filtered_buy_transactions]
    # filtered_sell_transactions = [df.compute() if hasattr(df, 'compute') else df for df in filtered_sell_transactions]

    # Проверка типов данных перед преобразованием
    print(type(filtered_buy_transactions[0]))
    print(type(filtered_sell_transactions[0]))

    # Объединение DataFrame
    filtered_buy_transactions_dd = [dd.from_pandas(df, npartitions=1) for df in filtered_buy_transactions]
    filtered_sell_transactions_dd = [dd.from_pandas(df, npartitions=1) for df in filtered_sell_transactions]
    filtered_transactions = dd.concat(filtered_buy_transactions_dd + filtered_sell_transactions_dd)

    # Выполнение и получение результатов
    wallets = filtered_transactions['Wallet_id'].drop_duplicates().compute()
    print(wallets.head())

    final_result = txs[txs['Wallet_id'].isin(wallets.tolist())]
    final_result = final_result.compute() if hasattr(final_result, 'compute') else final_result

    print(final_result.head())
    final_result.to_parquet('txs_of_wallets.parquet')
    print('End')

if __name__ == '__main__':
    main()