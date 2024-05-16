import pandas as pd
import functions as fu
from dask.distributed import Client
import dask.dataframe as dd
from memory_profiler import profile

INTERVALS_DIR = r'Traider_bot\bot, scripts, data\подбор моментов покупки и продажи (peaks)\подбор оптимальных параментров для расчета моментов покупки и продажи 0.2\btc_price_df_with_intervals.parquet'
TXS_DIR = r'Traider_bot\bot, scripts, data\парсинг транзакций\parsing\data'
SMA_MIN = 10  

# MEMORY_LIMIT = '9GB'
WORKERS = 3
THREADS_PER_WORKER = 2

TEST = 1

# @profile
def main():
    client = Client(threads_per_worker=THREADS_PER_WORKER, n_workers=WORKERS, local_directory='I:/dask-temp')#, memory_limit=MEMORY_LIMIT
    print("Dashboard link:", client.dashboard_link) 

    intervals = pd.read_parquet(INTERVALS_DIR)
    print('2')
    buy_intervals = intervals.loc[intervals['Buy'] == 1]
    sell_intervals = intervals.loc[intervals['Sell'] == 1]
    print('3')
    time_intervals_to_buy = fu.find_timestamps(buy_intervals, SMA_MIN)
    time_intervals_to_sell = fu.find_timestamps(sell_intervals, SMA_MIN)
    print('4')
    txs = fu.read_tx_dfs(TXS_DIR, TEST)
    time_intervals_to_buy_df = fu.create_time_intervals_df(time_intervals_to_buy)
    time_intervals_to_sell_df = fu.create_time_intervals_df(time_intervals_to_sell)
    filtered_transactions = fu.preprocess_transactions(txs, time_intervals_to_buy_df)
    filtered_transactions = fu.preprocess_transactions(txs, time_intervals_to_sell_df)
    # Выполнение и получение результатов
    wallets = filtered_transactions['Wallet_id'].drop_duplicates().compute()
    print(wallets.head())
    final_result = txs[txs['Wallet_id'].isin(wallets)].compute()
    print(final_result.head())
    print('End')

    #  код до final_result результ рабочий но ооооочень долго выполняется. предполагаю что как минимум операции вытаскивания транзакций из общего дф не оптимизированы
if __name__ == '__main__':
    main()