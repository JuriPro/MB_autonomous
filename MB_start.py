import logging

import yaml
import time
import typing

from connectors.binance import BinanceClient

from strategies import MACD_RSI_Strategy
from strategies import CandlesStrategy
from strategies import TradeChaosStrategy

logger = logging.getLogger()
logger.setLevel(logging.INFO) # For Debug purpose, change to "logging.DEBUG"

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('info.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)


def read_config(filename: str) -> typing.Tuple[str, str]:

    strategies = list()
    with open(filename, "r") as stream:
        try:
            # test = list(yaml.safe_load_all(stream))
            for item in yaml.safe_load_all(stream):
                # print(item)
                if 'platform' in item.keys():
                    platform = (item['platform'])
                    logger.info('Current platform from config file is %s', item['platform'].upper())
                if 'status' in item.keys():
                    if item['status']:
                        logger.info('%s strategy will set for %s', item['strategy_name'], item['symbol'])
                        strategies.append(item)
        except yaml.YAMLError as exc:
            logger.error(exc)
        finally:
            return platform, strategies


def process_strategies(strategies: list, exchanges: typing.Dict[str, typing.Union[BinanceClient]]):

    strat_connectors = {
                        'Candles': CandlesStrategy,
                        'RSI+MACD': MACD_RSI_Strategy,
                        'TradeChaos_1': TradeChaosStrategy
                        }

    for index, strategy in enumerate(strategies):

        client = exchanges[strategy['exchange'].capitalize()]
        symbol = strategy['symbol']
        contract = client.contracts[symbol]
        exchange = strategy['exchange'].capitalize()
        timeframe = strategy['timeframe']

        balance_pct = float(strategy['balance_pct'])
        take_profit = float(strategy['take_profit_pct'])
        stop_loss = float(strategy['stop_loss_pct'])

        additional_parameters = strategy['parameters']

        new_strategy = strat_connectors[strategy['strategy_name']](client, contract, exchange, timeframe, balance_pct,
                                         take_profit, stop_loss, additional_parameters)

        # Collects historical data. It is just one API call so that is ok, but be careful not to call methods
        # that would lock the function for too long.
        # For example don't make a query to a database containing billions of rows, your program would freeze.
        new_strategy.candles = client.get_historical_candles(contract, timeframe)

        if len(new_strategy.candles) == 0:
            logger.warning(f"No historical data retrieved for {contract.symbol}")

        # Add strategy to Connector
        client.subscribe_channel([contract], "bookTicker")
        client.subscribe_channel([contract], "aggTrade")
        client.strategies[index] = new_strategy


if __name__ == '__main__':

    logger.info(' --- MB_AUTONOMOUS IS STARTED --- ')

    # SPOT KEYS
    s_api_binance_test = r''
    s_secret_binance_test = r''
    # FUTURES KEYS
    f_api_binance_test = r''
    f_secret_binance_test = r''

    platform, strategies = read_config("marketConfig.yaml")

    if platform == 'binance_spot':
        binance = BinanceClient(s_api_binance_test, s_secret_binance_test, testnet=True, futures=False)
    elif platform == 'binance_futures':
        binance = BinanceClient(f_api_binance_test, f_secret_binance_test, testnet=True, futures=True)

    exchanges = {"Binance": binance}
    process_strategies(strategies, exchanges)

    try:
      while True:
        print('Do something...')
        time.sleep(60)
    except KeyboardInterrupt:
        logger.info('Exit by KeyBoard Interrupt...')
    finally:
        binance.close_socket()

