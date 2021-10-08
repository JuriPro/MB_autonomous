import logging
from typing import *
import time

from threading import Timer
import pandas as pd
from models import *

if TYPE_CHECKING:  # Import the connector class names only for typing purpose (the classes aren't actually imported)
    from connectors.binance import BinanceClient

logger = logging.getLogger()

# TF_EQUIV is used in parse_trades() to compare the last candle timestamp to the new trade timestamp
TF_EQUIV = {"1m": 60, '3m': 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}


class Strategy:
    def __init__(self, client: Union["BinanceClient"], contract: Contract, exchange: str,
                 timeframe: str, balance_pct: float, take_profit: float, stop_loss: float, strat_name: str):

        self.client = client

        self.contract = contract
        self.exchange = exchange
        self.tf = timeframe
        self.tf_equiv = TF_EQUIV[timeframe] * 1000
        self.balance_pct = balance_pct
        self.take_profit = take_profit
        self.stop_loss = stop_loss

        self.strat_name = strat_name

        self.ongoing_position = False

        self.candles: List[Candle] = []
        self.trades: List[Trade] = []

    def _add_log(self, msg: str):
        logger.info("%s", msg)

    def parse_trades(self, price: float, size: float, timestamp: int) -> str:

        """
        Parse new trades coming in from the websocket and update the Candle list based on the timestamp.
        :param price: The trade price
        :param size: The trade size
        :param timestamp: Unix timestamp in milliseconds
        :return:
        """

        timestamp_diff = int(time.time() * 1000) - timestamp
        if timestamp_diff >= 2000:
            logger.warning("%s %s: %s milliseconds of difference between the current time and the trade time",
                           self.exchange, self.contract.symbol, timestamp_diff)

        last_candle = self.candles[-1]

        # Same Candle
        if timestamp < last_candle.timestamp + self.tf_equiv:

            last_candle.close = price
            last_candle.volume += size

            if price > last_candle.high:
                last_candle.high = price
            elif price < last_candle.low:
                last_candle.low = price

            # Check Take profit / Stop loss
            for trade in self.trades:
                if trade.status == "open" and trade.entry_price is not None:
                    self._check_tp_sl(trade)

            return "same_candle"

        # Missing Candle(s)
        elif timestamp >= last_candle.timestamp + 2 * self.tf_equiv:

            missing_candles = int((timestamp - last_candle.timestamp) / self.tf_equiv) - 1

            logger.info("%s missing %s candles for %s %s (%s %s)", self.exchange, missing_candles, self.contract.symbol,
                        self.tf, timestamp, last_candle.timestamp)

            for missing in range(missing_candles):
                new_ts = last_candle.timestamp + self.tf_equiv
                candle_info = {'ts': new_ts, 'open': last_candle.close, 'high': last_candle.close,
                               'low': last_candle.close, 'close': last_candle.close, 'volume': 0}
                new_candle = Candle(candle_info, self.tf, "parse_trade")

                self.candles.append(new_candle)

                last_candle = new_candle

            new_ts = last_candle.timestamp + self.tf_equiv
            candle_info = {'ts': new_ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': size}
            new_candle = Candle(candle_info, self.tf, "parse_trade")
            self.candles.append(new_candle)

            return "new_candle"

        # New Candle
        elif timestamp >= last_candle.timestamp + self.tf_equiv:
            new_ts = last_candle.timestamp + self.tf_equiv
            candle_info = {'ts': new_ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': size}
            new_candle = Candle(candle_info, self.tf, "parse_trade")

            self.candles.append(new_candle)

            logger.info("%s New candle for %s %s", self.exchange, self.contract.symbol, self.tf)

            return "new_candle"

    def _check_order_status(self, order_id):

        """
        Called regularly after an order has been placed, until it is filled.
        :param order_id: The order id to check.
        :return:
        """

        order_status = self.client.get_order_status(self.contract, order_id)

        if order_status is not None:

            logger.info("%s order status: %s", self.exchange, order_status.status)

            if order_status.status == "filled":
                for trade in self.trades:
                    if trade.entry_id == order_id:
                        trade.entry_price = order_status.avg_price
                        trade.quantity = order_status.executed_qty
                        break
                return

        t = Timer(2.0, lambda: self._check_order_status(order_id))
        t.start()

    def _open_position(self, signal_result: int):

        """
        Open Long or Short position based on the signal result.
        :param signal_result: 1 (Long) or -1 (Short)
        :return:
        """

        # Short is not allowed on Spot platforms
        if self.client.platform == "binance_spot" and signal_result == -1:
            return

        trade_size = self.client.get_trade_size(self.contract, self.candles[-1].close, self.balance_pct)
        if trade_size is None:
            return

        order_side = "buy" if signal_result == 1 else "sell"
        position_side = "long" if signal_result == 1 else "short"

        self._add_log(f"{position_side.capitalize()} signal on {self.contract.symbol} {self.tf}")

        order_status = self.client.place_order(self.contract, "MARKET", trade_size, order_side)

        if order_status is not None:
            self._add_log(f"{order_side.capitalize()} order placed on {self.exchange} | Status: {order_status.status}")

            self.ongoing_position = True

            avg_fill_price = None

            if order_status.status == "filled":
                avg_fill_price = order_status.avg_price
            else:
                t = Timer(2.0, lambda: self._check_order_status(order_status.order_id))
                t.start()

            new_trade = Trade({"time": int(time.time() * 1000), "entry_price": avg_fill_price,
                               "contract": self.contract, "strategy": self.strat_name, "side": position_side,
                               "status": "open", "pnl": 0, "quantity": order_status.executed_qty, "entry_id": order_status.order_id})
            self.trades.append(new_trade)

    def _check_tp_sl(self, trade: Trade):

        """
        Based on the average entry price, calculates whether the defined stop loss or take profit has been reached.
        :param trade:
        :return:
        """

        tp_triggered = False
        sl_triggered = False

        price = self.candles[-1].close

        if trade.side == "long":
            if self.stop_loss is not None:
                if price <= trade.entry_price * (1 - self.stop_loss / 100):
                    sl_triggered = True
            if self.take_profit is not None:
                if price >= trade.entry_price * (1 + self.take_profit / 100):
                    tp_triggered = True

        elif trade.side == "short":
            if self.stop_loss is not None:
                if price >= trade.entry_price * (1 + self.stop_loss / 100):
                    sl_triggered = True
            if self.take_profit is not None:
                if price <= trade.entry_price * (1 - self.take_profit / 100):
                    tp_triggered = True

        if tp_triggered or sl_triggered:

            self._add_log(f"{'Stop loss' if sl_triggered else 'Take profit'} for {self.contract.symbol} {self.tf} "
                          f"| Current Price = {price} (Entry price was {trade.entry_price})")

            order_side = "SELL" if trade.side == "long" else "BUY"

            if not self.client.futures:
                # Make sure we don't sell more than what's in the available balance on Binance Spot
                current_balances = self.client.get_balances()
                if current_balances is not None:
                    if order_side == "SELL" and self.contract.base_asset in current_balances:
                        trade.quantity = min(current_balances[self.contract.base_asset].free, trade.quantity)

            order_status = self.client.place_order(self.contract, "MARKET", trade.quantity, order_side)

            if order_status is not None:
                self._add_log(f"Exit order on {self.contract.symbol} {self.tf} placed successfully")
                trade.status = "closed"
                self.ongoing_position = False


class MACD_RSI_Strategy(Strategy):

    def __init__(self, client, contract: Contract, exchange: str, timeframe: str, balance_pct: float,
                 take_profit: float, stop_loss: float, other_params: Dict):
        super().__init__(client, contract, exchange, timeframe, balance_pct, take_profit, stop_loss, "RSI+MACD")

        self._ema_fast = other_params['ema_fast']
        self._ema_slow = other_params['ema_slow']
        self._ema_signal = other_params['ema_signal']

        self._rsi_length = other_params['rsi_length']

    def _rsi(self) -> float:

        """
        Compute the Relative Strength Index.
        :return: The RSI value of the previous candlestick
        """

        close_list = []
        for candle in self.candles:
            close_list.append(candle.close)

        closes = pd.Series(close_list)

        # Calculate the different between the value of one row and the value of the row before
        delta = closes.diff().dropna()

        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0  # Keep only the negative change, others are set to 0

        avg_gain = up.ewm(com=(self._rsi_length - 1), min_periods=self._rsi_length).mean()
        avg_loss = down.abs().ewm(com=(self._rsi_length - 1), min_periods=self._rsi_length).mean()

        rs = avg_gain / avg_loss  # Relative Strength

        rsi = 100 - 100 / (1 + rs)
        rsi = rsi.round(2)

        return rsi.iloc[-2]

    def _macd(self) -> Tuple[float, float]:

        """
        Compute the MACD and its Signal line.
        :return: The MACD and the MACD Signal value of the previous candlestick
        """

        close_list = []
        for candle in self.candles:
            close_list.append(candle.close)  # Use only the close price of each candlestick for the calculations

        closes = pd.Series(close_list)  # Converts the close prices list to a pandas Series.

        ema_fast = closes.ewm(span=self._ema_fast).mean()  # Exponential Moving Average method
        ema_slow = closes.ewm(span=self._ema_slow).mean()

        macd_line = ema_fast - ema_slow
        macd_signal = macd_line.ewm(span=self._ema_signal).mean()

        return macd_line.iloc[-2], macd_signal.iloc[-2]

    def _check_signal(self):

        """
        Compute technical indicators and compare their value to some predefined levels to know whether to go Long,
        Short, or do nothing.
        :return: 1 for a Long signal, -1 for a Short signal, 0 for no signal
        """

        macd_line, macd_signal = self._macd()
        rsi = self._rsi()

        if rsi < 30 and macd_line > macd_signal:
            return 1
        elif rsi > 70 and macd_line < macd_signal:
            return -1
        else:
            return 0

    def check_trade(self, tick_type: str):

        """
        To be triggered from the websocket _on_message() methods. Triggered only once per candlestick to avoid
        constantly calculating the indicators. A trade can occur only if the is no open position at the moment.
        :param tick_type: same_candle or new_candle
        :return:
        """

        if tick_type == "new_candle" and not self.ongoing_position:
            signal_result = self._check_signal()

            if signal_result in [1, -1]:
                self._open_position(signal_result)


class CandlesStrategy(Strategy):

    def __init__(self, client, contract: Contract, exchange: str, timeframe: str, balance_pct: float,
                 take_profit: float, stop_loss: float, other_params: Dict):
        super().__init__(client, contract, exchange, timeframe, balance_pct, take_profit, stop_loss, "CandlesComparison")

        self._min_volume = other_params['min_volume']

    def _check_signal(self) -> int:

        """
        Use candlesticks OHLC data to define Long or Short patterns.
        :return: 1 for a Long signal, -1 for a Short signal, 0 for no signal
        """

        if self.candles[-1].close > self.candles[-2].high and self.candles[-1].volume > self._min_volume:
            return 1
        elif self.candles[-1].close < self.candles[-2].low and self.candles[-1].volume > self._min_volume:
            return -1
        else:
            return 0

    def check_trade(self, tick_type: str):

        """
        To be triggered from the websocket _on_message() methods
        :param tick_type: same_candle or new_candle
        :return:
        """

        if not self.ongoing_position:
            signal_result = self._check_signal()

            if signal_result in [1, -1]:
                self._open_position(signal_result)


class TradeChaosStrategy(Strategy):
    def __init__(self, client, contract: Contract, exchange: str, timeframe: str, balance_pct: float,
                 take_profit: float, stop_loss: float, other_params: Dict):
        super().__init__(client, contract, exchange, timeframe, balance_pct, take_profit, stop_loss, "TradeChaos_1")

        self._sma_fast = other_params['sma_fast']
        self._sma_slow = other_params['sma_slow']

        self.prev_tick_volume = 0.0
        self.prev_candle_mfi = 0.0

        self.prev_mfi = 0.0

        self.squat_bar_detected = False
        self.squat_bars_counter = 0

    # In this strategy Volume becomes TickVolume in the Candle
    def parse_trades(self, price: float, size: float, timestamp: int) -> str:

        """
        Parse new trades coming in from the websocket and update the Candle list based on the timestamp.
        :param price: The trade price
        :param size: The trade size
        :param timestamp: Unix timestamp in milliseconds
        :return:
        """

        timestamp_diff = int(time.time() * 1000) - timestamp
        if timestamp_diff >= 2000:
            logger.warning("%s %s: %s milliseconds of difference between the current time and the trade time",
                           self.exchange, self.contract.symbol, timestamp_diff)

        last_candle = self.candles[-1]

        # Same Candle
        if timestamp < last_candle.timestamp + self.tf_equiv:

            last_candle.close = price
            if self.prev_tick_volume != last_candle.volume:
                last_candle.volume += 1.0
                self.prev_tick_volume = last_candle.volume

            if price > last_candle.high:
                last_candle.high = price
            elif price < last_candle.low:
                last_candle.low = price

            # Check Take profit / Stop loss
            for trade in self.trades:
                if trade.status == "open" and trade.entry_price is not None:
                    self._check_tp_sl(trade)

            return "same_candle"

        # Missing Candle(s)
        elif timestamp >= last_candle.timestamp + 2 * self.tf_equiv:

            missing_candles = int((timestamp - last_candle.timestamp) / self.tf_equiv) - 1

            logger.info("%s missing %s candles for %s %s (%s %s)", self.exchange, missing_candles, self.contract.symbol,
                        self.tf, timestamp, last_candle.timestamp)

            for missing in range(missing_candles):
                new_ts = last_candle.timestamp + self.tf_equiv
                candle_info = {'ts': new_ts, 'open': last_candle.close, 'high': last_candle.close,
                               'low': last_candle.close, 'close': last_candle.close, 'volume': 0}
                new_candle = Candle(candle_info, self.tf, "parse_trade")

                self.candles.append(new_candle)

                last_candle = new_candle

            self.prev_tick_volume = 0.0

            new_ts = last_candle.timestamp + self.tf_equiv
            candle_info = {'ts': new_ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1}
            new_candle = Candle(candle_info, self.tf, "parse_trade")
            self.candles.append(new_candle)

            return "new_candle"

        # New Candle
        elif timestamp >= last_candle.timestamp + self.tf_equiv:
            new_ts = last_candle.timestamp + self.tf_equiv
            candle_info = {'ts': new_ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1}
            new_candle = Candle(candle_info, self.tf, "parse_trade")

            self.candles.append(new_candle)
            self.prev_tick_volume = 0.0

            logger.info("%s New candle for %s %s", self.exchange, self.contract.symbol, self.tf)

            return "new_candle"

    def _sma(self) -> Tuple[float, float]:

        close_list = []
        for candle in self.candles:
            close_list.append(candle.close)  # Use only the close price of each candlestick for the calculations

        closes = pd.Series(close_list)  # Converts the close prices list to a pandas Series.

        sma_fast = closes.rolling(self._sma_fast).mean()  # Moving Average method
        sma_slow = closes.rolling(self._sma_slow).mean()  # Moving Average method

        return sma_fast.iloc[-2], sma_slow.iloc[-2]

    def _get_trend_direction(self) -> str:

        # Detect dirty trend direction
        avg_price = (self.candles[-2].high - self.candles[-2].low) / 2

        if self.candles[-3].high < avg_price:
            quick_dirty_trend = '+'
        elif self.candles[-3].low > avg_price:
            quick_dirty_trend = '-'
        else:
            quick_dirty_trend = '='

        return quick_dirty_trend

    def _get_o_c_type_of_bar(self) -> Tuple[float, float]:

        # Define type of Bar for Open and Close price
        interval = (self.candles[-2].high - self.candles[-2].low) / 3

        if self.candles[-2].open > (self.candles[-2].high - interval):
            open_in_interval = '1'
        elif self.candles[-2].open < (self.candles[-2].low + interval):
            open_in_interval = '3'
        else:
            open_in_interval = '2'

        if self.candles[-2].close > (self.candles[-2].high - interval):
            close_in_interval = '1'
        elif self.candles[-2].close < (self.candles[-2].low + interval):
            close_in_interval = '3'
        else:
            close_in_interval = '2'

        return open_in_interval, close_in_interval

    def _get_tick_volume_trend(self) -> str:

        # Define TickVolume trend
        if self.candles[-2].volume > 1.1*self.candles[-3].volume:
            tick_volume = '+'
        elif self.candles[-2].volume < self.candles[-3].volume:
            tick_volume = '-'
        else:
            tick_volume = '='

        return tick_volume

    def _get_mfi_trend(self) -> Tuple[str, float]:

        # Define MFI index
        current_mfi = (self.candles[-2].high - self.candles[-2].low) / self.candles[-2].volume

        if current_mfi > self.prev_mfi:
            mfi_trend = '+'
        elif current_mfi < self.prev_mfi:
            mfi_trend = '-'
        else:
            mfi_trend = '='

        return mfi_trend, current_mfi

    def _check_signal(self) -> int:

        """
        Use candlesticks OHLC data to define Long or Short patterns.
        :return: 1 for a Long signal, -1 for a Short signal, 0 for no signal
        """

        trend_direction = self._get_trend_direction()  # {+,-,=}
        open_in_interval, close_in_interval = self._get_o_c_type_of_bar()  # [{1,2,3}, {1,2,3}]
        tick_volume_trend = self._get_tick_volume_trend()  # [+,-,=]
        mfi_trend, current_mfi = self._get_mfi_trend()  # {+,-,=}

        self.prev_mfi = current_mfi

        if self.squat_bars_counter > 0:
            self.squat_bars_counter -= 1

        if trend_direction == '-' and \
           open_in_interval == '1' and \
           close_in_interval == '3' and \
           tick_volume_trend == '+' and \
           mfi_trend == '-':
            self.squat_bars_counter = 5  # Add 5th Bars to Counter

        sma_fast, sma_slow = self._sma()

        if self.squat_bars_counter > 0 and self.squat_bars_counter <=5 and \
                sma_fast > sma_slow:  # Up Trend
            return 1
        elif sma_fast < sma_slow:  # Down Trend
            return -1
        else:
            return 0

    def check_trade(self, tick_type: str):

        """
        To be triggered from the websocket _on_message() methods
        :param tick_type: same_candle or new_candle
        :return:
        """
        if tick_type == "new_candle" and not self.ongoing_position:
            signal_result = self._check_signal()

            if signal_result in [1, -1]:
                self._open_position(signal_result)
