import backtrader as bt
import pandas as pd
from datetime import datetime

class SmaCrossStrategy(bt.Strategy):
    # This is our winning strategy from the optimization
    params = (
        ('short_period', 25),
        ('long_period', 80),
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.sma_short = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.short_period)
        self.sma_long = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.long_period)
        self.crossover = bt.indicators.CrossOver(self.sma_short, self.sma_long)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f"BUY EXECUTED, Price: {order.executed.price:.2f}, Size: {order.executed.size}, Cost: {order.executed.value:.2f}")
            elif order.issell():
                self.log(f"SELL EXECUTED, Price: {order.executed.price:.2f}, Size: {order.executed.size}, Cost: {order.executed.value:.2f}")
        self.order = None

    def next(self):
        if self.order:
            return
        if not self.position:
            if self.crossover > 0:
                self.log(f'BUY CREATE, Price: {self.dataclose[0]:.2f}')
                self.order = self.buy(size=0.1)
        else:
            if self.crossover < 0:
                self.log(f'SELL CREATE, Price: {self.dataclose[0]:.2f}')
                self.order = self.sell(size=0.1)

# --- Main part of the script ---
if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.addstrategy(SmaCrossStrategy)

    try:
        # --- FINAL DATA LOADING METHOD ---
        # Define the column names in the order they appear in your file
        # We use the lowercase names that backtrader expects
        column_names = ['date', 'close', 'high', 'low', 'open', 'volume']
        
        # Load the CSV using our new, robust settings
        dataframe = pd.read_csv(
            'btc_data.csv',
            skiprows=3,        # Skip the top 3 junk rows
            header=None,       # Tell pandas there is no header row to read
            names=column_names # Provide our own list of column names
        )
        
        # Convert the 'date' column to a proper datetime format and set it as the index
        dataframe['date'] = pd.to_datetime(dataframe['date'])
        dataframe.set_index('date', inplace=True)
        
        # Create a backtrader data feed from our clean pandas DataFrame
        data = bt.feeds.PandasData(dataname=dataframe)
        
        cerebro.adddata(data)
        cerebro.broker.set_cash(10000.0)
        cerebro.broker.setcommission(commission=0.001)

        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
        cerebro.run()
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        
        cerebro.plot()

    except FileNotFoundError:
        print("Error: btc_data.csv not found. Please run get_data.py first.")
    except Exception as e:
        print(f"An error occurred: {e}")