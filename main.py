# This is a sample Python script.
from bitcoin_util.bit_manager import BitManager


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    bitManager = BitManager()
    maketInfo = bitManager.getMarket(True)
    for market in maketInfo:
        print(market)

    # tickers = bitManager.getTickers(fiat="KRW")
    # for ticker in tickers:
    #     print(ticker)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/