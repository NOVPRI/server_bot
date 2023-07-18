import numpy
import pandas
import pandas as pd  # библиотеки
import schedule as sl
from openpyxl import load_workbook
from google.protobuf.timestamp_pb2 import Timestamp

import time  # стандартные
import math
from datetime import datetime, timedelta

from proto.grpcConnection import conn  # модули
from proto import marketdata_pb2, orders_pb2, operations_pb2, stoporders_pb2, common_pb2, instruments_pb2
from service import sub
from analyst import indicators, create_pivot
from service.tgSend import message_to as msg
import strategy as st
import config


def pick_candles(x, per='1_MIN', j=0):
    try:
        while True:
            t = (datetime.now() - timedelta(days=j + 1)).timestamp()
            seconds1 = int(t)
            nanos1 = int(t % 1 * 1e9)
            t2 = (datetime.now() - timedelta(days=j)).timestamp()
            seconds2 = int(t2)
            nanos2 = int(t2 % 1 * 1e9)
            start_time = Timestamp(seconds=seconds1, nanos=nanos1)
            end_time = Timestamp(seconds=seconds2, nanos=nanos2)
            kwargs = {
                'figi': x.figi,
                'from': start_time,
                'to': end_time,
                'interval': 'CANDLE_INTERVAL_' + per
            }
            historical_candles = user.market().GetCandles(marketdata_pb2.GetCandlesRequest(**kwargs), metadata=user.token)
            if not historical_candles.candles:
                j += 1
            else:
                return historical_candles.candles
    except Exception as e:
        msg(f"Не удалось взять исторические свечи {x['name']}")
        print(f"Не удалось взять исторические свечи {x['name']}, ОШИБКА:", str(e))


def trading_status(figi):
    try:
        ts = user.market().GetTradingStatus(marketdata_pb2.GetTradingStatusRequest(figi=figi), metadata=user.token)
        return ts.market_order_available_flag
    except Exception as e:
        msg("Не удалось получить информацию о работе биржи")
        print(f"Не удалось получить информацию о работе биржи, ОШИБКА:", str(e))


def new_df(candles, x):
    try:
        if not candles:
            return print(f"Пришел пустой набор исторических свеч {x['name']}")
        pd.set_option('display.max_rows', None)
        df = pd.DataFrame([{
            'time': sub.no_timestamp(c.time.seconds),
            'volume': c.volume,
            'open': sub.price(c.open),
            'close': sub.price(c.close),
            'high': sub.price(c.high),
            'low': sub.price(c.low),
            'finish': c.is_complete
        } for c in candles])


        # подключение индикаторов
        indicators(df, x)

        # подключение стратегий
        st.door(x, df)
        st.status(x, df)
        st.fix(x, df)

        # отслеживание сделок
        mass = []
        for item in operation(x.figi).operations:
            if item.type != 'Удержание комиссии за операцию':
                date = sub.no_timestamp(item.date.seconds)
                mass.append(date)
        df['🌟'] = df.apply(lambda row: sub.check_match(row, mass), axis=1)

        # сюда можно эксель

        # ----

        print(df[config.df].tail(config.tail))
        return df
    except Exception as e:
        msg(f"Не удалось построить дата фрейм {x['name']}")
        print("Не удалось построить дата фрейм, ОШИБКА:", str(e))


def get_portfolio(figi):  # переделать однажды
    try:
        info = user.operation().GetPortfolio(operations_pb2.PortfolioRequest(account_id=user.account), metadata=user.token)
        found = False
        for item in info.positions:
            if item.figi == figi:
                found = True
                if item.quantity.units < 0:
                    return "inShort"
                if item.quantity.units > 0:
                    return "inLong"
        if not found:
            return "void"
    except Exception as e:
        msg(f"Не удалось получить информацию о портфеле")
        print("Не удалось получить информацию о портфеле, ОШИБКА:", str(e))


def figi_info(figi):
    try:
        info = user.instruments().GetInstrumentBy(instruments_pb2.InstrumentRequest(id_type=1, id=figi), metadata=user.token)
        return sub.price(info.instrument.min_price_increment)
    except Exception as e:
        print("Не удалось получить информацию о фиге, ОШИБКА:", str(e))


def money_info():
    try:
        money = user.operation().GetWithdrawLimits(operations_pb2.WithdrawLimitsRequest(account_id=user.account), metadata=user.token)
        balance = ""
        for item in money.money:
            balance += f"{item.currency} = {sub.price(item)} | "
        return f'Баланс: {balance.rstrip("| ")}'
    except Exception as e:
        print("Не удалось узнать баланс, ОШИБКА:", str(e))


def operation(figi):
    t = (datetime.now() - timedelta(days=1)).timestamp()
    seconds1 = int(t)
    nanos1 = int(t % 1 * 1e9)
    t2 = (datetime.now() - timedelta(days=0)).timestamp()
    seconds2 = int(t2)
    nanos2 = int(t2 % 1 * 1e9)
    start_time = Timestamp(seconds=seconds1, nanos=nanos1)
    end_time = Timestamp(seconds=seconds2, nanos=nanos2)
    kwargs = {
        'account_id': user.account,
        'state': 1,
        'from': start_time,
        'to': end_time,
        'figi': figi
    }
    info = user.operation().GetOperations(operations_pb2.OperationsRequest(**kwargs), metadata=user.token)
    return info


def make_deal(status, x):
    if not x.shortly and status == 'SELL':
        return get_portfolio(x.figi)
    if status == 'BUY':
        return go_trade(x, 'BUY', 'покупка')
    if status == 'SELL':
        return go_trade(x, 'SELL', 'продажа')


def make_fix(x, portfolio):
    if portfolio == "inShort":
        return go_trade(x, 'BUY', 'фиксация шорта')
    if portfolio == "inLong":
        return go_trade(x, 'SELL', 'фиксация лонга')


def stop_exit(x, stop_id):
    try:
        stop = user.stop_order()
        info = stop.GetStopOrders(stoporders_pb2.GetStopOrdersRequest(account_id=user.account), metadata=user.token)
        for item in info.stop_orders:
            if item.figi == x.figi and item.stop_order_id != stop_id.stop_order_id:
                stop.CancelStopOrder(stoporders_pb2.CancelStopOrderRequest(account_id=user.account,
                                                                           stop_order_id=item.stop_order_id), metadata=user.token)
                print("заявки обновлены")
    except Exception as e:
        raise Exception(f"Ошибка при отмены стоп-заявки: {e}")


def stop_order(df, x, portfolio):
    try:
        if portfolio == 'inLong':
            direction = 2
            if pandas.isnull(df.iloc[-1].stop_long):
                print('стоп заглушка для лонга')
                price = round(df.iloc[-1].close*0.997, x.rund)
            else:
                price = df.iloc[-1].stop_long
        else:
            direction = 1
            if pandas.isnull(df.iloc[-1].stop_short):
                print('стоп заглушка для шорта')
                price = round(df.iloc[-1].close*1.003, x.rund)
            else:
                price = df.iloc[-1].stop_short

        pr = {
            'units': sub.nano_price(price, 'u'),
            'nano': sub.nano_price(price, 'n'),
        }
        kwargs = {
            'quantity': x['count'],
            'account_id': user.account,
            'stop_price': common_pb2.Quotation(**pr),
            'figi': x.figi,
            'stop_order_type': 2,
            'expiration_type': 1,
            'direction': direction,
        }
        print('установлен стоп')
        return user.stop_order().PostStopOrder(stoporders_pb2.PostStopOrderRequest(**kwargs), metadata=user.token)
    except Exception as e:
        msg(f"Ошибка выставления стоп заявки {x['name']}")
        print("Ошибка выставления стоп заявки, ОШИБКА:", str(e))


def go_trade(x, direction, deal):
    try:
        kwargs = {
            'figi': x.figi,
            'quantity': x['count'],
            'direction': f"ORDER_DIRECTION_{direction}",
            'account_id': user.account,
            'order_type': 'ORDER_TYPE_MARKET',
            'order_id': str(datetime.now().timestamp())
        }
        res = user.order().PostOrder(orders_pb2.PostOrderRequest(**kwargs), metadata=user.token)
        if res.execution_report_status == 5:
            msg(f"заявка исполнена только частично ({x['name']}), исполнено {res.lots_executed} из {res.lots_requested}")
            return print(f"заявка исполнена только частично, исполнено {res.lots_executed} из {res.lots_requested}")
        if res.execution_report_status != 1:
            msg(f"не удалось исполнить заявку {direction} ({x['name']})")
            return print(f"не удалось исполнить заявку {direction}")
        money = sub.price(res.total_order_amount)
        commission = sub.price(res.executed_commission) if not config.sandboxMode else 'недоступна в песочнице'
        msg(f"{deal.upper()} {x['name'].upper()}, {res.lots_executed} ШТ. \nЦена с учетом комиссии {money} \nСумма комиссии {commission}"
            f"\n{money_info()}")
        print(f"{deal.upper()} {x['name'].upper()} в размере {res.lots_executed} шт. Цена с учетом комиссии {money}, сумма комиссии {commission}")
        return get_portfolio(x.figi)
    except Exception as e:
        msg(f"Не удалось совершить сделку {x['name']}")
        print("Не удалось совершить сделку, ОШИБКА:", str(e))


def get_pivot_and_step(x, close, high, low, step):
    workbook = load_workbook('service/figi.xlsx')
    sheet = workbook.active
    sheet['B' + str(x.name + 3)].value = step
    sheet['C' + str(x.name + 3)].value = sub.rounding(step)
    pivot = create_pivot(high, low, close)
    sheet['D' + str(x.name + 3)].value = pivot.p
    sheet['E' + str(x.name + 3)].value = pivot.s1
    sheet['F' + str(x.name + 3)].value = pivot.r1
    sheet['G' + str(x.name + 3)].value = pivot.s2
    sheet['H' + str(x.name + 3)].value = pivot.r2
    sheet['I' + str(x.name + 3)].value = pivot.s3
    sheet['J' + str(x.name + 3)].value = pivot.r3
    sheet['K' + str(x.name + 3)].value = pivot.s4
    sheet['L' + str(x.name + 3)].value = pivot.r4
    sheet['M' + str(x.name + 3)].value = pivot.s5
    sheet['N' + str(x.name + 3)].value = pivot.r5
    workbook.save('service/figi.xlsx')


def preparation():
    excel = sub.load_excel()
    for index, row in excel.iterrows():
        pivot = pick_candles(row, 'DAY')[0]
        get_pivot_and_step(row, sub.price(pivot.close), sub.price(pivot.high), sub.price(pivot.low), figi_info(row.figi))
    return pd.read_excel('service/figi.xlsx', skiprows=1)


def bot(x):
    if trading_status(x.figi) is not True:
        return print(f"Торги закрыты")
    if not x.startT <= datetime.now().time() <= x.endT:
        return print(f"Торги открыты, но пользовательское время вне диапазона")
    df = new_df(pick_candles(x), x)
    portfolio = get_portfolio(x.figi)
    # фиксация
    # if portfolio != 'void' and df.iloc[-1].fix == 'FIX':
    #     print('зашли в фикс')
    #     portfolio = make_fix(x, portfolio)
    # сделки
    # if portfolio == "void" and not pandas.isnull(df.iloc[-1].status):
    #     portfolio = make_deal(df.iloc[-1].status, x)
    # # стоп
    # if portfolio != 'void':
    #     stop_exit(x, stop_order(df, x, portfolio))


def start_bot():
    for index, row in instruments.iterrows():
        print(f"\n――― {row.step} ―――――――――― {row['name'].upper()} ―{' 🚫 ' if not row.shortly else ''}――――――――――――――――――")
        bot(row)
        print(f"――――――――――――――――――――――――――――――――― {get_portfolio(row.figi)} ――――")
    print(f"\n■■■ {money_info()} ■■■■■■■■■■■ свечи {(datetime.now().minute - 1)} минуты ■■■■■■■■■■■\n")
    time.sleep(45)


def loop_bot():
    sl.every().minute.at(":50").do(start_bot)
    while True:
        sl.run_pending()


if __name__ == "__main__":
    user = conn()
    instruments = None
    if not instruments:
        instruments = preparation()
    choice = input("'start' чтобы запустить бота, или нажать 'enter' для теста: ")
    start_bot() if choice == '' else (loop_bot() if choice == 'start' else print('неверный ввод'))
    # loop_bot()
