import math
import numpy as num
from ta.momentum import KAMAIndicator
from ta.momentum import ROCIndicator
from ta.momentum import PercentageVolumeOscillator
from ta.momentum import tsi
from ta.trend import CCIIndicator
from ta.trend import aroon_up
from ta.trend import aroon_down
from ta.trend import ema_indicator
from ta.volume import volume_weighted_average_price
from ta.volume import on_balance_volume
from ta.volatility import average_true_range
from ta.volatility import DonchianChannel
from ta.volatility import BollingerBands
from ta.volume import AccDistIndexIndicator
from ta.volume import VolumePriceTrendIndicator
from ta.trend import MACD
from ta.volume import ChaikinMoneyFlowIndicator
from ta.volume import ForceIndexIndicator
from ta.volume import MFIIndicator
import pandas as pd


def create_pivot(highD, lowD, closeD):
    return Pivot(highD, lowD, closeD)


def indicators(df, x):  # подключить нужное
    tema(df)
    vol_density(df)
    acum(df, x)
    donchian(df)
    candle(df)
    HAiken(df)
    arunosc(df)
    cci_ind(df, x)
    kauf(df, x)
    macd(df)
    rox(df, x)
    intensiv(df, x)
    EMA(df)
    priceDensity(df, x)
    angeCalc(df)
    pick_bollinger(df, 0.38, '1')
    pick_bollinger(df, 1, '2')
    pick_bollinger(df, 2, '3')
    tsindex(df)
    supertrend(df, x)


def supertrend(df, x):
    # calculate ATR
    price_diffs = [df['high'] - df['low'], df['high'] - df['close'].shift(), df['close'].shift() - df['low']]
    true_range = pd.concat(price_diffs, axis=1).abs().max(axis=1)
    # default ATR calculation in supertrend indicator
    atr = true_range.ewm(alpha=1 / x.atr_period, min_periods=x.atr_period).mean()
    # HL2 is simply the average of high and low prices
    hl2 = (df['high'] + df['low']) / 2
    # upperband and lowerband calculation
    final_upperband = hl2 + (x.multiplier * atr)
    final_lowerband = hl2 - (x.multiplier * atr)

    # initialize Supertrend column to True
    supertrend = [True] * len(df)

    for i in range(1, len(df.index)):
        curr, prev = i, i - 1
        # if current close price crosses above upperband
        if df['close'][curr] > final_upperband[prev]:
            supertrend[curr] = True
        # if current close price crosses below lowerband
        elif df['close'][curr] < final_lowerband[prev]:
            supertrend[curr] = False
        # else, the trend continues
        else:
            supertrend[curr] = supertrend[prev]
            # adjustment to the final bands
            if supertrend[curr] is True and final_lowerband[curr] < final_lowerband[prev]:
                final_lowerband[curr] = final_lowerband[prev]
            if supertrend[curr] is False and final_upperband[curr] > final_upperband[prev]:
                final_upperband[curr] = final_upperband[prev]
        # to remove bands according to the trend direction
        if supertrend[curr] is True:
            final_upperband[curr] = num.nan
        else:
            final_lowerband[curr] = num.nan

        df['st'] = supertrend
        df['stop_short'] = round(final_upperband, x.rund)
        df['stop_long'] = round(final_lowerband, x.rund)


def tema(df):
    df['Tema13'] = ema_indicator(close=df['close'], window=3)
    df['Tema23'] = ema_indicator(close=df['Tema13'], window=3)
    df['Tema33'] = ema_indicator(close=df['Tema23'], window=3)
    df['temaF3'] = 3 * (df['Tema13'] - df['Tema23']) + df['Tema33']
    df['tema'] = ema_indicator(close=ema_indicator(close=df['temaF3'], window=3), window=3)


def tsindex(df):
    df['true_index'] = tsi(close=df['close'], window_fast=5, window_slow=100)




def pick_bollinger(df, wd, index):
    bollinger = BollingerBands(window=200, window_dev=wd, close=df['close'])
    df[f'BB_MA_{index}'] = bollinger.bollinger_mavg()
    df[f'BB_HB_{index}'] = bollinger.bollinger_hband()
    df[f'BB_LB_{index}'] = bollinger.bollinger_lband()
    df[f'BB_WP_{index}'] = bollinger.bollinger_wband()
    df.loc[(df[f'BB_WP_{index}'] > df[f'BB_WP_{index}'].shift()), f'vector_wbb_{index}'] = 'UP'





def angeCalc(df, range=3, minangle=1):
    angle_ema_per = 50
    df['angleLine'] = ema_indicator(close=df['close'], window=angle_ema_per)
    df['angle'] = abs(num.rad2deg(num.arctan2(df['angleLine'] - df['angleLine'].shift(range), range)))
    df.loc[(df['angle'] > minangle), 'angleImpulse'] = 'impulse'


def kauf(df, x):
    sma = df['close'].rolling(x.per_kama).mean()
    wma = df['close'].rolling(x.per_kama).apply(
        lambda z: num.sum(num.abs(z - num.mean(z)) * num.arange(1, len(z) + 1)) / num.sum(num.arange(1, len(z) + 1)))
    kama = sma + (wma * 2 / (2 + 1) - wma * 2 / (30 + 1))
    df['kama_line'] = kama
    df.loc[(df['kama_line'] > df['kama_line'].shift()), 'kama_vector'] = 'long'
    df.loc[(df['kama_line'] < df['kama_line'].shift()), 'kama_vector'] = 'short'
    df['change_kama'] = abs(((df['kama_line'] - df['kama_line'].shift(x.offset_kama)) / df['kama_line'].shift(x.offset_kama)) * 100)
    df.loc[(df['change_kama'] > x.kama_range), 'kama_speed'] = 'impulse'
    df.loc[(df['change_kama'] < x.kama_range), 'kama_speed'] = 'flat'


# 'acdist_vector'
def acum(df, x):
    acumdist = AccDistIndexIndicator(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'])
    df['acdist'] = acumdist.acc_dist_index()
    df['smoothed_acdist'] = round(ema_indicator(ema_indicator(close=df['acdist'], window=x.smooacum1), window=x.smooacum2), 2)
    df.loc[(df['smoothed_acdist'] < df['smoothed_acdist'].shift()), 'acdist_vector'] = 'distribution'
    df.loc[(df['smoothed_acdist'] > df['smoothed_acdist'].shift()), 'acdist_vector'] = 'accumulation'


def vol_density(df):
    per_vol1 = 3
    per_vol2 = 3
    per_vol3 = 3
    sm_ch1 = 1
    sm_ch2 = 1
    hold = 0.05
    df['vol_dens1'] = volume_weighted_average_price( high= df['high'], low= df['low'], close= df['close'], volume= df['volume'], window= per_vol1)
    df['vol_dens'] = ema_indicator(ema_indicator(close=df['vol_dens1'], window= per_vol2), window= per_vol3)
    df['change_vol_den1'] = abs(((df['vol_dens'] - df['vol_dens'].shift())/df['vol_dens'].shift())*100)
    df['change_vol_den'] = ema_indicator(ema_indicator( close= df['change_vol_den1'], window=sm_ch1),window= sm_ch2)
    df.loc[(df['change_vol_den'] > df['change_vol_den'].shift()), 'change_vol'] = 'up'
    df.loc[(df['change_vol_den'] < df['change_vol_den'].shift()), 'change_vol'] = 'down'
    df.loc[(df['change_vol'] == 'up') & (df['change_vol_den'] > hold), 'vector_change_vol_den'] = 'strong_volume'
    df.loc[(df['change_vol'] == 'down') & (df['change_vol_den'] > hold), 'vector_change_vol_den'] = 'Pivot'
    df.loc[(df['vol_dens'] > df['vol_dens'].shift()), 'vector_vol_dens'] = 'volume_impulse'
    df.loc[(df['vol_dens'] < df['vol_dens'].shift()), 'vector_vol_dens'] = 'volume_flat'
    df.loc[(df['vector_vol_dens'] == 'volume_flat') & (df['vector_vol_dens'].shift() == 'volume_impulse'), 'vector_volume'] = 'exit'
    df.loc[(df['vector_vol_dens'] == 'volume_impulse') & (df['vector_vol_dens'].shift() == 'volume_flat'), 'vector_volume'] = 'enter'
    #  'vector_change_vol_den', 'vector_vol_dens', 'vpt_vector', 'pvo_vector',


def donchian(df): #per_don
    per_don_emka = 20
    per_don_emka2 = 5
    don = DonchianChannel(high=df['high'], low=df['low'], close=df['close'], window=20, offset=0)
    df['don_up'] = don.donchian_channel_hband()
    df['don_down'] = don.donchian_channel_lband()
    df['don_average'] = don.donchian_channel_mband()
    df['don_width'] = don.donchian_channel_wband()
    df['don_avs'] = ema_indicator(close=df['don_average'], window=5)
    df['emka'] = ema_indicator(ema_indicator(close= df['close'], window= per_don_emka), window= per_don_emka2)
    df.loc[(df['don_down'] == df['don_down'].shift()),  'don_level'] = 'support'
    df.loc[(df['don_up'] == df['don_up'].shift()), 'don_level'] = 'resistance'
    df['don_dist'] = abs(df['don_average'] - df['emka'])
    # расстояние до средней линии должно сокращаться  а также должно быть либо сопротивление либо поддержка
    # df.loc[(df['don_average'] > df['don_average'].shift()) , 'don_vector'] = 'positive'
    # df.loc[(df['don_average'] < df['don_average'].shift()) , 'don_vector'] = 'negative'
    # df.loc[(df['don_average'] == df['don_average'].shift()), 'don_vector'] = 'sleep'
    df.loc[(df['don_down'] == 'support') & (df['emka'] > df['emka'].shift()), 'don_vector'] = 'positive'
    df.loc[(df['don_up'] == 'resistance') & (df['emka'] < df['emka'].shift()), 'don_vector'] = 'negative'
    # 'don_vector' 'don_dist'


def candle(df):
    cdl = (df['high'] + df['low']) / 1.99
    df.loc[(df['close'] > cdl), 'candles'] = 'bull_hammer'  # тоже что пин-бар ( Пинокио) закрытие выше серердины спреда свечи
    df.loc[(df['close'] < cdl), 'candles'] = 'bear_hammer'  # тоже что пин-бар ( Пинокио) закрытие выше серердины спреда свечи
    df.loc[(df['open'] > df['close'].shift()) & (df['close'] > df['open'].shift()), 'candles'] = 'bear_absorbing'  # поглощение
    df.loc[(df['open'] < df['close'].shift()) & (df['close'] < df['open'].shift()), 'candles'] = 'bull_absorbing'  # поглощение
    df.loc[(df['open'] == df['close'].shift()) & (df['close'] == df['open'].shift()), 'candles'] = 'rails'


def HAiken(df):
    df['HA_Close'] = round(((df['open'] + df['high'] + df['low'] + df['close']) / 4), 3)
    ha_close_values = df['HA_Close'].values
    length = len(df)
    ha_open = num.zeros(length, dtype=float)
    ha_open[0] = (df['open'][0] + df['close'][0]) / 2
    for i in range(0, length - 1):
        ha_open[i + 1] = round(((ha_open[i] + ha_close_values[i]) / 2), 3)
    df['HA_Open'] = ha_open
    df['HA_High'] = round(df[['HA_Open', 'HA_Close', 'high']].max(axis=1), 3)
    df['HA_Low'] = round(df[['HA_Open', 'HA_Close', 'low']].min(axis=1), 3)
    df['HA_EMA'] = df['HA_Close']
    df.loc[(df['HA_Open'] > df['HA_Close']), 'HA_vector'] = 'down'
    df.loc[(df['HA_Open'] < df['HA_Close']), 'HA_vector'] = 'up'


def arunosc(df):
    sm_arun_osc = 5
    length = 10
    high = df['high']  # предполагаем, что данные хранятся в DataFrame df
    low = df['low']
    highestbars = high.rolling(window=length+1).max().shift(1)  # сдвигаем на 1 бар назад
    upper = 100 * (highestbars + length) / length
    lowestbars = low.rolling(window=length+1).min().shift(1)
    lower = 100 * (lowestbars + length) / length
    df['osc_arun'] = upper - lower
    df['arun_osc'] = ema_indicator(ema_indicator(ema_indicator(close=df['osc_arun'], window= sm_arun_osc),window= sm_arun_osc), window= sm_arun_osc)
    df.loc[(df['arun_osc'] > df['arun_osc'].shift()), 'arun_vector'] = 'up'
    df.loc[(df['arun_osc'] < df['arun_osc'].shift()), 'arun_vector'] = 'down'



def cci_ind(df, x):
    cci_indicator = CCIIndicator(high=df['high'], low=df['low'], close=df['close'], window=x.per_cci, constant=0.015)
    df['cci'] = cci_indicator.cci()
    df['sm_cci'] = ema_indicator(ema_indicator(close=df['cci'], window=x.sm_cci), window=x.sm_cci)
    df.loc[(df['sm_cci'] >= df['sm_cci'].shift()), 'cci_vector'] = 'long'
    df.loc[(df['sm_cci'] < df['sm_cci'].shift()), 'cci_vector'] = 'short'


def macd(df):  # 'macd', 'macd_signal'
    slow_per = 200
    fast_per = 10
    per_smacd = 5
    slow_per2 = 50
    fast_per2 = 10
    per_smacd2 = 5
    ema_macd1 = ema_indicator(close=df['close'], window=fast_per)
    ema_macd2 = ema_indicator(close=df['close'], window=slow_per)
    df['macds1'] = ((ema_macd1 - ema_macd2) / ema_macd2) * 100  # macd1 macds1
    df['macd1'] = ema_indicator(close=df['macds1'], window=per_smacd)  # сглаженное процентное расхождение
    ema_macd3 = ema_indicator(close=df['close'], window=fast_per2)
    ema_macd4 = ema_indicator(close=df['close'], window=slow_per2)
    df['macds2'] = ((ema_macd3 - ema_macd4) / ema_macd4) * 100
    df['macd2'] = ema_indicator(close=df['macds2'], window=per_smacd2)  # сглаженное процентное расхождение
    #df.loc[(df['macd1'] > df['macd1'].shift()), 'macd_vector200'] = 'up'
    df.loc[(df['macd1'] > df['macd1'].shift()) & (df['macd1'] > 0), 'macd_vector200'] = 'up'
    #df.loc[(df['macd1'] < df['macd1'].shift()), 'macd_vector200'] = 'down'
    df.loc[(df['macd1'] < df['macd1'].shift()) & (df['macd1'] < 0), 'macd_vector200'] = 'down'
    df.loc[(df['macd1'] < 0.05) & (df['macd1'] > -0.05), 'macd_zone'] = 'Стагнирует'
    df.loc[(df['macd1'] < 0.75) & (df['macd1'] > 0.05), 'macd_zone'] = 'Движуха'
    df.loc[(df['macd1'] > -0.75) & (df['macd1'] < -0.05), 'macd_zone'] = 'Движуха'
    df.loc[(df['macd1'] > 0.75), 'macd_zone'] = 'Перекуп'
    df.loc[(df['macd1'] > 0.88), 'macd_zone'] = 'Сильный перекуп'
    df.loc[(df['macd1'] < -0.75), 'macd_zone'] = 'Перепрод'
    df.loc[(df['macd1'] < -0.88), 'macd_zone'] = 'Сильный перепрод'


def rox(df, x):  # ЭТО ТОЖЕ САМОЕ ЧТО И РАЗМАХ( В СТАТИСТИКЕ) ... ИЗМЕРЯЕТ АМЛИТУДУ за определенный период
    roxi = ROCIndicator(close=df['close'], window=x.roc_per)
    df['roc'] = roxi.roc()
    df['vector_roc'] = ema_indicator(ema_indicator(close=df['roc'], window=x.sm1roc), window=x.sm2roc)
    df.loc[(df['vector_roc'] < df['vector_roc'].shift()) & (df['vector_roc'] < x.roc_range), 'roc_vector'] = 'DOWN'
    df.loc[(df['vector_roc'] >= df['vector_roc'].shift()) & (df['vector_roc'] > x.roc_range), 'roc_vector'] = 'UP'


def priceDensity(df, x):
    # === Если плотность цены растет значит цена зажимается в более узком диапазоне и это флэт.
    # === Если цена наоборот кудато летит то плотность будет падать и это импульс
    df['atr'] = average_true_range(high=df['high'], low=df['low'], close=df['close'], window=x.atrPer)
    df['chislitel'] = df['atr'].rolling(x.pdPer).sum()
    df['znamenatel'] = df['high'].rolling(x.pdPer).max() - df['low'].rolling(x.pdPer).min()
    df['pd'] = df['chislitel'] / df['znamenatel']
    df['smoothedPD'] = round(ema_indicator(close=ema_indicator(close=df['pd'], window=x.sm1pd), window=x.sm2pd), 2)
    df.loc[(df['smoothedPD'] >= df['smoothedPD'].shift()), 'priceDensity'] = 'flat'
    df.loc[(df['smoothedPD'] < df['smoothedPD'].shift()), 'priceDensity'] = 'impulse'


def intensiv(df, x):
    # =================ИНТЕНСИВНОСЬ ИЗМЕНЕНИЯ СКОЛЬЗЯЩЕЙ ====================================================
    df['Tema13'] = ema_indicator(close=df['close'], window=x.flatTema1)
    df['Tema23'] = ema_indicator(close=df['Tema13'], window=x.flatTema1)
    df['Tema33'] = ema_indicator(close=df['Tema23'], window=x.flatTema1)
    df['temaF3'] = 3 * (df['Tema13'] - df['Tema23']) + df['Tema33']

    df['flatTema'] = ema_indicator(close=ema_indicator(close=df['temaF3'], window=x.smrngT1), window=x.smrngT2)
    df['flat'] = abs(((df['flatTema'] - df['flatTema'].shift(x.offset1)) / df['flatTema'].shift(x.offset1)) * 100)
    df.loc[(df['flatTema'] > df['flatTema'].shift()), 'isFlat1'] = 'up'
    df.loc[(df['flatTema'] < df['flatTema'].shift()), 'isFlat1'] = 'down'
    df.loc[(df['flat'] > x.flatRange1) & (df['isFlat1'] == 'up'), 'tema_speed'] = 'long'
    df.loc[(df['flat'] > x.flatRange1) & (df['isFlat1'] == 'down'), 'tema_speed'] = 'short'


def EMA(df):
    #df['HA_Close'] = round(((df['open'] + df['high'] + df['low'] + df['close']) / 4), 3)
    df['ema200'] = ema_indicator(close=df['close'], window=200)
    df['ema20'] = ema_indicator(close=df['close'], window=20)
    df['ema50'] = ema_indicator(close=df['close'],window=50)
    df.loc[(df['ema20'] > df['ema20'].shift()), 'emaVector20'] = 'UP' # 'emaVector20'] = 'UP'/'DOWN'; 'emaVector50'] = 'UP'/'DOWN' ; 'emaVector200'] = 'UP'/'DOWN'
    df.loc[(df['ema20'] < df['ema20'].shift()), 'emaVector20'] = 'DOWN'
    df.loc[(df['ema50'] > df['ema50'].shift()), 'emaVector50'] = 'UP'
    df.loc[(df['ema50'] < df['ema50'].shift()), 'emaVector50'] = 'DOWN'
    df.loc[(df['ema200'] > df['ema200'].shift()), 'emaVector200'] = 'UP'
    df.loc[(df['ema200'] < df['ema200'].shift()), 'emaVector200'] = 'DOWN'
    #df.loc[(df['emaVector'] != df['emaVector'].shift()), 'signal_ema'] = 'Переворот'


# PIVOT
def tema(df):
    df['Tema13'] = ema_indicator(close=df['close'], window=1)
    df['Tema23'] = ema_indicator(close=df['Tema13'], window=1)
    df['Tema33'] = ema_indicator(close=df['Tema23'], window=1)
    df['temaF3'] = 3 * (df['Tema13'] - df['Tema23']) + df['Tema33']
    df['tema'] = ema_indicator(close=ema_indicator(close=df['temaF3'], window=1), window=1)



class Pivot:
    def __init__(self, high, low, close):
        self.high = high
        self.low = low
        self.close = close
        self.p = (high+low+close)/3
        self.s1 = 2*self.p-high
        self.r1 = 2*self.p-low
        self.s2 = self.p - (high - low)
        self.r2 = self.p + (high - low)
        self.s3 = self.p * 2 - (2 * high - low)
        self.r3 = self.p * 2 + (high - 2 * low)
        self.s4 = self.p * 3 - (3 * high - low)
        self.r4 = self.p * 3 + (high - 3 * low)
        self.s5 = self.p * 4 - (4 * high - low)
        self.r5 = self.p * 4 + (high - 4 * low)



