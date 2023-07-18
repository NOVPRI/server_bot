def door(x, df):
    if x.st_door == 'bbma':
        df.loc[(df['tema'] > df['BB_HB_1']) | (df['tema'] < df['BB_LB_1']), 'door'] = 'open'
    else:
        df['door'] = '❌'


def status(x, df):
    if x.st_status == 'first':
        df.loc[(df['emaVector20'] == 'UP') & (df['macd_vector200'] == 'up') & (df['tema'] > df['BB_MA_1']) & (df['door'] == 'open') & (
                    df['emaVector200'] == 'UP') & (x.P < df['close']), 'status'] = 'BUY'
        df.loc[(df['emaVector20'] == 'DOWN') & (df['macd_vector200'] == 'down') & (df['tema'] < df['BB_MA_1']) & (df['door'] == 'open') & (
                    df['emaVector200'] == 'DOWN') & (x.P > df['close']), 'status'] = 'SELL'
    else:
        df['status'] = '❌'


def fix(x, df):
    if x.st_fix == 'B_z_tema':
        df.loc[(df['BB_HB_1'].shift() < df['tema'].shift()) & (df['BB_HB_1'] > df['tema']), 'fix'] = 'FIX'
        df.loc[(df['BB_HB_1'].shift() > df['tema'].shift()) & (df['BB_HB_1'] < df['tema']), 'fix'] = '⇈'
        df.loc[(df['BB_LB_1'].shift() > df['tema'].shift()) & (df['BB_LB_1'] < df['tema']), 'fix'] = 'FIX'
        df.loc[(df['BB_LB_1'].shift() < df['tema'].shift()) & (df['BB_LB_1'] > df['tema']), 'fix'] = '⇊'
    else:
        df['fix'] = '❌'
