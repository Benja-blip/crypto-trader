import pandas as pd
import numpy as np
from logbook import Logger

from catalyst.api import (
    order_target_percent,
    symbols,
)
from catalyst.exchange.utils.stats_utils import get_pretty_stats
from catalyst.utils.run_algo import run_algorithm

algo_namespace = 'CMomentum13'
log = Logger('CMomentum13')

# , 'xrp_usd', 'eth_usd', 'eos_usd', 'ltc_usd'


def initialize(context):

    log.info('initializing algo')
    context.assets = symbols('btc_usd')


def _handle_data(context, data):

    cash = context.portfolio.cash
    log.info('base currency available: {cash}'.format(cash=cash))

    current_price = data.current(context.assets, 'price')
    current_price_df = current_price.to_frame()
    current_price_df.columns = ['current_price']

    fifteen_min_high_df = data.history(
        context.assets, fields='price', bar_count=15, frequency='1T').max(axis=0).to_frame()
    fifteen_min_high_df.columns = ['15_min_high']
    current_and_high = current_price_df.join(fifteen_min_high_df, how='outer')

    last_five_min_df = data.history(context.assets, fields='price', bar_count=1, frequency='5T').iloc[0].to_frame()
    last_five_min_df.columns = ['last_five_min']
    current_high_and_five = current_and_high.join(last_five_min_df, how='outer')

    print(current_high_and_five)

    twelve_hr_low_df = data.history(context.assets, fields='price', bar_count=24, frequency='30T').min(axis=0).to_frame()
    twelve_hr_low_df.columns = ['twelve_hr_low']
    last_sixty_df = data.history(context.assets, fields='price', bar_count=12, frequency='5T')
    # print("last 45 minutes:")
    # print(last_sixty_df)
    initial_bar_df = last_sixty_df.iloc[0:3].mean(axis=0).to_frame()
    initial_bar_df.columns = ['initial_bar']
    first_bar_df = last_sixty_df.iloc[3:6].mean(axis=0).to_frame()
    first_bar_df.columns = ['first_bar']
    second_bar_df = last_sixty_df.iloc[6:9].mean(axis=0).to_frame()
    second_bar_df.columns = ['second_bar']
    third_bar_df = last_sixty_df.iloc[9:12].mean(axis=0).to_frame()
    third_bar_df.columns = ['third_bar']

    twelve_hr_low_df['within_low_range'] = last_five_min_df['last_five_min'] < (twelve_hr_low_df['twelve_hr_low'] * 1.002)
    first_bar_df['pos_first_bar'] = first_bar_df['first_bar'] > initial_bar_df['initial_bar']
    first_set_df = twelve_hr_low_df.join(first_bar_df, how='outer')
    second_bar_df['pos_second_bar'] = second_bar_df['second_bar'] > first_bar_df['first_bar']
    third_bar_df['pos_third_bar'] = third_bar_df['third_bar'] > first_bar_df['first_bar']
    second_set_df = second_bar_df.join(third_bar_df, how='outer')

    # print(first_set_df)
    # print(second_set_df)

    all_factors_combined_df = first_set_df.join(second_set_df, how='outer')
    print(all_factors_combined_df)

    pairs_to_buy = pd.DataFrame(all_factors_combined_df.query(
        'within_low_range and pos_first_bar and pos_second_bar and pos_third_bar'))

    print(pairs_to_buy)

    orders = context.blotter.open_orders
    positions = context.portfolio.positions

    for ind in pairs_to_buy.index:
        # last_sale_price = current_high_and_five.loc[ind, 'current_price']
        if orders:
            log.info('skipping bar until all open orders execute')
            return
        elif len(positions) == 2:
            log.info('Order limit reached.')
            return
        elif context.portfolio.cash <= 100:
            log.info('Too much cash currently in use.')
            return
        elif len(pairs_to_buy) == 0:
            print('Currency pair does not meet requirements.')
        else:
            order_target_percent(asset=ind, target=0.95)

    for ind in context.portfolio.positions:
        pos_amount = context.portfolio.positions[ind].amount
        cost_basis = context.portfolio.positions[ind].cost_basis
        last_sale_price = context.portfolio.positions[ind].last_sale_price
        loss = (last_sale_price * pos_amount) - (cost_basis * pos_amount)
        if last_sale_price <= (cost_basis * .995):
            log.info('closing position, taking loss: {}'.format(loss))
            order_target_percent(
                asset=ind,
                target=0
            )

    for ind in context.portfolio.positions:
        pos_amount = context.portfolio.positions[ind].amount
        cost_basis = context.portfolio.positions[ind].cost_basis
        last_sale_price = context.portfolio.positions[ind].last_sale_price
        # percent_gain = ((current_price - cost_basis) / cost_basis) * 100
        recent_high = fifteen_min_high_df.loc[ind, '15_min_high']
        log.info('fifteen minute high: {}'.format(recent_high))
        # log.info('percent gain: {}').format(percent_gain)
        profit = (last_sale_price * pos_amount) - (cost_basis * pos_amount)
        if last_sale_price >= (cost_basis * 1.1) and (last_sale_price < recent_high * .999):
            log.info('closing position, taking profit: {}'.format(profit))
            order_target_percent(
                asset=ind,
                target=0
            )


def handle_data(context, data):
    log.info('handling bar {}'.format(data.current_dt))
    # try:
    _handle_data(context, data)
    # except Exception as e:
    #     log.warn('aborting the bar on error {}'.format(e))
    #     context.errors.append(e)

    log.info('completed bar {}'.format(
        data.current_dt
    ))

    # if len(context.errors) > 0:
    #     log.info('the errors:\n{}'.format(context.errors))


def analyze(context, stats):
    log.info('the daily stats:\n{}'.format(get_pretty_stats(stats)))

    exchange = list(context.exchanges.values())[0]
    quote_currency = exchange.quote_currency.upper()


if __name__ == '__main__':
    live = True
    if live:
        run_algorithm(
            capital_base=1000,
            initialize=initialize,
            handle_data=handle_data,
            analyze=analyze,
            exchange_name='bittrex',
            live=True,
            algo_namespace='Cmomentum13',
            quote_currency='usd',
            simulate_orders=True,
        )
    else:
        run_algorithm(
            capital_base=1000,
            data_frequency='minute',
            initialize=initialize,
            handle_data=handle_data,
            analyze=analyze,
            exchange_name='bitfinex',
            algo_namespace='CMomentum13',
            quote_currency='usd',
            simulate_orders=True,
            start=pd.to_datetime('2019-09-27', utc=True),
            end=pd.to_datetime('2019-09-30', utc=True),
        )