"""Trading module includes Quik connection to MOEX, NASDAQ connection to off website"""
import logging
import inspect
import time
import json
import re
import os
import platform
import itertools
from os import sep, mkdir
from os.path import exists
from subprocess import Popen, PIPE
from datetime import datetime as dt
import requests # type: ignore # pylint: disable=import-error


TMT_REBOOT = 600


logger = logging.getLogger('XAU')

def get_nasdaq_idx(in_idx_name='XAU'):
    """Get info from NASDAQ"""
    result = {'result': False, 'content': ''}
    idx_name = in_idx_name
    str_out = f'Index is {idx_name}' # pylint: disable=redefined-outer-name
    logger.debug(str_out)
    nasdaq_url = 'https://indexes.nasdaqomx.com/index/FundamentalData'
    cookies = {
                '__RequestVerificationToken': ('Vw3TAJP8BaxBKTW6PWQWe-9pg-'
                                            'oXb6l7I6OlSD-'
                                            '3M6FQDIKIlVwhJorA4w3BfH5fpglRRfqfi-'
                                            'U3PndTVzYlurIYU6pnL_t8Z7c0rrYRtAQ1'),
                'NSC_W.HJX.XXX.443': ('14b5a3d90fc929f377553f0905123e00e846a'
                                    '375d562bfcf213568d44958e278b8b3c186'),
                'visid_incap_2594168': ('VYYgLwO/Sci8MMkIY00waUhq12YAA'
                                        'AAAQUIPAAAAAADVCXsVA3eH6zYg4F9cST4u'),
                'nlbi_2594168': 'hpFGIHpD2B7hdV9USCe9RgAAAAADnaNjNqnLEZC5NDLbeApS',
                '_biz_uid': 'ebd99242490d494bf101106d0a18334a',
                '_ga': 'GA1.1.557386950.1725393485',
                '_mkto_trk': 'id:303-QKM-463&token:_mch-nasdaqomx.com-1725393485337-45399',
                '_biz_flagsA': ('%7B%22Version%22%3A1%2C%22View'
                                'Through%22%3A%221%22%2C%22X'
                                'Domain%22%3A%221%22%2C%22Mkto%22%3A%221%22%7D'),
                'incap_ses_1686_2594168': ('69paenxeeiXg87fwHOBlFzES32Y'
                                           'AAAAAS8n0Dn/nMvlXIEjU+rEhJg=='),
                '_biz_nA': '16',
                '_biz_pendingA': '%5B%5D',
                '_ga_5YP0JZFRFE': 'GS1.1.1725895220.6.0.1725895220.60.0.0',
               }

    headers = {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://indexes.nasdaqomx.com',
                'priority': 'u=1, i',
                'referer': 'https://indexes.nasdaqomx.com/Index/Overview/XAU',
                'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                            'AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Chrome/128.0.0.0 Safari/537.36'),
                'x-requested-with': 'XMLHttpRequest',
              }

    data = {'id': 'XAU',}
    try:
        response = requests.post(nasdaq_url,
                                 cookies=cookies,
                                 headers=headers,
                                 data=data,
                                 timeout=4)
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get NASDAQ index request'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result
    if response.status_code != 200:
        str_out = (f'Get NASDAQ index status. '
                   f'Status code is {str(response.status_code)}')
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result
    try:
        response_j = json.loads(response.content)
        result = {'result': True, 'content': response_j}
        return result
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get NASDAQ index content'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result


def set_logging(in_app_name, in_app_path, in_app_run_mode):
    """Set logger"""
    result = {'result': False, 'content': ''}
    LOG_START_TIME = re.sub(r"\W+", "_", str(time.ctime()))
    LOG_FMT_STRING = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_DIR = f'{in_app_path}{sep}logs'
    if not exists(LOG_DIR):
        mkdir(LOG_DIR)
    LOG_FILENAME = f'{LOG_DIR}{sep}{in_app_name}_{LOG_START_TIME}.log'
    log_handlers = [logging.StreamHandler()]
    if in_app_run_mode == 'PROD':
        log_handlers.append(logging.FileHandler(LOG_FILENAME))
    logger = logging.getLogger(in_app_run_mode)
    logging.basicConfig(format=LOG_FMT_STRING,
                        datefmt='%d.%m.%Y %H:%M:%S',
                        level=logging.INFO, # NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=log_handlers)
    result = {'result': True, 'content': logger}
    return result


def tb_init(in_table_name, in_conn=None, in_c=None):
    """Get table initialization"""
    result = {'result': False, 'content': ''}
    table_name = in_table_name
    logger.info('Create table %s', table_name)
    try:
        with in_conn:
            ti_statement = (f'create table if not exists "{table_name}" '
                            '(date text, '
                            'price float);')
            in_c.execute(ti_statement)
        result = {'result': True, 'content': ''}
        return result
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Table initialization error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result


def trigger_init(in_trigger_name, in_conn=None, in_c=None):
    """Get table initialization"""
    result = {'result': False, 'content': ''}
    trigger_name = in_trigger_name
    logger.info('Create trigger %s', trigger_name)
    try:
        with in_conn:
            ti_statement = (f'create table if not exists tb_sync '
                            '(tb_name text, tb_rowid text, '
                            'date text);')
            in_c.execute(ti_statement)
        with in_conn:
            ti_statement = (f'create table if not exists analytics '
                            '(current_base_price text, past_base_price text, '
                            'min_base text, diff_base text, '
                            'koef_base text, dname text, '
                            'current_dname text, past_dname text, '
                            'min_dname text, diff_dname text, '
                            'dname_koef text);')
            in_c.execute(ti_statement)
        with in_conn:
            ti_statement = ("""create trigger if not exists selg_sync_trigger 
                            after insert on SELG
                            begin
                                 insert into tb_sync values('SELG', (select max(rowid) from SELG), (SELECT strftime('%Y-%m-%d %H:%M:%S', datetime('now'))));
                            end;""")
            in_c.execute(ti_statement)
        with in_conn:
            ti_statement = ("""create trigger if not exists plzl_sync_trigger 
                            after insert on PLZL
                            begin
                                 insert into tb_sync values('PLZL', (select max(rowid) from PLZL), (SELECT strftime('%Y-%m-%d %H:%M:%S', datetime('now'))));
                            end;""")
            in_c.execute(ti_statement)
        with in_conn:
            ti_statement = ("""create trigger if not exists xau_sync_trigger 
                            after insert on XAU
                            begin
                                 insert into tb_sync values('XAU', (select max(rowid) from XAU), (SELECT strftime('%Y-%m-%d %H:%M:%S', datetime('now'))));
                            end;""")
            in_c.execute(ti_statement)
        result = {'result': True, 'content': ''}
        return result
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Table initialization error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result



def exit_script(in_qp_provider=None):
    """Exit script with quik connnection closing"""
    result = {'result': False, 'content': ''}
    qp_provider = in_qp_provider
    try:
        qp_provider.close_connection_and_thread()
        result = {'result': True, 'content': ''}
        return result
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Exit script error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result


def tb_init_deels(in_table_name, in_conn=None, in_c=None):
    """Deels table initialization"""
    result = {'result': False, 'content': ''}
    table_name = in_table_name
    try:
        with in_conn:
            tid_statement = (f'create table if not exists "{table_name}" '
                              '(deel_date text, '
                              'status float, '
                              'amount text);')
            in_c.execute(tid_statement)
        result = {'result': True, 'content': f'Statement "{tid_statement}" done'}
        return result
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Table initialization deels error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result


def check_quik_connection(in_qp_provider=None):
    """Checking QUIK connection"""
    result = {'result': False, 'content': ''}
    qp_provider = in_qp_provider
    # QUIK terminal - QUIK server connection state
    is_connected = qp_provider.is_connected()['data']
    if is_connected == 0:
        str_out = 'QUIK connection failed' # pylint: disable=redefined-outer-name
        logger.critical(inspect.currentframe().f_code.co_name)
        logger.critical(str_out)
        time.sleep(TMT_REBOOT)
        os.system('shutdown /r')
    result = {'result': True, 'content': 'QUIK connection is OK'}
    return result


def get_price_back(in_table_name='',
                   in_datetime='2024-09-04 21:00:00',
                   in_conn=None,
                   in_c=None):
    """Get price"""
    result = {'result': False, 'content': ''}
    gpb_dt = in_datetime
    table_name = in_table_name
    try:
        with in_conn:
            gpb_statement = f'select price from "{table_name}" where date = "{gpb_dt}" limit 1;'
            price = in_c.execute(gpb_statement).fetchone()
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get price back error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result
    if price:
        price = price[0]
        result = {'result': True, 'content': price}
        return result
    else:
        str_out = f'Failed to get past price for {table_name}' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result


def get_date_back(in_table_name, in_conn=None, in_c=None):
    """Get date from DB"""
    result = {'result': False, 'content': ''}
    table_name = in_table_name
    try:
        with in_conn:
            gdb_statement = f'select date from "{table_name}" order by rowid desc limit 1;'
            date = in_c.execute(gdb_statement).fetchone()
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get date back error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result
    if date:
        date = date[0]
        result = {'result': True, 'content': date}
        return result
    else:
        str_out = f'Failed to get past date for {table_name}' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result


def hist_select_config(in_c=None, in_conn=None):
    """Choose config"""
    result = {'result': False, 'content': ''}
    try:
        with in_conn:
            sc_query = 'select rowid, config \
                        from configs \
                        where result_ru=0 and result_en=0 \
                        limit 1;'
            sc_config_arr = in_c.execute(sc_query).fetchone()
            sc_config_final = json.loads(sc_config_arr[1])
            logger.info(type(sc_config_final))
            sc_config_final["rowid"] = sc_config_arr[0]
            logger.debug("##################")
            logger.debug(sc_config_final)
            result = {'result': True, 'content': sc_config_final}
    except Exception as ex: # pylint: disable=broad-exception-caught
        logger.critical(str(ex))
        logger.critical("Selection configuration")
        result = {'result': False, 'content': ''}
    return result


# legacy, will replaced soon
def is_internet(in_hosts=[]): # pylint: disable=dangerous-default-value
    """Internet checking"""
    result = {'result': False, 'content': ''}
    for host in in_hosts:
        logger.info('Check host %s', host)
        if platform.system() == 'Linux':
            command = f'ping -c 4 {host}'
            code_page = 'UTF-8'
            ttl_record = 'ttl'
        else:
            command = f'ping {host}'
            code_page = '866'
            ttl_record = 'TTL'
        sp = Popen(command, stderr=PIPE, stdout=PIPE, shell=True)
        out, err = sp.communicate()
        if err:
            logger.info('Error during checking host %s', host)
            continue
        if ttl_record not in out.decode(code_page):
            logger.debug('Output is %s', out.decode(code_page))
            logger.info('Error during decode check result host %s', host)
            continue
        else:
            result = {'result': True, 'content': 'Internet connection is OK'}
            return result
    str_out = 'There are no available hosts'
    f_name = inspect.currentframe().f_code.co_name
    result = {'result': False, 'content': f'{f_name}. {str_out}'}
    return result


def on_trans_reply(data):
    """transaction reply processing"""
    result = {'result': False, 'content': ''}
    str_out = f'OnTransReply: {data}' # pylint: disable=redefined-outer-name
    logger.info(str_out)
    order_num = int(data['data']['order_num'])  # Номер заявки на бирже
    str_out = f'Номер транзакции: {data["data"]["trans_id"]}, Номер заявки: {order_num}'
    logger.info(str_out)
    result = {'result': True, 'content': ''}
    return result


def open_long(in_class_code='QJSIM',
              in_sec_code='SBER',
              in_quantity=1,
              in_qp_provider=None,
              in_last_price =None):
    """Open buy deel"""
    result = {'result': False, 'content': ''}
    class_code = in_class_code
    quantity = in_quantity
    qp_provider = in_qp_provider
    last_price = in_last_price
    # Ищем первый счет с режимом торгов тикера
    accounts_bunch = (acc for acc in qp_provider.accounts if class_code in acc['class_codes'])
    account = next(accounts_bunch, None)
    if not account:  # Если счет не найден
        str_out = f'Торговый счет для режима торгов {class_code} не найден' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result
    client_code = account['client_code'] if account['client_code'] else ''
    trade_account_id = account['trade_account_id']
    # si = qp_provider.get_symbol_info(in_class_code, sec_code)
    # 19-и значный номер заявки на бирже / номер стоп заявки на сервере.
    # Будет устанавливаться в обработчике события ответа на транзакцию пользователя
    # order_num = 0
    # Номер транзакции задается пользователем с 1 и каждый раз увеличиваться на 1
    trans_id = itertools.count(1)
    # Обработчики подписок
    # Ответ на транзакцию пользователя. Если транзакция выполняется из QUIK, то не вызывается
    # qp_provider.on_trans_reply = on_trans_reply
    # qp_provider.on_order = lambda data:
    # logger.info(f'OnOrder: {data}')  # Получение новой / изменение существующей заявки
    # qp_provider.on_stop_order = lambda data:
    # logger.info(f'OnStopOrder: {data}')  # Получение новой / изменение существующей стоп заявки
    # qp_provider.on_trade = lambda data: logger.info(f'OnTrade: {data}')
    # # Получение новой / изменение существующей сделки
    # qp_provider.on_futures_client_holding =
    # lambda data: logger.info(f'OnFuturesClientHolding: {data}')
    # # Изменение позиции по срочному рынку
    # qp_provider.on_depo_limit = lambda data: logger.info(f'OnDepoLimit: {data}')
    # Изменение позиции по инструментам
    # qp_provider.on_depo_limit_delete =
    # lambda data: logger.info(f'OnDepoLimitDelete: {data}')
    # Удаление позиции по инструментам
    # Новая рыночная заявка (открытие позиции)
    q_p_t_p = qp_provider.quik_price_to_price(class_code, in_sec_code, last_price * 1.01)
    market_price = qp_provider.price_to_quik_price(class_code, in_sec_code, q_p_t_p) if account['futures'] else 0
    transaction = {  # Все значения должны передаваться в виде строк
        'TRANS_ID': str(next(trans_id)),  # Следующий номер транзакции
        'CLIENT_CODE': client_code,  # Код клиента
        'ACCOUNT': trade_account_id,  # Счет
        'ACTION': 'NEW_ORDER',  # Тип заявки: Новая лимитная/рыночная заявка
        'CLASSCODE': class_code,  # Код режима торгов
        'SECCODE': in_sec_code,  # Код тикера
        'OPERATION': 'B',  # B = покупка, S = продажа
        'PRICE': str(market_price),  # Цена исполнения по рынку
        'QUANTITY': str(quantity),  # Кол-во в лотах
        'TYPE': 'M'}  # L = лимитная заявка (по умолчанию), M = рыночная заявка
    str_out = f'Заявка отправлена на рынок: {qp_provider.send_transaction(transaction)["data"]}'
    logger.info(str_out)
    result = {'result': True, 'content': ''}
    return result
    # # Закрываем соединение для запросов и поток обработки функций обратного вызова
    # # Новая рыночная заявка (закрытие позиции)
    # market_price = qp_provider.price_to_quik_price(class_code, sec_code,
    # qp_provider.quik_price_to_price(class_code, sec_code, last_price * 0.99))
    #  if account['futures'] else 0
    # # Цена исполнения по рынку. Для фьючерсных заявок цена больше последней
    # при покупке и меньше последней при продаже. Для остальных заявок цена = 0
    # logger.info(f'Заявка {class_code}.{sec_code} на продажу минимального
    # лота по рыночной цене')
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'OPERATION': 'S',  # B = покупка, S = продажа
    #     'PRICE': str(market_price),  # Цена исполнения по рынку
    #     'QUANTITY': str(quantity),  # Кол-во в лотах
    #     'TYPE': 'M'}  # L = лимитная заявка (по умолчанию),
    # M = рыночная заявка
    # logger.info(f'Заявка отправлена на рынок:
    # # Новая лимитная заявка
    # Лимитная цена на 1% ниже последней цены сделки
    # limit_price = qp_provider.price_to_quik_price(class_code, sec_code,
    #  qp_provider.quik_price_to_price(class_code,
    # sec_code, last_price * 0.99))
    # logger.info(f'Заявка {class_code}.{sec_code} на покупку минимального
    # лота по лимитной цене {limit_price}')
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'OPERATION': 'B',  # B = покупка, S = продажа
    #     'PRICE': str(limit_price),  # Цена исполнения
    #     'QUANTITY': str(quantity),  # Кол-во в лотах
    #     'TYPE': 'L'}  # L = лимитная заявка (по умолчанию),
    # M = рыночная заявка
    # logger.info(f'Заявка отправлена в стакан:
    # # Удаление существующей лимитной заявки
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'TRANS_ID': str(next(trans_id)),  # Следующий номер транзакции
    #     'ACTION': 'KILL_ORDER',  # Тип заявки: Удаление существующей заявки
    #     'ORDER_KEY': str(order_num)}  # Номер заявки
    # logger.info(f'Удаление заявки {order_num} из стакана:
    # # Новая стоп заявка
    # stop_price = qp_provider.price_to_quik_price(class_code, sec_code,
    # qp_provider.quik_price_to_price(class_code, sec_code, last_price * 1.01))
    # # Стоп цена на 1% выше последней цены сделки
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'OPERATION': 'B',  # B = покупка, S = продажа
    #     'PRICE': str(last_price),  # Цена исполнения
    #     'STOPPRICE': str(stop_price),  # Стоп цена исполнения
    #     'EXPIRY_DATE': 'GTC'}  # Срок действия до отмены
    # logger.info(f'Стоп заявка отправлена на сервер:
    # # Удаление существующей стоп заявки
    # transaction = {
    #     'ACTION': 'KILL_STOP_ORDER',  # Тип заявки:
    # Удаление существующей заявки
    #     'STOP_ORDER_KEY': str(order_num)}  # Номер заявки
    # print(f'Удаление стоп заявки с сервера: {qp_provider.send_transaction(transaction)["data"]}')


def close_long(in_class_code='QJSIM',
               in_sec_code='SBER',
               in_quantity=1,
               in_qp_provider=None,
               in_last_price=None):
    """Close buy"""
    result = {'result': False, 'content': ''}
    quantity = in_quantity
    qp_provider = in_qp_provider
    last_price = in_last_price
    cl_accounts_bunch = (acc for acc in qp_provider.accounts if in_class_code in acc['class_codes'])
    account = next(cl_accounts_bunch, None)
    if not account:  # Если счет не найден
        str_out = f'Торговый счет для режима торгов {in_class_code} не найден' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result
    client_code = account['client_code'] if account['client_code'] else ''
    # Счет
    trade_account_id = account['trade_account_id']
    # si = qp_provider.get_symbol_info(class_code, in_sec_code)
    # 19-и значный номер заявки на бирже / номер стоп заявки на сервере.
    # Будет устанавливаться в обработчике события ответа на транзакцию пользователя
    # order_num = 0
    # Номер транзакции задается пользователем.
    # Он будет начинаться с 1 и каждый раз увеличиваться на 1
    trans_id = itertools.count(1)
    # Обработчики подписок
    # Ответ на транзакцию пользователя. Если транзакция выполняется из QUIK,
    # то не вызывается
    # qp_provider.on_trans_reply = on_trans_reply
    # qp_provider.on_order = lambda data: logger.info(f'OnOrder: {data}')
    # # Получение новой / изменение существующей заявки
    # qp_provider.on_stop_order =
    # lambda data: logger.info(f'OnStopOrder: {data}')
    # # Получение новой / изменение существующей стоп заявки
    # qp_provider.on_trade = lambda data: logger.info(f'OnTrade: {data}')
    # # Получение новой / изменение существующей сделки
    # qp_provider.on_futures_client_holding =
    # lambda data: logger.info(f'OnFuturesClientHolding: {data}')
    # # Изменение позиции по срочному рынку
    # qp_provider.on_depo_limit = lambda data: logger.info(f'OnDepoLimit: {data}')
    # # Изменение позиции по инструментам
    # qp_provider.on_depo_limit_delete =
    # lambda data: logger.info(f'OnDepoLimitDelete: {data}')  # Удаление позиции по инструментам
    # Новая рыночная заявка (закрытие позиции)
    q_p_t_p = qp_provider.quik_price_to_price(in_class_code, in_sec_code, last_price * 0.99)
    # Цена исполнения по рынку. Для фьючерсных заявок цена больше последней при покупке и
    # меньше последней при продаже. Для остальных заявок цена = 0
    market_price = qp_provider.price_to_quik_price(in_class_code, in_sec_code, q_p_t_p) if account['futures'] else 0
    str_out = f'Заявка {in_class_code}.{in_sec_code} на продажу минимального лота по рыночной цене'
    logger.info(str_out)
    transaction = {  # Все значения должны передаваться в виде строк
        'TRANS_ID': str(next(trans_id)),  # Следующий номер транзакции
        'CLIENT_CODE': client_code,  # Код клиента
        'ACCOUNT': trade_account_id,  # Счет
        'ACTION': 'NEW_ORDER',  # Тип заявки: Новая лимитная/рыночная заявка
        'CLASSCODE': in_class_code,  # Код режима торгов
        'SECCODE': in_sec_code,  # Код тикера
        'OPERATION': 'S',  # B = покупка, S = продажа
        'PRICE': str(market_price),  # Цена исполнения по рынку
        'QUANTITY': str(quantity),  # Кол-во в лотах
        'TYPE': 'M'}  # L = лимитная заявка (по умолчанию), M = рыночная заявка
    str_out = f'Заявка отправлена на рынок: {qp_provider.send_transaction(transaction)["data"]}'
    logger.info(str_out) # pylint: disable=redefined-outer-name
    result = {'result': True, 'content': 'Deel succesfully closed'}
    return result
    # # Новая лимитная заявка
    # limit_price = qp_provider.price_to_quik_price(class_code, sec_code,
    # qp_provider.quik_price_to_price(class_code, sec_code, last_price * 0.99))
    # # Лимитная цена на 1% ниже последней цены сделки
    # logger.info(f'Заявка {class_code}.{sec_code}
    # на покупку минимального лота по лимитной цене {limit_price}')
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'OPERATION': 'B',  # B = покупка, S = продажа
    #     'PRICE': str(limit_price),  # Цена исполнения
    #     'QUANTITY': str(quantity),  # Кол-во в лотах
    #     'TYPE': 'L'}  # L = лимитная заявка (по умолчанию), M = рыночная заявка
    # logger.info(f'Заявка отправлена в стакан:
    # {qp_provider.send_transaction(transaction)["data"]}')
    # # Удаление существующей лимитной заявки
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'TRANS_ID': str(next(trans_id)),  # Следующий номер транзакции
    #     'ACTION': 'KILL_ORDER',  # Тип заявки: Удаление существующей заявки
    #     'SECCODE': sec_code,  # Код тикера
    #     'ORDER_KEY': str(order_num)}  # Номер заявки
    # logger.info(f'Удаление заявки {order_num} из стакана:
    # {qp_provider.send_transaction(transaction)["data"]}')
    # # Новая стоп заявка
    # stop_price = qp_provider.price_to_quik_price(class_code, sec_code,
    # qp_provider.quik_price_to_price(class_code, sec_code, last_price * 1.01))
    # # Стоп цена на 1% выше последней цены сделки
    # transaction = {  # Все значения должны передаваться в виде строк
    #     'OPERATION': 'B',  # B = покупка, S = продажа
    #     'PRICE': str(last_price),  # Цена исполнения
    #     'QUANTITY': str(quantity),  # Кол-во в лотах
    #     'STOPPRICE': str(stop_price),  # Стоп цена исполнения
    #     'EXPIRY_DATE': 'GTC'}  # Срок действия до отмены
    # logger.info(f'Стоп заявка отправлена на сервер:
    # {qp_provider.send_transaction(transaction)["data"]}')
    # # Удаление существующей стоп заявки
    # transaction = {
    #     'TRANS_ID': str(next(trans_id)),  # Следующий номер транзакции
    #     'ACTION': 'KILL_STOP_ORDER',
    # Тип заявки: Удаление существующей заявки
    #     'STOP_ORDER_KEY': str(order_num)}  # Номер заявки
    # print(f'Удаление стоп заявки с сервера:
    # {qp_provider.send_transaction(transaction)["data"]}')


def fix_deel(in_tb_name, in_state, in_quant, in_conn=None, in_c=None):
    """Deel fixation"""
    result = {'result': False, 'content': ''}
    state = in_state
    tb_name = in_tb_name
    if state == 'done':
        with in_conn:
            fd_statement = f"update '{tb_name}_deels' set status = '{state}';"
            in_c.execute(fd_statement)
        result = {'result': True, 'content': 'Transaction processed'}
        return result
    else:
        with in_conn:
            fd_1_statement = f"insert into '{tb_name}_deels' \
                            values('{dt.now()}', '{state}', '{in_quant}');"
            in_c.execute(fd_1_statement)
            result = {'result': True, 'content': 'Setting transaction processed'}
            return result


def get_active_deels(in_table_name, in_conn=None, in_c=None):
    """Get active deel"""
    result = {'result': False, 'content': ''}
    table_name = in_table_name
    try:
        with in_conn:
            gad_statement = f'select status from "{table_name}" where status == "active";'
            state = in_c.execute(gad_statement).fetchone()
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get active deel error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result
    if state:
        state = state[0]
        result = {'result': True, 'content': state}
        return result
    else:
        str_out = f'Failed to get past price for {table_name}' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result


def close_deel(in_sym, in_conn=None, in_c=None):
    """Close deel"""
    result = {'result': False, 'content': ''}
    table_name = f'{in_sym}_deels'
    try:
        with in_conn:
            cd_statement = (f'select deel_date from "{table_name}" '
                         'where status = "active" limit 1;')
            date = in_c.execute(cd_statement).fetchone()
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Close deel error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result
    if date:
        date = date[0]
        result = {'result': True, 'content': date}
        return result
    else:
        str_out = f'Failed to get past date for {table_name}' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result


def get_current_balance(in_qp_provider):
    """Get current balance"""
    result = {'result': False, 'content': ''}
    qp_provider = in_qp_provider
    try:
        gcb_cur_balance = qp_provider.get_money_limits()['data'][0]['currentbal']
        result = {'result': True, 'content': gcb_cur_balance}
        return result
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get current balance error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result


def get_lot_price(in_class_code='QJSIM',
                  in_sec_code='SBER',
                  in_qp_provider=None,
                  in_last_price=None):
    """Get lot price"""
    result = {'result': False, 'content': ''}
    last_price = in_last_price
    qp_provider = in_qp_provider
    glp_si = qp_provider.get_symbol_info(in_class_code, in_sec_code)
    scale = glp_si['scale']
    glp_last_price = qp_provider.get_param_ex(in_class_code, in_sec_code, 'LAST')['data']
    glp_last_price = float(glp_last_price['param_value'])
    # min_price_step = si['min_price_step']
    lot_size = int(glp_si['lot_size'])
    # Если есть лот и стоимость шага цены
    glp_lot_price = round(last_price*lot_size*1.20, scale)
    # // min_price_step * step_price
    result = {'result': True, 'content': glp_lot_price}
    return result


def get_deel_quant(in_table_name, in_conn=None, in_c=None):
    """Get deel quantity"""
    result = {'result': False, 'content': ''}
    table_name = in_table_name
    try:
        with in_conn:
            gdq_statement = (f'select amount from "{table_name}_deels" '
                         'where status == "active" limit 1;')
            state = in_c.execute(gdq_statement).fetchone()
    except Exception as ex: # pylint: disable=broad-exception-caught
        str_out = 'Get deel quantity error'
        f_name = inspect.currentframe().f_code.co_name
        logger.critical(str_out)
        logger.critical(f_name)
        logger.critical(str(ex))
        result = {'result': False, 'content': f'{str_out}. {f_name}. {str(ex)}'}
        return result
    if state:
        state = state[0]
        result = {'result': True, 'content': state}
        return result
    else:
        str_out = f'Failed to get amount price for {table_name}' # pylint: disable=redefined-outer-name
        f_name = inspect.currentframe().f_code.co_name
        result = {'result': False, 'content': f'{f_name}. {str_out}'}
        return result
