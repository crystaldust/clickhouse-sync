import itertools
import random
from os import environ

from alive_progress import alive_bar

from ck import CKServer

src_server: CKServer = None
dst_server: CKServer = None
bars = ['smooth', 'classic', 'classic2', 'brackets', 'blocks', 'bubbles', 'solid', 'checks', 'circles', 'squares',
        'halloween', 'filling', 'notes', 'ruler', 'ruler2', 'fish', 'scuba']
spinners = ['classic', 'stars', 'twirl', 'twirls', 'horizontal', 'vertical', 'waves', 'waves2', 'waves3', 'dots', 'it',
            'triangles', 'brackets', 'bubbles', 'circles', 'squares', 'flowers', 'elements', 'loving', 'notes',
            'notes2', 'arrow', 'arrows', 'arrows2', 'radioactive', 'boat', 'fish', 'fish2', 'fishes']
random.shuffle(bars)
random.shuffle(spinners)
bars_iter = itertools.cycle(bars)
spinners_iter = itertools.cycle(spinners)


def init():
    dst_ck_host = environ.get('DST_CK_HOST')
    dst_ck_port = environ.get('DST_CK_PORT')
    dst_ck_user = environ.get('DST_CK_USER')
    dst_ck_pass = environ.get('DST_CK_PASS')
    dst_ck_db = environ.get('DST_CK_DB')

    src_ck_host = environ.get('SRC_CK_HOST')
    src_ck_port = environ.get('SRC_CK_PORT')
    src_ck_user = environ.get('SRC_CK_USER')
    src_ck_pass = environ.get('SRC_CK_PASS')
    src_ck_db = environ.get('SRC_CK_DB')
    global src_server, dst_server
    src_server = CKServer(src_ck_host, src_ck_port, src_ck_user, src_ck_pass, src_ck_db)
    dst_server = CKServer(dst_ck_host, dst_ck_port, dst_ck_user, dst_ck_pass, dst_ck_db)


def sync(tables, batch_size=5000):
    if not src_server or not dst_server:
        init()
    for table in tables:
        i = 0
        total_inserted = 0
        total = src_server.execute_no_params(f'SELECT count() FROM {table}')[0][0]
        with alive_bar(total, title=f'Sync {table}', manual=True, bar=next(bars_iter),
                       spinner=next(spinners_iter)) as bar:
            while True:
                read_result = src_server.execute_no_params(
                    f'SELECT * FROM {table} LIMIT {batch_size} OFFSET {i * batch_size}')
                i += 1
                if read_result:
                    num_inserted = dst_server.execute(f'INSERT INTO {table} VALUES', read_result)
                    total_inserted += num_inserted
                    bar(total_inserted / total)
                    # for _ in range(num_inserted):
                    #     bar()
                else:
                    break
