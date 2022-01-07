import prodigy
import multiprocessing
import os
from sanic.log import logger



def start_new_instance(
    dataset, task, source_file, label_file, port=8080):

    os.environ['PRODIGY_BASIC_AUTH_USER'] = 'babar'
    os.environ['PRODIGY_BASIC_AUTH_PASS'] = 'babar'

    logger.info(f'Starting new instance on port {port}')
    p = multiprocessing.Process(
        target=prodigy.serve,
        name=f'{dataset}-progeny',
        args=(f'{task} {dataset} {source_file} --label {label_file}',),
        kwargs={'port': port}
    )
    p.start()

    del(os.environ['PRODIGY_BASIC_AUTH_USER'])
    del(os.environ['PRODIGY_BASIC_AUTH_PASS'])

    return p
