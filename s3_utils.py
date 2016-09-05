# -*- coding: utf-8 -*-
"""
s3工具
"""
import commands
import datetime
import json
import logging
import os
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)-5s] %(message)s',
                    datefmt='%m-%d %H:%M:%S')

# retry config
DEFAULT_RETRY_TIMES = 10  # 默认重试次数
DEFAULT_RETRY_SECONDS = 0.15  # 重试间隔


def run_cmd(cmd, retrys=DEFAULT_RETRY_TIMES, interval=DEFAULT_RETRY_SECONDS):
    """带重试执行shell命令"""
    rs = None
    for i in range(retrys + 1):
        if i > 0:
            logging.info('after %0.2fs will retry..', interval)
            time.sleep(interval)

        logging.debug('try to run: %s', cmd)
        status, output = commands.getstatusoutput(cmd)
        if status == 0:
            rs = output
            break
        else:
            logging.error('fail to exec: %s %s', cmd, output)

    return rs


def list_object_keys(bucket, prefix):
    list_cmd = 'aws s3api list-objects --bucket %s --prefix %s' % (bucket, prefix)
    s = run_cmd(list_cmd)
    if s is None:
        raise IOError('fail to list objects: %s/%s' % (bucket, prefix))

    # parse
    json_obj = json.loads(s)
    return [s3_obj['Key'] for s3_obj in json_obj['Contents']]


def download_object(bucket, k, save_folder):
    file_name = os.path.basename(k)
    abs_file_name = '%s/%s' % (save_folder, file_name)
    abs_file_name_temp = abs_file_name + '.downloading'
    if os.path.exists(abs_file_name) or os.path.exists(abs_file_name_temp):
        logging.debug('%s is exists', abs_file_name)
        return

    run_cmd('mkdir -p %s' % save_folder)

    down_cmd = 'aws s3api get-object --bucket %s --key %s %s' % (bucket, k, abs_file_name_temp)
    s = run_cmd(down_cmd)
    if s is None:
        os.remove(abs_file_name_temp)
        raise IOError('fail to download object: %s/%s' % (bucket, k))

    os.rename(abs_file_name_temp, abs_file_name)
    logging.info('success to download object: %s/%s', bucket, k)


def get_date_hour(h=0, fmt='%Y%m%d-%H'):
    dt = datetime.datetime.now()
    dt = dt + datetime.timedelta(hours=h)
    return dt.strftime(fmt)


# test
if __name__ == '__main__':
    print get_date_hour()
    print list_object_keys('xxxx', 'test')
