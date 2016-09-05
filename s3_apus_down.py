# -*- coding: utf-8 -*-
"""
在一小时之内，持续不断分文件下载
"""
import Queue
import logging
import optparse
import os
import threading
import time

from s3_utils import get_date_hour, list_object_keys, download_object

# the break of get s3_keys
SYNC_INTERVAL = 60

# args from command line
bucket = None
prefix = None
save_folder = None
date_hour = None
list_once = False
work = 20

# control run state
is_stop_list = False
downloading_keys = []
lock = threading.Lock()
download_queue = Queue.Queue()


# put s3_key to downloading list
def add_downloading(k):
    with lock:
        downloading_keys.append(k)
        logging.info('ADD++ %d task is running', len(downloading_keys))


# remove s3_key to downloading list
def finish_downloading(k):
    with lock:
        downloading_keys.remove(k)
        logging.info('DEL-- %d task is running', len(downloading_keys))


# check the s3_key is downloading
def is_already_down(k):
    file_name = os.path.basename(k)
    abs_file_name = '%s/%s' % (save_folder, file_name)
    abs_file_name_temp = abs_file_name + '.downloading'
    return k in downloading_keys or os.path.exists(abs_file_name) or os.path.exists(abs_file_name_temp)


# consume the download_queue and download
def do_down():
    while True:
        task_args = None
        try:
            task_args = download_queue.get(timeout=2)
            download_object(*task_args)
            download_queue.task_done()
        except Queue.Empty:
            if is_stop_list:
                break
        except Exception, e:
            logging.error(e)
        if task_args:
            finish_downloading(task_args[1])

    logging.info('down worker is stoped')


# get list of s3_key and add download task to download_queue
def do_list():
    global is_stop_list
    while True:
        current_hour = get_date_hour()
        try:
            # list files
            logging.info('list %s %s', bucket, prefix)
            keys = list_object_keys(bucket, prefix)

            # add download task to queue
            for k in keys:
                if is_already_down(k):
                    continue
                add_downloading(k)
                download_queue.put([bucket, k, save_folder])
        except Exception, e:
            logging.error(e)
        finally:
            if list_once or current_hour != date_hour:
                is_stop_list = True
                break

        time.sleep(SYNC_INTERVAL)

    logging.info('list worker is stoped')


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-b", "--bucket", dest="bucket", default='launcher-ec2-logs', help="s3 bucket")
    parser.add_option("-l", "--logname", dest="logname", help="log name, like: api/record")
    parser.add_option("-f", "--folder", dest="folder", help="local data folder, default: {logname}/{datahour}.tmp")
    parser.add_option("-d", "--datahour", dest="datahour", help="date hour, default: current data hour")
    parser.add_option("-w", "--work", dest="work", default=20, help="work count")
    (options, args) = parser.parse_args()

    # only logname is required
    if options.logname is None:
        parser.print_help()
        exit(1)

    bucket = options.bucket
    logname = options.logname
    save_folder = options.folder
    work = options.work

    date_hour = get_date_hour()
    list_once = False
    if options.datahour:
        date_hour = options.datahour
        list_once = True

    prefix = '%s/%s' % (logname, date_hour)

    if options.folder is None:
        # TODO debug save path
        save_folder = '/data/tmp/importer/test/%s/%s.tmp' % (os.path.basename(logname), date_hour)

    logging.info('begin download: %s %s to %s', bucket, prefix, save_folder)

    threads = []

    # download threads, consumer of download_queue
    for i in range(work):
        t = threading.Thread(target=do_down)
        threads.append(t)
        t.start()

    # list threads, producer of download_queue
    t = threading.Thread(target=do_list)
    threads.append(t)
    t.start()

    # waiting for all thead
    [t.join() for t in threads]
    logging.info('down finished')
