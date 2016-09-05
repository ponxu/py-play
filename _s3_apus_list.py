# -*- coding: utf-8 -*-
"""
列出s3文件，形成下载任务
"""
import time

from s3_utils import TASK_FILE

for i in range(9999999):
    with open(TASK_FILE, 'a+') as f:
        f.write('>>>>>>>>>>>>>>>>>>>>>>>>>>>> %d\n' % i)
    time.sleep(1)
