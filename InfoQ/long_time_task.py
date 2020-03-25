#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, time, random

from multiprocessing import Pool


def task():
    print('Parent process %s.' % os.getpid())
    p = Pool(4)
    for i in range(5):
        p.apply_async(main, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')


def main(name):
    print('Run Task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(random.random() * 3)
    print('Task %s runs %0.2f seconds.' % (name, (time.time() - start)))


if __name__ == '__main__':
    main("哈哈哈")
    task()
