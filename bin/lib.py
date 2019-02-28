import logging
import time

import numpy


def wait():
    sleep_time = .1 + numpy.random.beta(2,5)
    logging.debug('Sleeping for {} seconds'.format(sleep_time))
    time.sleep(sleep_time)
