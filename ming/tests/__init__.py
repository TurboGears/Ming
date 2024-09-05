import base64
from contextlib import contextmanager
import os
import random


@contextmanager
def push_seed(seed):
    rstate = random.getstate()
    random.seed(seed)
    try:
        yield
    finally:
        random.setstate(rstate)


def make_encryption_key(seed=__name__):
    with push_seed(seed):
        return base64.b64encode(os.urandom(96)).decode('ascii')
