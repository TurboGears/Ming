import warnings
warnings.warn(
    'ming.orm.base is deprecated. Please use ming.odm.base instead',
    DeprecationWarning)
from ming.odm.base import *
