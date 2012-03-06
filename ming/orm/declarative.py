import warnings
warnings.warn(
    'ming.orm.declarative is deprecated. Please use ming.odm.declarative instead',
    DeprecationWarning)
from ming.odm.declarative import *
