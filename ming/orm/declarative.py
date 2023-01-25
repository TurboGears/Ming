import warnings
warnings.warn(
    'ming.orm.declarative is deprecated. Please use ming.odm.declarative instead',
    DeprecationWarning, stacklevel=2)
from ming.odm.declarative import *
