import warnings
warnings.warn(
    'ming.orm.base is deprecated. Please use ming.odm.base instead',
    DeprecationWarning, stacklevel=2)
from ming.odm.base import *
