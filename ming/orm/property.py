import warnings
warnings.warn(
    'ming.orm.property is deprecated. Please use ming.odm.property instead',
    DeprecationWarning, stacklevel=2)
from ming.odm.property import *
