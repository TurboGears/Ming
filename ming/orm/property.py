import warnings
warnings.warn(
    'ming.orm.property is deprecated. Please use ming.odm.property instead',
    DeprecationWarning)
from ming.odm.property import *
