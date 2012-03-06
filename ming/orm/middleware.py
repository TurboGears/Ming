import warnings
warnings.warn(
    'ming.orm.middleware is deprecated. Please use ming.odm.middleware instead',
    DeprecationWarning)
from ming.odm.middleware import *
