import warnings
warnings.warn(
    'ming.orm.middleware is deprecated. Please use ming.odm.middleware instead',
    DeprecationWarning, stacklevel=2)
from ming.odm.middleware import *
