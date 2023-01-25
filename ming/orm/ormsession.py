import warnings
warnings.warn(
    'ming.orm.ormsession is deprecated. Use ming.odm.odmsession instead',
    DeprecationWarning, stacklevel=2)
from ming.odm.odmsession import *

ORMSession = ODMSession
ThreadLocalORMSession = ThreadLocalODMSession
ContextualORMSession = ContextualODMSession
