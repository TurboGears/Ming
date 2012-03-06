import warnings
warnings.warn(
    'ming.orm.ormsession is deprecated. Use ming.odm.odmsession instead',
    DeprecationWarning)
from ming.odm.odmsession import *

ORMSession = ODMSession
ThreadLocalORMSession = ThreadLocalODMSession
ContextualORMSession = ContextualODMSession
