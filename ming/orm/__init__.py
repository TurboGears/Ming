import warnings
warnings.warn(
    'ming.orm is deprecated. Please use ming.odm instead',
    DeprecationWarning, stacklevel=2)
from ming.odm import *

ORMSession=ODMSession
ThreadLocalORMSession=ThreadLocalODMSession
ContextualORMSession=ContextualODMSession

