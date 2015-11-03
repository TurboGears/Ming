import warnings
warnings.warn(
    'ming.orm is deprecated. Please use ming.odm instead',
    DeprecationWarning)
from ming.odm import *

ORMSession=ODMSession
ThreadLocalORMSession=ThreadLocalODMSession
ContextualORMSession=ContextualODMSession

