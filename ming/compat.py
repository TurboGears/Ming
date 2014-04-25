def smallest_diff_key(A, B):
    """return the smallest key adiff in A such that A[adiff] != B[bdiff]"""
    diff_keys = [k for k in A if A.get(k) != B.get(k)]
    if diff_keys:
        return min(diff_keys)
    return None

def base_cmp(a, b):
    return (a > b) - (a < b)

# http://stackoverflow.com/questions/3484293/is-there-a-description-of-how-cmp-works-for-dict-objects-in-python-2
def dict_cmp(A, B):
    if len(A) != len(B):
        return base_cmp(len(A), len(B))
    adiff = smallest_diff_key(A, B)
    bdiff = smallest_diff_key(B, A)
    if adiff is None or bdiff is None:
        return 0
    if adiff != bdiff:
        return base_cmp(adiff, bdiff)
    return base_cmp(A[adiff], B[bdiff])
