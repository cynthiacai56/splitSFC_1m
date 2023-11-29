from numba import jit, int32, int64


###############################################################################
######################      Morton conversion in 2D      ######################
###############################################################################

@jit(int64(int32))
def Expand2D(n):
    """
    Encodes the 64 bit morton code for a 31 bit number in the 2D space using
    a divide and conquer approach for separating the bits.
    1 bit is not used because the integers are not unsigned

    Args:
        n (int): a 2D dimension

    Returns:
        int: 64 bit morton code in 2D

    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
    """
    if n < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")

    b = n & 0x7fffffff
    b = (b ^ (b << 16)) & 0x0000ffff0000ffff
    b = (b ^ (b << 8)) & 0x00ff00ff00ff00ff
    b = (b ^ (b << 4)) & 0x0f0f0f0f0f0f0f0f
    b = (b ^ (b << 2)) & 0x3333333333333333
    b = (b ^ (b << 1)) & 0x5555555555555555
    return b

@jit(int64(int32, int32))
def EncodeMorton2D(x, y):
    """
    Calculates the 2D morton code from the x, y dimensions

    Args:
        x (int): the x dimension
        y (int): the y dimension

    Returns:
        int: 64 bit morton code in 2D

    """
    return Expand2D(x) + (Expand2D(y) << 1)


