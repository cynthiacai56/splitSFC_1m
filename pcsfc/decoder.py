import numpy as np
from numba import jit, int32, int64

@jit(int32(int64))
def Compact2D(m):
    """
    Decodes the 64 bit morton code into a 32 bit number in the 2D space using
    a divide and conquer approach for separating the bits.
    1 bit is not used because the integers are not unsigned

    Args:
        n (int): a 64 bit morton code

    Returns:
        int: a dimension in 2D space

    Raises:
        Exception: ERROR: Morton code is always positive
    """
    if m < 0:
        # raise Exception("""ERROR: Morton code is always positive""")
        m = -m

    m &= 0x5555555555555555
    # print(m)
    m = (m ^ (m >> 1)) & 0x3333333333333333
    # print(m)
    m = (m ^ (m >> 2)) & 0x0f0f0f0f0f0f0f0f
    # print(m)
    m = (m ^ (m >> 4)) & 0x00ff00ff00ff00ff
    # print(m)
    m = (m ^ (m >> 8)) & 0x0000ffff0000ffff
    # print(m)
    m = (m ^ (m >> 16)) & 0x00000000ffffffff
    # print(m)
    return m


@jit(int32(int64))
def DecodeMorton2DX(mortonCode):
    """
    Calculates the x coordinate from a 64 bit morton code

    Args:
        mortonCode (int): the 64 bit morton code

    Returns:
        int: 32 bit x coordinate in 2D

    """
    return Compact2D(mortonCode)


@jit(int32(int64))
def DecodeMorton2DY(mortonCode):
    """
    Calculates the y coordinate from a 64 bit morton code

    Args:
        mortonCode (int): the 64 bit morton code

    Returns:
        int: 32 bit y coordinate in 2D

    """
    return Compact2D(mortonCode >> 1)
