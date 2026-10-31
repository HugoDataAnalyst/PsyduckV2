"""
S2 cell id -> corner coordinates, without an S2 dependency.

Golbat's weather table stores each cell's id and centre but not its corners,
which the weather sync needs to match a cell against every area it overlaps.
This is the part of S2 that decodes a cell id (face + Hilbert position) into
its i/j leaf coordinates and projects the cell's four corners back to
latitude/longitude - ported from the reference implementation (s2sphere /
Google S2), quadratic projection. Cells never change, so corners are cached.
"""
from __future__ import annotations

import math
from functools import lru_cache

_MAX_LEVEL = 30
_LOOKUP_BITS = 4
_SWAP_MASK = 1
_INVERT_MASK = 2
_POS_TO_IJ = ((0, 1, 3, 2), (0, 2, 3, 1), (3, 2, 0, 1), (3, 1, 0, 2))
_POS_TO_ORIENTATION = (_SWAP_MASK, 0, 0, _INVERT_MASK | _SWAP_MASK)
_LOOKUP_IJ = [0] * (1 << (2 * _LOOKUP_BITS + 2))


def _init_lookup(level: int, i: int, j: int, orig: int, pos: int, orientation: int) -> None:
    if level == _LOOKUP_BITS:
        _LOOKUP_IJ[(pos << 2) + orig] = (((i << _LOOKUP_BITS) + j) << 2) + orientation
        return
    level, i, j, pos = level + 1, i << 1, j << 1, pos << 2
    r = _POS_TO_IJ[orientation]
    for index in range(4):
        _init_lookup(level, i + (r[index] >> 1), j + (r[index] & 1), orig, pos + index,
                     orientation ^ _POS_TO_ORIENTATION[index])


for _o in range(4):
    _init_lookup(0, 0, 0, _o, 0, _o)


def _face_ij(cell_id: int) -> tuple[int, int, int]:
    face = cell_id >> 61
    bits = face & _SWAP_MASK
    i = j = 0
    for k in range(7, -1, -1):
        nbits = _MAX_LEVEL - 7 * _LOOKUP_BITS if k == 7 else _LOOKUP_BITS
        bits += ((cell_id >> (k * 2 * _LOOKUP_BITS + 1)) & ((1 << (2 * nbits)) - 1)) << 2
        bits = _LOOKUP_IJ[bits]
        i += (bits >> (_LOOKUP_BITS + 2)) << (k * _LOOKUP_BITS)
        j += ((bits >> 2) & ((1 << _LOOKUP_BITS) - 1)) << (k * _LOOKUP_BITS)
        bits &= _SWAP_MASK | _INVERT_MASK
    return face, i, j


def _st_to_uv(s: float) -> float:
    return (4 * s * s - 1) / 3 if s >= 0.5 else (1 - 4 * (1 - s) * (1 - s)) / 3


def _uv_to_latlon(face: int, u: float, v: float) -> tuple[float, float]:
    x, y, z = ((1, u, v), (-u, 1, v), (-u, -v, 1), (-1, -v, -u), (v, -1, -u), (v, u, -1))[face]
    return math.degrees(math.atan2(z, math.hypot(x, y))), math.degrees(math.atan2(y, x))


@lru_cache(maxsize=20_000)
def corners(cell_id: int) -> tuple[tuple[float, float], ...]:
    """The cell's 4 corners as (lat, lon), in Golbat's webhook `polygon` order. Accepts signed int64 ids."""
    cid = cell_id & 0xFFFFFFFFFFFFFFFF
    level = _MAX_LEVEL - ((cid & -cid).bit_length() - 1) // 2
    face, i, j = _face_ij(cid)
    size = 1 << (_MAX_LEVEL - level)
    i0, j0 = i & -size, j & -size
    scale = 1 / (1 << _MAX_LEVEL)
    return tuple(_uv_to_latlon(face, _st_to_uv(ci * scale), _st_to_uv(cj * scale))
                 for ci, cj in ((i0, j0), (i0 + size, j0), (i0 + size, j0 + size), (i0, j0 + size)))
