"""marufs ioctl constants and ctypes structures.

Encodes Linux ioctl numbers using the standard _IOC convention:
  bits[31:30] = direction (NONE=0, WRITE=1, READ=2)
  bits[29:16] = size of argument struct
  bits[15:8]  = type/magic byte
  bits[7:0]   = command number (nr)

Matches kernel header: marufs_layout.h.
"""

import ctypes

# ---------------------------------------------------------------------------
# Linux ioctl encoding helpers
# ---------------------------------------------------------------------------

_IOC_NRBITS = 8
_IOC_TYPEBITS = 8
_IOC_SIZEBITS = 14
_IOC_DIRBITS = 2

_IOC_NRSHIFT = 0
_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
_IOC_DIRSHIFT = _IOC_SIZESHIFT + _IOC_SIZEBITS

_IOC_NONE = 0
_IOC_WRITE = 1
_IOC_READ = 2


def _IOC(dir: int, type: int, nr: int, size: int) -> int:  # noqa: A002, N802
    return (
        (dir << _IOC_DIRSHIFT)
        | (type << _IOC_TYPESHIFT)
        | (nr << _IOC_NRSHIFT)
        | (size << _IOC_SIZESHIFT)
    )


def _IOW(type: int, nr: int, size: int) -> int:  # noqa: A002, N802
    return _IOC(_IOC_WRITE, type, nr, size)


def _IOR(type: int, nr: int, size: int) -> int:  # noqa: A002, N802
    return _IOC(_IOC_READ, type, nr, size)


def _IOWR(type: int, nr: int, size: int) -> int:  # noqa: A002, N802
    return _IOC(_IOC_READ | _IOC_WRITE, type, nr, size)


# ---------------------------------------------------------------------------
# marufs magic byte and limits
# ---------------------------------------------------------------------------

MARUFS_MAGIC = 0x58  # ord('X')
MARUFS_NAME_MAX = 63  # max name length (kernel MARUFS_NAME_MAX, on-disk index)

# ---------------------------------------------------------------------------
# ctypes structures matching kernel-side structs (marufs_layout.h)
# ---------------------------------------------------------------------------


class MarufsNameOffsetReq(ctypes.Structure):
    """Argument for MARUFS_IOC_NAME_OFFSET / MARUFS_IOC_CLEAR_NAME."""

    _pack_ = 1
    _fields_ = [
        ("name", ctypes.c_char * (MARUFS_NAME_MAX + 1)),
        ("offset", ctypes.c_uint64),
        ("name_hash", ctypes.c_uint64),
    ]


class MarufsFindNameReq(ctypes.Structure):
    """Argument for MARUFS_IOC_FIND_NAME (global name lookup)."""

    _pack_ = 1
    _fields_ = [
        ("name", ctypes.c_char * (MARUFS_NAME_MAX + 1)),
        ("region_name", ctypes.c_char * (MARUFS_NAME_MAX + 1)),
        ("offset", ctypes.c_uint64),
        ("name_hash", ctypes.c_uint64),
    ]


class MarufsBatchFindEntry(ctypes.Structure):
    """Single entry for MARUFS_IOC_BATCH_FIND_NAME."""

    _pack_ = 1
    _fields_ = [
        ("name", ctypes.c_char * (MARUFS_NAME_MAX + 1)),
        ("region_name", ctypes.c_char * (MARUFS_NAME_MAX + 1)),
        ("offset", ctypes.c_uint64),
        ("name_hash", ctypes.c_uint64),
        ("status", ctypes.c_int32),
        ("_pad", ctypes.c_uint8 * 4),
    ]


class MarufsBatchFindReq(ctypes.Structure):
    """Header for MARUFS_IOC_BATCH_FIND_NAME."""

    _pack_ = 1
    _fields_ = [
        ("count", ctypes.c_uint32),
        ("found", ctypes.c_uint32),
        ("entries", ctypes.c_uint64),
    ]


MARUFS_BATCH_FIND_MAX = 32  # max entries per batch find ioctl call


class MarufsBatchNameOffsetEntry(ctypes.Structure):
    """Single entry for MARUFS_IOC_BATCH_NAME_OFFSET."""

    _pack_ = 1
    _fields_ = [
        ("name", ctypes.c_char * (MARUFS_NAME_MAX + 1)),
        ("offset", ctypes.c_uint64),
        ("name_hash", ctypes.c_uint64),
        ("status", ctypes.c_int32),
        ("_pad", ctypes.c_uint8 * 4),
    ]


class MarufsBatchNameOffsetReq(ctypes.Structure):
    """Header for MARUFS_IOC_BATCH_NAME_OFFSET."""

    _pack_ = 1
    _fields_ = [
        ("count", ctypes.c_uint32),
        ("stored", ctypes.c_uint32),
        ("entries", ctypes.c_uint64),
    ]


MARUFS_BATCH_STORE_MAX = 32  # max entries per batch store ioctl call


class MarufsPermReq(ctypes.Structure):
    """Argument for MARUFS_IOC_PERM_GRANT / MARUFS_IOC_PERM_REVOKE / MARUFS_IOC_PERM_SET_DEFAULT."""

    _pack_ = 1
    _fields_ = [
        ("node_id", ctypes.c_uint32),
        ("pid", ctypes.c_uint32),
        ("perms", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class MarufsChownReq(ctypes.Structure):
    """Argument for MARUFS_IOC_CHOWN (ownership transfer to caller)."""

    _pack_ = 1
    _fields_ = [
        ("reserved", ctypes.c_uint32),
    ]


# ---------------------------------------------------------------------------
# Permission flags
# ---------------------------------------------------------------------------

PERM_READ = 0x0001
PERM_WRITE = 0x0002
PERM_DELETE = 0x0004
PERM_ADMIN = 0x0008
PERM_IOCTL = 0x0010
PERM_GRANT = 0x0020
PERM_ALL = 0x003F

# ---------------------------------------------------------------------------
# ioctl command numbers
# ---------------------------------------------------------------------------

MARUFS_IOC_NAME_OFFSET = _IOW(
    MARUFS_MAGIC, 1, ctypes.sizeof(MarufsNameOffsetReq)
)  # register name-ref in global index
MARUFS_IOC_FIND_NAME = _IOWR(
    MARUFS_MAGIC, 2, ctypes.sizeof(MarufsFindNameReq)
)  # global name lookup → (region_name, offset)
MARUFS_IOC_CLEAR_NAME = _IOW(
    MARUFS_MAGIC, 3, ctypes.sizeof(MarufsNameOffsetReq)
)  # remove name-ref from global index
MARUFS_IOC_PERM_GRANT = _IOW(
    MARUFS_MAGIC, 10, ctypes.sizeof(MarufsPermReq)
)  # grant permissions
MARUFS_IOC_PERM_REVOKE = _IOW(
    MARUFS_MAGIC, 11, ctypes.sizeof(MarufsPermReq)
)  # revoke permissions
MARUFS_IOC_PERM_SET_DEFAULT = _IOW(
    MARUFS_MAGIC, 13, ctypes.sizeof(MarufsPermReq)
)  # set default perms
MARUFS_IOC_CHOWN = _IOW(
    MARUFS_MAGIC, 14, ctypes.sizeof(MarufsChownReq)
)  # transfer ownership to caller
MARUFS_IOC_BATCH_FIND_NAME = _IOWR(
    MARUFS_MAGIC, 4, ctypes.sizeof(MarufsBatchFindReq)
)  # batch global name lookup
MARUFS_IOC_DAX_MMAP = _IOWR(
    MARUFS_MAGIC, 5, 16
)  # DAX mmap (xcfs_dax_mmap_req: u64 size + u64 addr)
MARUFS_IOC_BATCH_NAME_OFFSET = _IOWR(
    MARUFS_MAGIC, 6, ctypes.sizeof(MarufsBatchNameOffsetReq)
)  # batch name-ref registration
MARUFS_IOC_DMABUF_EXPORT = _IOWR(
    MARUFS_MAGIC, 0x50, 16
)  # DMA-BUF export (xcfs_dmabuf_req: u64 size + s32 fd + u32 flags)
