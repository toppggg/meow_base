# Taken from https://www.oreilly.com/library/view/python-cookbook/0596001673/ch04s25.html
# flags are removed
import os

# needs win32all to work on Windows
if os.name == 'nt':
    import win32con, win32file, pywintypes
    LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
    LOCK_SH = 0 # the default
    LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
    __overlapped = pywintypes.OVERLAPPED(  )

    def lock(file):
        pass
        # hfile = win32file._get_osfhandle(file.fileno(  ))
        # win32file.LockFileEx(hfile, None, 0, 0xffff0000, __overlapped)

    def unlock(file):
        # hfile = win32file._get_osfhandle(file.fileno(  ))
        # win32file.UnlockFileEx(hfile, 0, 0xffff0000, __overlapped)
        pass

elif os.name == 'posix':
    from fcntl import LOCK_EX, LOCK_SH, LOCK_NB

    def lock(file):
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)

    def unlock(file):
        # lock_handle.close()
        pass
else:
    raise RuntimeError("PortaLocker only defined for nt and posix platforms")