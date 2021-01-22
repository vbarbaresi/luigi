""" Library of utils shared between contrib modules """
import os
import io


class _DeleteOnCloseFile(io.FileIO):
    def close(self):
        super(_DeleteOnCloseFile, self).close()
        try:
            os.remove(self.name) # pylint: disable=E1101
        except OSError:
            # Catch a potential threading race condition and also allow this
            # method to be called multiple times.
            pass

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True