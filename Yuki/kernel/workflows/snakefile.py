"""Snakefile builder used by workflow backends."""
# pylint: disable=cyclic-import

class SnakeFile:
    """ Helper class to write a snakefile
    """

    def __init__(self, file_path):
        """ Initialize the class use a path
        create a file if it is not initially exists
        """
        self.file_path = file_path
        self.contents = ""

    def addline(self, string, index):
        """Add a line to the snakefile with given indentation level."""
        self.contents += (" "*index*4 + string + "\n")

    def write(self):
        """Write the snakefile contents to disk."""
        with open(self.file_path, "w", encoding='utf-8') as f:
            f.write(self.contents)
