from shutil import ExecError


class DataSourceNotMatched(Exception):
    pass

class FileExtensionError(Exception):
    pass

class NoDataLoaded(Exception):
    pass