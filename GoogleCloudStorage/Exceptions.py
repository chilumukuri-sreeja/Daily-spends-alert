class DownloadFileError(Exception):
    '''Raised when downloadFile operation on Google Cloud Storage gives an error'''

class MetadataError(Exception):
    '''Raised when fetching metadata gives an error'''

class UploadFileError(Exception):
    '''Raised when uploadFile operation on Google Cloud Storage gives an error'''