import enum

DEFAULT_PORT = 53045

DEFAULT_XPUB_PORT = 53044

DEFAULT_XSUB_PORT = 53043

# Enum to hold application type
# CLIENT_APP = connects to service, standard building block
# SERVICE_APP = opens a connection and routes traffic (exactly how to be determined!)
class AppType(enum.Enum):
    CLIENT_APP = 1
    SERVICE_APP = 2