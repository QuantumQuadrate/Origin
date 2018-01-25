"""
Load up Origin server package content in a convient way
"""

from origin.server.origin_measurement_validation import measurement_validation

from origin.server.origin_template_validation import template_validation

from origin.server.origin_destination import Destination


# if you dont want to install these modules then just comment the ones you dont want to use
from origin.server.origin_hdf5_destination import HDF5Destination
#from origin.server.origin_mysql_destination import MySQLDestination
#from origin.server.origin_filesystem_destination import FilesystemDestination
#from origin.server.origin_mongodb_destination import MongoDBDestination
