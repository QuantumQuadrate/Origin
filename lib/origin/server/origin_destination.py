"""
This module provides the Destination class that holds all the basic API methods
for interfacing with a destination.

Destintations are databases, filesystems, file formats (HDF5, CSV), etc.
"""

import struct
import numpy as np


from origin.server import template_validation
from origin.server import measurement_validation
from origin import data_types, current_time, TIMESTAMP, registration_validation

###############################################################################
#
#   Metadata format:
#
#   knownStreams : {
#       `stream name` : {
#           stream     : `stream_name`,
#           id         : `stream_id`,
#           version    : `version`,
#           key_order  : `key_order`,
#           format_str : `format_str`,
#           definition : { # most recent definition object
#               `field1` : { "type":data_type, "key_index": `index` },
#               `field2` : { "type":data_type, "key_index": `index` },
#               ...
#           }
#           # optional dict of older versions, version number is the hash
#           versions    : {
#               1   : `defintion_obj`, # see above for definition format
#               2   : `defintion_obj`,
#               ...
#           }
#       },
#       ...
#   }
#
#   # current versiion definitions
#   knownStreamVersions : {
#       stream  : `definition_obj,
#       stream  : `definition_obj,
#       ...
#   }
#
###############################################################################


class Destination(object):
    """!@brief A class representing a data storage location, such as a database,
    filesystem, or file format.

    This class defines a common API that a real destination class must inherit
    from. Destination specific methods that are required will raise a
    NotImplementedError if not overridden.
    """

    def __init__(self, logger, config):
        """!@brief Initialize the destination.

        @param logger pass in a logger object
        @param config configuration object
        """

        self.logger = logger
        self.config = config
        self.known_streams = {}
        self.known_stream_versions = {}

        self.connect()
        self.read_stream_def_table()

    def connect(self):
        """!@brief Prepare the backend.

        This method must be overwritten in the specific implementation.
        """
        raise NotImplementedError

    def close(self):
        """!@brief Close the backend connection cleanly.

        This method must be overwritten in the specific implementation.
        """
        raise NotImplementedError

    def read_stream_def_table(self):
        """!@brief Read stored metadata and populate the knownStreams, and
        knownStreamVersions dictionaries.

        This method must be overwritten in the specific implementation.
        """
        raise NotImplementedError

    def create_new_stream(self, stream, version, template, key_order):
        """!@brief Create a new stream or create a new version of a stream based on
        a stream template.

        This method also enters format_str into the knownStreams dict.

        @param stream a string containing the stream name
        @param version the version number of the new stream
        @param template the template dictionary for the new stream
        @param key_order an ordered list of key strings that define the order that
            data is expected in
        @return stream_id the id number for the new stream
        """

        # generate format_str for stream if possible, strings are not supported
        # in binary, do early to trigger exception before write to disk
        err, format_str = self.format_string(template, key_order)
        if err > 0:
            format_str = ''
        stream_id = self.get_stream_id(stream)

        definition = {}

        if key_order is not None:
            for i, key in enumerate(key_order):
                k = key.strip()
                definition[k] = {"type": template[k], "key_index": i}
        else:
            for key in template:
                k = key.strip()
                definition[k] = {"type": template[k], "key_index": -1}

        if version > 1:
            stream_obj = self.known_streams[stream]
        else:
            stream_obj = {
                "stream"     : stream.strip(),
                "id"         : stream_id,
                "versions"   : []
            }

        stream_obj["version"] = version
        stream_obj["key_order"] = key_order
        stream_obj["format_str"] = format_str
        stream_obj["definition"] = definition
        stream_obj["versions"].append({
            "version"    : version,
            "key_order"  : key_order,
            "format_str" : format_str,
            "definition" : definition,
        })

        # update the stream inventory
        self.known_streams[stream] = stream_obj
        self.known_stream_versions[stream] = stream_obj['definition']
        # id might get updated, so check just in case
        stream_id = self.create_new_stream_destination(stream_obj)
        return stream_id

    def create_new_stream_destination(self, stream_obj):
        """!@brief Store a new stream or creates a new version of a stream in the
        destination.

        This method must be overwritten in the specific implementation.

        @param stream_obj the stream template dictionary
        @return stream_id the id number for the new stream
        """
        raise NotImplementedError

    def format_string(self, template, key_order):
        """!@brief Generate a format string for unpacking native data packets.

        The format string conresponds to the codes in the struct package.

        @param template the stream template dictionary
        @param key_order an ordered list of key strings that defines the order
            that native format data is expected
        @return a tuple of (error, format_str) where error=0 for success.
        """
        if key_order is None:
            return (1, "No key_order specified")

        format_str = '!'  # use network byte order
        try:
            format_str += data_types[self.config.get("Server", "timestamp_type")]["format_char"]
        except KeyError:
            format_str += data_types["uint"]["format_char"]

        for key in key_order:
            self.logger.debug('key: %s', key)
            dtype = template[key]
            try:
                format_str += data_types[dtype]["format_char"]
                if not data_types[dtype]["binary_allowed"]:
                    msg = "Unsupported type '{}' in binary data.".format(dtype)
                    return (1, msg)
            except KeyError:
                return (1, "Type \"{}\" not recognized.".format(dtype))
        return (0, format_str)

    # returns version and streamID
    def register_stream(self, stream, template, key_order=None):
        """!@brief Register a new stream or new version of a stream with the server.

        @param stream a string holding the stream name
        @param template the stream definition dictionary
        @return a tuple of (error, stream_ver) where error=0 for success, and
            stream_ver is a byte string that serves as a unique identifier.
        """
        update = False
        dest_version = None
        stream = stream.strip()
        self.logger.info("Attempt to register stream {}".format(stream))
        if stream not in self.known_streams.keys():
            update = True
            dest_version = 1
        else:
            stream_id = self.known_streams[stream]["id"]
            dest_version = self.known_streams[stream]["version"]
            if template_validation(template, self.known_stream_versions[stream]):
                msg = "Known stream, {} matching current defintion."
                msg += " No database modification needed."
                self.logger.info(msg.format(stream))
                update = False
            # its a new version
            else:
                update = True
                dest_version += 1

        if update:
            valid, msg = registration_validation(stream, template, key_order)
            if not valid:
                return (1, msg)
            stream_id = self.create_new_stream(stream, dest_version, template, key_order)
            if stream_id < 0:
                return (1, 'server error')
            # update the current streams after all that
            self.read_stream_def_table()
        return (0, struct.pack("!II", stream_id, dest_version))

    def insert_measurement(self, stream, measurements):
        """!@brief Save formated measurement to the destination.

        This method must be overwritten in the specific implementation.

        @param stream a string holding the stream name
        @param measurements a dictionary containing the data
        """
        raise NotImplementedError

    def measurement(self, stream, measurements):
        """!@brief Perfom measurement validation, timestamp data if missing field,
        then save to destination.

        @return a tuple of (error, result_text, measurements)
            error: 0 for successful operation
            result_text: message to return to client
            measurements: processed data, empty dict if error
        """
        if stream not in self.known_streams.keys():
            msg = (
                "Trying to add a measurement to data on an unknown stream: {}"
            )
            self.logger.warning(msg.format(stream))
            return (1, "Unknown stream", {})

        if not measurement_validation(measurements, self.known_stream_versions[stream]):
            msg = "Measurement didn't validate against the existing format"
            self.logger.warning(msg)
            return (1, "Invalid measurements against schema", {})

        try:
            if measurements[TIMESTAMP] == 0:
                raise KeyError
        except KeyError:
            measurements[TIMESTAMP] = current_time(self.config)

        self.insert_measurement(stream, measurements)
        result = 0
        result_text = ""
        return (result, result_text, measurements)

    def measurement_ordered(self, stream, time_stamp, measurements):
        """!@brief Process a list of implicitly ordered measurements, then save to
        destination.

        The measurement list order is defined by the key order from when the
        stream was registered.

        @param stream a string holding the stream name
        @param time_stamp the time_stamp from the data message, 0 if not
            specified
        @param measurements an ordered list of the measurement data
        @return a tuple of (error, result_text, measurements)
            error: 0 for successful operation
            result_text: message to return to client
            measurements: processed data, empty dict if error
        """
        meas = {}
        for key in self.known_stream_versions[stream]:
            idx = self.known_stream_versions[stream][key]["key_index"]
            meas[key] = measurements[idx]
        meas[TIMESTAMP] = time_stamp
        return self.measurement(stream, meas)

    def measurement_binary(self, stream, measurements):
        """!@brief Process a binary list of implicitly ordered measurements, then save
        to destination.

        @param stream a string holding the stream name
        @param measurements an ordered byte array of the measurement data
        @return a tuple of (error, result_text, measurements)
            error: 0 for successful operation
            result_text: message to return to client
            measurements: processed data, empty dict if error
        """
       fmtstr =  self.known_streams[stream]["format_str"]
        try:
            dtuple = struct.unpack_from(fmtstr, measurements)
        except:
            msg = 'Error unpacking stream data.'
            msg2 = msg + ' stream: `{}` format_str: `{}`'
            msg2 += ' measurements bytes: `{}`'
            self.logger.error(msg2)
            return (1, msg, {})
        meas = list(dtuple[1:])
        time_stamp = dtuple[0]
        return self.measurement_ordered(stream, time_stamp, meas)

    def find_stream(self, stream_id):
        """!@brief Look up a stream name based on the stream id number

        @param stream_id the id number for the stream
        @return the stream name corresponding to the stream_id
        """
        for stream in self.known_streams:
            if self.known_streams[stream]["id"] == stream_id:
                return stream
        raise ValueError

    def get_raw_stream_data(self, stream, start=None, stop=None, fields=[]):
        """!@brief Read stream data from storage between the timestamps given by
        time = [start,stop].

        This method must be overwritten in the specific implementation.

        @param stream a string holding the stream name
        @param start 32b unix timestamp that defines the start of the data
            window
        @param stop 32b unix timestamp that defines the end of the data window
        @param fields A list that contains the fields for which data is
            desired, the value of the dictionary key is arbitrary.
        @return a tuple with (error, data, msg)
            error: 0 for a successful operation
            data: data is dictionary with fields as the keys and data lists as
                the values
            msg: holds an error msg or '' if no error
        """
        raise NotImplementedError

    def get_stat_stream_data(self, stream, start=None, stop=None, fields=[]):
        """!@brief Get statistics on the stream data during the time window defined by
        time = [start, stop].

        @param stream a string holding the stream name
        @param start 32b unix timestamp that defines the start of the data
            window
        @param stop 32b unix timestamp that defines the end of the data window
        @param fields A list that contains the fields for which data is
            desired, the value of the dictionary key is arbitrary.
        @return a tuple with (error, data, msg)
            error: 0 for a successful operation
            data: data is dictionary with fields as the keys and statistical
                data sub-dictionaries as the values
            msg: holds an error msg or '' if no error
        """
        try:
            result, stream_data, result_text = self.get_raw_stream_data(
                stream,
                start=start,
                stop=stop,
                fields=fields
            )
            data = {}
            for field in stream_data:
                if field == TIMESTAMP:
                    data[field] = {
                        'start': stream_data[field][0],
                        'stop': stream_data[field][1]
                    }
                elif self.known_stream_versions[stream][field]['type'] == 'string':
                    # TODO: figure out how to handle strings
                    data[field] = stream_data[field]
                else:
                    # some stats need to be converted back to the native python
                    # type for JSON serialization
                    dtype = data_types[self.known_stream_versions[stream][field]['type']]["type"]
                    avg = np.nanmean(stream_data[field])
                    std = np.nanstd(stream_data[field])
                    max = dtype(np.nanmax(stream_data[field]))
                    min = dtype(np.nanmin(stream_data[field]))
                    data[field] = {
                        'average': avg,
                        'standard_deviation': std,
                        'max': max,
                        'min': min
                    }

        except Exception:
            self.logger.exception("Exception in server code:")
            msg = "Could not process request."
            result, data, result_text = (1, {}, msg)

        return(result, data, result_text)

    def validate_time_range(self, start, stop):
        """!@brief Make sure the time range is valid, if not rearrange start and stop
        times.

        @param start 32b unix timestamp that defines the start of the data
            window
        @param stop 32b unix timestamp that defines the end of the data window
        @return tuple of validated (start, stop) times in 32b format
        """
        try:
            stop = long(stop) * 2**32
        except TypeError:
            self.logger.debug("Using default stop time")
            stop = current_time(self.config)
        try:
            start = long(start) * 2**32
        except TypeError:
            self.logger.debug("Using default start time")
            start = stop - 5 * 60L * 2**32  # 5 minute range default
        if start > stop:
            msg = "Requested read range out of order. Swapping range."
            self.logger.warning(msg)
            msg = "Read request time range (start, stop): ({},{})"
            self.logger.debug(msg.format(start, stop))
            return (stop, start)

        msg = "Read request time range (start, stop): ({},{})"
        self.logger.debug(msg.format(start, stop))
        return (start, stop)

    def print_stream_info(self):
        """!@brief Print user readable display of current streams in destination"""
        for stream in self.known_stream_versions:
            self.logger.info("")
            self.logger.info("=" * 20 + " {} ".format(stream) + "=" * 20)
            self.logger.info("  stream_id: {}".format(self.known_streams[stream]['id']))
            for field_name in self.known_stream_versions[stream]:
                self.logger.info("  field: {} ({})".format(
                    field_name,
                    self.known_stream_versions[stream][field_name]['type']
                ))
        self.logger.info("")

    def get_stream_id(self, stream):
        """!@brief Generate a new stream id.

        The stream id is found by dynamically checking the id of all known
        streams and incrementing.

        This method could be overwritten if the destination has a built in
        way to track the total number of streams.

        @param stream a string holding the name of the stream
        @return stream_id a unique stream_id
        """
        if stream in self.known_streams:
            return self.known_streams[stream]['id']
        stream_id = 0
        for s in self.known_streams:
            sid = self.known_streams[s]['id']
            if sid > stream_id:
                stream_id = sid
        return stream_id + 1
