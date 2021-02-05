from origin.client import server_connection

from origin.client import float_field
from origin.client import integer_field
from origin.client import string_field
from origin import data_types, registration_validation


import zmq
import struct
import json


def decode(measurement_type: str) -> str:
    """
    Decodes a measurement type of it happens to be a specific class
    TODO is this necessary? Those classes are empty.
    Args:
        measurement_type : string describing the data type a measurement should be. If not in
            data_types, checks and translates for a 'float_field', 'integer_field' or
            'integer_field' class

    Returns:
        The data type description string. If measurement_type is a valid key for data_types,
            the argument is returned, other wise the argument is translated (if valid) or a KeyError
            is raised

    Raises:
        KeyError if measurement_type is not a valid data type
    """
    try:
        tmp = data_types[measurement_type]
    except KeyError:
        if measurement_type == float_field:
            return "float"
        if measurement_type == integer_field:
            return "int"
        if measurement_type == string_field:
            return "string"
        raise
    else:
        return measurement_type


def declaration_formatter(stream, records, key_order):
    dec_str = [stream]
    for key in key_order:
        dec_str.append(':'.join([key, records[key]]))
    return ','.join(dec_str)


def format_stream_declaration(stream, records, key_order, encoding_format):
    measurements = records.keys()
    sent_dict = {}
    for m in measurements:
        try:
            decoded_type = decode(records[m])
        except KeyError as e:
            print(f"{records[m]} is not a valid data type. Programming error. Error should be "
                  f"caught before this")
            return None
        else:
            sent_dict[m] = decoded_type
    if (encoding_format is not None) and (encoding_format.lower() == "json"):
        if key_order is not None:
            msg = (
                "Warning: JSON formatting selected and a key order has been "
                "defined. JSON object order is not gaurenteed therefore it is "
                "not recommended to use binary data packets."
            )
            print(msg)
        # make deterministic
        return json.dumps((stream, sent_dict), sort_keys=True)
    else:
        return declaration_formatter(stream, sent_dict, key_order)


class Server:
    def __init__(self, config):
        self.config = config

    def ping(self):
        return True

    def register_stream(self, stream, records, key_order=None, data_format=None, timeout=1000):
        valid = registration_validation(stream, records, key_order)

        if not valid:
            print("invalid stream declaration")
            return None

        port = self.config.get('Server', "register_port")
        msgport = self.config.get('Server', "measure_port")
        if data_format == 'json':
            port = self.config.get('Server', "json_register_port")
            msgport = self.config.get('Server', "json_measure_port")

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, timeout)
        socket.setsockopt(zmq.LINGER, 0)
        host = self.config.get('Server', "ip")
        socket.connect("tcp://%s:%s" % (host, port))

        if (key_order is None) and (data_format is None):
            key_order = records.keys()
        registerComm = format_stream_declaration(stream, records, key_order, data_format)

        if registerComm is None:
            if data_format is None:
                data_format = "comma-separated values"
            print("can't format stream into {}".format(data_format))
            return None

        socket.send(registerComm, zmq.NOBLOCK)
        try:
            confirmation = socket.recv()
        except Exception as e:
            print(f"Problem registering stream: {stream}\nError = {e}")
            print("Server did not respond in time")  # TODO how do we  know ??
            return None
        return_code, msg = str(confirmation).split(',', 1)
        print(return_code, msg)

        if int(return_code) != 0:
            print(f"Problem registering stream {stream}")
            print(msg)
            return None

        streamID, version = struct.unpack("!II", msg)
        print(f"successfully registered with streamID: {streamID}, version: {version}")
        socket.close()  # I 'm pretty sure we need this here

        # error checking
        socket_data = context.socket(zmq.PUSH)
        socket_data.connect("tcp://%s:%s" % (host, msgport))
        return server_connection(
            self.config,
            stream,
            streamID,
            key_order,
            data_format,
            records,
            context,
            socket_data
        )
