from origin.client import server_connection

from origin.client import float_field
from origin.client import integer_field
from origin.client import string_field
from origin import data_types, registration_validation


import zmq
import struct
import json


def decode(measurementType):
    try:
        data_types[measurementType]
        return measurementType
    except:
        pass
    if measurementType == float_field:
        return "float"
    if measurementType == integer_field:
        return "int"
    if measurementType == string_field:
        return "string"
    return None


def declarationFormater(stream, records, keyOrder):
    decStr = [stream]
    for key in keyOrder:
        decStr.append(':'.join([key, records[key]]))
    return ','.join(decStr)


def formatStreamDeclaration(stream, records, keyOrder, format):
    measurements = records.keys()
    sentDict = {}
    for m in measurements:
        decodedType = decode(records[m])
        if decodedType is None:
            print "Programming error. Error should be caught before this"
            return None
        else:
            sentDict[m] = decodedType
    if (format is not None) and (format.lower() == "json"):
        if keyOrder is not None:
            msg = (
                "Warning: JSON formatting selected and a key order has been "
                "defined. JSON object order is not gaurenteed therefore it is "
                "not recommended to use binary data packets."
            )
            print msg
        # make deterministic
        return json.dumps((stream, sentDict), sort_keys=True)
    else:
        return declarationFormater(stream, sentDict, keyOrder)


class server:
    def __init__(self, config):
        self.config = config

    def ping(self):
        return True

    def registerStream(self, stream, records, keyOrder=None, format=None, timeout=1000):
        valid = registration_validation(stream, records, keyOrder)

        if not valid:
            print "invalid stream declaration"
            return None

        port = self.config.get('Server', "register_port")
        msgport = self.config.get('Server', "measure_port")
        if format == 'json':
            port = self.config.get('Server', "json_register_port")
            msgport = self.config.get('Server', "json_measure_port")

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, timeout)
        socket.setsockopt(zmq.LINGER, 0)
        host = self.config.get('Server', "ip")
        socket.connect("tcp://%s:%s" % (host, port))

        if (keyOrder is None) and (format is None):
            keyOrder = records.keys()
        registerComm = formatStreamDeclaration(stream, records, keyOrder, format)

        if(registerComm is None):
            if format is None:
                format = "comma-separated values"
            print "can't format stream into {}".format(format)
            return None

        socket.send(registerComm, zmq.NOBLOCK)
        try:
            confirmation = socket.recv()
        except:
            print("Problem registering stream: {}".format(stream))
            print("Server did not respond in time")
            return None
        returnCode, msg = confirmation.split(',', 1)
        print returnCode, msg

        if int(returnCode) != 0:
            print "Problem registering stream", stream
            print msg
            return None

        streamID, version = struct.unpack("!II", msg)
        msg = "successfully registered with streamID: {}, version: {}"
        print(msg.format(streamID, version))
        socket.close()  # I 'm pretty sure we need this here

        # error checking
        socket_data = context.socket(zmq.PUSH)
        socket_data.connect("tcp://%s:%s" % (host, msgport))
        return server_connection(self.config, stream, streamID, keyOrder, format, records, context, socket_data)
