
import sys
import json
import struct
import zmq


try:
    from origin.server import MySQLDestination
    mysql = True
except ImportError:
    print "MySQL destination not available."
try:
    from origin.server import HDF5Destination
    hdf5 = True
except ImportError:
    print "HDF5 destination not available."
from origin.server import FilesystemDestination
try:
    from origin.server import MongoDBDestination
    mongo = True
except ImportError:
    print "Mongo destination not available."


class MonServer(object):
    def __init__(self, logger, config):
        self.logger = logger
        self.streamfile = ""
        self.state = "UNKNOWN"
        self._strlist = {}
        self.config = config

        # create a destination object
        dest = config.get("Server", "destination").lower()
        if dest == "mysql":
            self.dest = MySQLDestination(logger, config)
        elif dest == "hdf5":
            self.dest = HDF5Destination(logger, config)
        elif dest == "filesystem":
            self.dest = FilesystemDestination(logger, config)
        elif dest == "mongodb":
            self.dest = MongoDBDestination(logger, config)
        elif dest == '':
            logger.critical("No destination specified in configs. Killing server...")
            sys.exit(1)
        else:
            logger.critical("Unrecognized destination {} specified. Killing server...".format(dest))
            sys.exit(1)

        # the publish socket does not use the event system that eveything else uses
        # I cant assign a class method to it, so it is defined here instead
        pub_addr = "tcp://*"
        pub_port = config.getint("Server", "pub_port")
        self.pub_socket = zmq.Context.instance().socket(zmq.PUB)
        self.pub_socket.bind("%s:%s" % (pub_addr, pub_port))

    # DATA HANDLER #######################################################
    def processDataMsg(self, msg, format='native'):
        # self.logger.info('data msg recieved...')
        for m in msg:
            if m != '':
                self.processSingleDataMsg(m, format)

    def processJSONDataMsg(self, msg):
        self.processDataMsg(msg, format='JSON')

    def processSingleDataMsg(self, msg, format='native'):
        messageDecoded = None
        stream = None
        result = None
        resultText = None
        fail = False
        binaryData = False

        try:
            if format == 'native':
                binaryData = True
                streamID = struct.unpack("!I", msg[:4])[0]
                stream = self.dest.find_stream(streamID)
                self.logger.debug('data msg recieved, for stream '+stream)
                data = msg[4:]  # remove first 4 bytes
            elif format == 'JSON':
                messageDecoded = json.loads(msg)
                stream = messageDecoded[0]
                streamID = self.dest.known_streams[stream]["id"]
            else:
                result = 1
                resultText = "Unrecognized data format `{}` specified".format(format)
                fail = True

        except Exception:
            result = 1
            resultText = "Failed to decode message from client"
            fail = True
            self.logger.exception("Exception in user code.")

        if not fail:
            if binaryData:
                result, resultText, meas = self.dest.measurement_binary(stream, data)

            else:
                if len(messageDecoded) != 3:
                    result = 1
                    resultText = "Measurement message didn't have all the required fields"
                elif (type(messageDecoded[1]) != int) and (type(messageDecoded[1]) != long):
                    result = 1
                    resultText = "Non-integer time sent"
                else:
                    # TODO: does this throw out the timestamp?
                    recordTime = messageDecoded[1]
                    measurements = messageDecoded[2]
                    result, resultText, meas = self.dest.measurement(stream, measurements)

        if result != 0:
            if binaryData:
                msg = ":".join("{:02x}".format(ord(c)) for c in msg)
            self.logger.warn("Got a message I can't do anything with. Error: {} Message: {}".format(resultText, msg))
        else:
            self.publish(streamID, meas)

    # REGISTRATION HANDLER ###############################################
    def registerStream(self, reg_stream, msg, format='native'):
        for m in msg:
            self.registerSingleStream(reg_stream, m, format)

    def registerJSONStream(self, reg_stream, msg):
        self.registerStream(reg_stream, msg, format='JSON')

    def registerSingleStream(self, reg_stream, msg, format='native'):
        messageDecoded = None
        stream = None

        result = None
        resultText = None
        fail = False
        keyOrder = []

        try:
            if format == 'native':
                messageDecoded = msg.split(',')
                recordDict = {}
                for entry in messageDecoded[1:]:
                    key, dtype = entry.split(':')
                    key = key.strip()
                    recordDict[key] = dtype.strip()
                    keyOrder.append(key)

            elif format == 'JSON':
                messageDecoded = json.loads(msg)
                recordDict = messageDecoded[1]
                rawKeyList = msg.split('{')[1]
                rawKeyList = rawKeyList.split('}')[0]
                rawKeyList = rawKeyList.split(',')
                for idx, rawKey in enumerate(rawKeyList):
                    for key in recordDict:
                        if rawKey.find('"{}"'.format(key)) != -1:
                            keyOrder.append(key.strip())
                            break
            else:
                result = 1
                resultText = "Unrecognized registration format `{}` specified".format(format)
                fail = True

        except:
            result = 1
            resultText = "Failed to decode message from client"
            fail = True

        if not fail:
            stream = messageDecoded[0]
            self.logger.info("Received registration of stream {}".format(stream))
            result, resultText = self.dest.register_stream(stream, recordDict, keyOrder)

        if result != 0:
            self.logger.warn("Unable to register stream. Got msg that is badly formatted: {}".format(msg))

        returnMessage = (str(result), resultText)
        reg_stream.send(','.join(returnMessage))

    # ALERT HANDLER ######################################################
    def processAlertMsg(self, alert_stream, msg):
        # Placeholder for testing alert messages
        try:
            messageDecoded = json.loads(msg)
        except ValueError:
            result = 1
            resultText = "Failed to decode alert"
            fail = True
        if not fail:
            self.logger.info("Received alert message")

        returnMessage = [result, resultText]
        alert_stream.send(json.dumps(returnMessage))

    def CheckAlerts(self):
        # Do Something to sim checking
        # Will drop in full alert chain after testing
        list = range(200)
        for l in list:
            d = l*l

    # READ REQUEST HANDLER ################################################
    def processReadMsg(self, read_stream, msgs):
        try:
            read_stream.send_json(self.processSingleReadMsg(msgs[0]))
        except:
            self.logger.exception('Unhandled exception in processReadMsg.')
            read_stream.send_json((1, dict(streams=self.dest.known_streams, error="Server Error")))

    def processSingleReadMsg(self, msg):
        # if msg is an empty JSON object then send back the list of known streams
        # this is used for subscriptions

        self.logger.debug("New read request")
        self.logger.debug(msg)
        if msg == '{}':
            return (0, dict(streams=self.dest.known_streams))

        result = 1  # error flag should be cleared during measurement
        try:
            request_obj = json.loads(msg)
            stream = request_obj['stream'].strip()
        except ValueError:
            data = dict(streams=self.dest.known_streams, error="Failed to decode request.")
        except KeyError:
            data = dict(streams=self.dest.known_streams, error="Request did not have stream property.")
        else:
            if 'start' in request_obj:
                start = request_obj['start']
            else:
                start = None
            if 'stop' in request_obj:
                stop = request_obj['stop']
            else:
                stop = None

            try:
                if 'fields' in request_obj:
                    # expecting a list of fields
                    fields = [f.strip() for f in request_obj['fields']]
                else:
                    # get all fields
                    fields = []
                self.logger.debug("Read request for stream `{}.{}` recieved.".format(stream, fields))
                if ('raw' in request_obj) and request_obj['raw']:
                    self.logger.debug("Raw data requested.")
                    result, data, resultText = self.dest.get_raw_stream_data(
                        stream,
                        fields=fields,
                        start=start,
                        stop=stop,
                        logger=self.logger,
                    )
                else:
                    result, data, resultText = self.dest.get_stat_stream_data(
                        stream,
                        fields=fields,
                        start=start,
                        stop=stop,
                        logger=self.logger,
                    )

            except Exception:
                self.logger.exception("Unexpected exception in read message code:")
                # return a list of valid options for the read
                resultText = "Server encountered an error."

            if result == 1:
                data = dict(stream=self.dest.known_streams, error=resultText)

        finally:
            return (result, data)

    # STREAMING PUBLISHER #################################################
    def publish(self, streamID, data):
        msg = [b"{0:04d}".format(streamID), json.dumps(data)]
        self.logger.debug("Sending data: " + str(msg))
        self.pub_socket.send_multipart(msg)
