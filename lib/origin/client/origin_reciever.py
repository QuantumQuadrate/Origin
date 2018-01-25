"""
This module provides a client generic reciever class that is intended to be
extended by reader and subscriber classes
"""

import zmq
import json


class Reciever(object):
    """!@brief A class representing a data stream reader to a data server.

    This class handles asynchronous read events with a data server.
    """

    def __init__(self, config, logger):
        """!@brief Initialize the subscriber

        @param config is a ConfigParser object
        """
        self.config = config
        self.known_streams = {}
        self.stream_list = []
        self.log = logger
        self.setup()

    def close(self):
        """@!brief Prepare to stop."""
        for sock in self.sockets:
            sock.close()
        self.context.term()

    def connect(self, socket, port):
        """!@brief Open a connection to the data server on the socket.

        @param socket The zmq socket object to connect
        @param port The port to connect on
        """
        try:
            socket.connect("tcp://{}:{}".format(self.ip, port))
        except:
            self.log.exception("Error connecting to data server")

    def get_available_streams(self):
        """!@brief Request the knownStreams object from the server.

        @return knownStreams
        """
        # Sending an empty JSON object requests an object containing the
        # available streams
        self.read_sock.send('{}')
        try:
            err, known_streams = json.loads(self.read_sock.recv())
        except:
            self.log.exception("Error connecting to data server")
        else:
            self.update_known_streams(known_streams['streams'])
        return self.known_streams

    def is_fields(self, stream, fields):
        """!@brief Check that all the requested fields exist in the stream.

        @param stream A string holding the stream name
        @param fields A list of strings holding the field names
        @return True if fields are defined in stream, False otherwise
        """
        ok = True
        for field in fields:
            if field in self.known_streams[stream]:
                ok = False
                msg = "field: `{}` not listed in known_streams['{}']"
                self.log.warning(msg.format(field, stream))
        return ok

    def is_stream(self, stream):
        """!@brief Check that the requested stream exists on the server.

        @param stream A string holding the stream name
        @return True if stream is in known_streams, False otherwise
        """
        return stream.strip() in self.stream_list

    def setup(self):
        """!@brief extract configuration settings from the config object.

        Child classes should define the necessary sockets after this.
        """
        self.ip = self.config.get('Server', 'ip')
        # save all ports, we dont want to expose the JSON ports
        self.read_port = self.config.getint('Server', 'read_port')
        self.sub_port = self.config.getint('Server', 'pub_port')
        self.alert_port = self.config.getint('Server', 'alert_port')
        self.register_port = self.config.getint('Server', 'register_port')
        self.measure_port = self.config.getint('Server', 'measure_port')
        self.timeout = self.config.getint('Reader', 'timeout')
        self.filter_len = self.config.getint('Subscriber', 'filter_len')
        # initialize the possible sockets
        self.context = zmq.Context()
        self.read_sock = self.context.socket(zmq.REQ)
        self.sub_sock = self.context.socket(zmq.SUB)
        self.alert_sock = self.context.socket(zmq.SUB)
        self.reg_sock = self.context.socket(zmq.REQ)
        self.meas_sock = self.context.socket(zmq.PUSH)
        # make a list of sockets for convience
        self.sockets = [
            self.read_sock,
            self.sub_sock,
            self.alert_sock,
            self.reg_sock,
            self.meas_sock
        ]

    def update_known_streams(self, known_streams):
        """@!brief Update the known_streams defintion with new data.

        @param known_streams A dictionary containing the streams that exist on
            the server.
        """
        self.known_streams = known_streams
        self.stream_list = self.known_streams.keys()
