"""
This module provides a client subscription class that holds all the basic API
methods for subscribing to a data stream.
"""

import json

import origin_reciever as reciever


class Reader(reciever.Reciever):
    """!@brief A class representing a data stream reader to a data server.

    This class handles asynchronous read events with a data server.
    """

    def __init__(self, config, logger):
        """!@brief Initialize the subscriber

        @param config is a ConfigParser object
        """
        # call the parent class initialization
        super(Reader, self).__init__(config, logger)
        # we only need the read socket for this class
        self.connect(self.read_sock, self.read_port)
        # request the available streams from the server
        self.get_available_streams()

    def get_stream_data(self, stream,
                        start=None, stop=None, fields=[], raw=False):
        """!@brief Request raw stream data in time window from sever.

        @param stream A string holding the stream name
        @param start 32b Unix timestamp that defines the start of the data
            window
        @param stop 32b Unix timestamp that defines the end of the data window
        @param fields A list of fields from the stream that should be returned
        @return data A dictionary containing data for each field in
            the time window
        """
        if not self.is_stream(stream):
            raise KeyError

        request = {
            'stream': stream.strip(),
            'start' : start,
            'stop'  : stop,
            'raw'   : raw,
        }
        if fields != []:
            if self.is_fields(stream, fields):
                request['fields'] = fields
            else:
                self.log.error('There was an issue with the specified fields.')
                return {}
        self.read_sock.send(json.dumps(request))
        try:
            msg = self.read_sock.recv()
            data = json.loads(msg)
        except:
            msg = "There was an error communicating with the server"
            self.log.exception(msg)
            data = (1, {'error': msg, 'stream': {}})

        if data[0] != 0:
            msg = "The server responds to the request with error message: `{}`"
            self.log.error(msg.format(data[1]["error"]))
            known_streams = data[1]['stream']
            if known_streams != {}:
                self.log.info('Updating stream definitions from server.')
                self.update_known_streams(known_streams)
            return {}
        else:
            return data[1]

    def get_stream_raw_data(self, stream, start=None, stop=None, fields=[]):
        """!@brief Request raw stream data in time window from sever.

        @param stream A string holding the stream name
        @param start 32b Unix timestamp that defines the start of the data
            window
        @param stop 32b Unix timestamp that defines the end of the data window
        @param fields A list of fields from the stream that should be returned
        @return data A dictionary containing raw data for each field in
            the time window
        """
        return self.get_stream_data(
            stream,
            start=start,
            stop=stop,
            fields=fields,
            raw=True
        )

    def get_stream_stat_data(self, stream,
                             start=None, stop=None, fields=[]):
        """!@brief Request stream data statistics in time window from sever.

        @param stream A string holding the stream name
        @param start 32b Unix timestamp that defines the start of the data
            window
        @param stop 32b Unix timestamp that defines the end of the data window
        @param fields A list of fields from the stream that should be returned
        @return data A dictionary containing statistical data for each field in
            the time window
        """
        return self.get_stream_data(
            stream,
            start=start,
            stop=stop,
            fields=fields,
            raw=False
        )
