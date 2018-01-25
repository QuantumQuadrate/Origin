"""
This module provides a client subscription class that holds all the basic API
methods for subscribing to a data stream.
"""

import zmq
import sys
import json

import origin_reciever as reciever

import multiprocessing


def sub_print(stream_id, data, log):
    """!@brief Default stream data callback.  Prints data.

    @param stream_id data stream id
    @param data data to be printed
    @param log logging object
    """
    log.info("[{}]: {}".format(stream_id, data))


def poller_loop(sub_addr, queue, log):
    # a hash table (dict) of callbacks to perform when a message is recieved
    # the hash is the data stream filter, the value is a list of callbacks
    subscriptions = {}
    context = zmq.Context()
    sub_sock = context.socket(zmq.SUB)
    # listen for one second, before doing housekeeping
    sub_sock.setsockopt(zmq.RCVTIMEO, 1000)
    sub_sock.connect(sub_addr)
    while True:
        try:
            cmd = queue.get_nowait()
            log.info(cmd)
            if cmd['action'] == 'SHUTDOWN':
                break

            if cmd['action'] == 'SUBSCRIBE':
                msg = 'Subscribing with stream filter: [{}]'
                stream_filter = cmd['stream_filter']
                log.info(msg.format(stream_filter))
                # add the callback to the list of things to do for the stream
                if stream_filter in subscriptions:
                    subscriptions[stream_filter].append(cmd['callback'])
                else:
                    subscriptions[stream_filter] = [cmd['callback']]
                    sub_sock.setsockopt_string(zmq.SUBSCRIBE, stream_filter)

                log.info("subscriptions: {}".format(subscriptions))

            if (cmd['action'] == 'UNSUBSCRIBE' or
                    cmd['action'] == 'REMOVE_ALL_CBS'):
                msg = 'Unsubscribing to stream filter: [{}]'
                log.info(msg.format(cmd['stream_filter']))
                sub_sock.setsockopt_string(zmq.UNSUBSCRIBE, stream_filter)

            if cmd['action'] == 'REMOVE_ALL_CBS':
                msg = 'Removing all callbacks for stream filter: [{}]'
                log.info(msg.format(cmd['stream_filter']))
                del subscriptions[cmd['stream_filter']]

        except multiprocessing.queues.Empty:
            pass
        except IOError:
            log.error('IOError, probably a broken pipe. Exiting..')
            sys.exit(1)
        except:
            log.exception("error encountered")

        try:
            [streamID, content] = sub_sock.recv_multipart()
            try:
                log.info("new data")
                for cb in subscriptions[streamID]:
                    cb(streamID, json.loads(content), log)
            except KeyError:
                msg = "An unrecognized streamID `{}` was encountered"
                log.error(msg.format(streamID))
        except zmq.ZMQError as e:
            if e.errno != zmq.EAGAIN:
                log.exception("zmq error encountered")
        except:
            log.exception("error encountered")

    log.info('Shutting down poller loop.')
    sub_sock.close()
    context.term()


class Subscriber(reciever.Reciever):
    """!@brief A class representing a data stream subscription to a data server
    """

    def __init__(self, config, logger, loop=poller_loop):
        """!@brief Initialize the subscriber

        @param config configuration object
        @param logger python logging object
        @param loop custom poller loop
        """
        # call the parent class initialization
        super(Subscriber, self).__init__(config, logger)
        # we need the read socket for this class so we can get stream defs
        self.connect(self.read_sock, self.read_port)
        # request the available streams from the server
        self.get_available_streams()
        # set up queue for inter-process communication
        self.queue = multiprocessing.Queue()
        # start process
        sub_addr = "tcp://{}:{}".format(self.ip, self.sub_port)
        self.loop = multiprocessing.Process(
            target=loop,
            args=(sub_addr, self.queue, logger)
        )
        self.loop.start()

    def close(self):
        super(Subscriber, self).close()
        self.queue.put({'action': 'SHUTDOWN'})

    def subscribe(self, stream, callback=None):
        """!@brief Subscribe to a data stream and assign a callback

        You can subscribe to multiple data streams simultaneously using the
        same socket.
        Just call subscribe again with a new filter.
        You can also register multiple callbacks for the same stream, by
        calling subscribe again.

        The callback has to be a standalone function, not a class method or it
        will throw an error.

        @param stream A string holding the stream name
        @param callback A callback function that expects a python dict with
            data
        @return success True if the data stream subscription was successful,
            False otherwise
        """
        try:
            stream_filter = self.get_stream_filter(stream)
        except KeyError:
            msg = "No stream matching string: `{}` found."
            self.log.error(msg.format(stream))
            return False
        msg = "Subscribing to stream: {} [{}]"
        self.log.info(msg.format(stream, stream_filter))

        # default is a function that just prints the data as it comes in
        if callback is None:
            callback = sub_print

        # send subscription info to the poller loop
        cmd = {
            'action'        : 'SUBSCRIBE',
            'stream_filter' : stream_filter,
            'callback'      : callback
        }
        self.log.info('sending cmd to process: {}'.format(cmd))
        self.queue.put(cmd)

    def get_stream_filter(self, stream):
        """!@brief Make the appropriate stream filter to subscribe to a stream

        @param stream A string holding the stream name
        @return stream_filter A string holding the filter to subscribe to the
            resquested data stream
        """
        stream_id = str(self.known_streams[stream]['id'])
        # ascii to unicode str
        stream_id = stream_id.zfill(self.filter_len)
        stream_id = stream_id.decode('ascii')
        self.log.info(stream_id)
        return stream_id

    def remove_callbacks(self, stream):
        """Remove all callbacks associate with the given stream.

        Calling this leaves the callbacks associated with the data stream.
        Call remove_callbacks if you want to remove the callbacks.

        @param stream A string holding the stream name
        """
        stream_filter = self.get_stream_filter(stream)
        self.queue.put({
            'action'        : 'REMOVE_ALL_CBS',
            'stream_filter' : stream_filter
        })

    def unsubscribe(self, stream):
        """Unsubscribe from stream at the publisher.

        Calling this leaves the callbacks associated with the data stream.
        Call remove_callbacks if you want to remove the callbacks.

        @param stream A string holding the stream name
        """
        stream_filter = self.get_stream_filter(stream)
        self.queue.put({
            'action'        : 'UNSUBSCRIBE',
            'stream_filter' : stream_filter
        })
