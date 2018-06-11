"""
This module provides a client subscription class that holds all the basic API
methods for subscribing to a data stream.
"""

import zmq
import sys
import json

import origin_reciever as reciever

import multiprocessing

import requests

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
    #a dict shows which channel is subscribed and unsubscribed:
    sub_list = {}
    #subscription is a dict
    context = zmq.Context()
    sub_sock = context.socket(zmq.SUB)
    # listen for one second, before doing housekeeping
    sub_sock.setsockopt(zmq.RCVTIMEO, 1000)
    sub_sock.connect(sub_addr)
            # cmd = {
            #     'action': 'SUBSCRIBE',
            #     'stream_filter': stream_filter,
            #     'callback': callback,
            #     'kwargs': kwargs,
            # }
    while True:
        try:
            #get command from command queue
            cmd = queue.get_nowait()
            if cmd['action'] == 'SHUTDOWN':
                break

            if cmd['action'] == 'SUBSCRIBE':
                msg = 'Subscribing with stream filter: [{}]'
                stream_filter = cmd['stream_filter']
                log.info(msg.format(stream_filter))

                # add subscribed channel info to dict
                #sub_list = {1:{kwargs},2:{kwargs}}
                sub_list[cmd['id']] = cmd['kwargs']
                log.info(sub_list)
                sub_list_json = json.dumps(sub_list)
                requests.put('http://127.0.0.1:5000/monitor', json=sub_list_json)

                # add the callback to the list of things to do for the stream
                if stream_filter not in subscriptions:
                    subscriptions[stream_filter] = []
                    #stream_filter is assigned as a key with an empty list
                    sub_sock.setsockopt_string(zmq.SUBSCRIBE, stream_filter)
                subscriptions[stream_filter].append({
                    'callback': cmd['callback'],
                    'kwargs': cmd['kwargs'],
                    'state': {},
                    'id': cmd['id']
                })

                log.info("subscriptions: {}".format(subscriptions[stream_filter]))

            if cmd['action'] == 'UPDATE_KW':
                msg = 'Updating channel'
                log.info(msg.format(cmd['stream_filter']))
                sub_sock.setsockopt_string(zmq.SUBSCRIBE, stream_filter)
                for cb in subscriptions[stream_filter]:
                    if cb['id'] == cmd['id']:
                        cb['kwargs'] = cmd['kwargs']

                sub_list[cmd['id']] = cmd['kwargs']
                sub_list_json = json.dumps(sub_list)
                requests.put('http://127.0.0.1:5000/monitor', json=sub_list_json)
                log.info('Keywords updated')
                log.info("subscriptions: {}".format(subscriptions[stream_filter]))


            if (cmd['action'] == 'UNSUBSCRIBE' or
                    cmd['action'] == 'REMOVE_ALL_CBS'):
                msg = 'Unsubscribing to stream filter: [{}]'
                log.info(msg.format(cmd['stream_filter']))
                sub_sock.setsockopt_string(zmq.UNSUBSCRIBE, stream_filter)

            if cmd['action'] == 'REMOVE_ALL_CBS':
                msg = 'Removing all callbacks for stream filter: [{}]'
                log.info(msg.format(cmd['stream_filter']))
                del subscriptions[cmd['stream_filter']]

            if cmd['action'] == 'RESET':
                msg = 'Resetting channel'
                log.info(msg.format(cmd['stream_filter']))
                for cb in subscriptions[stream_filter]:
                    #cb is a dict with all the info of each channel subscribed
                    #stream_filter is a list of all the cb dict.
                    if cb['id'] == cmd['id']:
                        cb['state']={}
                log.info('Done resetting')
                log.info("subscriptions: {}".format(subscriptions[stream_filter]))

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
                log.debug("new data")
                for cb in subscriptions[streamID]:
                    cb['state'] = cb['callback'](
                        streamID, json.loads(content),
                        cb['state'], log, **cb['kwargs']
                    )
            except KeyError:
                msg = "An unrecognized streamID `{}` was encountered"
                log.error(msg.format(streamID))
                log.error(subscriptions)
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
        #setup a process as obj.loop for poller-loop
        self.loop = multiprocessing.Process(
            target=loop,
            args=(sub_addr, self.queue, logger)
        )
        #start loop process every time subscriber class is called
        self.loop.start()
        self.id_list=[]
        self.last_index=0
        self.id=self.get_id()

    def close(self):
        super(Subscriber, self).close()
        self.send_command({'action': 'SHUTDOWN'})

    def subscribe(self, stream, callback=None, **kwargs):
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
            self.id = self.get_id()
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
            #kwargs is a dict including all the keyword parameters needed
        cmd = {
            'action': 'SUBSCRIBE',
            'stream_filter': stream_filter,
            'callback': callback,
            'kwargs': kwargs,
            'id' : self.id
        }
        self.log.info('sending cmd to process: {}'.format(cmd))
        self.send_command(cmd)

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
        self.send_command({
            'action': 'REMOVE_ALL_CBS',
            'stream_filter': stream_filter,
        })

    def unsubscribe(self, stream):
        """Unsubscribe from stream at the publisher.

        Calling this leaves the callbacks associated with the data stream.
        Call remove_callbacks if you want to remove the callbacks.

        @param stream A string holding the stream name
        """
        stream_filter = self.get_stream_filter(stream)
        self.send_command({
            'action': 'UNSUBSCRIBE',
            'stream_filter': stream_filter,
        })

    def reset(self, stream, id):
        stream_filter = self.get_stream_filter(stream)
        self.send_command({
            'action': 'RESET',
            'stream_filter': stream_filter,
            'id': id
        })

    def update(self, stream, id, **kwargs):
        stream_filter = self.get_stream_filter(stream)
        self.send_command({
            'action': 'UPDATE_KW',
            'stream_filter': stream_filter,
            'kwargs': kwargs,
            'id': id
        })


    def send_command(self, cmd):
        #function/method to put command into a queue
        self.queue.put(cmd)

    def get_id(self):
        #function to assign unique id index to each subscribed channel
        while self.last_index in self.id_list:
            self.last_index+=1
        self.id_list.append(self.last_index)
        return self.id_list[-1]
