import json
import sys
import zmq
import struct
import ctypes
import traceback

from origin import data_types, TIMESTAMP


# returns string and size tuple
def make_format_string(config, key_order, records):
    try:
        ts_type = config.get('Server',"timestamp_type")
    except KeyError:
        ts_type = "uint"
    ts_size = data_types[ts_type]["size"]
    fstr = "!I" + data_types[ts_type]["format_char"]# network byte order

    data_length = ts_size
    for entry in key_order:
        data_length += data_types[records[entry]]["size"]
        fstr += data_types[records[entry]]["format_char"]
    return fstr, data_length


class ServerConnection:
    def __init__(self, config, stream, streamID, key_order, data_format, records, context, socket):
        self.config = config
        self.stream = stream
        self.streamID = streamID
        self.key_order = key_order
        try:
            self.data_format = data_format.lower()
        except AttributeError:
            self.data_format = None
        self.records = records
        self.context = context
        self.socket = socket

        self.socket.setsockopt(zmq.SNDTIMEO,2000)

        if key_order is None:
            self.format_string, self.data_size = (None, None)
        else:
            self.format_string, self.data_size = make_format_string(self.config, key_order, records)

    def send(self,**kwargs):
        msg_data = [ self.streamID ]
        try:
            msg_data.append(kwargs[TIMESTAMP])
        except KeyError:
            #print "No timestamp specified, server will timestamp on arrival"
            msg_data.append(0)

        if self.data_format is None:
            for k in self.key_order:
                msg_data.append(kwargs[k])
            try:
                self.socket.send(self.format_record(msg_data), zmq.NOBLOCK)
            except zmq.Again:
                print("Connection to Server Failed")
                #self.socket.close()
                exit(1)
            except Exception as e:
                print(f"Uncaught exception: {e}")
                print('-'*60)
                traceback.print_exc(file=sys.stdout)
                print('-'*60)
                print("Exiting")
                #self.socket.close()
                exit(1)

        elif self.data_format == "json":
            msg_data[0] = self.stream
            msg_map = {}
            for k in kwargs.keys():
                if k != TIMESTAMP:
                    msg_map[k] = kwargs[k]
            msg_data.append(msg_map)
            try: 
                self.socket.send(json.dumps(msg_data), zmq.NOBLOCK)
            except zmq.Again:
                print("Connection to Server Failed")
                #self.socket.close()
                exit(1)
            except Exception as e:
                print(f"Uncaught exception: {e}")
                print('-' * 60)
                traceback.print_exc(file=sys.stdout)
                print('-' * 60)
                print("Exiting")
                # self.socket.close()
                exit(1)

    def close(self):
        print("closing socket")
        self.socket.close()
        
    def format_record(self, data):
        return struct.pack( self.format_string, *data )
