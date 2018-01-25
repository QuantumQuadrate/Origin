"""
This module extends the Destination class to work with a mongo database.
"""

import pymongo

from origin.server import Destination
from origin import data_types, TIMESTAMP

class MongoDBDestination(Destination):
    '''A class for storing data in a mongodb database.'''

    def connect(self):
        self.client = pymongo.MongoClient(
            host=self.config.get("MongoDB", "server_ip"),
            port=self.config.getint("MongoDB", "port")
        )
        self.db = self.client[self.config.get("MongoDB", "db")]

    def close(self):
        self.client.close()

    def read_stream_def_table(self):
        known_streams = {}
        known_stream_versions = {}
        if "known_streams" in self.db.collection_names():
            for stream_obj in self.db.known_streams.find():
                known_streams[stream_obj["stream"]] = stream_obj
                known_stream_versions[stream_obj["stream"]] = stream_obj["definition"]
        else:
            self.db.known_streams.create_index([('id', pymongo.ASCENDING)], unique=True)
        self.known_streams = known_streams
        self.known_stream_versions = known_stream_versions

    def create_new_stream_destination(self, stream_obj):
        stream_id = stream_obj["id"]
        # update/create document in db
        self.db.known_streams.replace_one({'id': stream_id}, stream_obj, upsert=True)
        return stream_id

    def insert_measurement(self, stream, measurements):
        stream_obj = self.known_streams[stream]
        stream_collection = "{}_{}".format(stream, stream_obj["version"])
        str_meas = {TIMESTAMP: measurements[TIMESTAMP]}
        for key in measurements: # mongodb doesn't do unsigned ints, because its dumb
            if key != TIMESTAMP: # could be smarter about this, but I'm tired
                str_meas[key] = str(measurements[key])
        self.db[stream_collection].insert_one(str_meas)

    # read stream data from storage between the timestamps given by time = [start,stop]
    def get_raw_stream_data(self, stream, start=None, stop=None, definition=None):
        start, stop = self.validate_time_range(start, stop)

        stream_obj = self.known_streams[stream]
        stream_collection = "{}_{}".format(stream, stream_obj["version"])

        if definition is None:
            definition = self.known_streams[stream]["definition"]

        field_list = [field for field in definition]
        data = {TIMESTAMP: []}
        for field in field_list:
            data[field] = []

        results = self.db[stream_collection].find({"$and": [
            {TIMESTAMP: {"$gte": start}},
            {TIMESTAMP: {"$lte": stop}}
        ]})

        for meas in results:
            for field in field_list:
                # recast to native type from string because mongo is stupid
                dtype = data_types[self.known_stream_versions[stream][field]['type']]
                data[field].append(dtype['type'](meas[field]))
            dtype = data_types[self.config.get("Server", "timestamp_type")]   
            data[TIMESTAMP].append(dtype['type'](meas[TIMESTAMP]))

        return (0, data, '')
