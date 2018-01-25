"""
This module extends the Destination class to work with the HDF5 file format.
"""

import os
import json

import h5py
import numpy as np

from origin.server import Destination
from origin import data_types, TIMESTAMP


class HDF5Destination(Destination):
    '''A class for storing data in an HDF5 file.'''

    def connect(self):
        data_dir = self.config.get('HDF5', 'data_path')
        if not os.path.exists(data_dir):
            data_dir = os.path.join(self.config.get("Server", 'var_path'), data_dir)

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            self.logger.info("Creating data directory at: " + self.config.get('HDF5', 'data_path'))

        h5f = os.path.join(data_dir, self.config.get('HDF5', 'data_file'))
        try:
            self.hdf5_file = h5py.File(h5f, 'r+')
            self.logger.info("Opened data file: {}".format(h5f))
        except IOError:
            try:
                self.hdf5_file = h5py.File(h5f, 'w')
                self.logger.info("New data file: {}".format(h5f))
            except IOError:
                self.logger.error("Unable to create data file: {}".format(h5f))

    def close(self):
        '''Disconnects and prepares to stop'''
        self.hdf5_file.flush()
        self.hdf5_file.close()

    def read_stream_def_table(self):
        known_streams = {}
        known_stream_versions = {}
        try:
            known_streams = json.loads(self.hdf5_file.attrs['knownStreams'])
        except KeyError:
            self.logger.debug("known_streams attribute not found")
            known_streams = {}

        for stream in known_streams:
            known_stream_versions[stream] = known_streams[stream]['definition']

        self.known_streams = known_streams
        self.known_stream_versions = known_stream_versions
        self.print_stream_info()

    def create_new_stream_destination(self, stream_obj):
        stream = stream_obj['stream']
        version = stream_obj['version']
        if version == 1:    # create a new stream group under root
            stream_group = self.hdf5_file.create_group(stream)
        else:
            stream_group = self.hdf5_file[stream]
        # create a new subgroup for this instance of the current stream
        stream_ver = stream_group.create_group(stream + '_' + str(version))

        # soft link is not working?
        stream_group.attrs['currentVersion'] = stream_ver.name
        # stream_group['currentVersion'] = h5py.SoftLink(stream_ver)

        # data sets for each field plus the timestamp
        # also make a buffer dataset for each field a as a pseudo circular buffer
        chunksize = (self.config.getint('HDF5', 'chunksize'),)
        buff_size = chunksize
        #print buff_size
        tstype = self.config.get('Server', 'timestamp_type')
        compression = self.config.get('HDF5', 'compression')
        stream_ver.create_dataset(
            TIMESTAMP
            , chunksize
            , maxshape=(None,)
            , dtype=data_types[tstype]['numpy']
            , chunks=chunksize
            , compression=compression
        )
        stream_ver.create_dataset(
            TIMESTAMP + "_buffer"
            , buff_size
            , maxshape=buff_size
            , dtype=data_types[tstype]['numpy']
            , chunks=chunksize
        )
        template = stream_obj['definition']
        for field in template:
            dtype = data_types[template[field.strip()]['type']]['numpy']
            try:
                stream_ver.create_dataset(
                    field
                    , chunksize
                    , maxshape=(None,)
                    , dtype=dtype
                    , chunks=chunksize
                    , compression=compression
                )
                stream_ver.create_dataset(
                    field + "_buffer"
                    , buff_size
                    , maxshape=buff_size
                    , dtype=dtype
                    , chunks=chunksize
                )
            except RuntimeError:
                self.logger.exception("Server error occured.")
                return -1

        self.hdf5_file.attrs['knownStreams'] = json.dumps(self.known_streams)
        stream_ver.attrs['definition'] = json.dumps(self.known_stream_versions[stream])
        return stream_obj['id']

    def insert_measurement(self, stream, measurements):
        dgroup = self.hdf5_file[self.hdf5_file[stream].attrs['currentVersion']]
        if 'row_count' in dgroup.attrs:
            row_count = dgroup.attrs['row_count']
            row_count_buffer = dgroup.attrs['row_count_buffer'] + 1
            buffer_size = dgroup[TIMESTAMP + '_buffer'].shape[0]
            if row_count_buffer == buffer_size:
                msg = "Buffer is full. Moving completed chunk to archive and "
                msg += "wrapping pointer around."
                self.logger.debug(msg)
                length = dgroup[TIMESTAMP].shape[0]
                chunksize = self.config.getint('HDF5', 'chunksize')
                for field in measurements:
                    dgroup[field][row_count:] = dgroup[field+'_buffer']
                    dgroup[field].resize((length+chunksize,))
                row_count += buffer_size
                row_count_buffer = 0
        else:
            row_count = 0
            row_count_buffer = 0

        dgroup.attrs['row_count'] = row_count # move pointer for next entry
        dgroup.attrs['row_count_buffer'] = row_count_buffer # move pointer for next entry

        for field in measurements:
            if field != TIMESTAMP: # if we dont check it will throw KeyError
                dtype = self.known_stream_versions[stream][field]['type']
                if dtype == "string":
                    measurements[field] = measurements[field].encode("ascii", "ignore")
            dgroup[field+"_buffer"][row_count_buffer] = measurements[field]
        self.hdf5_file.flush()

    def get_raw_stream_data(self, stream, start=None, stop=None, fields=[]):
        '''read stream data from storage between the timestamps given by time = [start,stop]
        only takes timestamps in second
        '''
        start, stop = self.validate_time_range(start, stop)
        self.logger.debug("Read request time range (start, stop): ({},{})".format(start, stop))
        # get data from buffer
        try:
            stream_group = self.hdf5_file[self.hdf5_file[stream].attrs['currentVersion']]
        except KeyError:
            msg = "Requested stream `{}` does not exist.".format(stream)
            return (1, {}, msg)

        if fields == []:
             fields = self.known_stream_versions[stream].keys()

        # read ring buffers and pointers in case the pointer advances during read
        raw_data = {}
        try:
            raw_data[TIMESTAMP] = {
                'pointer': stream_group.attrs['row_count_buffer'], # pointer in ring buffer
                'array':  stream_group[TIMESTAMP + '_buffer']
            }
        except KeyError:
            # no data has been saved yet, so all queries are out of range
            msg = "Stream declared, but no data saved."
            return (1, {}, msg)

        try:
            for field in fields:
                raw_data[field] = {
                    'pointer': stream_group.attrs['row_count_buffer'], # pointer in ring buffer
                    'array':  stream_group[field + '_buffer']
                }
        except KeyError:
            msg = "Requested stream field `{}.{}` does not exist.".format(stream, field)
            return (1, {}, msg)

        # correct the ring buffer order to linear
        pointer = raw_data[TIMESTAMP]['pointer']+1
        array = raw_data[TIMESTAMP]['array']

        # check if the buffer has the requested range first, it usually will be
        buff_start = array[pointer]
        # if this is our first pass then the buffer starts at position 0
        # we can tell because if the timestamp is 0, it has not been overwritten yet
        if buff_start == 0:
            buff_start = array[0]

        if buff_start > start: # if not go look for it
            # data after pointer has already been saved to the archive
            raw_data[TIMESTAMP] = array[:pointer]
            for field in fields:
                raw_data[field] = raw_data[field]['array'][:pointer]

            raw_data = self.get_archived_stream_data(
                stream_group,
                start,
                stop,
                raw_data,
                fields
            )

        else:   # otherwise all requested data is contained in the buffer
            raw_data[TIMESTAMP] = np.concatenate((array[pointer:], array[:pointer]))
            for field in fields:
                field_pointer = raw_data[field]['pointer']+1
                offset = field_pointer - pointer
                array = raw_data[field]['array']
                length = len(array)
                # in the ring buffer the oldest data could have been overwritten during the read
                if offset < 0:
                    self.logger.debug("negative offset")
                    offset += length
                raw_data[field] = np.concatenate((
                    np.full(offset, None)
                    , array[pointer+offset:]
                    , array[:pointer+offset]
                ))[:length]

        # now filter the data down to the requested range
        idx_start = None
        idx_stop = None # when slicing None goes to end of list not -1
        for i, time_stamp in enumerate(raw_data[TIMESTAMP]):
            if (idx_start is None) and (time_stamp >= start):
                idx_start = i
            elif time_stamp > stop:
                idx_stop = i-1
                break

        if idx_start is None:
            msg = "error in indexing (index start, index stop): ({},{})"
            self.logger.warning(msg.format(idx_start, idx_stop))
            msg = "No data in requested time window."
            return (1, {}, msg)

        data = {}
        # json method cant handle numpy array
        data[TIMESTAMP] = raw_data[TIMESTAMP][idx_start:idx_stop].tolist()

        for field in fields:
            # json method cant handle numpy array
            data[field] = raw_data[field][idx_start:idx_stop].tolist()
        return (0, data, '')

    def get_archived_stream_data(self, stream_group, start, stop, buffer_data, fields):
        '''get raw_data in range from the archived data'''
        time_dset = stream_group[TIMESTAMP]
        row_pointer = stream_group.attrs['row_count']
        # if there is no old data then just stop now
        if row_pointer == 0:
            return buffer_data

        old_data = {}
        # make as big as it needs to be then resize
        old_data[TIMESTAMP] = np.zeros(row_pointer)
        for field in fields:
            old_data[field] = np.zeros(row_pointer)
        chunksize = self.config.getint('HDF5', 'chunksize')
        row_pointer -= chunksize
        # pull data out by chunksize until the end of the data set,
        # or the start time has been reached
        while row_pointer >= 0:
            chunk = time_dset[row_pointer:row_pointer+chunksize]
            pnt = row_pointer
            if chunk[0] < stop:
                old_data[TIMESTAMP][pnt:pnt+chunksize] = chunk
                for field in fields:
                    old_data[field][pnt:pnt+chunksize] = stream_group[field][pnt:pnt+chunksize]
            if chunk[0] < start:
                old_data[TIMESTAMP] = np.concatenate((old_data[TIMESTAMP][pnt:], buffer_data[TIMESTAMP]))
                for field in fields:
                    old_data[field] = np.concatenate((old_data[field][pnt:], buffer_data[field]))
                break
            row_pointer -= chunksize
        return old_data
