"""
This module extends the Destination class to work with a MySQL database.
"""

import sys
import logging

from mysql.connector import connection, errorcode
import mysql.connector

from origin.server import Destination
from origin import data_types, TIMESTAMP


class MySQLConnWrapper(connection.MySQLConnection):
    def execute(self, sql, values=None, logger=None):
        if logger is None:
            logger = logging.getLogger(__name__)
        try:
            cursor = self.cursor()
            logger.debug("how bout now?")
            cursor.execute(sql, values)
        except mysql.connector.OperationalError as err:
            if err.errno == -1 or err.errno == 2055:
                logger.error("MySQL disconnect event")
                # mysql connection not available
                self.connect()
                cursor = self.cursor()
                cursor.execute(sql, values)
            else:
                raise err
        except mysql.connector.Error as err:
            logger.error(
                'Unexpected mysql error encountered when trying to'
                ' execute `{}` with values `{}`. errmsg: {}'.format(sql, values, err)
            )
        return cursor

    def execute_list(self, sqls):
        try:
            cursor = self.cursor()
        except mysql.connector.OperationalError as err:
            if err.errno == -1:
                # mysql connection not available
                self.connect()
                cursor = self.cursor()
            else:
                raise err
        except mysql.connector.Error as err:
            logger = logging.getLogger(__name__)
            logger.error(
                'Unexpected mysql error encountered when trying to'
                ' execute `{}` with values `{}`. errmsg: {}'.format(sqls, err)
            )

        for sql in sqls:
            cursor.execute(sql)
        return cursor


class MySQLDestination(Destination):
    '''A class for storing data in a MySQL database.'''

    def connect(self):
        db = self.config.get("MySQL", "db")
        self.cnx = MySQLConnWrapper(
            user=self.config.get("MySQL", "user"),
            password=self.config.get("MySQL", "password"),
            host=self.config.get("MySQL", "server_ip"),
            database=db
        )

        try:
            self.cnx.database = db
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.logger.warning('No database exists, attempting to create a new database.')
                self.create_database()
                self.cnx.database = db
            else:
                self.logger.exception('Unexpected mysql exception when connecting to database.')
        except Exception:
            self.logger.exception('Unexpected exception when connecting to database.')

    def create_database(self, db=''):
        '''Creates a new database default name comes from the specification in the config file'''
        query = "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'"
        if db == '':
            db = self.config.get("MySQL", "db")
        try:
            self.cnx.connect()
            cursor = self.cnx.execute(query.format(db))
        except mysql.connector.Error:
            self.logger.exception('Unexpected mysql exception when creating database.')
            self.logger.critical('Could not connect or create database. Stopping...')
            cursor.close()
            sys.exit(1)
        except Exception:
            self.logger.exception('Unexpected exception when creating database.')
            self.logger.critical('Could not connect or create database. Stopping...')
            cursor.close()
            sys.exit(1)
        cursor.close()
        self.cnx.disconnect()

    def close(self):
        self.cnx.close()

    def read_stream_def_table(self):
        # make a table for the list of streams
        stream_creation = (
            "CREATE TABLE IF NOT EXISTS `origin_streams` ( "
            " `id` INT NOT NULL AUTO_INCREMENT,"
            " `name` VARCHAR(1024),"
            " `version` INT,"
            " PRIMARY KEY (`id`)"
            " ) "
        )

        # make a table for all the stream fields
        stream_field_creation = (
            "CREATE TABLE IF NOT EXISTS"
            " `origin_stream_fields` ("
            " `id` INT NOT NULL AUTO_INCREMENT,"
            " `stream_id` INT NOT NULL,"
            " `stream_name` VARCHAR(1024) NOT NULL,"
            " `field_name` VARCHAR(1024) NOT NULL,"
            " `version` INT NOT NULL,"
            " `field_type` VARCHAR(100),"
            " `key_index` INT,"
            " PRIMARY KEY (`id`)"
            # need to add permissions to use REFERENCES command?!?
            # " FOREIGN KEY (`stream_id`) REFERENCES `origin_streams` (`id`)"
            " )"
        )

        # no json object to read, so we need to build from data
        # could change to store data in a json blob later
        current_stream_versions = []
        query = "SELECT id,name,version from origin_streams"
        self.cnx.connect()
        cursor = self.cnx.execute_list([stream_creation, stream_field_creation, query])

        for id, name, version in cursor:
            current_stream_versions.append((id, name, version))
            self.logger.debug(current_stream_versions)

        known_stream_versions = {}
        known_streams = {}

        for id, name, version in current_stream_versions:
            query = (
                "SELECT field_name,field_type,key_index"
                " FROM origin_stream_fields"
                " WHERE stream_id = {} and version = {}"
            ).format(id, version)

            self.logger.debug(query)
            cursor.execute(query)

            definition = {}
            for field_name, field_type, key_index in cursor:
                definition[field_name] = {"type": field_type, "key_index": key_index}

            known_stream_versions[name] = definition
            # generate key_order this should probably be nicer
            key_order = [None] * len(known_stream_versions[name])
            template = {}
            for key in definition:
                if definition[key]["key_index"] == -1:
                    self.logger.warning("No key order specified for stream `%s`", name)
                    key_order = []
                    break
                key_order[definition[key]["key_index"]] = key
                template[key] = definition[key]["type"]

            # create the struct format string to decode incoming data messages
            err, format_str = self.format_string(template, key_order)
            if err > 0:
                format_str = ''

            # not including older versions since it is hard right now
            known_streams[name] = {
                "stream": name,
                "id": id,
                "version": version,
                "key_order": key_order,
                "format_str": format_str,
                "definition": definition,
                "versions": []
            }

        self.known_stream_versions = known_stream_versions
        self.known_streams = known_streams
        self.print_stream_info()
        self.cnx.commit()
        cursor.close()
        self.cnx.disconnect()

    def create_new_stream_destination(self, stream_obj):
        stream = stream_obj["stream"]
        version = stream_obj["version"]

        # add new stream to the current stream list
        if version == 1:
            # if this a brand new stream, then add a new row
            query = """INSERT INTO origin_streams (name, version) VALUES (\"{}\",{})"""
            query = query.format(stream, version)
        else:
            # if we are updating an existing stream, update the row
            query = "UPDATE origin_streams SET version={} WHERE name=\"{}\""
            query = query.format(version, stream)
        self.cnx.connect()
        cursor = self.cnx.execute(query)
        # streamID = cursor.lastrowid #this doesn't seem to work with update, even though it should
        cursor.execute("SELECT id FROM origin_streams WHERE name=\"{}\" LIMIT 1".format(stream))
        stream_id = cursor.fetchone()[0]
        # overwrite streamID using the correct one
        self.known_streams[stream]['id'] = stream_id

        # enter the stream template information into the origin_stream_fields table
        fields = []
        definition = stream_obj['definition']
        for field_name in definition.keys():
            idx = None
            field_type = definition[field_name]['type']
            idx = definition[field_name]['key_index']
            try:
                fields.append((field_name, data_types[field_type]["mysql"]))
            except KeyError:
                pass

            query = (
                'INSERT INTO origin_stream_fields'
                ' (stream_id, stream_name, field_name, version, field_type, key_index)'
                ' VALUES ({},"{}","{}",{},"{}",{})'
            ).format(stream_id, stream, field_name, version, field_type, idx)
            self.logger.debug(query)
            cursor.execute(query)

        # make the new data stream table based on the template provided
        query = (
            "CREATE TABLE IF NOT EXISTS `measurements_{}_{}` ("
            # " `id` BIGINT NOT NULL AUTO_INCREMENT," # id field
            " `{}` {},"  # timestamp field
        )
        try:
            ts_dtype = data_types[self.config.get("Server", "timestamp_type")]["mysql"]
        except KeyError:
            ts_dtype = "INT UNSIGNED"
        query = query.format(stream, version, TIMESTAMP, ts_dtype)

        for i in range(0, len(fields)):
            f0, f1 = fields[i]
            query += " `{}` {},".format(f0, f1)
        query += "PRIMARY KEY (`{}`))".format(TIMESTAMP)
        # self.logger.debug(query)
        cursor.execute(query)

        # update pointer to the current version of the stream if it exists
        query = "DROP VIEW IF EXISTS measurements_{} ".format(stream)
        cursor.execute(query)
        query = """CREATE VIEW measurements_{}
                AS SELECT * FROM measurements_{}_{}""".format(stream, stream, version)
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()
        self.cnx.disconnect()
        return stream_id

    def insert_measurement(self, stream, measurements):
        measurement_array = []
        keys = measurements.keys()
        keys.sort()
        for k in keys:
            measurement_array.append((k, measurements[k]))

        fmt = ["("]
        values = []
        for entry in measurement_array:
            fmt += [entry[0], ',']
            values.append(entry[1])
        fmt[-1] = ")"
        value_placeholders = '(' + ','.join(["%s"]*len(measurement_array)) + ')'

        version = self.known_streams[stream]["version"]
        query = """INSERT INTO \"measurements_{}_{}\" {} VALUES {}"""
        query = query.format(stream, version, ''.join(fmt), value_placeholders)
        # self.logger.debug(query)
        # self.logger.debug(values)
        try:
            self.cnx.connect()
            cursor = self.cnx.execute(query, values)
        except mysql.connector.IntegrityError:
            self.exception('Error writing data to mysql server.')
        self.cnx.commit()
        cursor.close()
        self.cnx.disconnect()

    def insert_measurements(self, stream, version, measurements, logger=None):
        measurement_array = []
        keys = measurements[0].keys()
        keys.sort()
        for m in measurements:
            for k in keys:
                # make big 1D list
                measurement_array.append(m[k])

        fmt = '(`' + '`,`'.join(keys) + '`)'
        value_placeholders = '(' + ','.join(["%s"]*len(keys)) + ')'
        value_placeholders = ','.join([value_placeholders]*len(measurements))

        query = """INSERT INTO measurements_{}_{} {} VALUES {}"""
        query = query.format(stream, version, ''.join(fmt), value_placeholders)
        try:
            self.cnx.connect()
            cursor = self.cnx.execute(query, measurement_array)
        except mysql.connector.Error as err:
            msg = 'Error with mysql server. Error code: {}, msg: {}'.format(err.errno, err)
            if logger is None:
                self.logger.exception(msg)
            else:
                logger.exception(msg)
            raise err
        else:
            self.cnx.commit()
            cursor.close()
            self.cnx.disconnect()

    # read stream data from storage between the timestamps given by time = [start,stop]
    def get_raw_stream_data(self, stream, start=None, stop=None, fields=[], logger=None):
        if logger is None:
            logger = self.logger
        start, stop = self.validate_time_range(start, stop)

        if stream not in self.known_streams:
            logger.info("Unknown stream `{}` requested, updating stream list...")
            self.read_stream_def_table()

        # check if stream is still not there
        if stream not in self.known_streams:
            msg = "Requested stream `{}` does not exist.".format(stream)
            logger.debug(msg)
            return (1, {}, msg)

        if fields == []:
            fields = self.known_stream_versions[stream].keys()
        else:
            # check that the requestd fields are all in the stream defintion
            bad_field = False
            for f in fields:
                if f not in self.known_stream_versions[stream]:
                    msg = "Requested stream field `{}.{}` does not exist. Updating..."
                    logger.debug(msg.format(stream, f))
                    bad_field = True
                    break
            if bad_field:
                self.read_stream_def_table()
                for f in fields:
                    if f not in self.known_stream_versions[stream]:
                        msg = "Requested stream field `{}.{}` does not exist."
                        logger.debug(msg.format(stream, f))
                        return (1, {}, msg.format(stream, f))

        fields.append(TIMESTAMP)
        # TODO: use the built in method for putting strings together that escapes teh sql correctly
        query = 'SELECT %s FROM measurements_%s_%d WHERE %s BETWEEN %d AND %d'
        values = (
            '`' + '`,`'.join(fields) + '`',  # escape possible keywords/reserved words
            stream,
            self.known_streams[stream]["version"],
            TIMESTAMP,
            start,
            stop
        )
        logger.debug(query % values)

        self.cnx.connect()
        cursor = self.cnx.execute(query % values, logger=logger)

        data = {}

        for field in fields:
            data[field] = []

        try:
            logger.debug("I'm fetching")
            results = cursor.fetchall()
            logger.debug("I'm done")
        except mysql.connector.InterfaceError:
            logger.error("InterfaceError occured")
            err = 1
            msg = "Server encountered an error."
            results = []
        except ValueError:
            logger.error("Recieved `Packet is no an error packet error` ... ")
            err = 1
            msg = "Server encountered an error."
        except IndexError:
            logger.error("Recieved `Bytearray index out of range` ... ")
            err = 1
            msg = "Server encountered an error."
        else:
            err = 0
            logger.debug("query rowcount: {}".format(cursor.rowcount))
            if cursor.rowcount <= 0:
                msg = 'No data detected in existing stream range. {}\t{}\n{} - {}'
                logger.error(msg.format(stream, fields, start, stop))
                logger.debug("query results: {}".format(results))
                msg = "Stream declared, but no data in range."
                err = 1
        cursor.close()
        self.cnx.disconnect()
        logger.debug("error state: {}".format(err))

        for row in results:
            for i, field in enumerate(fields):
                data[field].append(row[i])
        if err == 0:
            return (0, data, '')
        return (1, {}, msg)
