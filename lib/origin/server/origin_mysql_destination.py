import mysql.connector
from origin.server import destination
from origin import data_types, timestamp
import ConfigParser

class mysql_destination(destination):
    def connect(self):
        self.cnx = mysql.connector.connect(user=self.config.get("MySQL","user"),
                                           password=self.config.get("MySQL","password"),
                                           host=self.config.get("MySQL","server_ip"),
                                           database=self.config.get("MySQL","db"))
        self.cursor = self.cnx.cursor()

    def readStreamDefTable(self):
        streamCreation = (
            "CREATE TABLE IF NOT EXISTS origin_streams ( "
            " id INT NOT NULL AUTO_INCREMENT,"
            " name varchar(1024),"
            " version integer,"
            " PRIMARY KEY (id) "
            " ) "
        )

        streamFieldCreation = (
            "CREATE TABLE IF NOT EXISTS "
            " origin_stream_fields (  "
            " stream_name varchar(1024), "
            " field_name varchar(1024), "
            " version integer, "
            " field_type varchar(100), "
            " keyIndex integer"
            " )"
        )

        cursor = self.cursor
        cursor.execute(streamCreation)
        cursor.execute(streamFieldCreation)

        currentStreamNamesVersions= []
        query = "SELECT id,name,version from origin_streams"
        cursor.execute(query)
        for id,name,version in cursor:
            currentStreamNamesVersions.append((id,name,version))

        currentStreamNameDefinitions = {}
        currentStreamVersions = {}

        for id,name,version in currentStreamNamesVersions:
            query = "SELECT field_name,field_type,keyIndex FROM origin_stream_fields WHERE stream_name=\"%s\" and version=%d"%(name,version)
            cursor.execute(query)
            definition = {}
            for field_name,field_type,keyIndex in cursor:
                definition[field_name] = {"type":field_type, "keyIndex":keyIndex}
            currentStreamNameDefinitions[name] = definition
            # generate keyOrder this should probably be nicer
            keyOrder = [None] * len(currentStreamNameDefinitions[name])
            template = {}
            for key in definition:
                keyOrder[definition[key]["keyIndex"]] = key
                template[key] = definition[key]["type"]

            err, formatStr = self.formatString(template,keyOrder)
            if err > 0:
                formatStr = ''

            currentStreamVersions[name] = {
                    "version": version, 
                    "id": id, 
                    "keyOrder": keyOrder, 
                    "formatStr": formatStr
            }
            
        for stream in currentStreamNameDefinitions.keys():
            for field_name in currentStreamNameDefinitions[stream].keys():
                print "  Field: %s (%s)"%(field_name,currentStreamNameDefinitions[stream][field_name])
        self.knownStreamVersions = currentStreamVersions
        self.knownStreams = currentStreamNameDefinitions
        self.cnx.commit()

    def createNewStream(self,stream,version,template,keyOrder):
        cursor = self.cursor
        if version == 1:
            query = "INSERT INTO origin_streams (name, version) VALUES (\"%s\",%d)"%(stream,version)
        else:
            query = "UPDATE origin_streams SET version=%d WHERE name=\"%s\""%(version,stream) 
        print cursor.execute(query)
        #streamID = cursor.lastrowid #this doesn't seem to work with update, even though it should
        cursor.execute("SELECT id FROM origin_streams WHERE name=\"%s\" LIMIT 1"%(stream))
        streamID = cursor.fetchone()[0]
        #print "streamID: ", streamID

        fields = []
        for fieldName in template.keys():
            idx = None
            fieldType = template[fieldName]
            try:
                fields.append( (fieldName,data_types[fieldType]["mysql"]) )
            except KeyError:
                pass
            try:
                for i, k in enumerate(keyOrder):
                    if k == fieldName:
                        idx = i
                        break
            except TypeError:
                pass
            query = """INSERT INTO origin_stream_fields 
                    VALUES ("%s","%s",%d,"%s",%d)"""%(stream,fieldName,version,template[fieldName],idx)
            cursor.execute(query)

        query = "CREATE TABLE IF NOT EXISTS measurements_%s_%d (id BIGINT NOT NULL AUTO_INCREMENT,%s "%(stream,version, timestamp)
        try:
            query += data_types[self.config.get("Server","timestamp_type")]["mysql"]
        except KeyError:
            query += "INT UNSIGNED"
        query += ","


        for i in range(0,len(fields)):
            query = query + "%s %s,"%(fields[i][0],fields[i][1])
        query = query + "PRIMARY KEY (id))"
        cursor.execute(query)
        query = "DROP VIEW IF EXISTS measurements_%s "%(stream)
        cursor.execute(query)
        query = "CREATE VIEW measurements_%s AS select * from measurements_%s_%d"%(stream,stream,version)
        cursor.execute(query)
        self.cnx.commit()
        return streamID

    def insertMeasurement(self,stream,measurements):
        measurementArray = []
        keys = measurements.keys()
        keys.sort()
        for k in keys:
            measurementArray.append((k,measurements[k]))

        fmt = ["("]
        values = []
        for entry in measurementArray:
            fmt += [entry[0],',']
            values.append(entry[1])
        fmt[-1] =  ")"
        valuePlaceholders = "(" + ','.join(["%s"]*len(measurementArray)) + ")"

        query = "INSERT INTO measurements_%s_%d %s VALUES %s"%(stream,self.knownStreamVersions[stream]["version"],''.join(fmt),valuePlaceholders)

        self.cursor.execute(query,values)
        self.cnx.commit()

    # read stream data from storage between the timestamps given by time = [start,stop]
    def getRawStreamData(self,stream,start=None,stop=None,definition=None):
        start, stop = self.validateTimeRange(start,stop)
        self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))

        if definition is None:
            definition = self.knownStreams[stream]
        
        fieldList = [ field for field in definition ]
        query = "SELECT %s FROM measurements_%s_%d WHERE %s BETWEEN %d AND %d"
        values = (
            ",".join(fieldList),
            stream, 
            self.knownStreamVersions[stream]["version"], 
            timestamp, 
            start, 
            stop
        )
        #print query % values
        self.cursor.execute(query % values)

        data = {}
        for field in fieldList:
            data[field] = []

        for row in self.cursor.fetchall():
            for i, field in enumerate(fieldList):
                data[field].append(row[i])

        return data
