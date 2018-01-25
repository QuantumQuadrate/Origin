# ![](bloop.png)

# Origin

Origin is a simple monitoring and alert server, based on ZeroMQ and JSON messaging, making it extremely portable and easy to use.

## How it works

The server exposes two ports, a registration port and a measurement port.
To begin logging a new data source, a registration packet is sent to the monitoring server registration port describing the data that will be logged.
The server creates a new stream entry based on the registration information.
Then the device can send data to the server for logging to the measurement port, where it is automatically entered into the database.

The alert server ...

## Server Installation
If you just want to connect to an existing server see below.


### Dependencies
The default data storage is h5py.

* python 2.7
* [pyzmq](http://zeromq.org/bindings:python)
* [numpy](http://www.numpy.org/)

You then need one of these backends (or none if you are using the filesystem backend)
* [h5py](http://docs.h5py.org/en/latest/build.html) (default)
* [mysql.connector](http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip)
* [pymongo](https://api.mongodb.com/python/current/)


```bash
pip install pyzmq numpy
```
Then at least one of the following, I recommend h5py:
```bash
pip install h5py
pip install http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip
pip install pymongo
```

For windows check the recommended installation stuff on the packages website (or use canopy or a package manager).
The pip install for mysql.connector worked for me on windows, and is probably the easiest way to do it.

If you want to use `mysql` or `mongodb` then you will need to uncomment the import line out in `lib/origin/server/__init__.py`:
```bash
vim lib/origin/server/__init__.py
```

```python
from origin_measurement_validation import measurement_validation

from origin_template_validation import template_validation

from origin_destination import destination


# if you dont want to install these modules then just comment the ones you dont want to use
from origin_hdf5_destination import hdf5_destination
#from origin_mysql_destination import mysql_destination
from origin_filesystem_destination import filesystem_destination # this one should be fine since its standard libs
#from origin_mongodb_destination import mongodb_destination
```

### OPTIONAL: Configure MySQL (only if you want to use it)

You will need a MySQL server, which can be set up locally, then make a new database and add a user that has read/write permissions.

```bash
mysql -u root -p
```

```sql
CREATE DATABASE origin_test;
CREATE USER '_user_'@'localhost' IDENTIFIED BY '_password_';
GRANT ALL PRIVILEGES ON origin_test.* to '_user_'@'localhost';
FLUSH PRIVILEGES;
```

### Configure Origin

We now need to enter project specific information to the configuration file for the server.

```bash
git clone http://github.com/Orthogonal-Systems/Origin
cd Origin
vim config/origin-server-test.cfg
```

To begin with we are just going to test the server locally so use the `config/origin-server-test.cfg` file, later when you want to connect to an actual server use the `config/origin-server.cfg` (or whatever you want).

```python
[Server]
ip                  = 127.0.0.1 ; change to origin server address for deployment
register_port       = 5558
measure_port        = 5559
alert_port          = 5560
read_port           = 5561
pub_port            = 5562
json_register_port  = 5563
json_measure_port   = 5564

timestamp_type      = uint64

# pick the back end you want to use
destiniation        = hdf5
#destiniation        = mysql
#destiniation        = filesystem
#destiniation        = mongodb

alert_check_period  = 120 ; units of seconds

[MySQL]
server_ip = 127.0.0.1
db        = origin_test
user      = test
password  = test

[HDF5]
data_path    = data
data_file    = origin_test.hdf5
chunksize    = 1024 ; 2**10 no exponents, import fails
compression  = gzip ; False for no compression

[FileSystem]
data_path    = data/origin_test
info_file    = knownStreams.json

[MongoDB]
server_ip = 127.0.0.1
port      = 27017
db        = origin_test
# no SSL yet
#user      = test
#password  = test
```

### Run the server

You should now be able to run the server using the test configuration.

```bash
python bin/origin-server test
```

Monitor the logs in another terminal session.
```bash
tail -f var/ORIGIN.log
```
Windows open powershell, cd to Origin directory:
```powershell
Get-Content var\ORIGIN.log -wait
```

And run the toy inserter.
```bash
python bin/origin-toy-client test
```

The logs should show a new table added to the database.

```bash
2016-07-23 19:56:24,315 - Monitor - INFO - Successfully Started Logging
2016-07-23 19:56:24,315 - Monitor - INFO - Creating data directory at: [x\x\x]\Origin\var\data
2016-07-23 19:56:24,316 - Monitor - INFO - New data file: [x\x\x]\Origin\var\data\origintest.hdf5
2016-07-23 19:56:24,318 - Monitor - DEBUG - knownStreamVersions attribute not found
2016-07-23 19:56:24,322 - Monitor - INFO - IOLoop Configured
2016-07-23 20:02:09,812 - Monitor - INFO - Received registration of stream toy
2016-07-23 20:02:09,813 - Monitor - INFO - Attempt to register stream toy
```

### Checking data entry

You can either open the HDF5 directly with a viewer application.
Or you can use the example reader, which uses the built in API.

```bash
python bin/origin-toy-reader test
```

The server should be responding with alternating statistics and raw data for the toy stream.

### OPTIONAL: MySQL checking data entry
You can check that the data was actually inserted.

```bash
mysql -u _user_ -p
```

```sql
USE origintest;
SELECT * FROM measurements_toy;
```

```bash
+----+-----------------+-----------+-----------+
| id | measurementTime | toy1      | toy2      |
+----+-----------------+-----------+-----------+
|  1 |      1465326087 |   0.88671 |  0.980564 |
|  2 |      1465326092 |  0.624428 |  0.228306 |
|  3 |      1465326097 |   0.77994 |  0.616023 |
|  4 |      1465326102 |  0.122097 |  0.148885 |
|  5 |      1465326107 |  0.781741 |  0.851939 |
|  6 |      1465326112 | 0.0570459 |  0.891965 |
|  7 |      1465326117 |  0.197933 |  0.736311 |
|  8 |      1465326122 |  0.515716 | 0.0314361 |
|  9 |      1465326127 |  0.669868 |  0.737255 |
| 10 |      1465326132 |  0.445365 |  0.298424 |
| 11 |      1465326137 |  0.938865 |  0.320306 |
| 12 |      1465326142 |   0.38073 |  0.745032 |
| 13 |      1465326147 |  0.872034 |  0.987444 |
+----+-----------------+-----------+-----------+
13 rows in set (0.00 sec)
```

### Finish

Once the coniguration object `configSite` is edited, logging from an external source can be start.
To do so just run on the server:

```bash
python bin/origin-server
```

and the monitoring device:
```bash
python bin/origin-toy-client
```

## Possible destinations (backends)

### SQL
* mapd

### NoSQL
* Cassandra
* Riak
* HBase
