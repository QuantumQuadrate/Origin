#!/usr/bin/env python

import sys
import os
import time
from PIL import Image, ImageStat

# first find ourself
fullBinPath  = os.path.abspath(os.getcwd() + "/" + sys.argv[0])
fullBasePath = os.path.dirname(os.path.dirname(fullBinPath))
fullLibPath  = os.path.join(fullBasePath, "lib")
fullCfgPath  = os.path.join(fullBasePath, "config")
sys.path.append(fullLibPath)

from origin.client import server
from origin import current_time, TIMESTAMP

if len(sys.argv) > 1:
  if sys.argv[1] == 'test':
    configfile = os.path.join(fullCfgPath, "origin-server-test.cfg")
  else:
    configfile = os.path.join(fullCfgPath, sys.argv[1])
else:
  configfile = os.path.join(fullCfgPath, "origin-server.cfg")

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(configfile)

def get_ule_state():
    im = Image.open('/dev/shm/mjpeg/cam.jpg').convert('L')
    stat = ImageStat.Stat(im)
    total = stat.mean[0]# subtract background
    locked = 0
    if( total > 25):
        locked = 1
    return (total,locked)

# something that represents the connection to the server
# might need arguments.... idk
serv = server(config)


# alert the server that we are going to be sending this type of data
print "registering stream..."
connection = serv.registerStream(
  stream="ULETrans780",
  records={
  "trans":"float",
  "lock":"uint8"
})
print "success"

# perhaps print some useful message. Perhaps try to reconnect....
# print "problem establishing connection to server..."
# sys.exit(1)


# This might need to be more complicated, but you get the gist. Keep sending records forever
time.sleep(1)
avgs=20
lockState=0
lastLock = -1

while True:
    ts = current_time(config)
    trans = 0
    lock = 1
    for i in range(avgs):
        tempTrans,tempLock =get_ule_state()
        trans += tempTrans/avgs
        if(tempLock != 1):
            lock = 0

    if(lock!=1):
        if(lockState==1):
            print ""
            print "!"*60
            print "*"*60
            print "!"*60
            print "780 Laser Unlock Event Detected"
            os.system('echo 1 > /sys/class/gpio/gpio15/value')
            print time.strftime("%Y-%m-%d %H:%M")
            if(lastLock>0):
                print "uptime: {} hours".format((ts-lastLock)/(3600*2**32))
            print "!"*60
            print "*"*60
            print "!"*60
            print ""
        lockState = 0
    elif lock:
        if (lockState==0):
            lockState=1
            lastLock = ts
            print ""
            print "*"*60
            print "780 Laser Lock Aquired"
            os.system('echo 0 > /sys/class/gpio/gpio15/value')
            print time.strftime("%Y-%m-%d %H:%M")
            print "*"*60
            print ""
        # business as usual
        
    data = { TIMESTAMP: ts, "trans": trans, "lock": lock }
    #print "sending...."
    connection.send(**data)
    #print("time: {}\ntransmission: {}\nlock state: {}".format(ts,trans,lock))
    #time.sleep(1)
