""" Laser lockmonitor that uses information from cavity transmission
and piezovoltage"""

#!/usr/bin/env python

import sys
import os
import time
import serial
import numpy as np
from PIL import Image, ImageStat
os.system("echo 15 > /sys/class/gpio/export")
os.system("echo out > /sys/class/gpio/gpio15/direction")
os.system("echo 0 > /sys/class/gpio/gpio15/value")
      
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
    pix=np.array(im)
    pix2=np.percentile(pix,95)
    #print pix2
    locked = 0
    if( pix2 > 22):
        locked = 1
    return (pix2,locked)

def get_piezovoltage(serialdevice):
    serialdevice.write("XR?\r\n")
    response=serialdevice.readline()
    word=response.split()[-1] # breakdown the reply into multiple words, and pick the last one that contains voltage readings
    voltage=float(word[0:-1])
    return voltage

def set_piezovoltage(serialdevice,set_voltage):
    if set_voltage>0 and set_voltage<150:
      serialdevice.write("XV{}\r\n".format(set_voltage))
      response=serialdevice.readline()
      print response
      print "setting voltage to {} V".format(set_voltage)

def get_piezostate(setpoint,tolerance,reading):
  #if setpoint<0 or setpoint>150:
  #  print "check if your setpoint is greater than 0 and less than 150 V"
  if abs(setpoint-reading)<tolerance:
    return 1
  else:
    return 0

# something that represents the connection to the server
# might need arguments.... idk
serv = server(config)

# alert the server that we are going to be sending this type of data
print "registering stream..."
connection = serv.registerStream(
  stream="ULETrans960",
  records={
  "trans":"float",
  "lock":"uint8",
  "cavitymode":"uint8",
  "piezovoltage":"float"
})
print "success"

# perhaps print some useful message. Perhaps try to reconnect....
# print "problem establishing connection to server..."
# sys.exit(1)

# Connects to piezocontroller(MDT694A) via RS232-USB interface
ser=serial.Serial('/dev/ttyUSB0',
                  baudrate=115200,
                  bytesize=serial.EIGHTBITS,
                  parity=serial.PARITY_NONE,
                  stopbits=serial.STOPBITS_ONE)
# Identify piezocontroller
time.sleep(0.1) # wait so serial comm is initialized
ser.write("I\r\n")
response=ser.readline()
print response


# This might need to be more complicated, but you get the gist. Keep sending records forever
time.sleep(1)
avgs=20
lockState=0
piezoState_past=0
piezovoltage=get_piezovoltage(ser)
setpoint=piezovoltage
tolerance=1.0
print "This piezovoltage will be used to filter wrong cavity modes : {} V".format(piezovoltage)
piezoState_now=get_piezostate(setpoint,tolerance,setpoint)
piezoState_past=piezoState_now

lastLock = -1

while True:
    ts = current_time(config)
    trans = 0
    lock = 1
    piezovoltage=get_piezovoltage(ser)	
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
            print "960 Laser Unlock Event Detected"
            os.system('echo 1 > /sys/class/gpio/gpio15/value')
            piezovoltage=get_piezovoltage(ser)
            piezoState_now=get_piezostate(setpoint,tolerance,piezovoltage)
            print "piezovoltage is at {}, setpoint was {}".format(piezovoltage,setpoint)
            #set_piezovoltage(ser,setpoint)
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
            piezovoltage=get_piezovoltage(ser)
            piezoState_now=get_piezostate(setpoint,tolerance,piezovoltage)
            #set_piezovoltage(ser,setpoint)
            print ""
            print "*"*60
            print "960 Laser Lock Acquired."
            if piezoState_now==1:
              print "Correct cavity mode."
              print "piezovoltage is at {}, setpoint was {}".format(piezovoltage,setpoint)
              os.system('echo 0 > /sys/class/gpio/gpio15/value')
            elif piezoState_now==0:
              print "Incorrect cavity mode."
              print "piezovoltage is at {}, setpoint was {}".format(piezovoltage,setpoint)
            print time.strftime("%Y-%m-%d %H:%M")
            print "*"*60
            print ""
        # business as usual
        
    data = { TIMESTAMP: ts, "trans": trans, "lock": lock , "cavitymode" : piezoState_now, "piezovoltage" : piezovoltage}
    #print "sending...."
    connection.send(**data)
    #print("time: {}\ntransmission: {}\nlock state: {}".format(ts,trans,lock))
    #time.sleep(1)
