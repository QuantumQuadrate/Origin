#!/usr/bin/env python

# Python script to communicate with the
# Agilent U1253A / U1272A / U1273A etc.
# found originally on http://goo.gl/Gycv9H

# For more information on the protocol, check
# http://blog.philippklaus.de/2014/02/agilent-u1273a/
# and http://goo.gl/oIJi96

import sys
import time
import serial
import os

# first find ourself
fullBinPath  = os.path.abspath(os.getcwd() + "/" + sys.argv[0])
fullBasePath = os.path.dirname(os.path.dirname(fullBinPath))
fullLibPath  = os.path.join(fullBasePath, "lib")
fullCfgPath  = os.path.join(fullBasePath, "config")
sys.path.append(fullLibPath)

from origin.client import server
from origin import current_time, TIMESTAMP

if len(sys.argv) > 2:
  if sys.argv[2] == 'test':
    configfile = os.path.join(fullCfgPath, "origin-server-test.cfg")
  else:
    configfile = os.path.join(fullCfgPath, sys.argv[2])
else:
  configfile = os.path.join(fullCfgPath, "origin-server.cfg")

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(configfile)


def init_meter(com_port):
    global meter
    print('meter, starting open:')
    meter = serial.Serial(com_port, 9600, timeout=.5)
    time.sleep(1)
    print('meter,  done open')
    print(meter)
    print('meter,  reseting meter:')
    meter.write("RST\n")
    time.sleep(0.25)
    response = meter.read(100)
    print(response)
    print('meter,  identifying meter:')
    meter.write("*IDN?\n")
    time.sleep(0.25)
    response = meter.read(100)
    print(response)
    print('meter,  Battery:')
    meter.write("SYST:BATT?\n")
    time.sleep(0.25)
    response = meter.read(100)
    print(response)
    print('meter,  Config:')
    meter.write("CONF?\n")
    time.sleep(0.25)
    response = meter.read(100)
    print(response)

    return

def read_meter(second='no'):
    global meter

    if second != 'yes' :
        #print ('not in second')
        meter.write("FETC?\n")
    else :
        #print ('yes in second')
        meter.write("FETC? @2\n")

    #time.sleep(0.05)
    responsestr = meter.read(20)
    #print ('>' + responsestr + '<', len(responsestr))
#    try:
    #print responsestr
    response = float(responsestr)
    return response

def close_meter():
    global meter
    print('meter, starting close')
    print(meter)
    meter.close()
    print('meter, closed')

    return meter

if __name__=='__main__':
    try:
        meter = sys.argv[1]
    except:
        print('Please provide device as first argument. Exiting.')
        sys.exit(2)

    init_meter(meter) # Initialize the multimeter
    # alert the server that we are going to be sending this type of data
    serv = server(config)
    print "registering stream..."
    connection = serv.registerStream(
        stream="Rb_CoilThermistor",
		records={
			"thermistor":"float"
		},
		timeout=10000
	)
    print "success"

    while True:
        ts = current_time(config)
        try:
            thermistor = read_meter()
            print thermistor
            data = { TIMESTAMP: ts, "thermistor": thermistor }
            connection.send(**data)
        except:
            print "comm error"
            print(type(thermistor))
        time.sleep(0.5)
    close_meter()
