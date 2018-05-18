#!/usr/bin/env python

import sys
import os.path
import ConfigParser
import pprint
import logging
import time
import numpy as np
import requests
from origin.client.origin_subscriber import Subscriber


# define a custom function to pass to the poller loop
#send email messages function
def send_messages(ch,status,adrs):
    """adrs is the email address receiving messages,
    ch is the channel number,
    status is string value unlocked/relocked/out of range"""
    return requests.post(
        "https://api.mailgun.net/v3/sandbox614407adee87476b873974bc39be24f9.mailgun.org/messages",
        auth=("api", "key-1ecf921dc7a279fb94acc63130f0775d"),
        data={
            "from": "Excited User <mailgun@sandbox614407adee87476b873974bc39be24f9.mailgun.org>",
            "to": [adrs],
            "subject": ch+' is '+status,
            "text": ch+' is '+status})

# MUST BE DEFINED BEFORE SUBSCRIBER INSTANTIATION
def piezo_monitor(stream_id, data, state, log, buflen=100, trigstd=3, init=30, ch=None ,filename=None,adrs=None):
    """
    buflen is the length of the circular buffer
    trigstd is units of std deviation from average
    init is the index of initial elements filled in buffer before sending alarm
    ch is the channel number,ch=str,data[ch]=float,
    filename is name of saved csv file
    """
    skip = False
    #initialization:
    if 'prev_data' not in state:
        empty = np.zeros(buflen)
        empty[:] = np.nan
        state['prev_data'] = empty #'prev_data' is an array with 10 data in memory
        state['index'] = 0
        state['time'] = [] # a list of time points where unlock occurs
        state['error'] = False
        skip = True
    try:
        if not skip: #exclude initialization index
            if state['error'] == False: #no error occured in last data
                #warning if data is detected to be out of range,skip alarm and dont put in data untill it's in range
                if data[ch] in [0,4095]:
                    log.warning(ch+' Out of range')
                    state['error'] = True
                    try:
                        send_messages(ch,'out of range',adrs)
                    except Exception:
                        log.warning('Input adress as a string')
                else:
                    #after initialization, calculate mean and std of previous data
                    mean = np.nanmean(state['prev_data'])
                    std = np.nanstd(state['prev_data'])
                    state['prev_data'][state['index']] = data[ch]
                    #alarm condition: compare new data to previous mean and std values
                    if not np.isnan(state['prev_data'][init-1]): #if first init data is filled in
                        if abs(data[ch] - mean) > std*trigstd:
                            state['error'] = True
                            log.info(ch+' unlock!?')
                            #the time of unlock alarm
                            t = time.time()
                            log.debug("[{}]: {}".format(stream_id, state))
                            with open(filename,'a') as f:
                                f.write(str(t)+'\n')
                            try:
                                send_messages(ch,'unlocked',adrs)
                            except Exception:
                                log.warning('Input adress as a string')
                    # putting new data into memory
                    state['index'] += 1
                    #cycle index if it gets to buffer length
                    if state['index'] > (buflen-1):
                        state['index'] = 0
            else: #state['error'] = True, error occured
                if abs(data[ch]-2048)< 500:
                    state['error'] = False
                    log.info('relocked')
                    try:
                        send_messages(ch,'relocked',adrs)
                    except Exception:
                        log.warning('Input adress as a string')

    except KeyError:
        log.error('Problem accessing key `c3`. Are you subscribed to the right stream?')

    return state


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fLog = logging.FileHandler("f.log")
    fLog.setLevel(logging.DEBUG)
    fLog.setFormatter(formatter)
    logger.addHandler(fLog)

    # first find ourself
    fullBinPath = os.path.abspath(os.getcwd() + "/" + sys.argv[0])
    fullBasePath = os.path.dirname(os.path.dirname(fullBinPath))
    fullCfgPath = os.path.join(fullBasePath, "config")

    if len(sys.argv) > 1:
        if sys.argv[1] == 'test':
            configfile = os.path.join(fullCfgPath, "origin-server-test.cfg")
        else:
            configfile = os.path.join(fullCfgPath, sys.argv[1])
    else:
        configfile = os.path.join(fullCfgPath, "origin-server.cfg")

    config = ConfigParser.ConfigParser()
    config.read(configfile)

    sub = Subscriber(config, logger)

    logger.info("streams")
    print('')
    pprint.pprint(sub.known_streams.keys())

    stream = 'FNODE_ADCS'

    if stream not in sub.known_streams:
        print("stream not recognized")
        sub.close()
        sys.exit(1)

    print("subscribing to stream: %s" % (stream,))
    # sub.subscribe(stream)
    # can use arbitrary callback
    # if you need to use the same base callback for multiple streams pass in specific
    # parameters through kwargs
    sub.subscribe(stream, callback=piezo_monitor, buflen=200, trigstd=12, init=30, ch='c3',filename='RbMOT.csv')
    sub.subscribe(stream, callback=piezo_monitor, buflen=200, trigstd=15, init=30, ch='c4',filename='RbHF.csv')
    sub.subscribe(stream, callback=piezo_monitor, buflen=200, trigstd=6, init=30, ch='c5',filename='CsHF.csv',adrs='zwang833@wisc.edu')

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        sub.close()
        logger.info('closing')
