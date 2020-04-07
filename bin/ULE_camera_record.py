#!/usr/bin/env python

import sys
import os
import time
from PIL import Image, ImageStat
import numpy as np

# first find ourself
fullBinPath  = os.path.abspath(os.getcwd() + "/" + sys.argv[0])
fullBasePath = os.path.dirname(os.path.dirname(fullBinPath))
fullLibPath  = os.path.join(fullBasePath, "lib")
fullCfgPath  = os.path.join(fullBasePath, "config")
sys.path.append(fullLibPath)

from origin.client import server
from origin import current_time, TIMESTAMP

def get_ule_state():
    im = Image.open('/dev/shm/mjpeg/cam.jpg').convert('L')
    stat = ImageStat.Stat(im)
    total = stat.mean[0]-22.8# subtract background
    recordlight.append(total)
recordlight=[]
while True:
    get_ule_state()
    a = np.asarray(recordlight)
    np.savetxt("960lightrecord.csv", a, delimiter=",")
    

