import logging
import numpy as np
import time
from piezo_lock_monitor import piezo_monitor
import tsv

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#constant
stream_id = 12345
state = {}
#read tsv data file
# fil='adcs_data_20180516.tsv'
# channel='c4'
#
# def test(f,ch):
#     reader = tsv.TsvReader(open(f))
#     for each_line in reader: #each_line is a list of data for all ch at a time
#         try:
#             each_data = float(each_line[6])
#             data = {ch:each_data} #data of 'c3'
#             piezo_monitor(stream_id,data,state,logger,buflen=50,trigstd=4,init=40,ch=channel,filename='test.csv')
#         except ValueError:
#             pass
#
# test(fil,channel)

#normal distribution
# mu = 10
# sigma = 1
# size = 1000
# lam = 10
# np.random.seed(0)
# normal_data = np.random.normal(mu,sigma,size) #array of random normal distribution data
# poisson_data = np.random.poisson(lam,size) #array of random poisson distribution data
# exp_data = np.random.standard_exponential(size) #array of random expotential distribution data

test_data = [0,0,0,1207,1208,1205,1280,1283,1380,4095,4095]
for each_data in test_data:
    data = {'c3':each_data}
    piezo_monitor(stream_id,data,state,logger,buflen=2,trigstd=2,init=2,filename='test.csv')

    #print 2 out of range and 2 unlock


#test data
# for each_data in normal_data:
#     data = {'c3':each_data}
#     piezo_monitor(stream_id,data,state,logger,buflen=10,trigstd=2,init=2,filename='normal')
#
# for each_data in poisson_data:
#     data = {'c3':each_data}
#     piezo_monitor(stream_id,data,state,logger,buflen=10,trigstd=2,init=2,filename='poisson')
#
# for each_data in exp_data:
#     data = {'c3':each_data}
#     piezo_monitor(stream_id,data,state,logger,buflen=10,trigstd=2,init=2,filename='exp')
