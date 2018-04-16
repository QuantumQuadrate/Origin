from multiprocessing import Process
import time

def f(name):
    for i in range(5):
        print "hello", name
        time.sleep(1)

if __name__ == '__main__':
    p = Process(target=f, args=('bob',))
    print 'j'
    p.start()
    time.sleep(1)
    print 'g'
    p.join()
