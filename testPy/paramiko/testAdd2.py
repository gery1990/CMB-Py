import time
import sys, os

if __name__ == '__main__':
    seconde = sys.argv[1]
    time.sleep(int(seconde))
    os.system('mkdir %s' % str(os.getpid()))
    print "done"
