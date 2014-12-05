import random
import time
#
#41 bit timestamp
#8 bit generator_id
#14 bit random number
#
def nonsequential_key(generator_id):
    now = int(time.time()*1000)
    rmin = 1
    rmax = 2**8 - 1
    rdm = random.randint(1, rmax)
    return ((now << 22) + (generator_id << 14) + rdm )

print nonsequential_key(10)

