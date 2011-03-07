#!/usr/bin/python

import random
import struct
from optparse import OptionParser

def gen_lognorm_outdegree(vertices, filename):
    f = open(filename, "wb")
    mu = 4
    sigma = 1.3
    R = []
    for i in xrange(vertices):
        outdegree = int(random.lognormvariate(mu, sigma))
        data = struct.pack('i', outdegree)
        f.write(data)
    f.close()


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-v", "--vertices", dest="vertices",
                      help="number of vertices from the graph", type="int")
    parser.add_option("-o", "--output", dest="filename",
                      help="output filename", type="string")
    (options, args) = parser.parse_args()
    if not options.vertices or not options.filename:
        parser.print_help()
    else:
        gen_lognorm_outdegree(options.vertices, options.filename)

