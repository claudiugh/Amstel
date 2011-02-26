#!/usr/bin/python

import random
from optparse import OptionParser

def generate_wheel_graph(V, E, filename):
    with open(filename, 'w') as f:
        for i in xrange(V):
            value = random.randint(1, V)
            f.write("V%d %d %d" % (i, value, E))
            for j in xrange(E):
                target = (i + 1 + j) % V
                cost = 1
                f.write(" V%d %d" % (target, cost))
            f.write("\n")

def main():
    parser = OptionParser()
    parser.add_option("-v", "--vertices", dest="vertices",
                      help="number of vertices from the graph", type="int")
    parser.add_option("-e", "--edges", dest="edges", type="int",
                      help="number of edges per vertex")
    parser.add_option("-o", "--output", dest="filename",
                      help="output filename", type="string")
    (options, args) = parser.parse_args()
    if not options.filename or not options.vertices or not options.edges:
        parser.print_help()
    else:
        generate_wheel_graph(options.vertices, options.edges, options.filename)


if __name__ == '__main__':
    main()
