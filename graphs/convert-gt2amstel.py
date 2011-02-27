#!/usr/bin/python

#
# Converter from GTgraph to Amstel input format.
# It assumes that the edges are sorted by source.
#

from optparse import OptionParser

def dump_vertex(fout, u, edges):
    fout.write("%s %d %d" %(u, 0, len(edges)))
    for e in edges:
        (v, w) = e
        fout.write(" %s %s" %(v, w))
    fout.write("\n")

def gt2amstel(input_filename, output_filename):
    fin = open(input_filename, 'r')
    fout = open(output_filename, 'w')
    edges = []
    prev_u = None
    for line in fin:
        parts = line.split()
        if parts[0] == 'a':
            u = parts[1]
            v = parts[2]
            w = parts[3]
            if u != prev_u and prev_u:
                dump_vertex(fout, prev_u, edges)
                edges = []
            edges.append((v, w));
            prev_u = u
    dump_vertex(fout, prev_u, edges)
    fin.close()
    fout.close()

def main():
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="input", type="string",
                      help="input filename, in GT format")
    parser.add_option("-o", "--output", dest="output", type="string",
                      help="output filename, in Amstel format")
    (options, args) = parser.parse_args()
    if not options.input or not options.output:
        parser.print_help()
    else:
        gt2amstel(options.input, options.output)

if __name__ == '__main__':
    main()

