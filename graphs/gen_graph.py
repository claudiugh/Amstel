#!/usr/bin/python
"""
Script for generating graphs used in testing Amstel
"""

import random
import struct
from optparse import OptionParser

def write_amstel_format(vertices, vertex_values, edges, filename):
    """
    write the graph given by number of vertices (0 .. vertices - 1), 
    value on each vertex and the edges (edges[i] contains a list of 
    pairs (target, value))
    """
    f = open(filename, 'wt')
    for i in xrange(vertices):
        f.write("V%d %d %d" % (i, vertex_values[i], len(edges[i])))
        for e in edges[i]:
            (target, value) = e
            f.write(" V%d %d" % (target, value))
        f.write("\n")
    f.close()


def generate_wheel_graph(vertices, edges_count, filename):
    """
    generate a simple wheel graph. A vertex i is connected to the next
    edges_count vertices, with an unitary cost.
    The vertex values is an integer in [1, vertices].
    """
    vertex_values = []
    edges = []
    for i in xrange(vertices):
        value = random.randint(1, vertices)
        vedges = []
        for j in xrange(edges_count):
            target = (i + 1 + j) % vertices
            cost = 1
            vedges.append((target, cost))
        vertex_values.append(value)
        edges.append(vedges)
    write_amstel_format(vertices, vertex_values, edges, filename)


def generate_lognorm_outdegree(vertices, filename):
    """
    generate a lognormal distribution outdegree in a binary file
    """
    f = open(filename, "wb")
    mu = 4
    sigma = 1.3
    for i in xrange(vertices):
        outdegree = int(random.lognormvariate(mu, sigma))
        data = struct.pack('i', outdegree)
        f.write(data)
    f.close()


def generate_nondir_lognorm(vertices, filename):
    """
    Generate a non-directed weighted graph with a lognormal distribution
    of the outdegree.
    Both edge targets and values are picked randomly without introducing
    duplicates and by keeping the outdegree as given by the lognormal
    distribution.
    """
    max_edge_value = 10
    mu = 2
    sigma = 1.1
    outdegree = {}
    edges = {}
    edge_values = {}
    ordered_vertices = []
    for i in xrange(vertices):
        edges[i] = []
        edge_values[i] = []
        outdegree[i] = int(random.lognormvariate(mu, sigma)) % vertices
        ordered_vertices.append((i, outdegree[i]))
    ordered_vertices.sort(key=lambda vertex: vertex[1], reverse=True)
    for vertex in ordered_vertices:
        vid = vertex[0]
        available = []
        for i in xrange(vertices):
            if i != vid and outdegree[i] > 0 and not i in edges[vid]:
                available.append(i)
        random.shuffle(available)
        bidir_edges = min(outdegree[vid], len(available))
        for i in xrange(bidir_edges):
            target = available[i]
            value = random.randint(1, max_edge_value)
            edges[vid].append(target)
            edge_values[vid].append(value)
            edges[target].append(vid)
            edge_values[target].append(value)
            outdegree[vid] -= 1
            outdegree[target] -= 1
    # we consider 0 values for vertices
    vertex_values = []
    compact_edges = []
    for i in xrange(vertices):
        vertex_values.append(0)
        compact_edges.append([])
        for e in xrange(len(edges[i])):
            compact_edges[i].append((edges[i][e], edge_values[i][e]))
    write_amstel_format(vertices, vertex_values, compact_edges, filename)


def main():
    parser = OptionParser()
    parser.add_option("-t", "--type", dest="type", type="string",
                      help="wheel, lnod, lnndir"); 
    parser.add_option("-v", "--vertices", dest="vertices",
                      help="number of vertices from the graph", type="int")
    parser.add_option("-e", "--edges", dest="edges", type="int",
                      help="number of edges per vertex")
    parser.add_option("-o", "--output", dest="filename",
                      help="output filename", type="string")
    (options, args) = parser.parse_args()

    if not options.type in ("wheel", "lnod", "lnndir") or \
            not options.filename or not options.vertices:
        parser.print_help()
    else:
        graph_type = options.type
        if graph_type == "wheel":
            if not options.edges:
                print "It is required to specify the number of edges for the "\
                    "wheel graph \n"
                parser.print_help()
            else:
                print "Generating wheel graph with %d vertices and %d edges " \
                    % (options.vertices, options.edges)
                generate_wheel_graph(options.vertices, options.edges, 
                                     options.filename)
        elif graph_type == "lnod":
            print "Generating lognornal distribution outdegree for " \
                " %d vertices " % (options.vertices)
            generate_lognorm_outdegree(options.vertices, options.filename)
        elif graph_type == "lnndir":
            print "Generating non-directed weighted graph with lognormal " \
                "distribution of the outdegree with %d vertices " \
                % (options.vertices)
            generate_nondir_lognorm(options.vertices, options.filename)
            

if __name__ == '__main__':
    main()
