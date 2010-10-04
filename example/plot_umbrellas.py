#!/usr/bin/python

import os
import optparse
from configobj import ConfigObj, flatten_errors
import tables
from math import pi
import matplotlib
matplotlib.use('ps')
import matplotlib.pyplot as plt

import numpy

#from pydr import setup_config

def main():    
    usage = """
        usage: %prog [options]
    """
    
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config_file", default="config.ini", help="Config file [default: %default]")
    (options, args) = parser.parse_args()

    if not os.path.exists(options.config_file):
        raise Exception('No config file found!')
    
    #config = setup_config(options.config_file, create=False)
    config = ConfigObj(options.config_file)
    project_path = os.path.abspath(os.path.dirname(config.filename))

    # setup the plot
    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    for r_id, r_config in config['replicas'].items():
        coordinate = float(r_config['coordinate'])
        k = float(r_config['k'])
        #rad2deg = (lambda x: x*1)
        rad2deg = (lambda x: x*180./pi)

        degrees = []
        for rad in data_chi1:
            deg = rad2deg(rad)
            if deg < 0:
                deg += 360
            degrees.append(deg)
        
        # generate umbrellas for each coord/k
        x = numpy.arange(coordinate-10.0,coordinate+10.1,0.1)
        y = [ 0.5*k*(i-coordinate)*(i-coordinate) for i in x ]
        plt.plot(x,y)
    
    # complete and show the plot
    ax.set_xlim(0, 360)
    ax.set_ylim(0, 5)
    ax.set_xlabel(r'Coordinate')
    ax.set_ylabel(r'Harmonic Restraint')
    ax.set_title('')
    # ax.grid(True)
    plot_path = os.path.join(project_path, 'umbrellas.ps')
    plt.savefig(plot_path)
    #plt.show()
    
if __name__=='__main__':
    main()
