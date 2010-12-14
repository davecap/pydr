import optparse
import numpy
import numpy.linalg
import scipy.stats
from math import pi
import tables
from configobj import ConfigObj, flatten_errors
import sys
import os
import json

from flask import Flask, request, render_template_string, url_for, send_file
app = Flask(__name__)

_config = None
_project_path = None
_h5_filename = 'analysis.h5'

def main():
    global _config
    global _project_path
    
    usage = """
        usage: %prog [options] <config.ini files>
    """
    parser = optparse.OptionParser(usage)
    parser.add_option("-x", dest="x_column", default=None, help="X Column REQUIRED")    
    # parser.add_option("-y", dest="y_column", default=None, help="Y Column REQUIRED")
    options, args = parser.parse_args()
    
    if not args:
        parser.error('No config.ini file found!')
    
    _config = ConfigObj(args[0])
    _project_path = os.path.abspath(os.path.dirname(_config.filename))
    
    app.run()

#
# Model
#

def get_column(h5_file, path):
    """ Get data from a single column over time (frame) """
    h5f = tables.openFile(h5_file, mode="r")
    h5_field = path.split('/')[-1]
    ps1 = path.split('/')
    ps1.pop()
    h5_table = '/'.join(ps1)

    tbl = h5f.getNode(h5_table)
    field_data = list(tbl.read(field=h5_field))
    h5f.close()
    data = [ { 'x':float(x), 'y':float(y) } for x,y in enumerate(field_data) ]
    return data

def get_paths(h5_file):
    paths = []
    h5f = tables.openFile(h5_file, mode="r")
    for g in h5f.root._v_children.keys():
        for t in h5f.root._v_children[g]._v_children.keys():
            try:
                for c in h5f.root._v_children[g]._v_children[t].description._v_names:
                    paths.append("/%s/%s/%s" % (g, t, c))
            except:
                paths.append("/%s/%s" % (g, t))
    h5f.close()
    return paths


#
# Views/Controllers
#

@app.route('/')
@app.route('/<replica>')
def index(replica=None):
    global _config
    global _project_path
    global _h5_filename
    
    if replica:
        path = request.args.get('path', None)
        if not path:
            return render_template_string(_REPLICA_TEMPLATE, replica=replica, paths=get_paths(os.path.join(_project_path, replica, _h5_filename)))
        else:
            return render_template_string(_PLOT_TEMPLATE, data=json.dumps(get_column(os.path.join(_project_path, replica, _h5_filename), path)))
    else:
        return render_template_string(_INDEX_TEMPLATE, replicas=_config['replicas'].keys())


@app.route('/protovis.js')
def protovis():
    return send_file('protovis.js')

_REPLICA_TEMPLATE = """<!doctype html>
<title>Replica</title>
<h1>Replica {{ replica }}</h1> <br /><br />
<ul>
{% for p in paths %}
    <li><a href="/{{ replica }}?path={{ p }}">{{ p }}</a></li>
{% endfor %}
</ul>
"""

_PLOT_TEMPLATE = """<!doctype html>
<html>
  <head>
    <title>Plot</title>
    <script type="text/javascript" src="/protovis.js"></script>
    <style type="text/css">

#fig {
  width: 430px;
  height: 425px;
}

    </style>
  </head>
  <body><div id="center"><div id="fig">
    <script type="text/javascript+protovis">

var data = {{ data }};

/* Sizing and scales. */
var w = 800,
    h = 600,
    x = pv.Scale.linear(0, 200).range(0, w),
    y = pv.Scale.linear(0, 100).range(0, h);
    
/* The root panel. */
var vis = new pv.Panel()
    .width(w)
    .height(h)
    .bottom(20)
    .left(20)
    .right(10)
    .top(5);

/* Y-axis and ticks. */
vis.add(pv.Rule)
    .data(y.ticks())
    .bottom(y)
    .strokeStyle(function(d) d ? "#eee" : "#000")
  .anchor("left").add(pv.Label)
    .visible(function(d) d > 0 && d < 1)
    .text(y.tickFormat);

/* X-axis and ticks. */
vis.add(pv.Rule)
    .data(x.ticks())
    .left(x)
    .strokeStyle(function(d) d ? "#eee" : "#000")
  .anchor("bottom").add(pv.Label)
    .visible(function(d) d > 0 && d < 100)
    .text(x.tickFormat);

/* The dot plot! */
vis.add(pv.Panel)
    .data(data)
  .add(pv.Dot)
    .left(function(d) x(d.x))
    .bottom(function(d) y(d.y))
    .size(3);
    
vis.render();

    </script>
  </div></div></body>
</html>

"""

_INDEX_TEMPLATE = """<!doctype html>
<title>Home</title>
<ul>
{% for r in replicas %}
    <li><a href="/{{ r }}">{{ r }}</a></li>
{% endfor %}
</ul>
"""

if __name__ == "__main__":
    main()
