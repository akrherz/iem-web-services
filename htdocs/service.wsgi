"""Our front-end to IEM JSON Services

1. IEM Requests come in like so:  /api/version/servicename.json?queryparams
2. Apache proxies to http://iem-web-services.local/service.wsgi
3. We profit
"""
import traceback
import sys
import os

import numpy as np
from paste.request import parse_formvars  # @UnresolvedImport

# Attempt to stop hangs within mod_wsgi and numpy
np.seterr(all='ignore')


def update_syspath():
    """Update our system.path"""
    basedir, _wsgi_filename = os.path.split(__file__)
    path = os.path.normpath("%s/.." % (basedir, ))
    if path not in sys.path:
        sys.path.insert(0, path)


update_syspath()
from pylib import dispatch  # NoPEP8 pylint: disable=wrong-import-position


def application(environ, start_response):
    """Our Application!"""
    fields = parse_formvars(environ)
    try:
        return dispatch(fields, environ, start_response)
    except Exception as _exp:
        sys.stderr.write("IWS Exception: %s\n" % (environ.get('REQUEST_URI'),))
        traceback.print_exc()
        res = ['Sorry, an unexpected error happened...']
        response_headers = [('Content-type', 'text/plain'),
                            ('Content-Length', str(len(res[0])))]
        start_response("500 Internal Server Error", response_headers)
        return res
