"""Our front-end to IEM JSON Services

1. IEM Requests come in like so:  /api/version/servicename.json?queryparams
2. Apache proxies to http://iem-web-services.local/service.wsgi
3. We profit
"""
import sys
import os

from paste.request import parse_formvars  # @UnresolvedImport


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
    return dispatch(fields, environ, start_response)
