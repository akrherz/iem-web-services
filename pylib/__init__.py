"""Makes us importable"""
import sys
import imp

import pandas as pd
import memcache


def dispatch(fields, environ, start_response):
    """Our main dispatcher"""
    cb = fields.get('callback', None)
    version = fields.get('_version', 1)
    service = fields.get('_service', 'helloworld')
    name = "pylib/services/%s" % (service, )
    (fn, pathname, description) = imp.find_module(name)
    mod = imp.load_module(name, fn, pathname, description)
    mc = memcache.Client(['iem-memcached:11211'], debug=0)
    mckey = None
    if hasattr(mod, 'get_mckey'):
        mckey = "/api/%s/%s/%s" % (version, service, mod.get_mckey(fields))
        res = mc.get(mckey)
        if res:
            response_headers = [('Content-type', 'application/json'),
                                ('Content-Length', str(len(res)))]
            start_response("200 OK", response_headers)
            if cb:
                return ["%s(%s)" % (cb, res)]
            return [res]
    res = mod.handler(version, fields, environ)
    if isinstance(res, pd.DataFrame):
        res = res.to_json(orient='table')

    if mckey:
        mc.set(mckey, res, 3600)
    response_headers = [('Content-type', 'application/json'),
                        ('Content-Length', str(len(res)))]
    start_response("200 OK", response_headers)

    return [res]
