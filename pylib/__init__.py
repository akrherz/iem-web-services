"""Makes us importable"""
import sys
import imp

import pandas as pd
import memcache


def get_header_by_extension(extension):
    """What is our mime-type for this extension"""
    if extension == 'txt':
        return "text/plain"
    return "application/json"


def dispatch(fields, environ, start_response):
    """Our main dispatcher"""
    cb = fields.get('callback', None)
    version = fields.get('_version', 1)
    service = fields.get('_service', 'helloworld')
    fmt = fields.get("_format", "json")
    name = "pylib/services/%s" % (service, )
    mod = sys.modules.get(name, None)
    if mod is None:
        (fn, pathname, description) = imp.find_module(name)
        mod = imp.load_module(name, fn, pathname, description)
    mc = memcache.Client(['iem-memcached:11211'], debug=0)
    mckey = None
    if hasattr(mod, 'get_mckey'):
        mckey = ("/api/%s/%s.%s/%s" % (version, service, fmt,
                                       mod.get_mckey(fields)))[:250]
        res = mc.get(mckey)
        if res:
            response_headers = [('Content-type', get_header_by_extension(fmt)),
                                ('Content-Length', str(len(res)))]
            start_response("200 OK", response_headers)
            if cb:
                # TODO this needs to be XSS safe
                return ["%s(%s)" % (cb, res)]
            return [res.encode('utf-8')]
    res = mod.handler(version, fields, environ)
    if isinstance(res, pd.DataFrame):
        # Needs this to avoid OverFlowError?
        data = res.to_json(orient='table', default_handler=str)
        res = data

    if mckey:
        mc.set(mckey, res, getattr(mod, 'CACHE_EXPIRE', 3600))
    response_headers = [('Content-type', get_header_by_extension(fmt)),
                        ('Content-Length', str(len(res)))]
    start_response("200 OK", response_headers)

    return [res.encode('utf-8')]
