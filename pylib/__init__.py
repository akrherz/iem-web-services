"""Makes us importable"""
import sys
import imp


def dispatch(fields, environ):
    """Our main dispatcher"""
    version = fields.get('version', 1)
    service = fields.get('service', 'helloworld')
    name = "pylib/services/%s" % (service, )
    (fn, pathname, description) = imp.find_module(name)
    mod = imp.load_module(name, fn, pathname, description)
    return mod.handler(version, fields, environ)
