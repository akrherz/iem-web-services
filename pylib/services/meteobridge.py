"""Something simple"""
import json
import sys


def handler(version, fields, environ):
    """Handle the request, return dict"""
    sys.stderr.write(repr(fields))   
    return json.dumps("OK")
