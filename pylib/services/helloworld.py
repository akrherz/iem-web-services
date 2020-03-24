"""Something simple"""
import json


def handler(version, fields, environ):
    """Handle the request, return dict"""
    return json.dumps({"hello": "world"})
