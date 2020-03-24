"""Stress test production"""
from __future__ import print_function
from tqdm import tqdm
import requests
from pyiem.util import get_dbconn


def main():
    """Go Main Go"""
    pgconn = get_dbconn("mesosite")
    cursor = pgconn.cursor()
    networks = []
    cursor.execute("""SELECT id from networks""")
    for row in cursor:
        networks.append(row[0])
    for fmt in ["json", "txt"] * 5:
        for network in tqdm(networks, desc=fmt):
            uri = ("http://iem.local/" "api/1/currents.%s?network=%s") % (fmt, network)
            req = requests.get(uri, timeout=30)
            if fmt == "json":
                data = req.json()
                if data["data"] and not isinstance(data["data"][0], dict):
                    print("uh oh, data is not a dict!")
                    print(type(data["data"][0]))
            if req.status_code != 200:
                print("status_code: %s" % (req.status_code,))
                print(req.content)


if __name__ == "__main__":
    main()
