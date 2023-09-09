#!/usr/bin/env python
import json
import pprint
import signal

import pybliometrics
from pybliometrics.scopus.exception import Scopus429Error, ScopusQueryError, Scopus400Error
from requests.exceptions import ConnectionError
from progressbar import progressbar

print = pprint.pprint
table = str.maketrans(dict.fromkeys('*'))

names =  ['Cassidy Laidlaw', 'Stuart Russell']



def handler(signum, frame):
    raise TimeoutError("scopus request timeout")


def try_scopus(name):
    try:
        name = name.translate(table).split(" ")
        last = name[-1]
        first = " ".join(name[:-1])
        # signal.signal(signal.SIGALRM, handler)
        # signal.alarm(10)
        try:
            query = f"authlast({last}) and authfirst({first})"
            reply = pybliometrics.scopus.AuthorSearch(query, count=1, download=True, max_entries=1e6, subscriber=True)
        except (TimeoutError, ConnectionError):
            return
        except (Scopus400Error):
            print(f"invalid query: {query}")
            return
        # signal.alarm(0)

        if not reply.get_results_size() or not reply.authors[0].eid:
            return
        id = reply.authors[0].eid

        result = pybliometrics.scopus.AuthorRetrieval(id)
        print(result.__dict__)

    except Scopus429Error:
        print("[!] please rotate API key")
        return

for name in names:
    try_scopus(name)






