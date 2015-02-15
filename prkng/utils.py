# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from __future__ import print_function

import sys
import os
import requests


def download_progress(url, filename, directory):
    """
    Downloads from ``url`` and shows a simple progress bar

    :param url: resource to download
    :param filename: destination filename
    :param directory: destination directory
    """
    req = requests.get(url, stream=True)

    resource_size = req.headers.get('content-length')

    full_path = os.path.join(directory, filename)

    with open(full_path, 'wb') as dest:
        downloaded = 0
        print("[", end='')
        if resource_size is None:
            # no content length header
            print("=" * 50, end='')
            for chunk in req.iter_content(1024):
                dest.write(chunk)
        else:
            print_every_bytes = int(resource_size) / 50
            next_print = 0
            for chunk in req.iter_content(1024):
                downloaded += len(chunk)
                dest.write(chunk)

                if downloaded >= next_print:
                    sys.stdout.write("=")
                    sys.stdout.flush()
                    next_print += print_every_bytes

        print("] Download complete...")
    return full_path
