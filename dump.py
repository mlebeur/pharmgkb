import os

import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall

import biothings.hub.dataload.dumper


class PharmgkbDumper(biothings.hub.dataload.dumper.LastModifiedHTTPDumper):

    SRC_NAME = "pharmgkb"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    SCHEDULE = None
    UNCOMPRESS = False
    SRC_URLS = [
    ]
    __metadata__ = {"src_meta": {}}

