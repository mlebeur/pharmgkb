import os

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader

from .parser import load_annotations


class AnnotationsUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    main_source = "pharmgkb"
    name = "dbsnp"
    __metadata__ = {"src_meta": {}}
    idconverter = None
    storage_class = biothings.hub.dataload.storage.BasicStorage

    def load_data(self, data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return load_dbsnp(data_folder)

    
    @classmethod
    def get_mapping(klass):
        return {}
