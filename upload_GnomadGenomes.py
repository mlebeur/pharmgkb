import os

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader

from .parser import load_druglabels


class DrugLabelsUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    main_source = "test5"
    name = "GnomadGenomes"
    __metadata__ = {"src_meta": {}}
    idconverter = None
    storage_class = biothings.hub.dataload.storage.BasicStorage

    def load_data(self, data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return load_GnomadGenomes(data_folder)

    @classmethod
    def get_mapping(klass):
        return {}
