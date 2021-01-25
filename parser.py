import os, pandas, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep

from biothings import config
logging = config.logger

def load_annotations(data_folder):
    infile = os.path.join(data_folder,"GnomadGenomes.1.1000.tsv")
    assert os.path.exists(infile)
    dat = pandas.read_csv(infile,sep="\t",squeeze=True,quoting=csv.QUOTE_NONE).to_dict(orient='records')
    results = {}
    for rec in dat:
        _id = rec["release"] + "_" + str(rec["chromosome"]) + "_" + str(rec["position"]) + "_" + rec["reference"] + "_" + rec["alternative"]        # remove NaN values, not indexable
        rec = dict_sweep(rec,vals=[np.nan])
        results.setdefault(_id,[]).append(rec)
        
    for _id,docs in results.items():
        doc = {"_id": _id, "annotations" : docs}
        yield doc
