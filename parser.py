import os, pandas, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep

from biothings import config
logging = config.logger


# we'll remove space in keys to make queries easier. Also, lowercase is preferred
# for a BioThings API. We'll an helper function from BioThings SDK
process_key = lambda k: k.replace(" ","_").lower()


def load_dbsnp(data_folder):
    infile = os.path.abspath("/opt/biothings/Dbsnp.tsv")
    assert os.path.exists(infile)
    dat = pandas.read_csv(infile,sep="\t",squeeze=True,quoting=csv.QUOTE_NONE).to_dict(orient='records')
    results = {}
    for rec in dat:
        _id = rec["release"] + "_" + str(rec["chromosome"]) + "_" + str(rec["position"]) + "_" + rec["reference"] + "_" + rec["alternative"] + "_" + rec["rsid"]         # remove NaN values, not indexable
        results.setdefault(_id,[]).append(rec)
        
    for _id,docs in results.items():
        doc = {"_id": _id, "dbsnp" : docs}
        yield doc


def load_GnomadGenomes(data_folder):
    infile = os.path.abspath("/opt/biothings/GnomadGenomes.1.1000.tsv")
    assert os.path.exists(infile)
    dat = pandas.read_csv(infile,sep="\t",squeeze=True,quoting=csv.QUOTE_NONE).to_dict(orient='records')
    results = {}
    for rec in dat:
        _id = rec["release"] + "_" + str(rec["chromosome"]) + "_" + str(rec["position"]) + "_" + rec["reference"] + "_" + rec["alternative"]        # remove NaN values, not indexable
        rec = dict_sweep(rec,vals=[np.nan])
        results.setdefault(_id,[]).append(rec)
        
    for _id,docs in results.items():
        doc = {"_id": _id, "gnomadgenomes" : docs}
        yield doc


def load_clinvar(data_folder):
    infile = os.path.abspath("/opt/biothings/Clinvar.1000.tsv")
    assert os.path.exists(infile)
    dat = pandas.read_csv(infile,sep="\t",squeeze=True,quoting=csv.QUOTE_NONE).to_dict(orient='records')
    results = {}
    for rec in dat:
        _id = rec["release"] + "_" + str(rec["chromosome"]) + "_" + str(rec["start"]) + "_" + str(rec["end"]) + "_" + rec["reference"] + "_" + rec["alternative"]        # remove NaN values, not indexable
        rec = dict_sweep(rec,vals=[np.nan])
        results.setdefault(_id,[]).append(rec)
        
    for _id,docs in results.items():
        doc = {"_id": _id, "clinvar" : docs}
        yield doc

