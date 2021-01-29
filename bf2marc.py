#!/usr/bin/env python

import yaml
import sys
import os

import subprocess

import glob

import datetime
from time import gmtime, strftime, sleep

from shutil import rmtree

from multiprocessing.dummy import Pool as ThreadPool

from lxml import etree
from io import StringIO, BytesIO

import itertools

from os import listdir
from os.path import isdir, isfile, join

from modules.config_parser import args

def Convert(config, j):
    config = config
    n = j["pos"]

# processing job is bf2marc xslt  
    
    graphcmd =  "cd %TMPDIR% && xsltproc %MODULES%/graphiphy.xsl  %TMPFILE% > %TMPFILE%.rdf "
    graphcmd = graphcmd.replace('%MODULES%', j["modulesdir"])
    graphcmd = graphcmd.replace('%TMPDIR%', j["tmpdir"])
    graphcmd = graphcmd.replace('%TMPFILE%', j["tmpfile"])
    print ("Graphcmd :", graphcmd)
    returned_value = subprocess.Popen(graphcmd, shell=True).wait()
#
    print("Processing job: " + str(n) + "; " + j["infile"] + "; " + j["outfile"])
    cmd = config["command"]
    cmd = cmd.replace('%INFILE%', j["infile"])
    cmd = cmd.replace('%OUTFILE%', j["tmpfile"])
    returned_value =  subprocess.Popen(cmd, shell=True).wait()


#    rmtree(j["tmpdir"])

def dircontents(path, files):
    for f in listdir(path):
        fpath = join(path, f)
        if isfile(fpath) and fpath.endswith('.xml'):
            files.append(fpath)
        elif isdir(fpath):
            dircontents(fpath, files)
    return files

config = yaml.safe_load(open(args.config))
print()
print("Config:")
print(config)
print()

jobconfig = config["bf2marc"]
print("Job config:")
print(jobconfig)
print()

files = []
files = dircontents(jobconfig["source_directory"], files)

#print(str(len(files)))
# files = files[:10]
#print(files)
#print(str(len(files)))
#print()

pos = 1
dirs = []
jobs = []
for f in files:
   infile = f
   fo = open(infile , "rb", 1)
   parser = etree.XMLParser(remove_blank_text=True)
   root = etree.XML(fo, parser)
   
   
   modulesdir = jobconfig["modules_directory"]
   tmpdir = jobconfig["tmp_processing_directory"]
   
   if jobconfig["target_directory_single_dir"]:
        outfile = f.replace(jobconfig["source_directory"], '')
        outfile = jobconfig["target_directory"]  + outfile
        tmpfile = tmpdir + f.replace(jobconfig["source_directory"], "")
   else:
        outfile = f.replace(jobconfig["source_directory"], jobconfig["target_directory"])
        dirs.append(outfile)
   j = {
        "pos": pos,
        "infile": infile,
        "outfile": outfile,
        "tmpdir": tmpdir,
        "modulesdir": modulesdir,
        "tmpfile": tmpfile
   }
   jobs.append(j)
   pos += 1

if jobconfig["clean_target_directory"]:
    for f in glob.glob(jobconfig["target_directory"] + "*", recursive=True):
        if isfile(f):
            os.unlink(f)

for d in dirs:
    os.makedirs(os.path.dirname(d), exist_ok=True)

print()
print("Number of threads: " + str(jobconfig["threads"]))
print("Total number of jobs: " + str(len(jobs)))
print()
st = datetime.datetime.now()
starttime = strftime("%Y-%m-%d %H:%M:%S", gmtime())


# make the Pool of workers
pool = ThreadPool(jobconfig["threads"])

# open the urls in their own threads
# and return the results
results = pool.starmap(Convert, zip(itertools.repeat(jobconfig), jobs))

# close the pool and wait for the work to finish
pool.close()
pool.join()

gt = gmtime()
endtime = strftime("%Y-%m-%d %H:%M:%S", gt)
et = datetime.datetime.now()
timedelta = et - st

print()
print()
print ("Task started at: " + starttime)
print ("Task ended at: " + endtime)
print ("Elapsed time: ", str(timedelta))
print()
print()


