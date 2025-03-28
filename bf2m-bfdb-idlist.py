#!/usr/bin/env python

#works on atom feed url of instances loaded (instance Ids in bfdb) works on a curl of an instance id: c0213880820001 
# https://preprod-8230.id.loc.gov/resources/instances/c0213880820001.marc-pkg.xml
#
import glob
import sys
from lxml import etree as ET
from lxml.builder import ElementMaker

import os
import shutil

import multiprocessing
import subprocess

import urllib

import argparse

import feedparser

import yaml
from datetime import date, timedelta
from modules.helpers import get_config
from modules.config_parser import args

# for each entry, curl the marc pck to the our dir, get next feed url (entry rel=next/@href)

class FileResolver(ET.Resolver):
    def resolve(self, url, pubid, context):
        return self.resolve_filename(url, context)

####################
#  Main Program 

print("*** BF to MARC List of Instance IDs ***")

print ()
####################

config=get_config(args )

config = yaml.safe_load(open(args.config))
job=args.job
jobconfig = config[job]
indir=jobconfig["source_directory"]
outdir=jobconfig["target_directory"]
filename=jobconfig["infile"]
infile=outdir+filename
idtype="bib"
curl=jobconfig["curl"]

parser = ET.XMLParser()
parser.resolvers.add(FileResolver())

efilename= outdir + '/error.txt'
# clean out the input dir: (rdf only)
files = glob.glob(indir+'*.rdf')
for f in files:
     os.remove(f)

outfile = outdir + filename.replace('txt','xml')


print()
print ("-----------------------------")
print("Job config:")
print(jobconfig)
print ("In dir is " , indir)
print ("Out dir is " , outdir)
print('results in :',outfile)

print ("-----------------------------")

bfstylesheet=jobconfig["bfstylesheet"]

bf2marc=ET.parse(bfstylesheet,parser)
bf2marcxsl=ET.XSLT(bf2marc)
biblist=open(infile ,'r')
bibids = biblist.read().splitlines()
curl = "curl -L 'https://preprod-8230.id.loc.gov/resources/instances/%BIBID%.marc-pkg.xml' > in/%OUTFILE%.rdf"

for bibid in bibids:
   #  print ("curling from metaproxy dev: "+ bibid)
    curlcmd = curl.replace('%BIBID%', bibid) 
    curlcmd = curlcmd.replace('%OUTFILE%', bibid) 
    returned_value = subprocess.Popen(curlcmd, shell=True).wait()



#============================
bibfiles=list(glob.glob(indir+'*.rdf')) 
counter = 0

# create output marcxml:collection:
M= ElementMaker(namespace="http://www.loc.gov/MARC21/slim" ,
    nsmap={"marc":"http://www.loc.gov/MARC21/slim"})
coll=M.collection()
with open(outfile,'wb') as out:
    for file in bibfiles:
        counter+=1
        if counter % 100 == 0:
            print(counter,'/',len(bibfiles))
        print ("converting to marc: "+file)
        bftree = ET.parse(file,parser)
        bfroot = bftree.getroot()
             # result has marc
        try:
                result=bf2marcxsl(bfroot)
        except:
                print("Unexpected error:", sys.exc_info()[0], sys.exc_info()[1] )
                for info in sys.exc_info():
                    print(info)
        record= ET.XML(bytes(result))
        coll.insert(counter,record)
    out.write(ET.tostring(coll))
out.close
print ("Done with ",job, " job : check: ", outfile)
#print(glob.glob("out/*xml"))
