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

# for each entry, curl the marc pck to the our dir, get next 
def get_feed_records(atom):
        
    for entry in atom.iterfind('.//{http://www.w3.org/2005/Atom}entry'):
        for id in entry:
            if( id.tag=="{http://www.w3.org/2005/Atom}id"  and "instances" in id.text ):
                bibid= id.text.rsplit("/")[4]
                curlcmd = curl.replace('%BIBID%', bibid)
                curlcmd = curlcmd.replace('%OUTFILE%', bibid)
                returned_value = subprocess.Popen(curlcmd, shell=True).wait()
# feeds may be more than one page
    for nexturl in atom.find('.//{http://www.w3.org/2005/Atom}link[@rel="next"]/@href')
        get_feed_records(nexturl)
         
##############
#       *** Main Program *****

print("*** BF to MARC testing tool ***")
print("*** Converts the latest feed to MARC ***")

print ()

####################
config=get_config(args) 


yesterday = date.today() - timedelta(days=1)
yesterday=yesterday.strftime('%Y-%m-%d')
config = yaml.safe_load(open(args.config))
job=args.job 
jobconfig = config[job] 
indir=jobconfig["source_directory"]
outdir=jobconfig["source_directory"]
feed=jobconfig["feed"]
feedurl=feed.replace('%YESTERDAY%',yesterday)
curl=jobconfig["curl"]
outfile = outdir + 'bf-'+yesterday+'-mrc.xml'
efilename= outdir + '/error.txt'

print()
print ("-----------------------------")
print("Job config:")
print(jobconfig)
print ("yesterday is ", yesterday)
print()
print ("feed url is ", jobconfig["feed"])
print ("In dir is " , indir)
print ("Out dir is " , outdir)
print('results in ',outdir,'/bf-',yesterday,'-mrc.xml')
print ("feed url is ", feedurl)
print ("-----------------------------")
infile = urllib.request.urlopen(feedurl).read()

atom=ET.XML(infile)
get_feed_records(atom)

bfstylesheet=jobconfig["bfstylesheet"]
bf2marc=ET.parse(bfstylesheet)
bf2marcxsl=ET.XSLT(bf2marc)

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
        bftree = ET.parse(file)
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
print ("Done with ",yesterday,: check: ", outdir,"/bf-",yesterday,"-mrc.xml")
#print(glob.glob("out/*xml"))
