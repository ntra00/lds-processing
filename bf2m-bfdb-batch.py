#!/usr/bin/env python

# Works on atom feed url of instances loaded (instance Ids in bfdb) works on a curl of an instance id: c0213880820001 
# Curls /resources/bfedits/2021-01-08/feed/1, then curls the instances in the feed, converts to marcxml 
# https://preprod-8230.id.loc.gov/resources/instances/21388082.marc-pkg.xml
# Designed to be a daily export of edited records
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

def get_feed_records(feedurl):
    feedurl=feedurl.replace(" ","+")

    with urllib.request.urlopen(feedurl) as response:
        page=response.read()
        atom=ET.XML(page)
        print(feedurl)
        for entry in atom.iterfind('.//{http://www.w3.org/2005/Atom}entry'):
            for id in entry:
                if( id.tag=="{http://www.w3.org/2005/Atom}id"  and "instances" in id.text ):
                    print(id.text)
#                    if job="daily":
 #                       bibid= id.text.rsplit("/")[4]
  #                  else:
                    bibid= id.text.rsplit("/")[3]
                    curlcmd = curl.replace('%BIBID%', bibid)
                    curlcmd = curlcmd.replace('%OUTFILE%', bibid)
                    returned_value = subprocess.Popen(curlcmd, shell=True).wait()

        ns={'atom': "http://www.w3.org/2005/Atom" }
        for nexturl in atom.xpath('//atom:link[@rel="next"]',namespaces=ns) :
             nextlink=nexturl.attrib['href']
             print("nextlink is " , nextlink)
             get_feed_records(nextlink)

####################
#  Main Program 

print("*** BF to MARC Yesterday's Atom Feed ***")
print("*** Converts the latest feed to MARC ***")

print ()
print("Set the Date to 'none' for yesterday, else format a specific date as YYYY-MM-DD") 
####################

config=get_config(args) 

config = yaml.safe_load(open(args.config))
job=args.job 
jobconfig = config[job] 
indir=jobconfig["source_directory"]
outdir=jobconfig["target_directory"]
curl=jobconfig["curl"]
feed=jobconfig["feed"]

parser = ET.XMLParser()
parser.resolvers.add(FileResolver())

efilename= outdir + '/error.txt'
# feed is either a daily feed (daily job or a search feed (searchfeed job)
if "searchfeed" in job:
    feedurl=feed
    outfile= outdir+job+".xml"
else:
    date2process=jobconfig["processdate"]
    if date2process=="none" :
        yesterday = date.today() - timedelta(days=1)
        yesterday = yesterday.strftime('%Y-%m-%d')
    else :
        yesterday=str(date2process)

    feedurl=feed.replace('%YESTERDAY%',yesterday)
    outfile = outdir + 'bf-'+yesterday+'-mrc.xml'

files = glob.glob(indir+'*')
for f in files:
    os.remove(f)

print()
print ("-----------------------------")
print("Job config:")
print(jobconfig)
if "daily" in job:
    print ("Yesterday is ", yesterday)
print()
print ("feed is ", feed)
print ("feed url is ", feedurl)
print ("In dir is " , indir)
print ("Out dir is " , outdir)
print('Results in :',outfile)

print ("-----------------------------")
get_feed_records(feedurl)

bfstylesheet=jobconfig["bfstylesheet"]

bf2marc=ET.parse(bfstylesheet,parser)
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
