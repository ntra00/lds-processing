#!/usr/bin/env python
####!/marklogic/data/ntra/fmenv/bin/python3
#####!/usr/bin/python  fails on from modules
###### !/usr/bin/env python: fails on lxml
######!/marklogic/data/kefo/work/fmenv/bin/python3

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
import urllib3
#import urllib2
import argparse

import feedparser

import yaml
from datetime import date, timedelta
from modules.helpers import get_config
from modules.config_parser import args

config=get_config(args)

#from modules.helpers import get_config

#       *** Main Program *****


print("*** BF to MARC testing tool ***")
print("*** Converts the latest feed to MARC ***")

####################

yesterday = date.today() - timedelta(days=1)
yesterday=yesterday.strftime('%Y-%m-%d')

config = yaml.safe_load(open(args.config))
#config=get_config(args)
job=args.job 
jobconfig = config[job] 
indir=jobconfig["source_directory"]
outdir=jobconfig["source_directory"]
feed=jobconfig["feed"]
print() 
print("Config:")
#print(config)
print()
print("Job config:")
print(jobconfig)
print ("yesterday is ", yesterday)
print()
print ("feed url is ", jobconfig["feed"])
print (indir)
print(outdir)
print('*** results #in out/bf', yesterday,'.xml ***')
#infile='bf-ids.txt'
#print("this works on processing server but skip for now:")
http = urllib3.PoolManager()

feedurl=feed.replace('%YESTERDAY%',yesterday)
#infile = http.request('GET',"https://preprod-8231.id.loc.gov/resources/bfedits/2021-01-08/feed/1")
print ("feed url final is ", feedurl)
infile= http.request('GET',feedurl)


#infile="today.xml"
curl=jobconfig["curl"]
#curl = "curl -L 'https://preprod-8230.id.loc.gov/resources/instances/%BIBID%.marc-pkg.xml' > in/%OUTFILE%.rdf"
outfile = outdir + 'mrc.xml'
efilename= outdir + '/error.txt'

#atom= ET.parse(bytes(infile)
atom=ET.XML(infile)
bf2marc=ET.parse("/marklogic/applications/bibframe2marc/bibframe2marc.xsl")
bf2marcxsl=ET.XSLT(bf2marc)
count=0
# curl -L http://preprod-8230.id.loc.gov/resources/instances/feed/22 > today.xml
for entry in atom.iterfind('.//{http://www.w3.org/2005/Atom}entry'):
    for id in entry:
        if( id.tag=="{http://www.w3.org/2005/Atom}id"  and "instances" in id.text ):
            bibid= id.text.rsplit("/")[4]
            curlcmd = curl.replace('%BIBID%', bibid)
            curlcmd = curlcmd.replace('%OUTFILE%', bibid)
            returned_value = subprocess.Popen(curlcmd, shell=True).wait()

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
        bftree =         ET.parse(file)
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
#os.system("cat out/mrc.xml")
#print(glob.glob("out/*xml"))
