#!/usr/bin/env python
#  works on list of specific lc recids in a text file, whether bibid or lccn
#  finds the marc record and saves it,
# this is not finished; it uses metaproxy, not ID or bfdb 
#  
import glob
import sys
from lxml import etree as ET
from lxml.builder import ElementMaker
import os
import shutil

import multiprocessing
import subprocess

import yaml

from datetime import date, timedelta, datetime

from modules.helpers import get_config
from modules.config_parser import args
# experiment from https://stackoverflow.com/questions/8831941/lxml-and-xsl-document-function
class FileResolver(ET.Resolver):
    def resolve(self, url, pubid, context):
        return self.resolve_filename(url, context)


def runxslt(parsedfile, stylesheet,parser):
    #print ("transforming with "+stylesheet)
    root = parsedfile.getroot()
    xslt = ET.parse(stylesheet,parser)
    transform = ET.XSLT(xslt)
    resultxml = transform(root)

    return resultxml;


print("*** MARC exporting tool                                                  ***")
print("*** Make a list of lccns in 'lccns.txt' or a list of bibids in 'ids.txt' ***")
print('*** If you want to use the lccns file, "./bf2m.py lccn"                  ***')
print('*** For bibids, use : "./bf2m.py"                                        ***')
print('*** results in "out/mrc.xm"                                              ***')
####################
# existing jowill script:
#	curl -L "http://mprxy-dev.loc.gov:210/LCDB?query=rec.id=$marcbibid&recordSchema=marcxml&maximumRecords=1" -o in/tmp.xml
#	xsltproc  getrecord.xsl in/tmp.xml > in/tmp1.xml
#	yaz-record-conv  /marklogic/id/marc2bibframe2/record-conv.vlp3.xml  in/tmp1.xml   |sed -e "s|idwebvlp03.loc.gov|id.loc.gov|g" | sed -e "s|mlvlp04.loc.gov:8080|id.loc.gov|g" | sed -e "s|//mlvlp06.loc.gov:8288|http://id.loc.gov|g"   > in/$marcbibid.rdf	
#	xsltproc -o out/$marcbibid.xml /marklogic/id/bf2marcbranch/bibframe2marc/bibframe2marc.xsl  in/$marcbibid.rdf
#	ls -ltr  */$marcbibid.*
#######

#config=get_config(args) 

config = yaml.safe_load(open(args.config))

job=args.job 
jobconfig = config[job] 
indir=jobconfig["source_directory"]
outdir=jobconfig["target_directory"]
filename=jobconfig["infile"]
infile=outdir+filename
utilsdir=jobconfig["utilsdir"]
metaproxybase=jobconfig["metaproxybase"]

if "lccn" in  filename :
    idtype="lccn"

else :
    idtype="bib"
# clean out the input dir: (rdf only)
files = glob.glob(indir+'*.rdf')
for f in files:
    os.remove(f)

timestamp = datetime.now()
timeprefix=timestamp.strftime('%y%m%d.%H%M')

#yesterday = date.today() - timedelta(days=1)
#yesterday = yesterday.strftime('%Y-%m-%d')
outfile = outdir + timeprefix+"."+filename.replace('txt','xml')

biblist=open(infile ,'r')
	# this ignores \n :
recids = biblist.read().splitlines()
if "dev" in metaproxybase :  
    schema="bibframe2a-dev"
else :
    schema="bibframe2a"
# get metaproxy biframe, but suppress error/processing output: " 2&> /dev/null "
curl = "curl -L '"+metaproxybase+"LCDB?query=%FIELD%=^%RECID%$&recordSchema=%SCHEMA%&maximumRecords=1'2&> /dev/null > in/%OUTFILE%.rdf"

if idtype == "lccn":
    field="bath.lccn"
else:
    field="rec.id"

curl =curl.replace("%FIELD%",field)    
curl =curl.replace("%SCHEMA%",schema)   

count = 0
for recid in recids: 
  #  print ("curling from metaproxy dev: "+ recid)
    if len(recid) < 7 or recid == "" :
        continue
    curlcmd = curl.replace('%RECID%', recid)
    curlcmd = curlcmd.replace('%OUTFILE%', recid)
    print (curlcmd)
    returned_value =  subprocess.Popen(curlcmd, shell=True).wait()

bffiles=list(glob.glob(indir+'*.rdf')) 

parser = ET.XMLParser()
parser.resolvers.add(FileResolver())

bf2marc=ET.parse(jobconfig["bfstylesheet"],parser) 
bf2marcxsl=ET.XSLT(bf2marc)

graphxsl = ET.parse(utilsdir+'graphiphy.xsl',parser)
graphtransform = ET.XSLT(graphxsl)

counter = 0

# create output marcxml:collection:
M= ElementMaker(namespace="http://www.loc.gov/MARC21/slim" ,
                nsmap={"marc":"http://www.loc.gov/MARC21/slim"})
coll=M.collection()
error_state = False
with open(outfile,'wb') as out:
#      for each bibframe record curled :
    for file in bffiles:
        counter+=1
        if counter % 100 == 0:
            print(counter,'/',len(bffiles))

        bftree = ET.parse(file,parser)
          # filter away the zs wrappers
        try:
            bfrdf = runxslt(bftree,utilsdir+"get-bf.xsl",parser)
        except:
            error_state = True
            logmessage = "BF record not found?"
        if error_state == False:
            bfroot = bfrdf.getroot()
           # don't need graphiphy because the curl is for a marc pkg in metaproxy
            
	       # result has marc
            try:
                result = bf2marcxsl(bfroot)
#                       result=transform(bf_input)
            except  OSError as err:
                print("OS error: {0}".format(err))
                error_state= True
            except: 
                print("Unexpected error:", sys.exc_info()[0])
                for error in bf2marcxsl.error_log:
                    print(error.message, error.line)
                error_state=True
                print("Skipping ",file, "for bf2marc error") 
                pass
            if error_state == False:
	       # convert xslt result to xml
                record= ET.XML(bytes(result))
	       # insert the record into the collection
                coll.insert(1,record)

    out.write(ET.tostring(coll))

out.close 

os.system("ls -ltr out/*.xml")
print("\n")
