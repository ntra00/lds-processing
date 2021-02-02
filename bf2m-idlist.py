#!/usr/bin/env python
#  works on list of specific lc bibids in a text file
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

from datetime import date, timedelta
from modules.helpers import get_config
from modules.config_parser import args


def runxslt(parsedfile, stylesheet):
    print ("transforming with "+stylesheet)
    root = parsedfile.getroot()
    xslt = ET.parse(stylesheet)
    transform = ET.XSLT(xslt)
    resultxml = transform(root)

    return resultxml;


#print("*** BF to MARC testing tool                                              ***")
#print("*** Make a list of lccns in 'lccns.txt' or a list of bibids in 'ids.txt' ***")
#print('*** If you want to use the lccns file, "./bf2m.py lccn"                  ***')
#print('*** For bibids, use : "./bf2m.py"                                        ***')
#print('*** results in "out/mrc.xm"                                              ***')
####################
# existing jowill script:
#	curl -L "http://mprxy-dev.loc.gov:210/LCDB?query=rec.id=$marcbibid&recordSchema=marcxml&maximumRecords=1" -o in/tmp.xml
#	xsltproc  getrecord.xsl in/tmp.xml > in/tmp1.xml
#	yaz-record-conv  /marklogic/id/marc2bibframe2/record-conv.vlp3.xml  in/tmp1.xml   |sed -e "s|idwebvlp03.loc.gov|id.loc.gov|g" | sed -e "s|mlvlp04.loc.gov:8080|id.loc.gov|g" | sed -e "s|//mlvlp06.loc.gov:8288|http://id.loc.gov|g"   > in/$marcbibid.rdf	
#	xsltproc -o out/$marcbibid.xml /marklogic/id/bf2marcbranch/bibframe2marc/bibframe2marc.xsl  in/$marcbibid.rdf
#	ls -ltr  */$marcbibid.*
#######

config=get_config(args) 

config = yaml.safe_load(open(args.config))
job=args.job 
jobconfig = config[job] 
indir=jobconfig["source_directory"]
outdir=jobconfig["target_directory"]
filename=jobconfig["infile"]
infile=indir+filename
utilsdir=jobconfig["utilsdir"]
    
if "lccn" in  filename :
    idtype="lccn"
    
else :
    idtype="bib"
    
files = glob.glob('indir*')
for f in files:
    os.remove(f)


outfile = outdir + replace(infile,'txt','xml')

biblist=open(infile ,'r')
	# this ignores \n :
bibids = biblist.read().splitlines()

if idtype == "lccn":
#    print("Getting lccns from :"+infile)
    curl = "curl -L 'http://mprxy-dev.loc.gov:210/LCDB?query=bath.lccn=%BIBID%&recordSchema=bibframe2a-dev&maximumRecords=1' > in/%OUTFILE%.rdf"
else:
 #   print("Getting bib ids from :"+infile)
    curl = "curl -L 'http://mprxy-dev.loc.gov:210/LCDB?query=rec.id=%BIBID%&recordSchema=bibframe2a-dev&maximumRecords=1' > in/%OUTFILE%.rdf"

count = 0
for bibid in bibids: 
  #  print ("curling from metaproxy dev: "+ bibid)
    curlcmd = curl.replace('%BIBID%', bibid)
    curlcmd = curlcmd.replace('%OUTFILE%', bibid)
    returned_value =  subprocess.Popen(curlcmd, shell=True).wait()

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
        

        bftree = ET.parse(file)
#        bfroot = bftree.getroot()
#        getbfxsl = ET.parse('get-bf.xsl')
 #       bftransform = ET.XSLT(getbfxsl)
  #      bfrdf = bftransform(bfroot)
        bfrdf=runxslt(bftree,utilsdir+"get-bf.xsl")

        bfroot = bfrdf.getroot()
        graphxsl = ET.parse(utilsdir+'graphiphy.xsl')
        graphtransform = ET.XSLT(graphxsl)
        graphed = graphtransform(bfroot)

        bf2marc=ET.parse(jobconfig["bfstylesheet"])
        bf2marcxsl=ET.XSLT(bf2marc)
        graphedxml=graphed.getroot()

	     # for each "graph/record"
        for c in graphedxml.iterfind('.//{http://id.loc.gov/ontologies/lclocal/}graph'):
              	   E = ElementMaker(namespace="http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
      	 	       nsmap={"lclocal":"http://id.loc.gov/ontologies/lclocal/",
    		   	      "rdfs":"http://www.w3.org/2000/01/rdf-schema#",
		              "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		              "madsrdf":"http://www.loc.gov/mads/rdf/v1#",
		              "bf":"http://id.loc.gov/ontologies/bibframe/",
		              "bflc":"http://id.loc.gov/ontologies/bflc/"})

	   #f=E.RDF()
        #   for node in c:
		 #     f.append(node)
 	   #xmlfile=open("bf2mlist.rdf","wb")
        #   xmlfile.write(ET.tostring(f))
         #  xmlfile.close
	      
	       # result has marc
           result = bf2marcxsl(f)
	       # convert xslt result to xml
           record= ET.XML(bytes(result))
	       # insert the record into the collection
           coll.insert(1,record)   

    out.write(ET.tostring(coll))
  
out.close      

os.system("cat out/mrc.xml")
print("\n")
#print(glob.glob("out/*xml"))

