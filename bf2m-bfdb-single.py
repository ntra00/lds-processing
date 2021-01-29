#!/usr/bin/env python
#  works on list of specific lc bibids (instance Ids in bfdb)
# works on a curl of an instance id: c0213880820001 https://preprod-8230.id.loc.gov/resources/instances/c0213880820001.marc-pkg.xml
#  
import glob
import sys
from lxml import etree as ET
from lxml.builder import ElementMaker
import os
import shutil
import json
import multiprocessing
import subprocess


#       ***    Main Program    *****
#print("*** BF to MARC testing tool                                              ***")
#print("*** Make a list of instance IDs in 'bf-ids.txtt' ***")
#print('*** results in "out/mrc.xml"                                              ***')
####################

indir= "in/"
shutil.rmtree("in")
os.mkdir("in")
outdir="out/"

if len(sys.argv) >1 :
    path_to_data = sys.argv[1]
else:
                   # path_to_data = "/marklogic/nate/python-tutorial/"
    path_to_data= os.getcwd()


infile='bf-ids.txt'

outfile = outdir + 'mrc.xml'
efilename= outdir + '/error.txt'

biblist=open(infile ,'r')
	# this ignores \n :
bibids = biblist.read().splitlines()

curl = "curl -L 'https://preprod-8230.id.loc.gov/resources/instances/%BIBID%.marc-pkg.xml' > in/%OUTFILE%.rdf"



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
           print(counter,'/',len(files))

	print ("starting "+file)

        bftree = ET.parse(file)

        bfroot = bftree.getroot()
        print("bfroot tag:"+bfroot.tag)

        bf2marc=ET.parse("/marklogic/applications/bibframe2marc/bibframe2marc.xsl")
        bf2marcxsl=ET.XSLT(bf2marc)


	       # result has marc
### new:
        try:
            result=bf2marcxsl(bfroot)
        except:
            print("Unexpected error:", sys.exc_info()[0], sys.exc_info()[1] )
            for info in sys.exc_info():
                print(info)

        record= ET.XML(bytes(result))
        coll.insert(counter,record)
###new:
    out.write(ET.tostring(coll))
  
out.close      

#os.system("cat out/mrc.xml")
#print(glob.glob("out/*xml"))

