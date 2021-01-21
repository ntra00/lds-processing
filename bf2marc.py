#!/usr/bin/python2

#####!/marklogic/data/kefo/work/fmenv/bin/python3.7
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
import urllib2

from datetime import date, timedelta

#       *** Main Program ***** 
print("*** BF to MARC testing tool ***") 
print("*** Converts the latest feed to MARC ***") 
print('*** results #in "out/mrc.xml" ***')
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
yesterday = date.today() - timedelta(days=1)
yesterday.strftime('%Y-%m-%d')

infile = urllib2.urlopen("https://preprod-8231.id.loc.gov/resources/bfedits/2021-01-12/feed/1").read()




curl = "curl -L 'https://preprod-8230.id.loc.gov/resources/instances/%BIBID%.marc-pkg.xml' > in/%OUTFILE%.rdf" 
outfile = outdir + 'mrc.xml' 
efilename= outdir + '/error.txt' 

#atom= ET.parse(bytes(infile)) 
atom=ET.XML(infile)
bf2marc=ET.parse("/marklogic/applications/bibframe2marc/bibframe2marc.xsl") 
bf2marcxsl=ET.XSLT(bf2marc)
count=0

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
