#!/usr/bin/python
#  works on files in a dir (daily transforms to bf, not exported from db)
# Takes a directory full of bf files, creates 1 or more marc files in marc:collection
import glob
import os
import sys
import shutil
import lxml
from lxml import etree as ET

from lxml.builder import ElementMaker
import json
import multiprocessing

def runxslt(parsedfile, xslconvert,transformname):
    print(ET.tostring(parsedfile))
    
    print ("transforming with "+transformname)
  
    root = parsedfile.getroot()    
    if (transformname=="bf2marc") :  
        print(ET.tostring(root))

    try:
         result = xslconvert(parsedfile)
         if (transformname=="bf2marc") :
		print(ET.tostring(parsedfile))
    except:
         print ("error using "+transformname) 
    print(ET.tostring(result))
    return result;


if len(sys.argv) >1 :
    path_to_data = sys.argv[1]
else:
		   # path_to_data = "/marklogic/nate/python-tutorial/"
    path_to_data= os.getcwd()

output_filename = path_to_data + '/out/mrc.xml'
efilename= path_to_data+ '/out/error.txt'
files = list(glob.glob(path_to_data + '/split_000000*.xml.rdf'))
shutil.rmtree("out")
os.mkdir("out")
print(len(files))

# stylesheets declared:
graphxsl= ET.XSLT( ET.parse('graphiphy.xsl'))
bf2marcxsl=ET.XSLT(ET.parse("/marklogic/applications/bibframe2marc/bibframe2marc.xsl"))

for file in files:
    outf=file.find('.xml')
    print (outf)
    outfil=file[len(path_to_data):outf]
    outfile=  'out'+outfil+'.mrc.xml'
    print(outfile)
    counter=0
    with open(outfile,'wb') as out:
        
        counter+=1
        print ("file: " +file)
        if counter % 100 == 0:
            print(counter,'/',len(files))
    #***********************  splits loading into bfdb alread are graphed; skip this entirely  
        tree = ET.parse(file)
#    root = tree.getroot()
 #   xslt = ET.parse('graphiphy.xsl')
  #  transform = ET.XSLT(xslt)
   # graphed = transform(root)

 #    graphed=runxslt(tree, graphxsl,"graphiphy")

    # bf2marc=ET.parse("/marklogic/applications/bibframe2marc/bibframe2marc.xsl")
    # bf2marcxsl=ET.XSLT(bf2marc)
 #   graphedxml=graphed.getroot()
#    print(ET.tostring(graphedxml))
    # create output marcxml:collection:
 
        M= ElementMaker(namespace="http://www.loc.gov/MARC21/slim" ,
                    nsmap={"marc":"http://www.loc.gov/MARC21/slim"})
        coll=M.collection()

    # for each "graph/record"
        root=tree.getroot()
        for c in root.iterfind('.//{http://id.loc.gov/ontologies/lclocal/}graph'):        
#	print(ET.tostring(c))

#        print (c.find("{http://id.loc.gov/ontologies/bibframe/}Work"))

            E = ElementMaker(namespace="http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
      	 	       nsmap={"lclocal":"http://id.loc.gov/ontologies/lclocal/",
    		   	      "rdfs":"http://www.w3.org/2000/01/rdf-schema#",
		              "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		              "madsrdf":"http://www.loc.gov/mads/rdf/v1#",
		              "bf":"http://id.loc.gov/ontologies/bibframe/",
		              "bflc":"http://id.loc.gov/ontologies/bflc/"})

            f=E.RDF(c.find("{http://id.loc.gov/ontologies/bibframe/}Work"), 
                c.find("{http://id.loc.gov/ontologies/bibframe/}Instance") 
                )
    

        

#        for nnn in w:
 #              print( nnn)
#		print (nnn.find("http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"))


#result has marc
#        print	ET.tostring(f)
 	    try:
	       result=bf2marcxsl(f)
#	          result = runxslt(f.getroot(), bf2marcxsl,"bf2marc")
            except OSError as err:
               print("OS error: {0}".format(err))
	    except ValueError:
               print("Could not convert data to an integer.")
	    except:
		#<class 'lxml.etree.XSLTApplyError'>
               print("Unexpected error:", sys.exc_info()[0], sys.exc_info()[1] )
               
               for info in sys.exc_info():
                   print(info)
     	       errorfile=open(efilename,'a')           
	       errorfile.write(ET.tostring(result))
               errorfile.close
  	       pass
			       # convert xslt result to xml
            record= ET.XML(bytes(result))
			#        print (ET.tostring(record))
        # insert the record into the collection
            coll.insert(1,record)   
        out.write(ET.tostring(coll))
######
  
out.close      
