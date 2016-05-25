#!/router/bin/python

#import stuff 
import oci
import getopt
import sys

#parce args

argv = sys.argv[1:]

try:
   opts, args = getopt.getopt(argv, 's:h',['source=','help'])
except getopt.error:
   usage()
   sys.exit(2)

for opt, arg in opts:
   if opt == 'h':
      print "get_snapmirrors.py -s <source vol or filer>"
      sys.exit()
   elif opt in ("-s","--source"):
      source = arg

#get snapmirror relationships
dwh_con = oci.oci_dwh_connect()
snapmirrors = dwh_con.get_snapmirror_relationships_via_source(source)

#format output nicely 

prev_source_vol = '' 

for snapmirror in snapmirrors:
   source_vol, source_total, source_used, dest_vol, dest_total, dest_used  = snapmirror
   if (source_vol != prev_source_vol):
      print "============================================\n\nSnapmirror Source: {0}".format(source_vol)
      print "  Used: {0}MB, Total: {1}MB".format(source_used,source_total)
      print "Destinations:"
      print "   {0}".format(dest_vol) 
      print "     Used: {0}MB, Total: {1}MB".format(dest_used,dest_total) 
   else: 
      print "   {0}".format(dest_vol)
      print "     Used: {0}MB, Total: {1}MB".format(dest_used,dest_total)
   prev_source_vol = source_vol

   
