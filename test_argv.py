from optparse import OptionParser

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename", default='xx',
                  help="write report to FILE", metavar="FILE")
parser.add_option("-q", "--quiet",
                  action="store_true", dest="verbose", default=False,
                  help="don't print status messages to stdout")

parser.add_option("-p", "--bucket", dest="prefix", help="prefix")

(options, args) = parser.parse_args()

print options,args
