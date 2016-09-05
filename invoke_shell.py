import commands

status, output = commands.getstatusoutput('ping -c 2 www.baidu.com')
print status
print output
