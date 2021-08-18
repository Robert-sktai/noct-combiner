ps -ef | grep python3 | grep noct | grep -v grep | awk '{print $2}' | tr '\n' ' ' | awk '{print "kill -9 " $0}' | sh
rm -f my.log*
