ps -ef | grep python3 | grep coordinator.py | awk '{print "kill -9 " $2}' | sh
rm -f my.log*
