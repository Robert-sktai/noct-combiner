ps | grep python3 | awk '{print "kill -9 " $1}' | sh
