ps | grep python3 | awk '{print "kill -9 " $1}' | sh
rm -f my.log
python3 remove_done_files.py
