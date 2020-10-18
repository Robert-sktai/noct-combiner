import os

path = "/home/svcapp_su/robert/workspace/noct-combiner/swing"

subdirs = sorted([f.path for f in os.scandir(path) if f.is_dir()])

for subdir in subdirs:
    lock_files = list(filter(lambda x: x.endswith(".dat.lock"), [f.path for f in os.scandir(subdir) if f.is_file()]))
    for lock_file in lock_files:
        if os.path.isfile(lock_file):
            print (f"Lock file removed: {lock_file}")
            os.remove(lock_file)
#    done_files = list(filter(lambda x: x.endswith(".dat.done"), [f.path for f in os.scandir(subdir) if f.is_file()]))
#    done_files = list(map(lambda x: x[:-len(".done")], done_files))
#    if len(done_files) > 0:
#        print (done_files)
    dat_files = list(filter(lambda x: x.endswith(".dat"), [f.path for f in os.scandir(subdir) if f.is_file()]))
    for dat_file in dat_files:
        print (dat_file)



# restart: delete .lock  suffix
