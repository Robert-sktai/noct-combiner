import os
import shutil
import time 
import sqlite3
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
f_handler = logging.FileHandler('check_noct.log')
f_handler.setLevel(logging.DEBUG)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)

original_path = "/swing/noct"
target_path = "/swing/noct_test"

def get_subdir_names(path):
    return set(sorted(filter(lambda x: not x.endswith("_tmp"), [f.path[len(path)+1:] for f in os.scandir(path) if f.is_dir()])))

def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)

def sync():
    original_subdirs = get_subdir_names(original_path)
    target_subdirs = get_subdir_names(target_path)
    new_subdirs = original_subdirs.difference(target_subdirs)
    for subdir in new_subdirs:
        src = os.path.join(original_path, subdir)
        dest = os.path.join(target_path, subdir)
        logger.info(f"*copy {src} to {dest}")
        try:
            os.mkdir(dest)
        except Exception as e:
            logger.error (e)
            logger.error (f"* Removed {dest} ")
            shutil.rmtree(dest)

while True:
    sync()
    time.sleep(0.5)
