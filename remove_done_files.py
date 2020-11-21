import os

from config import Config

config = Config()

def get_subdirs():
    return list(sorted(filter(lambda x: not x.endswith("_tmp"), [f.path for f in os.scandir(config.incoming_data_path) if f.is_dir()])))

if __name__ == "__main__":
    for subdir in get_subdirs():
        done_files = list(filter(lambda x: x.endswith(".dat.done"), [f.path for f in os.scandir(subdir) if f.is_file()]))
        for done_file in done_files:
            os.rename(done_file, done_file[:-len(".done")])
