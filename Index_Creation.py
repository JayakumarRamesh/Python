import hashlib
import os

# Folder Path
path = "C:\par"

# Change the directory
os.chdir(path)

# Read text File

md5_hash = hashlib.md5()

temp_lst = []


def read_text_file(file_path):
    a_file = open(file_path, "rb")
    content = a_file.read()
    md5_hash.update(content)

    digest = md5_hash.hexdigest()
    print(digest)
    return digest


# iterate through all file
for file in os.listdir():
    file_path = f"{path}\{file}"

    # call read text file function
    md5_val = read_text_file(file_path)
    out = 'fdwawsprod/actmgmt/parquet/' + file + '|' + md5_val
    temp_lst.append(out)

with open("indexfile_20220211.txt", mode="w") as file:
    file.write(",".join(temp_lst))
