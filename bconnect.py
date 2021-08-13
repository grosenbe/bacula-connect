#!/usr/bin/env python3
import psycopg2
import getpass
import base64

# Takes a job number (default to most recent), and list all files backed up, along with their sizes

B64_VALS = {
             '+': 62, '/': 63, '1': 53, '0': 52, '3': 55, '2': 54, '5': 57,
             '4': 56, '7': 59, '6': 58, '9': 61, '8': 60, 'A': 0, 'C': 2,
             'B': 1, 'E': 4, 'D': 3, 'G': 6, 'F': 5, 'I': 8, 'H': 7, 'K': 10,
             'J': 9, 'M': 12, 'L': 11, 'O': 14, 'N': 13, 'Q': 16, 'P': 15,
             'S': 18, 'R': 17, 'U': 20, 'T': 19, 'W': 22, 'V': 21, 'Y': 24,
             'X': 23, 'Z': 25, 'a': 26, 'c': 28, 'b': 27, 'e': 30, 'd': 29,
             'g': 32, 'f': 31, 'i': 34, 'h': 33, 'k': 36, 'j': 35, 'm': 38,
             'l': 37, 'o': 40, 'n': 39, 'q': 42, 'p': 41, 's': 44, 'r': 43,
             'u': 46, 't': 45, 'w': 48, 'v': 47, 'y': 50, 'x': 49, 'z': 51
           }

def decode_lstats(stats):
    fields = "st_dev st_ino st_mode st_nlink st_uid st_gid st_rdev st_size st_blksize st_blocks st_atime st_mtime st_ctime LinkFI st_flags data".split()
    out = {}

    for i, element in enumerate(stats.split()):
        result = 0
        for n, letter in enumerate(element):
            if letter:
                result = result << 6
                result += B64_VALS[letter]
        out[fields[i]] = result

    return out

def Connect():    
    PW = getpass.getpass()
    jobnum = '241'

    conn = psycopg2.connect(database="bacula",
                                   user='geoff',
                                   password=PW,
                                   host='thebox',
                                   port='5433')
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT path.path,filename.name,file.lstat '
                       + 'FROM filename '
                       + 'JOIN file ON file.filenameid = filename.filenameid '
                       + 'JOIN path ON path.pathid = file.pathid '
                       + 'WHERE file.jobid = '
                       + jobnum
                       + ' AND filename.name != \'\''
                       )
    except:
        print("Query failed!")

    rows = cursor.fetchall()

    totalSize = 0
    maxSize = 0
    maxFileName = ""
    for record in rows:
        out = decode_lstats(record[2])
        totalSize = totalSize + out["st_size"]
        if out["st_size"] > maxSize:
            maxSize = out["st_size"]
            maxPath = record[0]
            maxFileName = record[1]
        print("{0}{1}: {2}".format(record[0], record[1], out["st_size"]))

    print("{0} records retrieved".format(len(rows)))
    print("Total MB backed up: {0:.3f}".format(totalSize/1024/1024))
    print("largest file: {0}{1}: {2:.3f} MB".format(maxPath, maxFileName, maxSize/1024/1024))

if __name__ == "__main__":
    Connect()
