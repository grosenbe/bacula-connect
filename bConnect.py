#!/usr/bin/env python3
import psycopg2
import getpass
import argparse

# Takes a job number (default most recent) and list all files backed up, along
# with their sizes.

# decode_stats taken from https://gist.github.com/Xiol/ee9d6e9d44494ea8df85
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
    fields = """st_dev st_ino st_mode st_nlink st_uid st_gid
    st_rdev st_size st_blksize st_blocks st_atime st_mtime
    st_ctime LinkFI st_flags data""".split()
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='bacula',
                        help='Which database to connect to')
    parser.add_argument('-o', '--host', default='thebox',
                        help='Which host to connect to')
    parser.add_argument('-p', '--port', default='5433',
                        help='Which port to connect to')
    parser.add_argument('-u', '--user', default='geoff',
                        help='User name')
    parser.add_argument('-j', '--job', default='0',
                        help='Which job to parse')
    parser.add_argument('-s', '--summarize', default=False,
                        help='Print summary information', action='store_true')
    parser.add_argument('-q', '--quiet', default=False,
                        help='Suppress file-by-file output',
                        action='store_true')

    args = parser.parse_args()
    PW = getpass.getpass()

    conn = psycopg2.connect(database=args.database,
                            user=args.user,
                            password=PW,
                            host=args.host,
                            port=args.port)
    cursor = conn.cursor()

    job = args.job
    if job == '0':
        try:
            cursor.execute('SELECT clientid,jobid ' +
                           'FROM job ' +
                           'ORDER BY endtime DESC ' +
                           'LIMIT 1')
            rows = cursor.fetchall()
            job = str(rows[0][1])
            clientID = str(rows[0][0])
        except psycopg2.OperationalError as e:
            print("Query failed with error " + e.pgerror)
    else:
        cursor.execute('SELECT clientid '
                       + 'FROM job '
                       + 'WHERE jobid = '
                       + job)
        rows = cursor.fetchall()
        clientID = str(rows[0][0])

    try:
        cursor.execute('SELECT path.path,filename.name,file.lstat '
                       + 'FROM filename '
                       + 'JOIN file ON file.filenameid = filename.filenameid '
                       + 'JOIN path ON path.pathid = file.pathid '
                       + 'WHERE file.jobid = '
                       + job
                       + ' AND filename.name != \'\''
                       )
    except psycopg2.OperationalError as e:
        print("Query failed with error " + e.pgerror)

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

        if not args.quiet:
            print("{0}{1}: {2}".format(record[0], record[1], out["st_size"]))

    if args.summarize:
        cursor.execute('SELECT name FROM client WHERE clientid = '
                       + clientID)
        name = cursor.fetchall()
        print("{0} records retrieved for client {1}".format(len(rows),
                                                            str(name[0][0])))
        print("Total uncompressed size backed up: {0:.3f} MB"
              .format(totalSize/1024/1024))
        print("Largest file: {0}{1}: {2:.3f} MB"
              .format(maxPath, maxFileName, maxSize/1024/1024))


if __name__ == "__main__":
    Connect()
