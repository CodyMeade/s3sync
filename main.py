import os
import sys
import boto3
from boto3.session import Session
from boto3.s3.transfer import TransferConfig
import threading
import time
from colorama import Fore, Style
import logging
from datetime import datetime
session = Session(aws_access_key_id='', aws_secret_access_key='')
config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 1,
                        use_threads=True)
s2 = boto3.client('s3',aws_access_key_id='', aws_secret_access_key='')
# directory = 'C:\\test\\'
directory = 'E:\\mcds_production\\News Channel\\FromPromoVault\\'
bucket2sync = 'nbc-nc-g2'
s3 = session.resource('s3')
client = session.client('s3')
bucket = s3.Bucket(bucket2sync)

#class to monitor the progress of each upload


class ProgressPercentage(object):
    def __init__(self, filename):
        global bit
        self.start = time.time()
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        bit = 'GB'
        self.divide_by = 1000000000
        if float(os.path.getsize(filename)/1000) > 1:
            self._size = float(os.path.getsize(filename) / 1000)
            bit = 'KB'
            self.divide_by = 1000


        if float(os.path.getsize(filename))/1000000 > 1:
            self._size = float(os.path.getsize(filename))/1000000
            bit = 'MB'
            self.divide_by = 1000000


        if float(os.path.getsize(filename))/1000000000 > 1:
            self._size = float(os.path.getsize(filename))/1000000000
            bit = 'GB'
            self.divide_by = 1000000000


        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        global status
        with self._lock:
            self.elapsed = time.time() - self.start
            self._seen_so_far += (bytes_amount/self.divide_by)
            percentage = round(((self._seen_so_far / self._size) * 100),2)
            status = (f'[{round((self._seen_so_far),2)}{bit}] of [{round((self._size),2)}{bit}] '
                            f'=> Percent complete: [{percentage}%] => Time elapsed: [{round((self.elapsed),2)}s]')
            sys.stdout.write(f'\r ==> Status: UPLOADING' + status)


def get_objects():
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="nbc-nc-g2"):
        contents = (page["Contents"])
        for obj in contents:
            keys.append(obj["Key"])
        return keys
def getdirectoryobjects(directory):
    keys = []
    directory = os.listdir(directory)
    for objects in directory:
        os.path.basename(objects)
        keys.append(objects)
    return keys


def upload2s3(missingobj):
    for obj in missingobj:
        print(f'\r ==> Attempting upload of {obj} to {bucket2sync}')
        file_path = f'{directory}{obj}'
        # file_name, file_extension = os.path.splitext(f'{directory}{obj}')
        # file_extension = file_extension.replace('.', '').lower()
        key_path = f'{obj}'
        client.upload_file(file_path, bucket2sync, key_path, ExtraArgs={'ACL': 'bucket-owner-full-control'},
                           Config=config, Callback=ProgressPercentage(file_path))
        objs = list(bucket.objects.filter(Prefix=key_path))
        if len(objs) > 0 and objs[0].key == key_path:
            global upload
            upload = 'SUCCESSFUL'
            logging.info(
                            f'For {obj} upload to {bucket2sync} '
                            f'SUCCESS, Size: {float(os.path.getsize(file_path))} '
                            f'File Parms: {status}'
                            f'\n**************************************************************************'
                        )
            print(f'\r ==>',"Status: ", Fore.GREEN + f'{upload}',Style.RESET_ALL, status)
            print(f' ==> Upload of {obj} to {bucket2sync} complete')


        else:
            upload = 'FAILED'
            logging.info(
                f'For {obj} upload to {bucket2sync} '
                f'FAILED, Size: {float(os.path.getsize(file_path))} '
                f'File Parms: {status}'
                f'\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            )
            sys.stdout.write(f"\r ==> Status: {upload} {status}")
            sys.stdout.write(f' ==> Retrying upload of {obj} to {bucket2sync}')
def obj_discovery():
    missingobj = []
    for obj in dirobj:
        if obj not in bucketobj:
            if "#" in obj:
                file_path = f"{directory}//{obj}"
                logging.info(
                    f'For {obj} upload to {bucket2sync} '
                    f'NOT ATTEMPTED, Size: {float(os.path.getsize(file_path))} '
                    f'Not finished transfer to NETAPP. Incomplete media file.'
                    f'\n###########################################################################'
                )
                sys.stdout.write(f" ==> {obj} not added to queue. (incomplete media file)")

            else:
                missingobj.append(obj)
    return missingobj

def Flow_app():
    if missing == []:
        sys.stdout.write('\r ==> All the files have been uploaded.')
        time.sleep(2)
    else:
        sys.stdout.write(f"\r ==> Items out of sync in {bucket2sync}:")
        for obj in range(len(missing)):

            sys.stdout.write(
                f'\n ==> {missing[obj]}')
            time.sleep(1)
        sys.stdout.write(" ==> Queueing upload to S3 bucket.")
        time.sleep(2)
        upload2s3(missing)

if __name__ == "__main__":
    while True:
        d = datetime.now().strftime("%Y-%m")
        logfile = (f'{directory}{d}-logfile.log')
        logging.basicConfig(filename=logfile, level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(message)s:%(filename)s')
        bucketobj = get_objects()
        dirobj = getdirectoryobjects(directory)
        missing = obj_discovery()
        Flow_app()
        timeleft = 120
        for n in range(0,120):
            sys.stdout.write('\r ==> Checking system again in: {%s}s' % timeleft)
            timeleft -= 1
            time.sleep(1)
        sys.stdout.write("\r ==> Checking file system")
        time.sleep(2)


