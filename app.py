from flask import Flask
from flask import request
from flask import jsonify
import json
import requests
import threading
import time
import logging
import datetime
import multiprocessing


app = Flask(__name__)

PORT = "8000"
DOWNLOAD_DIRECTORY = 'downloads/'
TRACKER_DIRECTORY = 'trackers/'
date = datetime.date.today()
logging.basicConfig(filename="logs/"+str(date)+".log",
                    format='%(asctime)s %(message)s',
                    filemode='a+')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def updateDownloadStatus(name, status):
    with open(TRACKER_DIRECTORY + name + ".json", "w+") as fp:
        json.dump(status, fp)


def createEmptyFile(name, totalSize):
    logger.info("Created empty file of size " + str(totalSize) + " bytes")
    fp = open(DOWNLOAD_DIRECTORY + name, "w+")
    fp.write('\0' * totalSize)
    fp.close()


# returns the number of threads, requested by the client, default 4


def getNumberOfThreads(requestObj):
    try:
        numberOfThreads = requestObj['numberOfThreads']
        return numberOfThreads
    except:
        return multiprocessing.cpu_count()


# downloads a specific part of file

def downloadWhole(url, name):
    r = requests.get(url)
    fp = open(DOWNLOAD_DIRECTORY + name, "w+b")
    fp.write(r.content)


def downloadPart(start, end, url, name, part, downloadStatus):
    try:
        logger.info("Download started for chunk " +
                    str(start) + " to "+str(end))
        headers = {'Range': 'bytes=%d-%d' % (start, end)}

        r = requests.get(url, headers=headers, stream=True)

        downloadLocation = DOWNLOAD_DIRECTORY + name

        with open(downloadLocation, "r+b") as fp:
            fp.seek(start)
            fp.write(r.content)
        downloadStatus['thread_'+str(part)] = 'completed'
        updateDownloadStatus(name, downloadStatus)
        logger.info("Download completed for chunk " +
                    str(start) + " to "+str(end))

    except Exception as e:
        downloadStatus['thread_'+str(part)] = 'failed'
        updateDownloadStatus(name, downloadStatus)
        logger.error("Exception - downloadPart method - " + str(e))
        logger.error("Thread " + str(part+1) + " failed")


# downdloader function that calculates the sizes of chunks to be downloaded and starts threads for downloading them


def downloadFile(url, numberOfThreads):
    try:
        logger.info("Processing download url")
        r = requests.head(url)
        print(r.headers)
        remoteFileName = 'd_' + \
            str(int(time.time()))+'_'+url.split('/')[-1]
        if not 'content-length' in r.headers:
            t = threading.Thread(target=downloadWhole,
                                 kwargs={'url': url, 'name': remoteFileName})
            t.setDaemon(True)
            t.start()
            return True, None, None, remoteFileName
        else:
            totalSize = int(r.headers['content-length'])
            print(totalSize)
            partSize = int(totalSize) / numberOfThreads
            print(partSize)

            logger.debug("Url : " + url)
            logger.debug("Filename : " + remoteFileName)
            logger.debug("Total size : " + str(totalSize))
            logger.debug("Chunk size : " + str(partSize))
            logger.debug("Number of threads : " + str(numberOfThreads))

            downloadStatus = {
                'name': remoteFileName,
                'numberOfThreads': numberOfThreads
            }

            for i in range(numberOfThreads):
                downloadStatus['thread_'+str(i)] = 'pending'

            updateDownloadStatus(remoteFileName, downloadStatus)

            createEmptyFile(remoteFileName, totalSize)

            for i in range(numberOfThreads):
                start = int(partSize * i)
                end = int(start + partSize)

                if i == numberOfThreads-1:
                    end = totalSize

                t = threading.Thread(target=downloadPart,
                                     kwargs={'start': start, 'end': end, 'url': url, 'name': remoteFileName, 'part': i, 'downloadStatus': downloadStatus})
                t.setDaemon(True)
                t.start()
                downloadStatus['thread_'+str(i)] = 'started'
                logger.info("Stated thread " + str(i+1))

            return True, totalSize, partSize, remoteFileName
    except Exception as e:
        logger.error("Exception - downloadFile method " + str(e))
        return False, None, None, None


# index endpoint
@app.route('/', methods=['POST', 'GET'])
def home():
    response = {
        'success': True,
        'message': 'SETU code challenge',
        'data': None,
        'error': None
    }
    return jsonify(response)


# endpoint to download a file
@app.route('/download', methods=['POST'])
def download():
    try:
        logger.info("Download request received")
        requestObj = request.get_json()
        logger.debug("Request data : " + str(requestObj))

        if 'url' in requestObj:
            url = requestObj['url']
            numberOfThreads = getNumberOfThreads(requestObj)
            logger.info("Number of threads : " + str(numberOfThreads))
            success, totalSize, partSize, name = downloadFile(
                url, numberOfThreads)
            print(success, totalSize, partSize)
            response = {
                'success': True,
                'message': 'File download started',
                'data': {
                    'fileName': name
                },
                'error': None
            }
            return jsonify(response)

        else:
            logger.error("'url' parameter missing")

            response = {
                'success': False,
                'message': '"url" parameter missing',
                'data': None,
                'error': None
            }
            return jsonify(response), 400

    except Exception as e:
        logger.error("Exception - /download endpoint " + str(e))

        response = {
            'success': False,
            'message': 'Internal server error',
            'data': None,
            'error': None
        }
        return jsonify(response), 500

# endpoint to check the status of download
@app.route('/status/<fileName>', methods=['GET', 'POST'])
def status(fileName):
    try:
        with open(TRACKER_DIRECTORY+fileName+".json", "r") as fp:
            data = json.load(fp)
            response = {
                'success': True,
                'message': 'Status fetched successfully',
                'data': data,
                'error': None
            }
            return jsonify(response)
    except:
        response = {
            'success': False,
            'message': 'Failed to fetch download status',
            'data': None,
            'error': None
        }
        return jsonify(response)


print("Server running at http://127.0.0.1:"+PORT)
app.run(host='0.0.0.0', port=PORT, debug=True)
