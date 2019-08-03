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
from doctest import testmod

app = Flask(__name__)

PORT = "8000"
DOWNLOAD_DIRECTORY = 'downloads/'
TRACKER_DIRECTORY = 'trackers/'
LOGS_DIRECTORY = 'logs/'
date = datetime.date.today()
logging.basicConfig(filename=LOGS_DIRECTORY+str(date)+".log",
                    format='%(asctime)s %(message)s',
                    filemode='a+')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def updateDownloadStatus(name, status):
    with open(TRACKER_DIRECTORY + name + ".json", "w+") as fp:
        json.dump(status, fp)


def createEmptyFile(name, totalSize):
    logger.info(name + " - Created empty file of size " +
                str(totalSize) + " bytes")
    fp = open(DOWNLOAD_DIRECTORY + name, "w+")
    fp.write('\0' * totalSize)
    fp.close()


def getNumberOfThreads(requestObj):
    ''' 
    >>> getNumberOfThreads({"numberOfThreads": 20}) 
    20
    >>> getNumberOfThreads({}) 
    4
    '''
    if 'numberOfThreads' in requestObj:
        numberOfThreads = requestObj['numberOfThreads']
        return numberOfThreads
    else:
        return multiprocessing.cpu_count()


# downloads a specific part of file

def downloadWhole(url, name):
    numberOfThreads = 1
    downloadStatus = {
        'url': url,
        'name': name,
        'numberOfThreads': numberOfThreads
    }

    for i in range(numberOfThreads):
        downloadStatus['thread_'+str(i)] = 'pending'

    updateDownloadStatus(name, downloadStatus)

    try:
        r = requests.get(url)
        fp = open(DOWNLOAD_DIRECTORY + name, "w+b")
        fp.write(r.content)
        downloadStatus['thread_1'] = 'completed'
        updateDownloadStatus(name, downloadStatus)
        logger.info(name + " - Download complete")
    except Exception as e:
        downloadStatus['thread_1'] = 'failed'
        updateDownloadStatus(name, downloadStatus)
        logger.error(name + " - Exception - downloadWhole method - " + str(e))
        logger.error(name + " - Thread 1 failed")


def downloadPart(start, end, url, name, part, downloadStatus):
    try:
        logger.info(name + " - Download started for chunk " +
                    str(start) + " to "+str(end))
        headers = {'Range': 'bytes=%d-%d' % (start, end)}

        r = requests.get(url, headers=headers, stream=True)

        downloadLocation = DOWNLOAD_DIRECTORY + name

        with open(downloadLocation, "r+b") as fp:
            fp.seek(start)
            fp.write(r.content)
        downloadStatus['thread_'+str(part)] = 'completed'
        updateDownloadStatus(name, downloadStatus)
        logger.info(name + " - Download completed for chunk " +
                    str(start) + " to "+str(end))

    except Exception as e:
        downloadStatus['thread_'+str(part)] = 'failed'
        updateDownloadStatus(name, downloadStatus)
        logger.error(name + " - Exception - downloadPart method - " + str(e))
        logger.error(name + " - Thread " + str(part) + " failed")


# downdloader function that calculates the sizes of chunks to be downloaded and starts threads for downloading them


def downloadFile(url, numberOfThreads):
    ''' 
    >>> downloadFile("https://speed.hetzner.de/100MB.bin", 5)
    (True, 104857600, 20971520, 'd_1564818033_100MB.bin')
    '''
    try:
        logger.info("Processing download url")
        r = requests.head(url)
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
            partSize = int(totalSize) / numberOfThreads

            logger.debug("Url : " + url)
            logger.debug("Filename : " + remoteFileName)
            logger.debug("Total size : " + str(totalSize))
            logger.debug("Chunk size : " + str(partSize))
            logger.debug("Number of threads : " + str(numberOfThreads))

            downloadStatus = {
                'url': url,
                'contentLength': totalSize,
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
                logger.info(remoteFileName + " - Stated thread " + str(i))

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
            success, totalSize, partSize, name = downloadFile(
                url, numberOfThreads)
            if success:
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
                response = {
                    'success': False,
                    'message': 'File download failed',
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


# endpoint to check the status of download
@app.route('/logs/<date>/', methods=['GET', 'POST'])
def logs(date):
    try:
        with open(LOGS_DIRECTORY+date+".log", "rb") as fp:
            logsArray = []
            line = fp.readline()
            while(line):
                logsArray.append(line)
                line = fp.readline()

            response = {
                'success': True,
                'message': 'Logs fetched successfully',
                'data': logsArray,
                'error': None
            }
            return jsonify(response)
    except:
        response = {
            'success': False,
            'message': 'Failed to fetch logs',
            'data': None,
            'error': None
        }
        return jsonify(response)


@app.route('/retry/<fileName>/<part>', methods=['GET', 'POST'])
def retry(fileName, part):
    try:
        part = int(part)
        with open(TRACKER_DIRECTORY+fileName+".json", "rb") as fp:
            downloadStatus = json.load(fp)
            url = downloadStatus['url']
            contentLength = int(downloadStatus['contentLength'])
            numberOfThreads = int(downloadStatus['numberOfThreads'])

            partSize = int(contentLength) / numberOfThreads
            start = partSize*part
            end = min(start+partSize, contentLength)
            t = threading.Thread(target=downloadPart,
                                 kwargs={'start': start, 'end': end, 'url': url, 'name': fileName, 'part': part, 'downloadStatus': downloadStatus})
            t.setDaemon(True)
            t.start()
            response = {
                'success': True,
                'message': 'Download restarted',
                'data': None,
                'error': None
            }
            return jsonify(response)
    except Exception as e:
        response = {
            'success': False,
            'message': 'Failed to restart download',
            'data': None,
            'error': None
        }
        logger.error("Exception - retry endpoint " + str(e))
        return jsonify(response)


print("Server running at http://127.0.0.1:"+PORT)
app.run(host='0.0.0.0', port=PORT, debug=True)
