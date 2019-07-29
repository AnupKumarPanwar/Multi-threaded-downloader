from flask import Flask
from flask import request
from flask import jsonify
import json
import requests
import threading
import time
import logging
import datetime


app = Flask(__name__)

DOWNLOAD_DIRECTORY = 'downloads/'
date = datetime.date.today()
logging.basicConfig(filename="logs/"+str(date)+".log",
                    format='%(asctime)s %(message)s',
                    filemode='a+')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def createEmptyFile(name, totalSize):
    logger.info("Created empty file of size " + str(totalSize) + " bytes")
    fp = open(name, "w")
    fp.write('\0' * totalSize)
    fp.close()


# returns the number of threads, requested by the client, default 4


def getNumberOfThreads(requestObj):
    try:
        numberOfThreads = requestObj['numberOfThreads']
        return numberOfThreads
    except:
        return 4


# downloads a specific part of file


def downloadPart(start, end, url, name):
    try:
        logger.info("Download started for chunk " +
                    str(start) + " to "+str(end))
        headers = {'Range': 'bytes=%d-%d' % (start, end)}

        r = requests.get(url, headers=headers, stream=True)

        with open(name, "r+b") as fp:
            fp.seek(start)
            fp.write(r.content)
            print("start", start)
        logger.info("Download completed for chunk " +
                    str(start) + " to "+str(end))

    except Exception as e:
        logger.error("Exception - downloadPart method - " + str(e))


# downdloader function that calculates the sizes of chunks to be downloaded and starts threads for downloading them


def downloadFile(url, numberOfThreads):
    try:
        logger.info("Processing download url")
        r = requests.head(url)
        print(r.headers)
        name = DOWNLOAD_DIRECTORY + 'd_' + \
            str(int(time.time()))+'_'+url.split('/')[-1]
        totalSize = int(r.headers['content-length'])
        print(totalSize)
        partSize = int(totalSize) / numberOfThreads
        print(partSize)

        logger.debug("Url : " + url)
        logger.debug("Filename : " + name)
        logger.debug("Total size : " + str(totalSize))
        logger.debug("Chunk size : " + str(partSize))
        logger.debug("Number of threads : " + str(numberOfThreads))

        createEmptyFile(name, totalSize)

        for i in range(numberOfThreads):
            start = int(partSize * i)
            end = int(start + partSize)

            if i == numberOfThreads-1:
                end = totalSize

            t = threading.Thread(target=downloadPart,
                                 kwargs={'start': start, 'end': end, 'url': url, 'name': name})
            t.setDaemon(True)
            t.start()
            logger.info("Stated thread " + str(i+1))

        return True, totalSize, partSize
    except Exception as e:
        logger.error("Exception - downloadFile method " + str(e))
        return False, None, None


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
            success, totalSize, partSize = downloadFile(url, numberOfThreads)
            print(success, totalSize, partSize)
            return jsonify(requestObj)

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


app.run(host='0.0.0.0', port=8000)
