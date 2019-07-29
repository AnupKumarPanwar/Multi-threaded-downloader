from flask import Flask
from flask import request
from flask import jsonify
import json
import requests
import threading
import time

app = Flask(__name__)

DOWNLOAD_DIRECTORY = 'downloads/'


# def emptyFileUtility(name, start, content, totalSize, numberOfThreads):

#     if(start >= totalSize):
#         return

#     with open(name, "r+b") as fp:
#         try:
#             fp.seek(start)
#             fp.write(content)
#             emptyFileUtility(name, start+numberOfThreads, content, totalSize, numberOfThreads)
#         except:
#             time.sleep(1)
#             emptyFileUtility(name, start, content, totalSize, numberOfThreads)


# def createEmptyFile(name, totalSize, numberOfThreads):
    # for i in range(numberOfThreads):
    #     start = i

    #     t = threading.Thread(target=emptyFileUtility,
    #                          kwargs={'name': name, 'start': start, 'content': '\0', 'totalSize': totalSize, 'numberOfThreads': numberOfThreads})
    #     t.setDaemon(True)
    #     t.start()



def getNumberOfThreads(requestObj):
    try:
        numberOfThreads = requestObj['numberOfThreads']
        return numberOfThreads
    except:
        return 4


def downloadPart(start, end, url, name):

    headers = {'Range': 'bytes=%d-%d' % (start, end)}

    r = requests.get(url, headers=headers, stream=True)

    with open(name, "r+b") as fp:
        fp.seek(start)
        fp.write(r.content)
        print("start", start)


def downloadFile(url, numberOfThreads):
    try:
        r = requests.head(url)
        print(r.headers)
        name = DOWNLOAD_DIRECTORY + 'd_' + \
            str(int(time.time()))+'_'+url.split('/')[-1]
        totalSize = int(r.headers['content-length'])
        print(totalSize)
        partSize = int(totalSize) / numberOfThreads
        print(partSize)

        # createEmptyFile(name, totalSize, numberOfThreads)
        createEmptyFile(name, totalSize)

        for i in range(numberOfThreads):
            start = int(partSize * i)
            end = int(start + partSize)

            t = threading.Thread(target=downloadPart,
                                 kwargs={'start': start, 'end': end, 'url': url, 'name': name})
            t.setDaemon(True)
            t.start()

        return True, totalSize, partSize
    except Exception as e:
        print(str(e))
        return False, None, None


@app.route('/', methods=['POST', 'GET'])
def home():
    response = {
        'success': True,
        'message': 'SETU code challenge',
        'data': None,
        'error': None
    }
    return jsonify(response)


@app.route('/download', methods=['POST'])
def download():
    try:
        requestObj = request.get_json()

        if 'url' in requestObj:
            url = requestObj['url']
            numberOfThreads = getNumberOfThreads(requestObj)
            success, totalSize, partSize = downloadFile(url, numberOfThreads)
            print(success, totalSize, partSize)
            return jsonify(requestObj)

        else:
            response = {
                'success': False,
                'message': '"url" parameter missing',
                'data': None,
                'error': None
            }
            return jsonify(response), 400

    except:
        response = {
            'success': False,
            'message': 'Internal server error',
            'data': None,
            'error': None
        }
        return jsonify(response), 500


app.run(host='0.0.0.0', port=8000)
