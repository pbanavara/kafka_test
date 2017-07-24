"""
Testing client to bombard the client facing URL with 100000 + requests
"""
from urllib.parse import urlparse 
from threading import Thread
import requests, sys
import queue
import argparse
import json

concurrent = 200

def doWork():
    while True:
        url = q.get()
        status, url = getStatus(url)
        doSomethingWithResult(status, url)
        q.task_done()

def getStatus(ourl):
    try:
        json_obj = '{ "Data": { "site_id": 20, "device_token": "foQy614ogE0:APA91bFsu99k-zuX2T6tRLFxBYzx0eI7mXbvGFu6I1DVLBCofZzxUnx0nCf56eZLv77jHyNwVpU8vgzHHzahXTdleMfpVwrTHvLvPpM7aLI-Z9xse7er0497_iRvlslLTziWvChdwXVo", "campaign_name": "test-campaign-5", "event_name": "add-to-cart", "title": { "variable1": "test-campaign-5", "variable2": "Good Title again" }, "message": { "variable1": "test-campaign-5", "variable2": "Good message again" }, "notification_url": { "variable1": "harishhub" }, "notification_image": "" }, "PartitionKey":"myabcderg"}'
        r = requests.put(ourl, json=json.loads(json_obj))
        res = r.status_code 
        text = r.text
        return res, text
    except:
        return "error", ourl

def doSomethingWithResult(status, url):
    print(status, url)
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()  
    parser.add_argument("-no_of_requests" , "-Number of simultaneous requests to send")
    parser.add_argument("-url", "-Target URL")
    args = parser.parse_args()
    q = queue.Queue(concurrent * 2)
    for i in range(concurrent):
        t = Thread(target=doWork)
        t.daemon = True
        t.start()
    urls = []
    for k in range(int(args.no_of_requests)):
        urls.append(args.url)
    try:
        for url in urls:
            q.put(url.strip())
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)
