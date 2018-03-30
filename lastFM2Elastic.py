#!/usr/bin/python3.5
#-*- coding: utf-8 -*-
import json
import requests
import time
from pprint import pprint

def parseConfig():
    with open("lastFM2Elastic.conf","r") as f:
        params = json.load(f)
        f.close
    return params

def writeToDisk(data, user):
    tempDict = {}
    with open("data_{0:1}.json".format(user),"w") as f:
        for i in data:
            tempDict[i["timestamp"]] = {"artist":i["artist"], "title":i["title"], "datetime":time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(i["timestamp"])))} 
        json.dump(tempDict, f, indent=4)
            #f.write(data)
        f.close
    return True 

def getRecents(configs):
    bigTrackList = []
    rootUrl = "http://ws.audioscrobbler.com/2.0/"
    apiMethod = "user.getRecentTracks"
    api_key = configs["api_key"]
    user = configs["user"]
    page = 1 
    
    url = "{0:s}?method={1:s}&user={2:s}&api_key={3:s}&format=json&page={4:s}&limit=200".format(
            rootUrl, apiMethod, user, api_key, str(page))
    r = requests.get(url)

    if r.status_code == 200:
        data = json.loads(r.text)
        pages = int(data["recenttracks"]["@attr"]["totalPages"])
        print(pages)
    while page <= pages:
        print(page)
        r = requests.get(url)
        if r.status_code == 200:
            resp = json.loads(r.text)
            trackList = [{"timestamp":track["date"]["uts"], "artist":track["artist"]["#text"], "title":track["name"]} for track in resp["recenttracks"]["track"]]
            page+=1
            url = "{0:s}?method={1:s}&user={2:s}&api_key={3:s}&format=json&page={4:s}&limit=200".format(
                    rootUrl, apiMethod, user, api_key, str(page))
            for i in trackList:
                bigTrackList.append(i)
        else:
            print("ERROR, 200  not  received  ")
            print(page)
            break
    writeToDisk(bigTrackList, user)
    return bigTrackList

def pushToEs(data):
    count = 1
    for i in data:
        dateTime = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(int(i["timestamp"])))
        i["datetime"] = dateTime
        url = "http://192.168.0.10:9200/lastfm/data/{0:s}".format(str(i["timestamp"]))
        headers = {"Content-Type":"application/json"}
        r = requests.put(url, data=json.dumps(i), headers=headers)
        # print("{0:d} - {1:d}".format(count, r.status_code))
        count+=1

configs = parseConfig()
lastFMData = getRecents(configs)
pushToEs(lastFMData)



