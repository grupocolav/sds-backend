from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
from pickle import load
from datetime import date
from math import log
from flask import redirect
import json




class HomeApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)
        self.geojson=json.load(open("sds/etc/bogota.json","r"))

    def get_info(self):
        for idx,loc in enumerate(self.geojson["features"]):
            name=loc["properties"]["loc"]
            count=self.colav_db["documents"].count_documents({"$text":{"$search":name}})
            if count!=0:
                result=self.colav_db["documents"].aggregate([
                    {"$match":{"$text":{"$search":name}}},
                    { "$sort": { "score": { "$meta": "textScore" }, "posts": -1 } },
                    {"$limit":10},
                    {"$sample":{"size":1}},
                    {"$project":{"titles":1}}
                ])
                result=list(result)[0]
                self.geojson["features"][idx]["properties"]["article"]={
                    "title":result["titles"][0]["title"],
                    "id":result["_id"]
                }
            else:
                self.geojson["features"][idx]["properties"]["article"]={
                    "title":"",
                    "id":""
                }

        return {"data":self.geojson}
            

    @endpoint('/app/home', methods=['GET'])
    def app_home(self):
        """
        """
        
        info = self.get_info()
        if info:    
            response = self.app.response_class(
            response=self.json.dumps(info),
            status=200,
            mimetype='application/json'
            )
        else:
            response = self.app.response_class(
                response=self.json.dumps({}),
                status=400,
                mimetype='application/json'
            )

        response.headers.add("Access-Control-Allow-Origin", "*")
        return response