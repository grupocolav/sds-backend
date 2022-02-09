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

    def get_subjects_tree(self):
        medicine=self.colav_db["subjects"].find_one({"name":"Medicine"},{"works_count":1})
        psychology=self.colav_db["subjects"].find_one({"name":"Psychology"},{"works_count":1})
        data={
            "id":"A0",
            "value":{
                "items":[{
                    "text":"Temas de Salud"
                }]
            },
            "children":[
                {
                    "id":str(medicine["_id"]),
                    "value":{
                        "items":[
                            {
                                "text":"Medicine"
                            },
                            {
                                "text":"Productos",
                                "value":medicine["works_count"]
                            }
                        ]
                    },
                    "children":[]
                },
                {
                    "id":str(psychology["_id"]),
                    "value":{
                        "items":[
                            {
                                "text":"Psychology"
                            },
                            {
                                "text":"Productos",
                                "value":psychology["works_count"]
                            }
                        ]
                    },
                    "children":[]
                }
            ]
        }
        children=[]
        ids=[]
        for subject in self.colav_db["subjects"].find({"ancestors.display_name":"Medicine","level":1}):
            if str(subject["_id"]) in ids:
                continue
            ids.append(str(subject["_id"]))
            entry={
                "id":str(subject["_id"]),
                "value":{
                    "items":[
                        {
                            "text":subject["name"]
                        },
                        {
                            "text":"Productos",
                            "value":subject["works_count"]
                        }
                    ]
                }
            }
            children.append(entry)
        data["children"][0]["children"]=children

        children=[]
        ids=[]
        for subject in self.colav_db["subjects"].find({"ancestors.display_name":"Psychology","level":1}):
            if str(subject["_id"]) in ids:
                continue
            ids.append(str(subject["_id"]))
            entry={
                "id":str(subject["_id"]),
                "value":{
                    "items":[
                        {
                            "text":subject["name"]
                        },
                        {
                            "text":"Productos",
                            "value":subject["works_count"]
                        }
                    ]
                }
            }
            children.append(entry)
        data["children"][1]["children"]=children

        return data
        

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

        return {"data":self.geojson,"subject_tree":self.get_subjects_tree()}

            

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