from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
from pickle import load
from datetime import date
from math import log
from flask import redirect




class CompendiumApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)


    def get_topics(self):

        return {"data":""}


    def get_groups(self,limit=None):
        if limit:
            limit=int(limit)
        else:
            limit=10

        pipeline=[
            {"$match":{"type":"group"}},
            {"$project":{"name":1,"relations":1,"products_count":1,"products_by_year":1}},
            {"$sort":{"products_count":-1}},
            {"$limit":limit}
        ]
        products_by_year=list(self.colav_db["branches"].aggregate(pipeline))


        pipeline=[
            {"$match":{"type":"group"}},
            {"$project":{"name":1,"relations":1,"citations_count":1,"citations_by_year":1}},
            {"$sort":{"citations_count":-1}},
            {"$limit":limit}
        ]
        citations_by_year=list(self.colav_db["branches"].aggregate(pipeline))

        return {"data":{"products_by_year":products_by_year,"citations_by_year":citations_by_year}}


    def get_authors(self):

        return {"data":""}


    def get_grinstitutions(self):

        return {"data":""}
            

    @endpoint('/app/compendium', methods=['GET'])
    def app_compendium(self):
        """
        """
        
        data = self.request.args.get('data')


        if data=="groups":
            limit=self.request.args.get('limit')
            groups=self.get_groups(limit=limit)
            if groups:    
                response = self.app.response_class(
                response=self.json.dumps(groups),
                status=200,
                mimetype='application/json'
                )
            else:
                response = self.app.response_class(
                response=self.json.dumps({"status":"Request returned empty"}),
                status=204,
                mimetype='application/json'
            )
        elif data=="institutions":
            pass
        else:
            response = self.app.response_class(
                response=self.json.dumps({}),
                status=400,
                mimetype='application/json'
            )

        response.headers.add("Access-Control-Allow-Origin", "*")
        return response