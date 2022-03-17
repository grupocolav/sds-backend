from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
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
            {"$project":{"name":1,"relations":1,"products_count":1,"citations_count":1,"products_by_year":1,"subjects":1}},
            {"$sort":{"products_count":-1}},
            {"$limit":limit}
        ]
        products_by_year=[]
        for reg in self.colav_db["branches"].aggregate(pipeline):
            entry={
                "id":reg["_id"],
                "name":reg["name"],
                "products_count":reg["products_count"],
                "citations_count":reg["citations_count"],
                "affiliations":{
                    "institution":{
                        "name":reg["relations"][0]["name"],
                        "id":reg["relations"][0]["id"]
                    }
                },
                "products_by_year":reg["products_by_year"] if "products_by_year" in reg.keys() else [],
                "subjects":reg["subjects"][:limit] if len(reg["subjects"])>=limit else reg["subjects"]
            }
            products_by_year.append(entry)


        pipeline=[
            {"$match":{"type":"group"}},
            {"$project":{"name":1,"relations":1,"citations_count":1,"products_count":1,"citations_by_year":1,"subjects":1}},
            {"$sort":{"citations_count":-1}},
            {"$limit":limit}
        ]
        citations_by_year=[]
        for reg in self.colav_db["branches"].aggregate(pipeline):
            entry={
                "id":reg["_id"],
                "name":reg["name"],
                "products_count":reg["products_count"],
                "citations_count":reg["citations_count"],
                "affiliations":{
                    "institution":{
                        "name":reg["relations"][0]["name"],
                        "id":reg["relations"][0]["id"]
                    }
                },
                "citations_by_year":reg["citations_by_year"] if "citations_by_year" in reg.keys() else [],
                "subjects":reg["subjects"][:limit] if len(reg["subjects"])>=5 else reg["subjects"]
            }
            citations_by_year.append(entry)

        return {"data":{"products_by_year":products_by_year,"citations_by_year":citations_by_year}}


    def get_authors(self):

        return {"data":""}


    def get_institutions(self,limit=None):
        if limit:
            limit=int(limit)
        else:
            limit=10

        pipeline=[
            {"$match":{"type":"group"}},
            {"$project":{"name":1,"relations":1,"products_count":1,"citations_count":1,"products_by_year":1,"subjects":1}},
            {"$sort":{"products_count":-1}},
            {"$limit":limit}
        ]
        products_by_year=[]
        products_subjects=[]
        for reg in self.colav_db["branches"].aggregate(pipeline):
            entry={
                "id":reg["_id"],
                "name":reg["name"],
                "products_count":reg["products_count"],
                "citations_count":reg["citations_count"],
                "affiliations":{
                    "institution":{
                        "name":reg["relations"][0]["name"],
                        "id":reg["relations"][0]["id"]
                    }
                },
                "subjects":reg["subjects"][:limit] if len(reg["subjects"])>=limit else reg["subjects"]
            }
            products_subjects.append(entry)
            for prod in reg["products_by_year"]:
                products_by_year.append({
                    "year":prod["year"],
                    "name":reg["name"],
                    "value":prod["value"]
                })


        pipeline=[
            {"$match":{"type":"group"}},
            {"$project":{"name":1,"relations":1,"citations_count":1,"products_count":1,"citations_by_year":1,"subjects":1}},
            {"$sort":{"citations_count":-1}},
            {"$limit":limit}
        ]
        citations_by_year=[]
        citations_subjects=[]
        for reg in self.colav_db["branches"].aggregate(pipeline):
            entry={
                "id":reg["_id"],
                "name":reg["name"],
                "products_count":reg["products_count"],
                "citations_count":reg["citations_count"],
                "affiliations":{
                    "institution":{
                        "name":reg["relations"][0]["name"],
                        "id":reg["relations"][0]["id"]
                    }
                },
                "subjects":reg["subjects"][:limit] if len(reg["subjects"])>=limit else reg["subjects"]
            }
            citations_subjects.append(entry)
            for cit in reg["citations_by_year"]:
                citations_by_year.append({
                    "year":cit["year"],
                    "name":reg["name"],
                    "value":cit["value"]
                })

        return {"data":{"products_by_year":products_by_year,"citations_by_year":citations_by_year,
                        "citations_subjects":citations_subjects,"products_subjects":products_subjects}}
            

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
            limit=self.request.args.get('limit')
            groups=self.get_institutions(limit=limit)
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
        else:
            response = self.app.response_class(
                response=self.json.dumps({}),
                status=400,
                mimetype='application/json'
            )

        response.headers.add("Access-Control-Allow-Origin", "*")
        return response