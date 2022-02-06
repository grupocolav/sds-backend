from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
from pickle import load
from math import log
from datetime import date


class SubjectsApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self,idx=None):
        initial_year=0
        final_year=0

        if idx:
            result=self.colav_db['documents'].find({"subjects.id":ObjectId(idx)},
                {"year_published":1}).sort([("year_published",ASCENDING)]).limit(1)
            if result:
                result=list(result)
                if len(result)>0:
                    initial_year=result[0]["year_published"]
            result=self.colav_db['documents'].find({"subjects.id":ObjectId(idx)},
                {"year_published":1}).sort([("year_published",DESCENDING)]).limit(1)
            if result:
                result=list(result)
                if len(result)>0:
                    final_year=result[0]["year_published"]

        filters={
            "start_year":initial_year,
            "end_year":final_year
        }

        if idx:
            result=self.colav_db["subjects"].find_one({"_id":ObjectId(idx)})
        else:
            return None
        tree={
            "title":result["name"],
            "level":result["level"],
            "key":"0",
            "children":[]
        }
        count=0
        if result["related_concepts"]:
            for sub in result["related_concepts"]:
                if sub["level"]-1!=result["level"]:
                    continue
                entry={
                    "title":sub["display_name"],
                    "id":sub["id"] if "id" in sub.keys() else "",
                    "level":sub["level"],
                    "key":"0-"+str(count)
                }
                tree["children"].append(entry)
                count+=1
        parent={}
        if result["ancestors"]:
            for ancestor in result["ancestors"]:
                if ancestor["level"]!=result["level"]-1:
                    continue
                else:
                    parent={
                        "title":ancestor["display_name"],
                        "id":ancestor["id"] if "id" in ancestor.keys() else "",
                        "level":ancestor["level"]
                    }
                    break

        return {"data":{"tree":tree,"parent":parent},"filters":filters}

    def get_production(self,idx=None,start_year=None,end_year=None,sort=None,direction=None):
        return None
    
    def get_production_by_type(self,idx=None,max_results=100,page=1,start_year=None,end_year=None,sort="descending",direction=None,tipo=None):
        return None

    def get_institutions(self,idx=None,page=1,max_results=100,sort="citations",direction="descending"):
        return None
    
    def get_groups(self,idx=None,page=1,max_results=100,sort="citations",direction="descending"):
        return None

    def get_authors(self,idx=None,page=1,max_results=100):
        return None



    @endpoint('/app/subjects', methods=['GET'])
    def app_subjects(self):
       
        data = self.request.args.get('data')

        if data=="info":
            idx = self.request.args.get('id')
            info = self.get_info(idx)
            if info:    
                response = self.app.response_class(
                response=self.json.dumps(info),
                status=200,
                mimetype='application/json'
                )
            else:
                response = self.app.response_class(
                response=self.json.dumps({"status":"Request returned empty"}),
                status=204,
                mimetype='application/json' 
            )
        elif data=="production":
            idx = self.request.args.get('id')
            max_results=self.request.args.get('max')
            page=self.request.args.get('page')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            sort=self.request.args.get('sort')
            tipo = self.request.args.get('type')

            if tipo == None: 
                production=self.get_production(idx,max_results,page,start_year,end_year,sort,"descending")
            else:
                production=self.get_production_by_type(idx,max_results,page,start_year,end_year,sort,"descending",tipo)

            if production:
                response = self.app.response_class(
                response=self.json.dumps(production),
                status=200,
                mimetype='application/json'
                )
            else:
                response = self.app.response_class(
                response=self.json.dumps({"status":"Request returned empty"}),
                status=204,
                mimetype='application/json'
                )
        elif data=="authors":
            idx = self.request.args.get('id')
            max_results=self.request.args.get('max')
            page=self.request.args.get('page')
 
            authors=self.get_authors(idx,page,max_results)
            if authors:
                response = self.app.response_class(
                response=self.json.dumps(authors),
                status=200,
                mimetype='application/json'
                )
            else:
                response = self.app.response_class(
                response=self.json.dumps({"status":"Request returned empty"}),
                status=204,
                mimetype='application/json'
                )
        elif data=="groups":
            idx = self.request.args.get('id')
            max_results=self.request.args.get('max')
            page=self.request.args.get('page')

            groups=self.get_groups(idx,page,max_results)
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
            idx = self.request.args.get('id')
            max_results=self.request.args.get('max')
            page=self.request.args.get('page')

            groups=self.get_institutions(idx,page,max_results)
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
        elif data=="csv":
            idx = self.request.args.get('id')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            sort=self.request.args.get('sort')
            production_csv=self.get_csv(idx,start_year,end_year,sort,"descending")
            if production_csv:
                response = self.app.response_class(
                response=production_csv,
                status=200,
                mimetype='text/csv',
                headers={"Content-disposition":
                 "attachment; filename=authors.csv"}
                )
            else:
                response = self.app.response_class(
                response=self.json.dumps({"status":"Request returned empty"}),
                status=204,
                mimetype='application/json'
                )
        elif data=="json":
            idx = self.request.args.get('id')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            sort=self.request.args.get('sort')
            production_json=self.get_json(idx,start_year,end_year,sort,"descending")
            if production_json:
                response = self.app.response_class(
                response=production_json,
                status=200,
                mimetype='text/plain',
                headers={"Content-disposition":
                 "attachment; filename=authors.json"}
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