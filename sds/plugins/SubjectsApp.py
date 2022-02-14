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
            "start_year":initial_year if initial_year!=0 else "",
            "end_year":final_year if final_year!=0 else ""
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

    def get_venn(self,venn_query):
        venn_source={
            "scholar":0,"lens":0,"wos":0,"scopus":0,
            "scholar_lens":0,"scholar_wos":0,"scholar_scopus":0,"lens_wos":0,"lens_scopus":0,"wos_scopus":0,
            "scholar_lens_wos":0,"scholar_wos_scopus":0,"scholar_lens_scopus":0,"lens_wos_scopus":0,
            "scholar_lens_wos_scopus":0
        }
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["scholar"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":"lens"},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["lens"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":"wos"},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["wos"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":"scopus"}]
        venn_source["scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":"lens"},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["scholar_lens"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":"wos"},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["scholar_wos"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":"scopus"}]
        venn_source["scholar_scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":"lens"},
                {"source_checked.source":"wos"},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["lens_wos"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":"lens"},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":"scopus"}]
        venn_source["lens_scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":"wos"},
                {"source_checked.source":"scopus"}]
        venn_source["wos_scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":"lens"},
                {"source_checked.source":"wos"},
                {"source_checked.source":{"$ne":"scopus"}}]
        venn_source["scholar_lens_wos"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":{"$ne":"lens"}},
                {"source_checked.source":"wos"},
                {"source_checked.source":"scopus"}]
        venn_source["scholar_wos_scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":"lens"},
                {"source_checked.source":{"$ne":"wos"}},
                {"source_checked.source":"scopus"}]
        venn_source["scholar_lens_scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":{"$ne":"scholar"}},
                {"source_checked.source":"lens"},
                {"source_checked.source":"wos"},
                {"source_checked.source":"scopus"}]
        venn_source["lens_wos_scopus"]=self.colav_db['documents'].count_documents(venn_query)
        venn_query["$and"]=[{"source_checked.source":"scholar"},
                {"source_checked.source":"lens"},
                {"source_checked.source":"wos"},
                {"source_checked.source":"scopus"}]
        venn_source["scholar_lens_wos_scopus"]=self.colav_db['documents'].count_documents(venn_query)

        return venn_source

    def get_production(self,idx=None,max_results=100,page=1,start_year=None,end_year=None,sort=None,direction=None):
        papers=[]
        total=0
        open_access=[]
        
        if start_year:
            try:
                start_year=int(start_year)
            except:
                print("Could not convert start year to int")
                return None
        if end_year:
            try:
                end_year=int(end_year)
            except:
                print("Could not convert end year to int")
                return None
        
        if idx:
            if start_year and not end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)})
                venn_query={"year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)})  },
                    {"type":"gold"  ,"value":self.colav_db['documents'].count_documents({"open_access_status":"gold","year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)})   },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)}) },
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)}) },
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","year_published":{"$gte":start_year},"subjects.id":ObjectId(idx)}) }
                ])
            elif end_year and not start_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)})
                venn_query={"year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)})  },
                    {"type":"gold"  ,"value": self.colav_db['documents'].count_documents({"open_access_status":"gold","year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)})  },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)}) },
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)}) },
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","year_published":{"$lte":end_year},"subjects.id":ObjectId(idx)}) }
                ])
            elif start_year and end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)})
                venn_query={"year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)}) },
                    {"type":"gold"  ,"value":self.colav_db['documents'].count_documents({"open_access_status":"gold","year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)})  },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)})},
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)})},
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","year_published":{"$gte":start_year,"$lte":end_year},"subjects.id":ObjectId(idx)})}
                ])
            else:
                cursor=self.colav_db['documents'].find({"subjects.id":ObjectId(idx)})
                venn_query={"subjects.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","subjects.id":ObjectId(idx)}) },
                    {"type":"gold"  ,"value":self.colav_db['documents'].count_documents({"open_access_status":"gold","subjects.id":ObjectId(idx)})  },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","subjects.id":ObjectId(idx)})},
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","subjects.id":ObjectId(idx)})},
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","subjects.id":ObjectId(idx)})}
                ])
        else:
            cursor=self.colav_db['documents'].find() 
            venn_query={}
        total=cursor.count()
        if not page:
            page=1
        else:
            try:
                page=int(page)
            except:
                print("Could not convert end page to int")
                return None
        if not max_results:
            max_results=100
        else:
            try:
                max_results=int(max_results)
            except:
                print("Could not convert end max to int")
                return None
        

        if sort=="citations" and direction=="ascending":
            cursor.sort([("citations_count",ASCENDING)])
        if sort=="citations" and direction=="descending":
            cursor.sort([("citations_count",DESCENDING)])
        if sort=="year" and direction=="ascending":
            cursor.sort([("year_published",ASCENDING)])
        if sort=="year" and direction=="descending":
            cursor.sort([("year_published",DESCENDING)])

        cursor=cursor.skip(max_results*(page-1)).limit(max_results)

        for paper in cursor:
            entry={
                "id":paper["_id"],
                "title":paper["titles"][0]["title"],
                "citations_count":paper["citations_count"],
                "year_published":paper["year_published"],
                "open_access_status":paper["open_access_status"]
            }

            source=self.colav_db["sources"].find_one({"_id":paper["source"]["id"]})
            if source:
                entry["source"]={"name":source["title"],"id":str(source["_id"])}
            authors=[]
            for author in paper["authors"]:
                au_entry={}
                author_db=self.colav_db["authors"].find_one({"_id":author["id"]})
                if author_db:
                    au_entry={"full_name":author_db["full_name"],"id":author_db["_id"]}
                affiliations=[]
                for aff in author["affiliations"]:
                    aff_entry={}
                    group_entry={}
                    aff_db=self.colav_db["institutions"].find_one({"_id":aff["id"]})
                    if aff_db:
                        aff_entry={"name":aff_db["name"],"id":aff_db["_id"]}
                    branches=[]
                    if "branches" in aff.keys():
                        for branch in aff["branches"]:
                            if "id" in branch.keys():
                                branch_db=self.colav_db["branches"].find_one({"_id":branch["id"]})
                                if branch_db and branch_db["type"] != "department" and branch_db["type"]!="faculty":
                                    group_entry= ({"name":branch_db["name"],"type":branch_db["type"],"id":branch_db["_id"]})
                                    affiliations.append(group_entry)

                    affiliations.append(aff_entry)
                au_entry["affiliations"]=affiliations
                authors.append(au_entry)
            entry["authors"]=authors
            papers.append(entry)

        tipos = self.colav_db['documents'].distinct("publication_type.type",{"subjects.id":ObjectId(idx)})

        return {
            "open_access":open_access,
            "venn_source":self.get_venn(venn_query),
            "types":tipos
            }
    
    def get_production_by_type(self,idx=None,max_results=100,page=1,start_year=None,end_year=None,sort="descending",direction=None,tipo=None):
        total = 0

        if start_year:
            try:
                start_year=int(start_year)
            except:
                print("Could not convert start year to int")
                return None
        if end_year:
            try:
                end_year=int(end_year)
            except:
                print("Could not convert end year to int")
                return None

        if idx:

            if start_year and not end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year},"subjects.id":ObjectId(idx),
                    "publication_type.type":tipo})

            elif end_year and not start_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$lte":end_year},"subjects.id":ObjectId(idx),
                    "publication_type.type":tipo})

            elif start_year and end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year,"$lte":end_year},
                    "subjects.id":ObjectId(idx), "publication_type.type":tipo})

            else:
                cursor=self.colav_db['documents'].find({"subjects.id":ObjectId(idx),"publication_type.type":tipo})
        
        total=cursor.count()
        if not page:
            page=1
        else:
            try:
                page=int(page)
            except:
                print("Could not convert end page to int")
                return None
        if not max_results:
            max_results=100
        else:
            try:
                max_results=int(max_results)
            except:
                print("Could not convert end max to int")
                return None
        
        if sort=="citations" and direction=="ascending":
            cursor.sort([("citations_count",ASCENDING)])
        if sort=="citations" and direction=="descending":
            cursor.sort([("citations_count",DESCENDING)])
        if sort=="year" and direction=="ascending":
            cursor.sort([("year_published",ASCENDING)])
        if sort=="year" and direction=="descending":
            cursor.sort([("year_published",DESCENDING)])

        cursor=cursor.skip(max_results*(page-1)).limit(max_results)

        entry=[]

        for doc in cursor:
            authors=[]
            for author in doc["authors"]:
                au_entry={}
                author_db=self.colav_db["authors"].find_one({"_id":author["id"]})
                if author_db:
                    au_entry={"full_name":author_db["full_name"],"id":author_db["_id"]}
                affiliations=[]
                for aff in author["affiliations"]:
                    aff_entry={}
                    aff_db=self.colav_db["institutions"].find_one({"_id":aff["id"]})
                    if aff_db:
                        aff_entry={"name":aff_db["name"],"id":aff_db["_id"]}
                    
                    affiliations.append(aff_entry)
                au_entry["affiliations"]=affiliations
                authors.append(au_entry)

            try:
                if doc["publication_type"]["source"]=="lens":

                    source=self.colav_db["sources"].find_one({"_id":doc["source"]["id"]})

                    entry.append({
                    "id":doc["_id"],
                    "title":doc["titles"][0]["title"],
                    "citations_count":doc["citations_count"],
                    "year_published":doc["year_published"],
                    "open_access_status":doc["open_access_status"],
                    "source":{"name":source["title"],"id":str(source["_id"])},
                    "authors":authors
                    })

            except:
                continue
        return {"total":total,"page":page,"count":len(entry),"data":entry}

    def get_institutions(self,idx=None,page=1,max_results=100,sort="citations",direction="descending"):
        
        total_results = self.colav_db["institutions"].count_documents({"subjects.id":ObjectId(idx)})

        if not page:
            page=1
        else:
            try:
                page=int(page)
            except:
                print("Could not convert end page to int")
                return None
        if not max_results:
            max_results=100
        else:
            try:
                max_results=int(max_results)
            except:
                print("Could not convert end max to int")
                return None

        skip = (max_results*(page-1))

        cursor=self.colav_db["institutions"].find({"subjects.id":ObjectId(idx)})

        cursor=cursor.skip(skip).limit(max_results)

        if sort=="citations" and direction=="ascending":
            cursor.sort([("citations_count",ASCENDING)])
        if sort=="citations" and direction=="descending":
            cursor.sort([("citations_count",DESCENDING)])

        entry = []
        for reg in cursor:
            entry.append({"name":reg["name"],"id":reg["_id"],"citations":reg["citations_count"]})

        return {"total":total_results,"page":page,"count":len(entry),"data":entry}
    
    def get_groups(self,idx=None,page=1,max_results=100,sort="citations",direction="descending"):
        
        total_results = self.colav_db["branches"].count_documents({"type":"group","subjects.id":ObjectId(idx)})

        if not page:
            page=1
        else:
            try:
                page=int(page)
            except:
                print("Could not convert end page to int")
                return None
        if not max_results:
            max_results=100
        else:
            try:
                max_results=int(max_results)
            except:
                print("Could not convert end max to int")
                return None

        skip = (max_results*(page-1))

        cursor=self.colav_db["branches"].find({"type":"group","subjects.id":ObjectId(idx)})

        cursor=cursor.skip(skip).limit(max_results)

        if sort=="citations" and direction=="ascending":
            cursor.sort([("citations_count",ASCENDING)])
        if sort=="citations" and direction=="descending":
            cursor.sort([("citations_count",DESCENDING)])

        entry = []
        for reg in cursor:
            entry.append({"name":reg["name"],"id":reg["_id"],"citations":reg["citations_count"]})

        return {"total":total_results,"page":page,"count":len(entry),"data":entry}

    def get_authors(self,idx=None,page=1,max_results=100):
        if idx:
            pipeline=[
                {"$match":{"subjects.id":ObjectId(idx)}}
            ]

            pipeline.extend([
                {"$unwind":"$authors"},
                {"$project":{"authors":1,"citations_count":1}},
                {"$group":{"_id":"$authors.id","papers_count":{"$sum":1},"citations_count":{"$sum":"$citations_count"},"author":{"$first":"$authors"}}},
                {"$sort":{"citations_count":-1}},
                {"$project":{"author.id":1,"author.full_name":1,"author.affiliations.name":1,"author.affiliations.id":1,
                    "author.affiliations.branches.name":1,"author.affiliations.branches.type":1,"author.affiliations.branches.id":1,
                    "papers_count":1,"citations_count":1}}
            ])

            total_results = self.colav_db["authors"].count_documents({"subjects.id":ObjectId(idx)})

            if not page:
                page=1
            else:
                try:
                    page=int(page)
                except:
                    print("Could not convert end page to int")
                    return None
            if not max_results:
                max_results=100
            else:
                try:
                    max_results=int(max_results)
                except:
                    print("Could not convert end max to int")
                    return None

            skip = (max_results*(page-1))

            pipeline.extend([{"$skip":skip},{"$limit":max_results}])

            result= self.colav_db["documents"].aggregate(pipeline)
        
            entry = []

            for reg in result:
                group_name = ""
                group_id = ""
                if "branches" in reg["author"]["affiliations"][0]:
                    for i in range(len(reg["author"]["affiliations"][0]["branches"])):    
                        if reg["author"]["affiliations"][0]["branches"][i]["type"]=="group":
                            group_name = reg["author"]["affiliations"][0]["branches"][i]["name"]
                            group_id =   reg["author"]["affiliations"][0]["branches"][i]["id"]    

                entry.append({
                    "id":reg["_id"],
                    "name":reg["author"]["full_name"],
                    "papers_count":reg["papers_count"],
                    "citations_count":reg["citations_count"],
                    "affiliation":{"institution":{"name":reg["author"]["affiliations"][0]["name"], 
                                        "id":reg["author"]["affiliations"][0]["id"]},
                                   "group":{"name":group_name, "id":group_id}}
                })
            
        return {"total":total_results,"page":page,"count":len(entry),"data":entry}



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