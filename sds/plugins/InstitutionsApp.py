from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
from pickle import load
from datetime import date
from math import log
from flask import redirect


class InstitutionsApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self,idx):
        institution = self.colav_db['institutions'].find_one({"_id":ObjectId(idx)})
        if institution:
            entry={"id":institution["_id"],
                "name":institution["name"],
                "external_urls":institution["external_urls"],
                "logo":institution["logo_url"]
            }

        if(idx):
            result=self.colav_db['documents'].find({"authors.affiliations.id":ObjectId(idx)},{"year_published":1}).sort([("year_published",ASCENDING)]).limit(1)
            if result:
                result=list(result)
                if len(result)>0:
                    initial_year=result[0]["year_published"]
            result=self.colav_db['documents'].find({"authors.affiliations.id":ObjectId(idx)},{"year_published":1}).sort([("year_published",DESCENDING)]).limit(1)
            if result:
                result=list(result)
                if len(result)>0:
                    final_year=result[0]["year_published"]

            filters={
                "start_year":initial_year,
                "end_year":final_year
             }
            
            return {"data": entry, "filters": filters }
        else:
            return None
    
    def hindex(self,citation_list):
        return sum(x >= i + 1 for i, x in enumerate(sorted(list(citation_list), reverse=True)))


    def get_citations(self,idx=None,start_year=None,end_year=None):

 
        entry={
            "citations":0,
            "yearly_citations":[],
            "geo": []
        }

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
        


        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}}
        ]


        pipeline.extend([
            {"$match":{"citations":{"$ne":[]}}},
            {"$unwind":"$citations"},
            {"$lookup":{
                "from":"documents",
                "localField":"citations",
                "foreignField":"_id",
                "as":"citers"}
            },
            {"$unwind":"$citers"}])


        if start_year and not end_year:
            pipeline.extend([{"$match":{"citers.year_published":{"$gte":start_year}}}])
        elif end_year and not start_year:
            pipeline.extend([{"$match":{"citers.year_published":{"$lte":end_year}}}])
        elif start_year and end_year:
            pipeline.extend([{"$match":{"citers.year_published":{"$gte":start_year,"$lte":end_year}}}])

            


        geo_pipeline = pipeline[:] # a clone


        pipeline.extend([
            {"$group":{
                "_id":"$citers.year_published","count":{"$sum":1}}
            },
            {"$sort":{
                "_id":1
            }}
        ])


        geo_pipeline.extend([
            {"$unwind":"$citers.authors"},
            {"$project":{"citers.authors.affiliations":1}},
            {"$unwind":"$citers.authors.affiliations"},
            {"$lookup":{"from":"institutions","foreignField":"_id","localField":"citers.authors.affiliations.id","as":"affiliation"}},
            {"$project":{"affiliation.addresses.country":1,"affiliation.addresses.country_code":1}},
            {"$unwind":"$affiliation"},{"$group":{"_id":"$affiliation.addresses.country_code","count":{"$sum":1},
             "country": {"$first": "$affiliation.addresses.country"}}},{"$project": {"country": 1,"_id":1,"count": 1, "log_count": {"$ln": "$count"}}},
            {"$unwind": "$_id"}, {"$unwind": "$country"}
        ])


        for idx,reg in enumerate(self.colav_db["documents"].aggregate(pipeline)):
            entry["citations"]+=reg["count"]
            entry["yearly_citations"].append({"year":reg["_id"],"value":reg["count"]})




        for i, reg in enumerate(self.colav_db["documents"].aggregate(geo_pipeline)):
            entry["geo"].append({"country": reg["country"],
                                 "country_code": reg["_id"],
                                 "count": reg["count"],
                                 "log_count": reg["log_count"]}
                                 )
    
        return {"data": entry}

    
    def get_authors(self,idx=None,page=1,max_results=100):
        if idx:

            pipeline=[
                {"$match":{"authors.affiliations.id":ObjectId(idx)}}
            ]

            pipeline.extend([

                {"$unwind":"$authors"},
                {"$match":{"authors.affiliations.id":ObjectId(idx)}},
                {"$project":{"authors":1,"citations_count":1}},
                {"$group":{"_id":"$authors.id","papers_count":{"$sum":1},"citations_count":{"$sum":"$citations_count"},"author":{"$first":"$authors"}}},
                {"$sort":{"citations_count":-1}},
                {"$project":{"author.id":1,"author.full_name":1,"author.affiliations.name":1,"author.affiliations.id":1,
                    "author.affiliations.branches.name":1,"author.affiliations.branches.type":1,"author.affiliations.branches.id":1,
                    "papers_count":1,"citations_count":1}}

            ])

            total_results = self.colav_db["authors"].count_documents({"affiliations.id":ObjectId(idx)})


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



    def get_coauthors(self,idx=None,start_year=None,end_year=None,page=1,max_results=100):
        
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
            pipeline=[
                {"$match":{"authors.affiliations.id":ObjectId(idx)}}
            ]
 
            if start_year and not end_year:
                pipeline=[
                    {"$match":{"year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)}}
                ]
            elif end_year and not start_year:
                pipeline=[
                    {"$match":{"year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}}
                ]
            elif start_year and end_year:
                pipeline=[
                    {"$match":{"year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}}
                ]
                


        pipeline.extend([
            {"$unwind":"$authors"},
            {"$unwind":"$authors.affiliations"},
            {"$match":{"authors.affiliations.id":{"$ne":ObjectId(idx)}}},
            {"$group":{"_id":"$authors.affiliations.id","count":{"$sum":1}}},
            {"$sort":{"count":-1}},
            {"$unwind":"$_id"},
            {"$lookup":{"from":"institutions","localField":"_id","foreignField":"_id","as":"affiliation"}},
            {"$project":{"count":1,"affiliation.name":1}},
            {"$unwind":"$affiliation"}
        ])


        total_results = len(set([reg["authors"]["affiliations"]["id"] for reg in self.colav_db["documents"].aggregate(pipeline[:4])]))

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


        entry={
            "institutions":[],
            "geo":[],
            "institutions_network":{} #{"nodes":load(open("./nodes.p","rb")),"edges":load(open("./edges.p","rb"))}
        }
 
        entry["institutions"]=[
            {"name":reg["affiliation"]["name"],"id":reg["_id"],"count":reg["count"]} for reg in self.colav_db["documents"].aggregate(pipeline) if str(reg["_id"])!=idx
        ]
    
        countries=[]
        country_list=[]
        pipeline=[pipeline[0]]
        pipeline.extend([
            {"$unwind":"$authors"},
            {"$group":{"_id":"$authors.affiliations.id","count":{"$sum":1}}},
            {"$unwind":"$_id"},
            {"$lookup":{"from":"institutions","localField":"_id","foreignField":"_id","as":"affiliation"}},
            {"$project":{"count":1,"affiliation.addresses.country_code":1,"affiliation.addresses.country":1}},
            {"$unwind":"$affiliation"},
            {"$unwind":"$affiliation.addresses"}
            #{"$sort":{"count":-1}}
        ])
        for reg in self.colav_db["documents"].aggregate(pipeline,allowDiskUse=True):
            #print(reg)
            if str(reg["_id"])==idx:
                continue
            if not "country_code" in reg["affiliation"]["addresses"].keys():
                continue
            if reg["affiliation"]["addresses"]["country_code"] and reg["affiliation"]["addresses"]["country"]:
                if reg["affiliation"]["addresses"]["country_code"] in country_list:
                    i=country_list.index(reg["affiliation"]["addresses"]["country_code"])
                    countries[i]["count"]+=reg["count"]
                else:
                    country_list.append(reg["affiliation"]["addresses"]["country_code"])
                    countries.append({
                        "country":reg["affiliation"]["addresses"]["country"],
                        "country_code":reg["affiliation"]["addresses"]["country_code"],
                        "count":reg["count"]
                    })
        #sorted_geo=sorted(countries,key=lambda x:x["count"],reverse=True)
        #countries=sorted_geo
        for item in countries:
            item["log_count"]=log(item["count"])
        entry["geo"]=countries


        nodes=[]
        edges=[]

        nodes_idlist=[]
        edge_tuples=[]
        arango_edges=[]
        arango_nodes=[]
        arango_mongo_nodes={}
        query="FOR c IN institutions FILTER c.mongo_id=='"+idx+"' RETURN {_id:c._id,name:c.name}"
        result=list(self.arangodb.AQLQuery(query,rawResults=True,batchSize=1))
        if result:
            arangoid=result[0]["_id"]
            nodes.append({
                    "id":idx,
                    "degree":0,
                    "size":0,
                    "label":result[0]["name"]
                })
            query="FOR v,e,p IN 1..1 ANY '"+arangoid+"' GRAPH institutions_coauthorship RETURN {mongo_id:v.mongo_id,_id:v._id,name:v.name,weight:e.weight}" 
            for vertex in self.arangodb.AQLQuery(query,rawResults=True,batchSize=1):
                arango_nodes.append(vertex["_id"])
                
                arango_mongo_nodes[vertex["_id"]]=vertex["mongo_id"]
                node={
                    "id":vertex["mongo_id"],
                    "degree":0,
                    "size":0,
                    "label":vertex["name"]
                }
                if not node in nodes:
                    nodes.append(node)
                normal=(idx,vertex["mongo_id"])
                rever=(vertex["mongo_id"],idx)
                if not (normal in edge_tuples or rever in edge_tuples):
                    edge_tuples.append(normal)
                    edges.append({
                        "coauthorships":vertex["weight"],
                        "source":idx,
                        "sourceName":result[0]["name"],
                        "target":vertex["mongo_id"],
                        "targetName":vertex["name"],
                        "size":vertex["weight"]
                    })

            for node in arango_nodes:
                query="FOR e IN institution_coauthorship FILTER e._from=='"+node+"' RETURN {_from:e._from,_to:e._to,weight:e.weight}"
                for res in self.arangodb.AQLQuery(query,rawResults=True,batchSize=1):
                    if res["_to"] in arango_nodes and res["_from"] in arango_nodes:
                        if res["_to"]==res["_from"]:
                            continue
                        normal=(arango_mongo_nodes[res["_from"]],arango_mongo_nodes[res["_to"]])
                        rever=(arango_mongo_nodes[res["_to"]],arango_mongo_nodes[res["_from"]])
                        if not (normal in edge_tuples or rever in edge_tuples):
                            edge_tuples.append(normal)
                            #edges.append({"from":arango_mongo_nodes[res["_from"]],"to":arango_mongo_nodes[res["_to"]],"coauthorships":res["weight"]})
                            edges.append({
                                "coauthorships":res["weight"],
                                "source":arango_mongo_nodes[res["_from"]],
                                "sourceName":"",
                                "target":arango_mongo_nodes[res["_to"]],
                                "targetName":"",
                                "size":res["weight"]
                            })
            del(arango_nodes)
            del(arango_mongo_nodes)
            del(edge_tuples)
            total=max([e["coauthorships"] for e in edges]) if len(edges)>0 else 1
            degrees={}
            num_nodes=len(nodes)
            for edge in edges:
                edge["coauthorships"]=edge["coauthorships"]
                edge["size"]=10*log(1+edge["coauthorships"]/total,2)
                if edge["source"] in degrees.keys():
                    degrees[edge["source"]]+=1
                else:
                    degrees[edge["source"]]=1
                if edge["target"] in degrees.keys():
                    degrees[edge["target"]]+=1
                else:
                    degrees[edge["target"]]=1
            for node in nodes:
                if node["id"] in degrees.keys():
                    node["size"]=50*log(1+degrees[node["id"]]/(num_nodes-1),2)
                    node["degree"]=degrees[node["id"]]
            entry["institutions_network"]={"nodes":nodes,"edges":edges}
        else:
            aff=self.colav_db["institutions"].find_one({"_id":idx})

            entry["institutions_network"]={"nodes":[{
                "id":idx,
                "degree":0,
                "size":10,
                "label":aff["full_name"] if aff else "",
            }],"edges":[]}


        return {"total":total_results,"page":page,"count":len(entry["institutions"]),"data":entry}

    def get_subjects(self,idx=None,start_year=None,end_year=None,limit=10):
        initial_year=0
        final_year=0

        if not limit:
            limit=10
        else:
            try:
                limit=int(limit)
            except:
                print("Could not convert limit to int")
                return None

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
            result=self.colav_db["institutions"].find_one({"_id":ObjectId(idx)})
        else:
            return None

        if not "subjects_by_year" in result.keys():
            return None
        if not result["subjects_by_year"]:
            return None

        data=[]
        names=[]
        for key,val in result["subjects_by_year"].items():
            year=int(key)
            if start_year:
                if start_year>year:
                    continue
            if end_year:
                if end_year<year:
                    continue
            for sub in val:
                if sub["name"] in names:
                    data[names.index(sub["name"])]["value"]+=sub["value"]
                else:
                    data.append(sub)
                    names.append(sub["name"])
        
        sorted_data=sorted(data,key=lambda x:x["value"],reverse=True)
                
        return {"data":sorted_data[:limit],"total":len(data)}


    def get_groups(self,idx=None,page=1,max_results=100,sort="citations",direction="descending"):

        pipeline=[
            {"$match":{"type":"group","relations.id":ObjectId(idx)}},
            {"$project":{"_id":1,"name":1}},
            {"$lookup":{"from":"documents","localField":"_id","foreignField":"authors.affiliations.branches.id","as":"papers"}},
            {"$project":{"papers.citations_count":1,"name":1}},
            {"$unwind":"$papers"},
            {"$group":{"_id":"$_id","citations":{"$sum":"$papers.citations_count"},"name":{"$first":"$name"}}},
            {"$sort":{"citations":-1}}
        ]

        
        total_results = self.colav_db["branches"].count_documents({"type":"group","relations.id":ObjectId(idx)})


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

        #pipeline.extend([{"$skip":skip},{"$limit":max_results}])
        cursor=self.colav_db["branches"].find({"type":"group","relations.id":ObjectId(idx)})

        cursor=cursor.skip(skip).limit(max_results)

        if sort=="citations" and direction=="ascending":
            cursor.sort([("citations_count",ASCENDING)])
        if sort=="citations" and direction=="descending":
            cursor.sort([("citations_count",DESCENDING)])



        entry = []

        for reg in cursor:
            entry.append({"name":reg["name"],"id":reg["_id"],"citations":reg["citations_count"]})



        return {"total":total_results,"page":page,"count":len(entry),"data":entry}


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
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx),"publication_type.type":tipo})

            elif end_year and not start_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx),"publication_type.type":tipo})

            elif start_year and end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx),"publication_type.type":tipo})

            else:
                cursor=self.colav_db['documents'].find({"authors.affiliations.id":ObjectId(idx),"publication_type.type":tipo})


        
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
                        aff_entry={"institution":{"name":aff_db["name"],"id":aff_db["_id"]}}
                        break
                    else:
                        aff_entry={"institution":{"name":"","id":""}}
                    au_entry["affiliation"]=aff_entry
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

    def get_production(self,idx=None,start_year=None,end_year=None,sort=None,direction=None):
        
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
                venn_query={"year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","year_published":{ "$gte":start_year},"authors.affiliations.id":ObjectId(idx)})  },
                    {"type":"gold"  ,"value":self.colav_db['documents'].count_documents({"open_access_status":"gold","year_published":  {"$gte":start_year},"authors.affiliations.id":ObjectId(idx)})   },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)}) },
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)}) },
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)}) }
                ])
            elif end_year and not start_year:
                venn_query={"year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","year_published": {"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})  },
                    {"type":"gold"  ,"value": self.colav_db['documents'].count_documents({"open_access_status":"gold","year_published": {"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})  },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}) },
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}) },
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}) }
                ])
            elif start_year and end_year:
                venn_query={"year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green","year_published": {"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)}) },
                    {"type":"gold"  ,"value":self.colav_db['documents'].count_documents({"open_access_status":"gold","year_published":  {"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})  },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})},
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})},
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})}
                ])
            else:
                venn_query={"authors.affiliations.id":ObjectId(idx)}
                open_access.extend([
                    {"type":"green" ,"value":self.colav_db['documents'].count_documents({"open_access_status":"green" ,"authors.affiliations.id":ObjectId(idx)}) },
                    {"type":"gold"  ,"value":self.colav_db['documents'].count_documents({"open_access_status":"gold"  ,"authors.affiliations.id":ObjectId(idx)})  },
                    {"type":"bronze","value":self.colav_db['documents'].count_documents({"open_access_status":"bronze","authors.affiliations.id":ObjectId(idx)})},
                    {"type":"closed","value":self.colav_db['documents'].count_documents({"open_access_status":"closed","authors.affiliations.id":ObjectId(idx)})},
                    {"type":"hybrid","value":self.colav_db['documents'].count_documents({"open_access_status":"hybrid","authors.affiliations.id":ObjectId(idx)})}
                ])

        tipos = self.colav_db['documents'].distinct("publication_type.type",{"authors.affiliations.id":ObjectId(idx)})


        return {
            "open_access":open_access,
            "venn_source":self.get_venn(venn_query),
            "types":tipos

            }


    def get_csv(self,idx=None,start_year=None,end_year=None,sort=None,direction=None):
        papers=[]

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
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)})
            elif end_year and not start_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})
            elif start_year and end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})
            else:
                cursor=self.colav_db['documents'].find({"authors.affiliations.id":ObjectId(idx)})
        else:
            cursor=self.colav_db['documents'].find()

        if sort=="citations" and direction=="ascending":
            cursor.sort({"citations_count":pymongo.ASCENDING})
        if sort=="citations" and direction=="descending":
            cursor.sort({"citations_count":pymongo.DESCENDING})
        if sort=="year" and direction=="ascending":
            cursor.sort({"year_published":pymongo.ASCENDING})
        if sort=="year" and direction=="descending":
            cursor.sort({"year_published":pymongo.DESCENDING})

        csv_text="id\tpublication_type\ttitle\tabstract\tvolume\tissue\tstart_page\tend_page\tyear_published\tdate_published\t"
        csv_text+="funding_organization\tis_open_access\topen_access_status\tdoi\tjournal_name\tpublisher\tissn\t"
        csv_text+="author_id\tauthor_name\taffiliation_id\taffiliation_name\taffiliation_country\n"

        for paper in cursor:
            csv_text+=str(paper["_id"])
            csv_text+="\t"+paper["publication_type"]
            csv_text+="\t"+paper["titles"][0]["title"].replace("\t","").replace("\n","").replace("\r","")
            csv_text+="\t"+paper["abstract"].replace("\t","").replace("\n","").replace("\r","")
            csv_text+="\t"+str(paper["volume"])
            csv_text+="\t"+str(paper["issue"])
            csv_text+="\t"+str(paper["start_page"])
            csv_text+="\t"+str(paper["end_page"])
            csv_text+="\t"+str(paper["year_published"])
            try:
                ts=int(paper["date_published"])
                csv_text+="\t"+date.fromtimestamp(ts).strftime("%d-%m-%Y")
            except:
                csv_text+="\t"+""
            csv_text+="\t"+paper["funding_organization"].replace("\t","").replace("\n","").replace("\r","")
            csv_text+="\t"+str(paper["is_open_access"])
            csv_text+="\t"+paper["open_access_status"]
            doi_entry=""
            for ext in paper["external_ids"]:
                if ext["source"]=="doi":
                    doi_entry=ext["id"]
            csv_text+="\t"+str(doi_entry)

            source=self.colav_db["sources"].find_one({"_id":paper["source"]["id"]})
            if source:
                csv_text+="\t"+source["title"].replace("\t","").replace("\n","").replace("\r","")
                csv_text+="\t"+source["publisher"]
                serial_entry=""
                for serial in source["serials"]:
                    if serial["type"]=="issn" or serial["type"]=="eissn" or serial["type"]=="pissn":
                        serial_entry=serial["value"]
                csv_text+="\t"+serial_entry

            csv_text+="\t"+str(paper["authors"][0]["id"])
            author_db=self.colav_db["authors"].find_one({"_id":paper["authors"][0]["id"]})
            if author_db:
                csv_text+="\t"+author_db["full_name"]
            else:
                csv_text+="\t"+""
            aff_db=""
            if "affiliations" in paper["authors"][0].keys():
                if len(paper["authors"][0]["affiliations"])>0:
                    csv_text+="\t"+str(paper["authors"][0]["affiliations"][0]["id"])
                    aff_db=self.colav_db["institutions"].find_one({"_id":paper["authors"][0]["affiliations"][0]["id"]})
            if aff_db:
                csv_text+="\t"+aff_db["name"]
                country_entry=""
                if "addresses" in aff_db.keys():
                    if len(aff_db["addresses"])>0:
                        country_entry=aff_db["addresses"][0]["country"]
                csv_text+="\t"+country_entry
            else:
                csv_text+="\t"+""
                csv_text+="\t"+""
                csv_text+="\t"+""
            csv_text+="\n"
        return csv_text
    
    def get_json(self,idx=None,start_year=None,end_year=None,sort=None,direction=None):
        papers=[]

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
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year},"authors.affiliations.id":ObjectId(idx)})
            elif end_year and not start_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})
            elif start_year and end_year:
                cursor=self.colav_db['documents'].find({"year_published":{"$gte":start_year,"$lte":end_year},"authors.affiliations.id":ObjectId(idx)})
            else:
                cursor=self.colav_db['documents'].find({"authors.affiliations.id":ObjectId(idx)})
        else:
            cursor=self.colav_db['documents'].find()



        if sort=="citations" and direction=="ascending":
            cursor.sort({"citations_count":pymongo.ASCENDING})
        if sort=="citations" and direction=="descending":
            cursor.sort({"citations_count":pymongo.DESCENDING})
        if sort=="year" and direction=="ascending":
            cursor.sort({"year_published":pymongo.ASCENDING})
        if sort=="year" and direction=="descending":
            cursor.sort({"year_published":pymongo.DESCENDING})

        for paper in cursor:
            entry=paper
            source=self.colav_db["sources"].find_one({"_id":paper["source"]["id"]})
            if source:
                entry["source"]=source
            authors=[]
            for author in paper["authors"]:
                au_entry=author
                author_db=self.colav_db["authors"].find_one({"_id":author["id"]})
                if author_db:
                    au_entry=author_db
                affiliations=[]
                for aff in author["affiliations"]:
                    aff_entry=aff
                    aff_db=self.colav_db["institutions"].find_one({"_id":aff["id"]})
                    if aff_db:
                        aff_entry=aff_db
                    branches=[]
                    if "branches" in aff.keys():
                        for branch in aff["branches"]:
                            branch_db=self.colav_db["branches"].find_one({"_id":branch["id"]}) if "id" in branch.keys() else ""
                            if branch_db:
                                branches.append(branch_db)
                    aff_entry["branches"]=branches
                    affiliations.append(aff_entry)
                au_entry["affiliations"]=affiliations
                authors.append(au_entry)
            entry["authors"]=authors
            papers.append(entry)
        return str(papers)
    
 
    @endpoint('/app/institutions', methods=['GET'])
    def app_institutions(self):
        """
        """
        data = self.request.args.get('data')
        tipo = self.request.args.get('type')

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

            if tipo == None: 
                production=self.get_production(idx,start_year,end_year,sort,"descending")
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
        elif data=="citations":
            idx = self.request.args.get('id')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            citations=self.get_citations(idx,start_year,end_year)
            if citations:
                response = self.app.response_class(
                response=self.json.dumps(citations),
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
        elif data=="coauthors":
            idx = self.request.args.get('id')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            max_results=self.request.args.get('max')
            page=self.request.args.get('page')

            coauthors=self.get_coauthors(idx,start_year,end_year,page,max_results)
            if coauthors:
                response = self.app.response_class(
                response=self.json.dumps(coauthors),
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
        elif data=="subjects":
            idx = self.request.args.get('id')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            limit=self.request.args.get('limit')
            subjects=self.get_subjects(idx,start_year,end_year,limit)
            if subjects:
                response = self.app.response_class(
                response=self.json.dumps(subjects),
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
                 "attachment; filename=institutions.csv"}
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
                 "attachment; filename=institutions.json"}
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

