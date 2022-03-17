from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
from datetime import date

class TrendsApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self):
        covid_data={
            "id":self.colav_db["subjects"].find_one({"name":{"$regex":".*covid.*","$options":"i"}},{"_id":1}),
            "documents":self.colav_db["documents"].count_documents({"subjects.name":{"$regex":".*covid.*","$options":"i"}}),
            "authors":self.colav_db["authors"].count_documents({"subjects.name":{"$regex":".*covid.*","$options":"i"}}),
            "institutions":self.colav_db["institutions"].count_documents({"subjects.name":{"$regex":".*covid.*","$options":"i"}}),
            "groups":self.colav_db["branches"].count_documents({"type":"group","subjects.name":{"$regex":".*covid.*","$options":"i"}})
        }

        ods_data=[]
        for policy in self.colav_db["policies"].find({"abbreviations":"ODS"}):
            entry={
                "id":policy["_id"],
                "name":policy["name"],
                "index":policy["ids"]["ODS"],
                "documents":self.colav_db["documents"].count_documents({"policies.id":policy["_id"]}),
                "authors":self.colav_db["authors"].count_documents({"policies.id":policy["_id"]}),
                "institutions":self.colav_db["institutions"].count_documents({"policies.id":policy["_id"]}),
                "groups":self.colav_db["branches"].count_documents({"policies.id":policy["_id"]})
            }
            ods_data.append(entry)

        pdd_reg=self.colav_db["policies"].find_one({"abbreviations":"PDD"})
        pdd_data={
            "id":pdd_reg["_id"],
            "documents":self.colav_db["documents"].count_documents({"policies.id":pdd_reg["_id"]}),
            "authors":self.colav_db["authors"].count_documents({"policies.id":pdd_reg["_id"]}),
            "institutions":self.colav_db["institutions"].count_documents({"policies.id":pdd_reg["_id"]}),
            "groups":self.colav_db["branches"].count_documents({"policies.id":pdd_reg["_id"]})
        }

        pts_reg=self.colav_db["policies"].find_one({"abbreviations":"PTS"})
        pts_data={
            "id":pts_reg["_id"],
            "documents":self.colav_db["documents"].count_documents({"policies.id":pts_reg["_id"]}),
            "authors":self.colav_db["authors"].count_documents({"policies.id":pts_reg["_id"]}),
            "institutions":self.colav_db["institutions"].count_documents({"policies.id":pts_reg["_id"]}),
            "groups":self.colav_db["branches"].count_documents({"policies.id":pts_reg["_id"]})
        }

        return {
            "covid":covid_data,
            "ODS":ods_data,
            "PDD":pdd_data,
            "PTS":pts_data
        }


    @endpoint('/app/trends', methods=['GET'])
    def app_trends(self):

        trends=self.get_info()
        if trends:    
            response = self.app.response_class(
            response=self.json.dumps(trends),
            status=200,
            mimetype='application/json'
            )
        else:
            response = self.app.response_class(
            response=self.json.dumps({"status":"Request returned empty"}),
            status=204,
            )

        response.headers.add("Access-Control-Allow-Origin", "*")
        return response