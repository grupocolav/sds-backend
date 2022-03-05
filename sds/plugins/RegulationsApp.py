from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
import os
from flask import send_file



class RegulationsApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self):
        files=[]
        for filename in os.listdir('sds/etc/.'):
            if "pdf" in filename:
                files.append({"filename":filename,"size":os.stat('sds/etc/'+filename).st_size/1024})
        return {"data":files}

    @endpoint('/app/regulations', methods=['GET'])
    def app_regulations(self):
        data = self.request.args.get('data')
        filename = self.request.args.get('file')
        if data=="info":
            info=self.get_info()
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
        elif filename:
            if os.path.isfile('sds/etc/'+filename):
                response = send_file('sds/etc/'+filename,as_attachment=True)
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