from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
import os
import datetime as dt
import re
from flask import send_file



class RegulationsApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self):
        files=[]
        dates={}
        for filename in os.listdir('sds/etc/.'):
            if "pdf" in filename:
                date_str=re.findall("^[0-9]*-[0-9]*-[0-9]*",filename)[0]
                date=dt.datetime.strptime(date_str,"%Y-%m-%d")
                dates[date_str]=date.timestamp()
                files.append({"filename":filename,"date":date_str,"size":os.stat('sds/etc/'+filename).st_size/1024})
        files_sorted=sorted(files,key=lambda x:dates[x["date"]],reverse=True)
        return {"data":files_sorted}

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