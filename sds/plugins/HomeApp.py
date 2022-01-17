from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
from pickle import load
from datetime import date
from math import log
from flask import redirect

class HomeApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self):
        pass

    @endpoint('/app/home', methods=['GET'])
    def app_home(self):
        """
        """
        
        info = self.get_info(idx)
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