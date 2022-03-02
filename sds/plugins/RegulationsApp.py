from sds.sdsBase import sdsPluginBase, endpoint
from bson import ObjectId
from pymongo import ASCENDING,DESCENDING
import os



class AuthorsApp(sdsPluginBase):
    def __init__(self, sds):
        super().__init__(sds)

    def get_info(self):
        pass

    def get_file(self,file_name):
        pass

    @endpoint('/app/authors', methods=['GET'])
    def app_authors(self):
        data = self.request.args.get('data')
        file = self.request.args.get('file')
        if data=="info":
            info=self.get_info()
            if info:
                response = self.app.response_class(
                response=info,
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
        elif file:
            file=self.get_file()
            if info:
                response = self.app.response_class(
                response=info,
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