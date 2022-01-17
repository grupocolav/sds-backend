<center><img src="https://saludata.saludcapital.gov.co/osb/wp-content/uploads/2021/07/2.-LOGO-HEADER-SALUDATA.png"/></center>

# sds  
Data Server Gateway

# Description
Package to load data in MongoDB and ArangoDB and to serve the data on endpoints using flask.
The package is handling the endpoints  using a customised plugin system designed by us.


# Installation

## Dependencies
* Install nodejs >=10.x.x ex: 10.13.0
    * Debian based system: `apt-get install nodejs`
    * Redhat based system: `yum install nodejs`
    * Conda: `conda install nodejs==10.13.0`
* Install Apidocjs from https://github.com/apidoc/apidoc
* The other dependecies can be installed with pip installing this package.
* Install MongoDB
    * Debian based system: `apt-get install mongodb`
    * Redhat based system instructions [here](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-red-hat/)
    * Conda: `conda install mongodb mongo-tools`
* Install ArangoDB
    * Debian based system instructions [here](https://www.arangodb.com/download-major/debian/)
    * Redhat based system instructions [here](https://www.arangodb.com/download-major/redhat/)
    * Conda: `conda install -c conda-forge python-arango`
* Install package
    * python3 setup.py build
    * python3 setup.py develop --user

NOTE:

To start mongodb server on conda please run the next steps

`
mkdir -p $HOME/data/db 
`

`
mongodb mongod --dbpath $HOME/data/db/
`


# Usage
Let's start the server executing
```.sh
sds_server
```
Or using some command line options
```.sh
sds_server --port 8080 --db_ip x.x.x.x
```

where x.x.x.x is your mongodb ip

you can access to the apidoc documentation for the endpoints for example on: http://127.0.1.1:8888/apidoc/index.html

if depends of the ip and port that you are providing to sds.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



