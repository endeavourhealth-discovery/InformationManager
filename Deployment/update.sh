#!/bin/bash

############################
## Fetch packages from S3 ##
############################
rm -r ./archives
mkdir ./archives
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/graphdb.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/config.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/user.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/im.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/workflow.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Dev-IMAPI.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Dev-IMUI.zip' './archives'

#######################
## Shutdown services ##
#######################
/opt/tomcat/bin/shutdown.sh
export graphdbpid=`ps -ef | awk '$NF~"com.ontotext.graphdb.server.GraphDBServer" {print $2}'`
kill -9 $graphdbpid

######################
## Update databases ##
######################
rm -r /opt/graphdb/data
mkdir /opt/graphdb/data
unzip -q ./archives/config.zip -d /opt/graphdb/data/repositories
unzip -q ./archives/user.zip -d /opt/graphdb/data/repositories
unzip -q ./archives/im.zip -d /opt/graphdb/data/repositories
unzip -q ./archives/workflow.zip -d /opt/graphdb/data/repositories

###################
## Deploy latest ##
###################
rm /opt/tomcat/webapps/imapi.war
mv ./archives/IMAPI/api/build/libs/imapi.war /opt/tomcat/webapps/

rm -r /var/www/e2e
mv ./archives/IMUI/dist /var/www/e2e
chown -R www-data:www-data /var/www/e2e

#############
## Startup ##
#############
/opt/graphdb/bin/graphdb -d -s
/opt/tomcat/bin/startup.sh

#############
## Cleanup ##
#############
rm -r ./archives