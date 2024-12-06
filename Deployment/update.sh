#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto/

############################
## Fetch packages from S3 ##
############################
echo "Fetching latest packages...."
rm -r ./archives
mkdir ./archives
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/config.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/user.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/im.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/workflow.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Dev-IMAPI.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Dev-IMUI.zip' './archives'

#######################
## Shutdown services ##
#######################
echo "Shutting down services...."
echo "....Tomcat...."
/opt/tomcat/bin/shutdown.sh
export graphdbpid=`ps -ef | awk '$NF~"com.ontotext.graphdb.server.GraphDBServer" {print $2}'`
echo "....Graphdb ( $graphdbpid )...."
kill -9 $graphdbpid

######################
## Update databases ##
######################
echo "Deploying databases...."
rm -r /opt/graphdb/data
mkdir /opt/graphdb/data
echo "....config...."
unzip -q ./archives/config.zip -d /opt/graphdb/data/repositories
echo "....user...."
unzip -q ./archives/user.zip -d /opt/graphdb/data/repositories
echo "....im...."
unzip -q ./archives/im.zip -d /opt/graphdb/data/repositories
echo "....workflow...."
unzip -q ./archives/workflow.zip -d /opt/graphdb/data/repositories

###################
## Deploy latest ##
###################
echo "Deploying applications...."
echo "....API...."
unzip -q archives/Dev-IMAPI.zip -d ./archives/IMAPI
rm /opt/tomcat/webapps/imapi.war
mv ./archives/IMAPI/api/build/libs/imapi.war /opt/tomcat/webapps/

echo "....UI...."
unzip -Q archives/Dev-IMUI.zip -d ./archives/IMUI
rm -r /var/www/e2e
mv ./archives/IMUI/dist /var/www/e2e
chown -R www-data:www-data /var/www/e2e

#############
## Startup ##
#############
echo "Restarting services...."
echo "....Tomcat...."
/opt/graphdb/bin/graphdb -d -s
echo "....GraphDB...."
/opt/tomcat/bin/startup.sh

#############
## Cleanup ##
#############
echo "Housekeeping...."
rm -r ./archives

echo "Done."
