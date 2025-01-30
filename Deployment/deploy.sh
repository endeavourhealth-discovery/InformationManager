#!/bin/bash

##########################
## Change node hostname ##
##########################
export AWS_DEFAULT_REGION=eu-west-2
INSTANCE_ID=$(ec2metadata --instance-id)
SHORT_NODE_NAME=$(aws ec2 describe-tags --filter "Name=resource-id,Values=$INSTANCE_ID" | jq -r '.Tags[] | select(.Key == "Name") | .Value')
NODE_DNS_NAME="$SHORT_NODE_NAME"
echo $NODE_DNS_NAME > /etc/hostname
echo "127.0.0.1 $NODE_DNS_NAME" >> /etc/hosts
hostname $NODE_DNS_NAME

#####################
## Install Java 17 ##
#####################
wget -O - https://apt.corretto.aws/corretto.key | sudo gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | sudo tee /etc/apt/sources.list.d/corretto.list
sudo apt-get update
sudo apt-get install -y java-17-amazon-corretto-jdk

########################
## Install CodeDeploy ##
########################
sudo apt -y install ruby-full
sudo apt -y install wget
wget https://aws-codedeploy-eu-west-2.s3.eu-west-2.amazonaws.com/latest/install
chmod +x ./install
sudo ./install auto
systemctl start codedeploy-agent

############################
## Fetch packages from S3 ##
############################
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Nginx/dsm2.crt' /etc/ssl/nginx/dsm.crt
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Nginx/dsm2.key' /etc/ssl/nginx/dsm.key
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Nginx/collectd.conf' /etc/collectd/collectd.conf
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/cloudwatchlogs/file_amazon-cloudwatch-agent.json' /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.d/amazon-cloudwatch-agent.json
mkdir ./archives
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/graphdb.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/config.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/user.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/im.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/workflow.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/tomcat.tar.gz' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/setenv.sh' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Nginx/default.conf' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Dev-IMAPI.zip' './archives'
aws s3 cp 's3://endeavour-deployment/InformationModel(v2)/Dev-IMUI.zip' './archives'

#######################
## Install Tomcat 10 ##
#######################
useradd -m -d /opt/tomcat -U -s /bin/false tomcat
tar xzf ./archives/tomcat.tar.gz -C /opt/tomcat --strip-components=1
chown -R tomcat:tomcat /opt/tomcat/
chmod -R u+x /opt/tomcat/bin
rm -r /opt/tomcat/webapps/*
mv ./archives/setenv.sh /opt/tomcat/bin
chmod +x /opt/tomcat/bin/setenv.sh

###################
## Install NGINX ##
###################
wget http://nginx.org/keys/nginx_signing.key
apt-key add nginx_signing.key

echo "deb [arch=amd64] http://nginx.org/packages/mainline/ubuntu/ $(lsb_release -sc) nginx" >> /etc/apt/sources.list.d/nginx.list
echo "deb-src http://nginx.org/packages/mainline/ubuntu/ $(lsb_release -sc) nginx" >> /etc/apt/sources.list.d/nginx.list

apt update
apt install nginx


# Configure TLS
openssl dhparam -out /etc/nginx/dhparam.pem 4096

mkdir /etc/nginx/snippets
echo "ssl_certificate /etc/ssl/nginx/dsm.crt;" >> /etc/nginx/snippets/self-signed.conf
echo "ssl_certificate_key /etc/ssl/nginx/dsm.key;" >> /etc/nginx/snippets/self-signed.conf

echo "ssl_protocols TLSv1.2;" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_prefer_server_ciphers on;" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_dhparam /etc/nginx/dhparam.pem;" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512:ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384;" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_ecdh_curve secp384r1; # Requires nginx >= 1.1.0" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_session_timeout  10m;" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_session_cache shared:SSL:10m;" >> /etc/nginx/snippets/ssl-params.conf
echo "ssl_session_tickets off; # Requires nginx >= 1.5.9" >> /etc/nginx/snippets/ssl-params.conf
echo "#ssl_stapling on; # Requires nginx >= 1.3.7" >> /etc/nginx/snippets/ssl-params.conf
echo "#ssl_stapling_verify on; # Requires nginx => 1.3.7" >> /etc/nginx/snippets/ssl-params.conf
echo "resolver 8.8.8.8 8.8.4.4 valid=300s;" >> /etc/nginx/snippets/ssl-params.conf
echo "resolver_timeout 5s;" >> /etc/nginx/snippets/ssl-params.conf
echo "# Disable strict transport security for now. You can uncomment the following" >> /etc/nginx/snippets/ssl-params.conf
echo "# line if you understand the implications." >> /etc/nginx/snippets/ssl-params.conf
echo 'add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload";' >> /etc/nginx/snippets/ssl-params.conf
echo "add_header X-Frame-Options DENY;" >> /etc/nginx/snippets/ssl-params.conf
echo "add_header X-Content-Type-Options nosniff;" >> /etc/nginx/snippets/ssl-params.conf
echo 'add_header X-XSS-Protection "1; mode=block";' >> /etc/nginx/snippets/ssl-params.conf

# Fix nginx PID error
mkdir /etc/systemd/system/nginx.service.d
printf "[Service]\nExecStartPost=/bin/sleep 0.1\n" > /etc/systemd/system/nginx.service.d/override.conf
systemctl daemon-reload

# CloudWatchLogs
chmod 755 /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.d/amazon-cloudwatch-agent.json

export AWS_DEFAULT_REGION=eu-west-2
INSTANCE_ID=$(ec2metadata --instance-id)
SHORT_NODE_NAME=$(aws ec2 describe-tags --filter "Name=resource-id,Values=$INSTANCE_ID" | jq -r '.Tags[] | select(.Key == "Name") | .Value')
HOSTNAME="$SHORT_NODE_NAME"
sed -i -e "s/HOSTNAME/$HOSTNAME/g" /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.d/amazon-cloudwatch-agent.json

#################
## INSTALL IM2 ##
#################
# Extract (latest) builds
unzip -q archives/Dev-IMAPI.zip -d ./archives/IMAPI
unzip -q archives/Dev-IMUI.zip -d ./archives/IMUI

# Setup GraphDB
mkdir /opt/graphdb
unzip -q archives/graphdb.zip -d /opt/graphdb
mkdir /opt/graphdb/data
unzip -q archives/config.zip -d /opt/graphdb/data/repositories
unzip -q archives/user.zip -d /opt/graphdb/data/repositories
unzip -q archives/im.zip -d /opt/graphdb/data/repositories
unzip -q archives/workflow.zip -d /opt/graphdb/data/repositories
chmod +x /opt/graphdb/bin/graphdb

# Deploy to Tomcat
mv ./archives/IMAPI/api/build/libs/imapi.war /opt/tomcat/webapps/

# Deploy to Nginx
cp ./archives/default.conf /etc/nginx/conf.d/default.conf
mkdir /var/www
mv ./archives/IMUI/dist /usr/share/nginx/html
chown -R www-data:www-data /usr/share/nginx/html
service nginx restart

# Startup
/opt/graphdb/bin/graphdb -d -s
/opt/tomcat/bin/startup.sh

# Cleanup
rm -r archives
