# Local s3 and spark

## Setup local s3

    curl --output localstack-cli-3.7.0-linux-amd64-onefile.tar.gz \
        --location https://github.com/localstack/localstack-cli/releases/download/v3.7.0/localstack-cli-3.7.0-linux-amd64-onefile.tar.gz

    tar -zxvf localstack-cli-3.7.0-linux-amd64-onefile.tar.gz
    
    sudo mv localstack /usr/local/bin/
    
    localstack --version

    localstack start -d

## Library

S3A depends upon two JARs, alongside hadoop-common and its dependencies.

1. hadoop-aws JAR.
2. aws-java-sdk-bundle JAR

The versions of hadoop-common and hadoop-aws must be identical.
To import the libraries into a Maven build, add hadoop-aws JAR to the build dependencies; it will pull in a compatible aws-sdk JAR.

## Load data into local s3

cat > requests.csv <<EOF
"email1"
"email2"
"email3"
EOF

aws s3 cp requests.csv s3://otdm-magna-dev-landing/

