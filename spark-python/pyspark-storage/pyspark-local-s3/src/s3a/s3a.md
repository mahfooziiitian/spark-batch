# S3A

## Hadoop S3A

When running Hadoop clusters in or outside of Amazon Web Services (AWS), the main interaction between the Hadoop Distributed File System (HDFS) and Amazon S3 is handled by a connector called S3A.

## Setting up Amazon S3 Access Points

### Creating the resource

Creating an access point for a S3 bucket can be done through the AWS Management Console,
`AWS Command Line Interface (AWS CLI)`, `AWS SDK`, or `CloudFormation`.

### Configuring S3A with access points

`fs.s3a.bucket.{ACCESS_POINT_NAME}.accesspoint.arn`

    <property>
        <name>fs.s3a.bucket.finance.accesspoint.arn</name>
        <value>arn:aws:s3:eu-west-1:123456789012:accesspoint/finance</value>
        <description>Configure S3a traffic to use this AccessPoint</description>
    </property>

### Access point only access
    
    <property>
        <name>fs.s3a.accesspoint.required</name>
        <value>true</value>
    </property>

## Authentication property

    <property>
      <name>fs.s3a.access.key</name>
      <description>AWS access key ID.
       Omit for IAM role-based or provider-based authentication.</description>
    </property>
    
    <property>
      <name>fs.s3a.secret.key</name>
      <description>AWS secret key.
       Omit for IAM role-based or provider-based authentication.</description>
    </property>
    
    <property>
      <name>fs.s3a.aws.credentials.provider</name>
      <description>
        Comma-separated class names of credential provider classes which implement
        com.amazonaws.auth.AWSCredentialsProvider.
    
        These are loaded and queried in sequence for a valid set of credentials.
        Each listed class must implement one of the following means of
        construction, which are attempted in order:
        1. a public constructor accepting java.net.URI and
            org.apache.hadoop.conf.Configuration,
        2. a public static method named getInstance that accepts no
           arguments and returns an instance of
           com.amazonaws.auth.AWSCredentialsProvider, or
        3. a public default constructor.
    
        Specifying org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider allows
        anonymous access to a publicly accessible S3 bucket without any credentials.
        Please note that allowing anonymous access to an S3 bucket compromises
        security and therefore is unsuitable for most use cases. It can be useful
        for accessing public data sets without requiring AWS credentials.
    
        If unspecified, then the default list of credential provider classes,
        queried in sequence, is:
        1. org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider:
           Uses the values of fs.s3a.access.key and fs.s3a.secret.key.
        2. com.amazonaws.auth.EnvironmentVariableCredentialsProvider: supports
            configuration of AWS access key ID and secret access key in
            environment variables named AWS_ACCESS_KEY_ID and
            AWS_SECRET_ACCESS_KEY, as documented in the AWS SDK.
        3. com.amazonaws.auth.InstanceProfileCredentialsProvider: supports use
            of instance profile credentials if running in an EC2 VM.
      </description>
    </property>
    
    <property>
      <name>fs.s3a.session.token</name>
      <description>
        Session token, when using org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider
        as one of the providers.
      </description>
    </property>

## Authenticating via the AWS Environment Variables

    export AWS_ACCESS_KEY_ID=my.aws.key
    export AWS_SECRET_ACCESS_KEY=my.secret.key

## 

| classname	                                                   | description                     |
|--------------------------------------------------------------|---------------------------------|
| org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider	    | Session Credentials             |
| org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider	       | Simple name/secret credentials  |
| org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider	    | Anonymous Login                 |
| org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider	 | Assumed Role credentials        |
| com.amazonaws.auth.InstanceProfileCredentialsProvider	       | EC2 Metadata Credentials        |
| com.amazonaws.auth.EnvironmentVariableCredentialsProvider	   | AWS Environment Variables       |
| com.amazonaws.auth.profile.ProfileCredentialsProvider        | Using Named Profile Credentials |