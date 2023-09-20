Configuring S3-storage requires that you have a replica set with at least three-nodes deployed and access to S3 storage.
  If you don't have a three node replica set read the [Managing Units How-To for K8s charm](/t/charmed-mongodb-k8s-tutorial-managing-your-units/10611) 

Once you have a three-node replica set deployed, you can configure your settings for S3. Configurations are managed with the [s3-integrator charm](https://charmhub.io/s3-integrator).  Deploy and configure the s3-integrator charm:
```
juju deploy s3-integrator
juju run s3-integrator/leader sync-s3-credentials access-key=<access-key-here> secret-key=<secret-key-here>
juju config s3-integrator \
    path="mongodb-vm/demo" \
    region="us-west-2" \
    bucket="pbm-test-bucket-1"
```
The s3-integrator charm [accepts many configurations](https://charmhub.io/s3-integrator/configure) - enter whatever configurations are necessary for your s3 storage. 

To pass these configurations to Charmed MongoDB, relate the two applications:
```
juju relate s3-integrator mongodb-k8s
```

You can also update your configuration options after relating:
```
juju config s3-integrator \
    endpoint=<endpoint>
```