AmazonGlacierBackUpSystem
=========================

Amazon Glacier BackUp System (AGBUS) is a cli tool that works with Amazon Glacier to allow for simplified archive upload to Amazon Glacier, as well as metadata storage in a hosted mongoDB solution.

 Installation:
 -------------

 ```
 pip install agbus
 ```

Database Configuration File
---------------------------

- location: `~/.gbs/db_config`
- Only 1 attribute: ```dbUri```
        - Can define the connection string URI for any mongo instance
- example ```db_config```:

```
{
    "dbUri": "mongodb://localhost:27017"
}
```

AWS authentication
------------------

- AGBUS uses the awscli for authentication
- https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html


