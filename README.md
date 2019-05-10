AmazonGlacierBackUpSystem
=========================

Amazon Glacier BackUp System (AGBUS) is a cli tool that provides an interface to Amazon Glacier that is simpler than the aws cli. It can also be pointed to a mongoDB instance to store information about vaults, archives and more.

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


