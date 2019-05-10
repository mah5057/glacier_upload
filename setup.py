import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='agbus',  
     version='0.4',
     scripts=['bin/agbus'] ,
     author="Matt Hardock",
     author_email="mah5057@gmail.com",
     description="A simplified Amazon Glacier cli, integrated with a database.",
     long_description=long_description,
   long_description_content_type="text/markdown",
   url="https://github.com/mah5057/glacier_upload",
     packages=setuptools.find_packages(),
     install_requires=[
        'boto3==1.9.50',
        'pymongo==3.7.2'
     ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )
