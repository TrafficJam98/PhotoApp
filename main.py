#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   Yingqiao Gou
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#   Fall 2023
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  print()
  print(">> Enter a command:")
  print("   0 => end")
  print("   1 => stats")
  print("   2 => users")
  print("   3 => assets")
  print("   4 => download")
  print("   5 => download and display")
  print("   6 => upload")
  print("   7 => add user")

  cmd = int(input())
  return cmd


###################################################################
#
# stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  print("S3 bucket name:", bucketname)

  assets = bucket.objects.all()
  print("S3 assets:", len(list(assets)))

  #
  # MySQL info:
  #
  print("RDS MySQL endpoint:", endpoint)

  sql = """
    select
    (select count(*) from users) as s1,
    (select count(*) from assets) as s2;
  """

  row = datatier.retrieve_one_row(dbConn, sql)
  if row is None:
    print("Database operation failed...")
  elif row == ():
    print("Unexpected query failure...")
  else:
    print("# of users:", row[0])
    print("# of assets:", row[1])
    
###################################################################
#
# users
#
def users(dbConn):
  """
  Retrieves and outputs the users in the users
  table. The users are output in descending order
  by user id.

  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  sql = """
    select * from users order by userid desc
  """

  rows = datatier.retrieve_all_rows(dbConn, sql)
  if rows is None:
    print("Database operation failed...")
  elif rows == ():
    print("Unexpected query failure...")
  else:
    for i in range(len(rows)):
      print("User id:",rows[i][0])
      print("  Email:", rows[i][1])
      print("  Name:", rows[i][2],",",rows[i][3])
      print("  Folder:", rows[i][4])

###################################################################
#
# assets
#
def assets(dbConn):
  """
  Retrieves and outputs the assets in the assets
  table. The assets are output in descending order
  by asset id. 

  
  Parameters
  ----------

  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  sql = """
    select * from assets order by assetid desc
  """

  rows = datatier.retrieve_all_rows(dbConn, sql)
  if rows is None:
    print("Database operation failed...")
  elif rows == ():
    print("Unexpected query failure...")
  else:
    for i in range(len(rows)):
      print("Asset id:",rows[i][0])
      print("  User id:", rows[i][1])
      print("  Original name:", rows[i][2])
      print("  Key name:", rows[i][3])


###################################################################
#
# download & display
#
def download(dbConn, bucket, display):
  """
  Inputs an asset id, and then looks up that asset in the database, downloads the file, and renames it based on     the original filename.
  
  Parameters
  ----------

  dbConn: open connection to MySQL server
  bucket: S3 bucket object
  display: whether to diplay image or not. 
  
  Returns
  -------
  nothing
  """
  print("Enter asset id>")
  s = input()
  
  sql = """
    select bucketkey, assetname from assets where assetid = %s
  """
  
  row = datatier.retrieve_one_row(dbConn, sql, [s])

  if row is None:
    print("Database operation failed...")
  elif row == ():
    print("No such asset...")
  else:
    filename = awsutil.download_file(bucket, row[0])
    os.rename(filename, row[1])
    print("Downloaded from S3 and saved as '",row[1],"'")
    if display == True:
      image = img.imread(row[1])
      plt.imshow(image)
      plt.show()

###################################################################
#
# upload
#
def upload(dbConn, bucket):
  """
  Inputs the name of a local file, and a user id, and then uploads that file to the user’s          folder in S3. The file is given a unique name in S3 (use UUID module), and a row                  containing the asset’s information---user id, original filename, and full bucket key---is         inserted into the assets table.

  
  Parameters
  ----------

  dbConn: open connection to MySQL server
  bucket: S3 bucket object
  
  Returns
  -------
  nothing
  """
  print("Enter local file name>")
  s = input()
  if os.path.exists(s) == False:
    print("Local file '",s,"' does not exist...")
    os.abort()
  print("Enter user id>")
  id = input()
  sql1 = """
    select * from users where userid = %s
  """
  row1 = datatier.retrieve_one_row(dbConn, sql1, [id])
  if row1 is None or row1 == ():
    print("No such user...")
    os.abort()

  filename = row1[4]+'/'+str(uuid.uuid4())+'.jpg'
  key = awsutil.upload_file(s, bucket, filename)
  print("Uploaded and stored in S3 as '", key, "'")
  
  sql2 = """
  INSERT INTO 
  assets(userid, assetname, bucketkey)
  values(%s, %s, %s)
  """
  row2 = datatier.perform_action(dbConn, sql2, [id, s, key])
  
  sql3 = """
    select LAST_INSERT_ID()
  """
  row3 = datatier.retrieve_one_row(dbConn, sql3)
  print("Recorded in RDS under asset id", row3[0])


###################################################################
#
# add user
#
def adduser(dbConn):
  """
  Inputs data about a new user and inserts a new row into the users table.
  Input the new user’s email, last name, and first name.
  
  Parameters
  ----------

  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  print("Enter user's email>")
  email = input()
  print("Enter user's last (family) name>")
  ln = input()
  print("Enter user's first (given) name>")
  fn = input()

  folder = uuid.uuid4()
  
  sql1 = """
    INSERT INTO 
    users(email, lastname, firstname, bucketfolder)
    values(%s, %s, %s, %s)
  """
  row1 = datatier.perform_action(dbConn, sql1, [email, ln, fn, folder])
  
  sql2 = """
    select LAST_INSERT_ID();
  """
  row2 = datatier.retrieve_one_row(dbConn, sql2)
  print("Recorded in RDS under asset id", row2[0])

#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-config.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  elif cmd == 2:
    users(dbConn)
  elif cmd == 3:
    assets(dbConn)
  elif cmd == 4:
    download(dbConn, bucket, False)
  elif cmd == 5:
    download(dbConn, bucket, True)
  elif cmd == 6:
    upload(dbConn, bucket)
  elif cmd == 7:
    adduser(dbConn)
  else:
    print("** Unknown command, try again...")
    
  
  cmd = prompt()

#
# done
#
print()
print('** done **')
