#!/usr/bin/python

from __future__ import print_function
import rubrik_cdm
import getopt
import sys
import getpass
import urllib3
import time
import subprocess
from codecs import decode,encode
if int(sys.version[0]) >= 3:
  import urllib.parse
else:
  import urllib
urllib3.disable_warnings()

def get_sla_data (rubrik, vers, name):
  if int(sys.version[0]) < 3:
    name = urllib.quote_plus(name)
  else:
    name = urllib.parse.quote(name)
  sla_data = rubrik.get('v1', str("/sla_domain?primary_cluster=local&name=" + name))
  if sla_data['total'] == 0 and vers > 4:
    sla_data = rubrik.get('v2', str("/sla_domain?primary_cluster=local&name=" + name))
  return (sla_data)

def get_creds_from_file(file):
  with open(file) as fp:
    data = fp.read()
    fp.close()
    if int(sys.version[0]) > 2:
      data = str.encode(data)
      data = decode(data, 'uu')
      data = decode(str(data), 'rot13')
      data = data.replace("o'", "")
      lines = data.split('\\a')
    else:
      data = decode(bytes(data), 'uu')
      data = decode(data, 'rot13')
      lines = data.splitlines()
    for x in lines:
      if x == "":
        continue
      xs = x.split(':')
      if xs[0] == "rubrik":
        ntap_user = xs[1]
        ntap_password = xs[2]
    return (ntap_user, ntap_password)


def usage ():
  sys.stderr.write ("Usage: rbk_nas_backup.py [-b host:share] [-f fileset] [-c user:password] [-P pre_script] [-p post_script] [-h] rubrik\n")
  sys.stderr.write("-b | --backup= : specify a host and a share/export\n")
  sys.stderr.write("-f | --fileset= : specify a fileset\n")
  sys.stderr.write("-c | --creds= : specify a Rubrik user:passwd.  Note: This is not secure\n")
  sys.stderr.write("-P | --pre= : Specify a script to run before the backup\n")
  sys.stderr.write("-p | --post= : Specify a script to run after the backup\n")
  sys.stderr.write("-h | --help : Prints this message\n")
  sys.stderr.write("rubrik : Name or IP of Rubrik\n")
  exit (0)

sla = ""
backup = ""
pre_script = ""
post_script = ""
share_id = ""
fileset = ""
user = ""
password = ""
bu_status_url = ()
build_fileset = False
optlist, args = getopt.getopt(sys.argv[1:], 'P:p:s:f:b:c:h', ['pre=', 'post=', 'sla=','fileset=', 'backup=', 'creds=', 'help'])
for opt, a in optlist:
  if opt in ('-P', "--pre"):
    pre_script = a
  if opt in ('-p', "--post"):
    post_script = a
  if opt in ('-s', "--sla"):
    sla = a
    if int(sys.version[0]) < 3:
      sla_url = urllib.quote_plus(sla)
    else:
      sla_url = urllib.parse.quote(sla)
  if opt in ('-b', "--backup"):
    backup = a
  if opt in ('-f', "--fileset"):
    fileset = a
  if opt in ('-c', "--creds"):
    if ':' in a:
      (user, password) = a.split(':')
    else:
      (user, password) = get_creds_from_file(a)
  if opt in ('-h', "--help"):
    usage()
rubrik_cluster = args[0]
if backup == "":
  if int(sys.version[0]) < 3:
    backup = raw_input ("Backup (host:share): ")
  else:
    backup = input("Backup (host:share): ")
if fileset == "":
  if int(sys.version[0]) < 3:
    fileset = raw_input ("Fileset Name: ")
  else:
    fileset = input("Fileset Name: ")
if user == "":
  if int(sys.version[0]) < 3:
    user = raw_input ("User: ")
  else:
    user = input("User: ")
if password == "":
  password = getpass.getpass ("Password: ")
print("USER: " + user + " // PW: " + password)
rubrik = rubrik_cdm.Connect (rubrik_cluster, user, password)
version = rubrik.cluster_version().split('.')
version_maj = int(version[0])
(host, share) = backup.split(':')
if share.startswith("/"):
  share_type = "NFS"
else:
  share_type = "SMB"
hs_data = rubrik.get('internal', '/host/share')
for x in hs_data['data']:
  if x['hostname'] == host and x['exportPoint'] == share:
    share_id = x['id']
    break
if share_id == "":
  sys.stderr.write ("Share not found\n")
  exit (2)
fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset))
try:
  fs_id = fs_data['data'][0]['id']
except IndexError:
  template_id = ""
  fst_data = rubrik.get('v1', str("/fileset_template?share_type=" + share_type + "&name=" + fileset))  
  for t in fst_data['data']:
    if t['name'] == fileset:
      template_id = t['id']
      break
  if template_id == "":
    sys.stderr.write("Fileset not found\n")
    exit (2)
  if sla == "":
    sla_name = raw_input ("SLA Domain: ")
    sla_data = get_sla_data (rubrik, version_maj, sla_name)
    try:
      sla_id = sla_data['data'][0]['id']
    except IndexError:
      sys.stderr.write("Can't find SLA: " + sla_name + "\n")
      exit (2)
  fs_config = {}
  fs_config = {"shareId" : str(share_id), "templateId": str(template_id), "slaID" : str(sla_id)}
  fs_create = rubrik.post('v1', '/fileset', fs_config)
  fs_id = fs_create['id']
  build_fileset = True
if not build_fileset:
  if sla == "":
    sla_id = fs_data['data'][0]['configuredSlaDomainId']
    sla_name = fs_data['data'][0]['configuredSlaDomainName']
  else:
    found = False
    sla_data = get_sla_data (rubrik, version_maj, sla)
    for s in sla_data['data']:
      if s['name'] == sla:
        found = True
        sla_id = s['id']
        sla_name = s['name']
    if not found and version_maj > 4:
      sla_data = get_sla_data(rubrik, version_maj, sla)
      for s in sla_data['data']:
        if s['name'] == sla:
          found = True
          sla_id = s['id']
          sla_name = s['name']
    if not found:
      sys.stderr.write ("Can't find SLA: " + sla + "\n")
      exit (2)
if pre_script:
  print("Executing " + pre_script)
  subprocess.call(pre_script, shell=True)
print("Starting Backup...")
bu_config = {}
bu_config = {"slaId" : str(sla_id)}
bu_status = rubrik.post ('v1', '/fileset/' + str(fs_id) + "/snapshot", bu_config)
bu_status_url = str(bu_status['links'][0]['href']).split('/')
bu_status_path = "/" + "/".join(bu_status_url[5:])
bu_done = False
while not bu_done:
  bu_job_status = rubrik.get ('v1', bu_status_path)
  bu_status = str(bu_job_status['status'])
  if bu_status == "RUNNING" or bu_status == "QUEUED" or bu_status == "ACQUIRING" or bu_status == "FINISHING":
    time.sleep(5)
  elif bu_status == "SUCCEEDED":
    bu_done = True
  elif bu_status == "TO_CANCEL" or 'endTime' in bu_job_status:
    sys.stderr.write ("Job ended with status: " + str(bu_job_status['status']) + "\n")
    bu_done = True
  else:
    print("Status = " + bu_status)
    time.sleep(5)
if post_script and bu_status == "SUCCEEDED":
  print("Executing " + post_script)
  subprocess.call (post_script, shell=True)
