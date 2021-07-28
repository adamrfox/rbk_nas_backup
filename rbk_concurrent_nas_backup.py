#!/usr/bin/python
from __future__ import print_function
import sys
import time
import getopt
import getpass
import urllib3
urllib3.disable_warnings()
import rubrik_cdm


def usage():
    print("Usage goes here!")
    exit(0)

def dprint(message):
    if DEBUG:
        print(message)

def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return(val)

def get_hs_id(hs_data, host, share):
    for h in hs_data['data']:
        if h['hostname'] == host and h['exportPoint'] == share:
            return(h['id'])
    return("")

def get_sla_id(sla_data, name):
    for s in sla_data['data']:
        if s['name'] == name:
            return(s['id'])
    return("")

def get_fs_id(hs_id, fs_data):
    fs_id_list = []
    for f in fs_data['data']:
        try:
            if f['shareId'] == hs_id:
                fs_id_list.append(f['id'])
        except:
            continue
    return(fs_id_list)

def get_job_queue(hs_data, sla_data, fs_data, infile, def_sla):
    new_job_queue = []
    with open(infile) as fp:
        for line in fp:
            if not line or line.startswith('#'):
                continue
            line = line.rstrip()
            lf = line.split(',')
            hs_id = get_hs_id(hs_data, lf[0], lf[1])
            if hs_id == "":
                sys.stderr.write("Can't find " + lf[0] + ":" + lf[1] + ". Skipping\n")
                continue
            if len(lf) == 2:
                sla_id = get_sla_id(sla_data, def_sla)
            elif len(lf) == 3:
                sla_id = get_sla_id(sla_data, lf[2])
                if sla_id == "":
                    sys.stderr.write("Can't find SLA: " + lf[4] + ". Skipping\n")
                    continue
            fs_id_list = get_fs_id(hs_id, fs_data)
            if len(fs_id_list) == 0:
                sys.stderr.write("Can't find fileset for " + lf[0] + ":" + lf[1] + ". Skipping\n")
                continue
            elif len(fs_id_list) > 1:
                sys.stderr.write("Found multiple filsetsets for " + lf[0] + ":" + lf[1] + ". Skipping\n")
                continue
            new_job_queue.append({'host': lf[0], 'share': lf[1], 'hs_id': hs_id, 'sla_id': sla_id, 'fs_id': fs_id_list[0]})
    fp.close()
    return(new_job_queue)

if __name__ == "__main__":
    user = ""
    password = ""
    token = ""
    DEBUG = False
    job_queue = []
    job_index = 0
    max_jobs = 2
    jobs_running = []
    default_sla = ""
    timeout = 60
    NAS_DA = False
    running_status_list = ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']

    optlist, args = getopt.getopt(sys.argv[1:], 'hDc:t:ms:', ['--help', '--DEBUG', '--creds=', '--token=', '--max_jobs=', '--sla='])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
        if opt in ('-c', '--creds'):
            (user, password) = a.split(':')
        if opt in ('-t', '--token'):
            token = a
        if opt in ('-m', '--max_jobs'):
            max_jobs = int(a)
        if opt in ('-s', '--sla'):
            default_sla = a

    try:
        (infile, rubrik_host) = args
    except:
        usage()

    if token:
        rubrik = rubrik_cdm.Connect(rubrik_host, api_token=token)
    else:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")
        rubrik = rubrik_cdm.Connect(rubrik_host, user, password)
    hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
    if hs_data['total'] == 0:
        sys.stderr.write("No NAS Shares found.\n")
        exit(1)
    sla_data = rubrik.get('v2', '/sla_domain', timeout=timeout)
    if sla_data['total'] == 0:
        sys.stderr.write("No SLAs found?!\n")
        exit(2)
    fs_data = rubrik.get('v1', '/fileset', timeout=timeout)
    if fs_data['total'] == 0:
        sys.stderr.write("No Filesets found\n")
        exit(3)
    job_queue = get_job_queue(hs_data, sla_data, fs_data, infile, default_sla)
    while(job_queue or jobs_running):
        if len(jobs_running) < max_jobs:
            try:
                new_job = job_queue.pop(0)
                print ("Starting Backup of " + new_job['host'] + ":" + new_job['share'])
                bu_config = {'slaId': new_job['sla_id'], 'isPassthrough': NAS_DA}
                bu_status = rubrik.post('v1', '/fileset/' + str(new_job['fs_id']) + "/snapshot", bu_config, timeout=timeout)
                bu_status_url = str(bu_status['links'][0]['href']).split('/')
                bu_status_path = "/" + "/".join(bu_status_url[5:])
                jobs_running.append({'host': new_job['host'], 'share': new_job['share'], 'status': bu_status_path})
            except:
                pass
        for j in jobs_running:
            j_status = rubrik.get('v1', j['status'], timeout=timeout)
            job_status = str(j_status['status'])
            print("JOB STATUS: " + j['host'] + ":" + j['share'] + " : " + job_status)
            if job_status in running_status_list:
                continue
            else:
                jobs_running.remove(j)
        print('\n')
        time.sleep(10)


