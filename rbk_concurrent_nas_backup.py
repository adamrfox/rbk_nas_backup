#!/usr/bin/python
from __future__ import print_function
import sys
import time
import getopt
import getpass
from datetime import datetime
import operator
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

def get_job_queue(hs_data, sla_data, fs_data, infile, default_host, def_sla, default_fileset):
    new_job_queue = []
    def_fst_id = ""
    print("Generating Job Queue")
    with open(infile) as fp:
        for line in fp:
            line = line.rstrip()
            if not line or line.startswith('#'):
                continue
            lf = line.split(',')
            if default_host:
                host = default_host
                share = lf[0]
            else:
                host = lf[0]
                share = lf[1]
            hs_id = get_hs_id(hs_data, host, share)
            if hs_id == "":
                sys.stderr.write("Can't find " + lf[0] + ":" + lf[1] + ". Skipping\n")
                continue
            if def_sla:
                sla_id = get_sla_id(sla_data, def_sla)
                sla = def_sla
            else:
                sla_id = get_sla_id(sla_data, lf[-1])
                sla = lf[-1]
                if sla_id == "":
                    sys.stderr.write("Can't find SLA: " + sla + ". Skipping\n")
                    continue
            fs_id_list = get_fs_id(hs_id, fs_data)
            if len(fs_id_list) == 0:
                if not default_fileset:
                    sys.stderr.write("Can't find fileset for " + host + ":" + share + ". Skipping\n")
                    continue
                if not def_fst_id:
                    for f in fs_data['data']:
                        if f['name'] == default_fileset:
                            def_fst_id = f['id']
                            fs_id = f['id']
                else:
                    fs_id = f['id']
            elif len(fs_id_list) > 1:
                sys.stderr.write("Found multiple filsets for " + host + ":" + share + ". Skipping\n")
                continue
            else:
                fs_id = fs_id_list[0]
            new_job_queue.append({'host': host, 'share': share, 'hs_id': hs_id, 'sla_id': sla_id, 'fs_id': fs_id})
    fp.close()
    if RESTART:
        epoch = datetime.strptime("1970-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
        job_delete_queue = []
        running_jobs = []
        for j in new_job_queue:
            j_run = rubrik.get('v1', '/event/latest?event_status=Running&event_type=Backup&object_ids=' + str(j['hs_id']) + ',' + str(j['fs_id']))
            try:
                rj_id = j_run['data'][0]['latestEvent']['jobInstanceId']
                url = "/fileset/request/" + str(rj_id)
                running_jobs.append({'host': j['host'], 'share': j['share'], 'status': url})
                job_delete_queue.append(j)
            except:
                j_inst = rubrik.get('v1', '/event/latest?event_status=Success&event_type=Backup&object_ids=' + str(j['hs_id']) + ',' + str(j['fs_id']))
                try:
                    time = j_inst['data'][0]['latestEvent']['time']
                    time_dt = datetime.strptime(time[:-5], "%Y-%m-%dT%H:%M:%S")
                    j['time'] = int((time_dt - epoch).total_seconds())
                except:
                    j['time'] = 0
        for dj in job_delete_queue:
            new_job_queue.remove(dj)
        if SORT_ON_TIME:
            new_job_queue.sort(key=operator.itemgetter('time'))
        dprint("\nJOB_QUEUE: " + str(new_job_queue))
        print("QUEUE LENGTH:" + str(len(new_job_queue)))
        print("ALREADY RUNNING JOBS: " + str(len(running_jobs)))
        return(new_job_queue, running_jobs)
    return(new_job_queue, [])

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
    default_host = ""
    default_fileset = ""
    timeout = 60
    NAS_DA = False
    running_status_list = ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']
    RESTART = True
    SORT_ON_TIME = False

    optlist, args = getopt.getopt(sys.argv[1:], 'hDc:t:ms:Fn:f:dS', ['--help', '--DEBUG', '--creds=', '--token=', '--max_jobs=',
                                                               '--sla=', '--flush', '--nas_host=', '--fileset=', '--nas_da',
                                                                '--sort_on_time'])
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
        if opt in ('-F', '--flush'):
            RESTART = False
        if opt in ('-n', '--nas_host'):
            default_host = a
        if opt in ('-f', '--fileset'):
            default_fileset = a
        if opt in ('-d', '--nas_da'):
            NAS_DA = True
        if opt in ('-S', '--sort_on_time'):
            SORT_ON_TIME = True

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
    (job_queue, jobs_running) = get_job_queue(hs_data, sla_data, fs_data, infile, default_host, default_sla, default_fileset)
    while(job_queue or jobs_running):
        if len(jobs_running) < max_jobs:
            try:
                new_job = job_queue.pop(0)
                print ("Starting Backup of " + new_job['host'] + ":" + new_job['share'])
                bu_config = {'slaId': new_job['sla_id'], 'isPassthrough': NAS_DA}
                bu_status = rubrik.post('v1', '/fileset/' + str(new_job['fs_id']) + "/snapshot", bu_config, timeout=timeout)
                dprint("JOB: " + str(bu_status))
                bu_status_url = str(bu_status['links'][0]['href']).split('/')
                bu_status_path = "/" + "/".join(bu_status_url[5:])
                jobs_running.append({'host': new_job['host'], 'share': new_job['share'], 'status': bu_status_path})
                continue
            except:
                pass
        remove_queue = []
        for j in jobs_running:
            j_status = rubrik.get('v1', j['status'], timeout=timeout)
            job_status = str(j_status['status'])
            print("JOB STATUS: " + j['host'] + ":" + j['share'] + " : " + job_status)
            if job_status in running_status_list:
                continue
            else:
                remove_queue.append(j)
        for rj in remove_queue:
            jobs_running.remove(rj)
        print("Queued Jobs: " + str(len(job_queue)))
        print('')
        time.sleep(10)


