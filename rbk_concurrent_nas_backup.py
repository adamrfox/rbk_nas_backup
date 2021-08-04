#!/usr/bin/python
from __future__ import print_function
from __future__ import division
import sys
import time
import getopt
import getpass
from datetime import datetime
import operator
import urllib3
urllib3.disable_warnings()
import rubrik_cdm
from os import path



def usage():
    sys.stderr.write("Usage: rbk_concurrent_nas_backup.py [-hDdSF] [-c creds] [-t token] [-m jobs] [-s sla] [-n nas_host] [-f fileset] [-r minutes] file rubrik\n")
    sys.stderr.write("-h | --help : Prints this message\n")
    sys.stderr.write("-D | --DEBUG : Debug mode.  Verbose output for debugging\n")
    sys.stderr.write("-d | --nas_da : Set NAS DA when assigning a fileset to a share [default: False]\n")
    sys.stderr.write("-S | --sort_on_time : Sort jobs to be run by last backups time [default: False]\n")
    sys.stderr.write("-F | --flush : Don't try to restart clean (check for running and completed jobs)\n")
    sys.stderr.write("-c | --creds : Credentials for Rubrik [user:password]\n")
    sys.stderr.write("-t | --token : API Token for Rubrik\n")
    sys.stderr.write("-m | --max_jobs : Maximum number of concurrent backup jobs [default: 2]\n")
    sys.stderr.write("-s | --sla : Set a default SLA instead of specifying in the file\n")
    sys.stderr.write("-n | --nas_host : Set a default NAS host instead of specifying it in the file\n")
    sys.stderr.write("-f | --fileset : Set a default fileset instead of specifying it in the file\n")
    sys.stderr.write("-r | --report_time : Set a delay in reports to the screen in minutes [def: 0]\n")
    sys.stderr.write("file : Input file for jobs\n")
    sys.stderr.write("rubrik : Hostname or IP of the Rubrik cluster\n")
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

def rewrite_log_file(log_file, done_jobs):
    lc = open(log_file, "w")
    for dj in done_jobs:
        lc.write(dj['host'] + "," + dj['share'] + ",SUCCEEDED," + dj['date'] + "\n")
    lc.close()
    return

def compare_job_queues(done, new):
    for n in new:
        found = False
        for dj in done:
            if dj['host'] == n['host'] and dj['share'] == n['share']:
                found = True
                break
        if not found:
            return(False)
    return(True)

def check_job_status_log(log_file, new_job_queue):
    done_jobs = []
    to_do = False
    if not path.isfile(log_file):
        return([])
    with open(log_file) as fp:
        for line in fp:
            line = line.rstrip()
            if not line:
                continue
            (host, share, status, date)  = line.split(',')
            if status == "SUCCEEDED":
                done_jobs.append({'host': host, 'share': share, 'date': date})
            else:
                to_do = True
    fp.close()
    if compare_job_queues(done_jobs, new_job_queue):
        return([])
    if to_do:
        rewrite_log_file(log_file, done_jobs)
    return(done_jobs)

def get_fst_id(rubrik, def_fst):
    fst_data = rubrik.get('v1', '/fileset_template?name=' + def_fst, timeout=timeout)
    try:
        return(fst_data['data'][0]['id'])
    except:
        return("")

def add_template_to_share(hs_id, fst_id):
    payload = [{'shareId': hs_id, 'templateId': fst_id, 'isPassthrough': NAS_DA, 'enableSymlinkResolution': False, 'enableHardlinkSupport': False}]
    dprint("PAYLOAD " + str(payload))
    new_fs = rubrik.post('internal', '/fileset/bulk', payload, timeout=timeout)
    return(new_fs['data'][0]['id'])

def get_job_queue(hs_data, sla_data, fs_data, infile, default_host, def_sla, def_fst_id):
    new_job_queue = []
    completed_job_queue = []
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
                sys.stderr.write("Can't find " + host + ":" + share + ". Skipping\n")
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
                fs_id = add_template_to_share(hs_id, def_fst_id)
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
                if SORT_ON_TIME:
                    j_inst = rubrik.get('v1', '/event/latest?event_status=Success&event_type=Backup&object_ids=' + str(j['hs_id']) + ',' + str(j['fs_id']))
                    try:
                        time = j_inst['data'][0]['latestEvent']['time']
                        time_dt = datetime.strptime(time[:-5], "%Y-%m-%dT%H:%M:%S")
                        j['time'] = int((time_dt - epoch).total_seconds())
                    except:
                        j['time'] = 0
        for dj in job_delete_queue:
            new_job_queue.remove(dj)
        completed_job_queue = check_job_status_log(log_file, new_job_queue)
        print("NEW JOBS: " + str(new_job_queue))
        print("COMPLETED: " + str(completed_job_queue))
        if completed_job_queue:
            print("Purging Completed Jobs")
            for cj in completed_job_queue:
                for j in new_job_queue:
                    if j['host'] == cj['host'] and j['share'] == cj['share']:
                        new_job_queue.remove(j)
                        break
        else:
            fp = open(log_file, "w")
            fp.close()
        if SORT_ON_TIME:
            new_job_queue.sort(key=operator.itemgetter('time'))
        dprint("\nJOB_QUEUE: " + str(new_job_queue))
        print("QUEUE LENGTH:" + str(len(new_job_queue)))
        print("ALREADY RUNNING JOBS: " + str(len(running_jobs)))
        return(new_job_queue, running_jobs, completed_job_queue)
    return(new_job_queue, [], [])

def log_job(lfile, job, job_status):
    date_s = datetime.now().strftime("%Y-%m-%dT%H:%M")
    fp = open(lfile, "a")
    fp.write(job['host'] + "," + job['share'] + "," + job_status + "," + date_s + "\n")
    fp.close()
    return

def print_job_report(job, status, j_cnt):
    date_s = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if j_cnt == 1:
        print(date_s)
    print ("\tJOB STATUS: " + job['host'] + ":" + job['share'] + " : " + status)
    return

if __name__ == "__main__":
    user = ""
    password = ""
    token = ""
    DEBUG = False
    job_queue = []
    job_success = []
    job_fail = []
    max_jobs = 2
    jobs_running = []
    default_sla = ""
    default_host = ""
    default_fileset = ""
    timeout = 60
    NAS_DA = False
    running_status_list = ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING', 'TO_CANCEL']
    RESTART = True
    SORT_ON_TIME = False
    log_file = "job_log.csv"
    REPORT_DELAY = 0
    pct_done = 0.0

    optlist, args = getopt.getopt(sys.argv[1:], 'hDc:t:m:s:Fn:f:dSr:', ['--help', '--DEBUG', '--creds=', '--token=', '--max_jobs=',
                                                               '--sla=', '--flush', '--nas_host=', '--fileset=', '--nas_da',
                                                                '--sort_on_time', '--report_time='])
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
        if opt in ('-r', '--report_time'):
            REPORT_DELAY = int(a)

    try:
        (infile, rubrik_host) = args
    except:
        usage()
    sleep_cycles = REPORT_DELAY*60/10
    if token:
        rubrik = rubrik_cdm.Connect(rubrik_host, api_token=token)
    else:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")
        rubrik = rubrik_cdm.Connect(rubrik_host, user, password)
    if default_fileset:
        def_fst_id = get_fst_id(rubrik, default_fileset)
        if not def_fst_id:
            sys.stderr.write("Can't find default fileset template: " + default_fileset + "\n")
            exit(2)
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
    (job_queue, jobs_running, job_success) = get_job_queue(hs_data, sla_data, fs_data, infile, default_host, default_sla, def_fst_id)
    original_job_queue = len(job_queue) + len(jobs_running) + len(job_success)
    report_cycle = 0
    while(job_queue or jobs_running):
        report_cycle += 1
        if report_cycle > sleep_cycles:
            report_cycle = 1
        if (len(jobs_running) < max_jobs) and job_queue:
            try:
                report_cycle = 1
                new_job = job_queue.pop(0)
                print ("Starting Backup of " + new_job['host'] + ":" + new_job['share'])
                bu_config = {'slaId': new_job['sla_id'], 'isPassthrough': NAS_DA}
                bu_status = rubrik.post('v1', '/fileset/' + str(new_job['fs_id']) + "/snapshot", bu_config, timeout=timeout)
                dprint("JOB: " + str(bu_status))
                bu_status_url = str(bu_status['links'][0]['href']).split('/')
                bu_status_path = "/" + "/".join(bu_status_url[5:])
                jobs_running.append({'host': new_job['host'], 'share': new_job['share'], 'status': bu_status_path})
            except:
                pass
        remove_queue = []
        j_cnt = 0
        for j in jobs_running:
            j_cnt += 1
            j_status = rubrik.get('v1', j['status'], timeout=timeout)
            job_status = str(j_status['status'])
            if report_cycle == 1:
                print_job_report(j, job_status, j_cnt)
            if job_status in running_status_list:
                continue
            else:
                log_job(log_file, j, job_status)
                j['status'] = job_status
                if job_status == "SUCCEEDED":
                    job_success.append(j)
                else:
                    job_fail.append(j)
                remove_queue.append(j)
        for rj in remove_queue:
            jobs_running.remove(rj)
        if report_cycle == 1:
            print("\tQueued Jobs: " + str(len(job_queue)))
            job_num = len(job_success) + len(job_fail)
            pct_done = (job_num / original_job_queue) * 100
            print("\tQueue Progress: " + str(round(pct_done)) + "%")
            print('')
        time.sleep(10)
    if job_fail:
        print(str(len(job_fail)) + " Failed Job", end='')
        if len(job_fail) == 1:
            print(':')
        else:
            print('s:')
        for fj in job_fail:
            print("\t" + fj['host'] + ":" + fj['share'] + " : " + fj['status'])
    print("\nDone!")