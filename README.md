# rbk_nas_backup
A script to run a NAS backup with pre and post scripts

The idea here is to have Rubrik run a NAS backup and have the option for a pre-script and/or a post-script.  Here is the basic
syntax:
```
Usage: rbk_nas_backup.py [-b host:share] [-f fileset] [-c user:password] [-P pre_script] [-p post_script] [-h] rubrik
-b | --backup= : specify a host and a share/export
-f | --fileset= : specify a fileset
-c | --creds= : specify a Rubrik user:passwd.  Note: This is not secure
-P | --pre= : Specify a script to run before the backup
-p | --post= : Specify a script to run after the backup
-h | --help : Prints this message
rubrik : Name or IP of Rubrik
```

Most of the command-line options are optional and, if needed, the script will prompt the user for the needed information.

Security warnings:

Using -c is not seure as you are exposing the account credentials on the CLI.  If you don't want these exposed, don't use
this option and allow the script to prompt the user for the creds.  When prompted for the password, it is not echoed to the screen.

This code, by design, executes arbitrary code.  Be careful what code you allow it to execute.

Other notes:

If the fileset chosen is not currently associated with the share, the script will make that association.  In this case, an SLA domain will be required.  If it is not specified on the command line, the script will prompt the user for one.  The script does not create new SLA domains or new fileset templates. 

If the fileset chosen is already associated with share and no SLA is given on the CLI, the script will assume you want to use the SLA currently assigned to the fileset.  If no SLA is assigned to that fileset, then the backup will have "No SLA" and will be an Unmanaged Object and must be managed by the user on the Rubrik cluster.

The script does not currently show backup progress.  Job progress can be tracked on the Rubrik cluster.  Tracking the backup progress in the script could be done.  Raise an issue if this is important.

This script uses the Rubrik Python SDK.  That will need to be installed in order for the script to run.  The SDK is available here:  https://github.com/rubrikinc/rubrik-sdk-for-python.

The post script only runs if the backup job succeeds.  Raise an issue if an opion to over-ride this makes sense.
