import os
import sys
import yaml

def get_config(args):
    executable = sys.argv[0]
    if '/' in executable:
        executable = executable.split('/')[-1]
    try:
        if args.config.startswith('/'):
            config_file = args.config
        else:
            config_file = os.path.dirname(__file__) + "/../" + args.config
        config = yaml.safe_load(open(config_file))
    except:
        print()
        print("Config file not found or not specified. A config file is mandatory.")
        print()
        print("     Usage: " + sys.argv[0] + " --config example.yaml")
        print()
        sys.exit(0)

    if args.list == True:
        print()
        print("Jobs:")
        print()
        jobsfound = False
        for k in config:
            if config[k]["used_with"] == executable:
                if k != 'cds':
                    jobsfound = True
                    description = ""
                    if 'description' in config[k]:
                        description = " - " + config[k]["description"]
                    print("     " + k + description)
        if jobsfound == False or len(config) == 0:
            print("     No jobs found.")
        print()
        print()
        sys.exit(0)
    elif args.job == "":
        print()
        print("No job specified.")
        print()
        print("     Usage: " + sys.argv[0] + " --config example.yaml --job JOB")
        print()
        print("Note: The '--list' parameter can be used to list jobs associated with the script and config file.")
        print()
        sys.exit(0)
        
    return config