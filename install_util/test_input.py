import getopt
import re
import configparser
import os


class TestInput(object):
    def __init__(self):
        self.servers = list()
        self.clusters = dict()
        self.test_params = dict()
        self.elastic = list()
        self.cbbackupmgr = dict()
        self.membase_settings = None
        self.bkrs_client = None

    def param(self, name, *args):
        """Returns the paramater or a default value

        The first parameter is the name of property, the second
        parameter is the default value. If not default value is given,
        an exception will be raised.
        """
        if name in self.test_params:
            return TestInput._parse_param(self.test_params[name])
        elif len(args) == 1:
            return args[0]
        else:
            raise Exception("Parameter `{}` must be set "
                            "in the test configuration".format(name))

    def cbbackupmgr_param(self, name, *args):
        """Returns the config value from the ini whose key matches 'name' and is stored under the 'cbbackupmgr'
        section heading.

        Args:
            name (str): The key under which an expected value is stored.
            args (str): Expects a single parameter which will be used as the default if the requested key is not found.

        Returns:
            string: The value parsed from the ini file/default value if the given key is not found.

        Raises:
            Exception: If the given key does not exist in the ini and no default value is provided.
        """
        if name in self.cbbackupmgr:
            return TestInput._parse_param(self.cbbackupmgr[name])
        if len(args) == 1:
            return args[0]
        if self.cbbackupmgr["name"] != "local_bkrs":
            raise Exception(f"Parameter '{name}' must be set in the test configuration")

    @staticmethod
    def _parse_param(value):
        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        if value.lower() == "false":
            return False

        if value.lower() == "true":
            return True

        return value


class TestInputServer(object):
    def __init__(self):
        self.ip = ''
        self.internal_ip = ''
        self.hostname = ''
        self.ssh_username = ''
        self.ssh_password = ''
        self.ssh_key = ''
        self.rest_username = ''
        self.rest_password = ''
        self.services = ''
        self.port = ''
        self.cli_path = ''
        self.data_path = ''
        self.index_path = ''
        self.cbas_path = ''
        self.n1ql_port = ''
        self.index_port = ''
        self.fts_port = ''
        self.eventing_port = ''
        self.es_username = ''
        self.es_password = ''
        self.upgraded = False
        self.collections_map = {}
        self.cbbackupmgr = {}
        self.hosted_on_cloud = False
        self.dummy = False
        # To store the default n/w interface name (Eg: eth0 / enX0 / ...)
        self.default_interface = None

    def __str__(self):
        #ip_str = "ip:{0}".format(self.ip)
        ip_str = "ip:{0} port:{1}".format(self.ip, self.port)
        ssh_username_str = "ssh_username:{0}".format(self.ssh_username)
        return "{0} {1}".format(ip_str, ssh_username_str)

    def __repr__(self):
        #ip_str = "ip:{0}".format(self.ip)
        ip_str = "ip:{0} port:{1}".format(self.ip, self.port)
        ssh_username_str = "ssh_username:{0}".format(self.ssh_username)
        return "{0} {1}".format(ip_str, ssh_username_str)

    @property
    def cluster_ip(self):
        return self.internal_ip or self.ip


class TestInputMembaseSetting(object):
    def __init__(self):
        self.rest_username = ''
        self.rest_password = ''


class TestInputBuild(object):
    def __init__(self):
        self.version = ''
        self.url = ''


# we parse this and then pass it on to all the test case
class TestInputParser(object):
    @staticmethod
    def get_test_input(arguments):
        params = dict()
        if arguments.params:
            argument_split = [a.strip() for a in re.split("[,]?([^,=]+)=", arguments.params)[1:]]
            pairs = dict(list(zip(argument_split[::2], argument_split[1::2])))
            for pair in list(pairs.items()):
                if pair[0] == "vbuckets":
                    # takes in a string of the form "1-100,140,150-160"
                    # converts to an array with all those values inclusive
                    vbuckets = set()
                    for v in pair[1].split(","):
                        r = v.split("-")
                        vbuckets.update(list(range(int(r[0]), int(r[-1]) + 1)))
                    params[pair[0]] = sorted(vbuckets)
                else:
                    argument_list = [a.strip() for a in pair[1].split(",")]
                    if len(argument_list) > 1:
                        params[pair[0]] = argument_list
                    else:
                        params[pair[0]] = argument_list[0]

        input = TestInputParser.parse_from_file(arguments.ini)
        input.test_params = params
        for server in input.servers:
            if 'run_as_user' in input.test_params and input.test_params['run_as_user'] != server.rest_username:
                server.rest_username = input.test_params['run_as_user']
        if "num_clients" not in list(input.test_params.keys()) and input.clients:   # do not override the command line value
            input.test_params["num_clients"] = len(input.clients)
        if "num_nodes" not in list(input.test_params.keys()) and input.servers:
            input.test_params["num_nodes"] = len(input.servers)
        return input

    @staticmethod
    def parse_from_file(file):
        count = 0
        start = 0
        end = 0
        servers = list()
        ips = list()
        input = TestInput()
        config = configparser.ConfigParser(interpolation=None)
        config.read(file)
        sections = config.sections()
        global_properties = dict()
        cluster_ips = list()
        clusters = dict()
        client_ips = list()
        input.cbbackupmgr = dict()
        for section in sections:
            result = re.search('^cluster', section)
            if section == 'servers':
                ips = TestInputParser.get_server_ips(config, section)
            elif section == 'clients':
                client_ips = TestInputParser.get_server_ips(config, section)
            elif section == 'membase':
                input.membase_settings = TestInputParser.get_membase_settings(config, section)
            elif section == 'global':
                #get global stuff and override for those unset
                for option in config.options(section):
                    global_properties[option] = config.get(section, option)
            elif section == 'elastic':
                input.elastic = TestInputParser.get_elastic_config(config, section, global_properties)
            elif section == 'bkrs_client':
                input.bkrs_client = TestInputParser.get_bkrs_client_config(config, section,
                                                 global_properties, input.membase_settings)
            elif section == 'cbbackupmgr':
                input.cbbackupmgr = TestInputParser.get_cbbackupmgr_config(config, section)
            elif result is not None:
                cluster_list = TestInputParser.get_server_ips(config, section)
                cluster_ips.extend(cluster_list)
                clusters[count] = len(cluster_list)
                count += 1

        # Setup 'cluster#' tag as dict
        # input.clusters -> {0: [ip:10.1.6.210 ssh_username:root, ip:10.1.6.211 ssh_username:root]}
        for cluster_ip in cluster_ips:
            servers.append(TestInputParser.get_server(cluster_ip, config))
        servers = TestInputParser.get_server_options(servers, input.membase_settings, global_properties)
        for key, value in list(clusters.items()):
            end += value
            input.clusters[key] = servers[start:end]
            start += value

        # Setting up 'servers' tag
        servers = []
        for ip in ips:
            servers.append(TestInputParser.get_server(ip, config))
        input.servers = TestInputParser.get_server_options(servers, input.membase_settings, global_properties)

        if 'cbbackupmgr' not in sections:
            input.cbbackupmgr["name"] = "local_bkrs"

        if 'bkrs_client' not in sections:
            input.bkrs_client = None

        # Setting up 'clients' tag
        input.clients = client_ips

        return input

    @staticmethod
    def get_server_options(servers, membase_settings, global_properties):
        for server in servers:
                if server.ssh_username == '' and 'username' in global_properties:
                    server.ssh_username = global_properties['username']
                if server.ssh_password == '' and 'password' in global_properties:
                    server.ssh_password = global_properties['password']
                if server.ssh_key == '' and 'ssh_key' in global_properties:
                    server.ssh_key = os.path.expanduser(global_properties['ssh_key'])
                if not server.port and 'port' in global_properties:
                    server.port = global_properties['port']
                if server.cli_path == '' and 'cli' in global_properties:
                    server.cli_path = global_properties['cli']
                if server.rest_username == '' and membase_settings.rest_username != '':
                    server.rest_username = membase_settings.rest_username
                if server.rest_password == '' and membase_settings.rest_password != '':
                    server.rest_password = membase_settings.rest_password
                if server.data_path == '' and 'data_path' in global_properties:
                    server.data_path = global_properties['data_path']
                if server.index_path == '' and 'index_path' in global_properties:
                    server.index_path = global_properties['index_path']
                if server.cbas_path == '' and 'cbas_path' in global_properties:
                    server.cbas_path = global_properties['cbas_path']
                if server.services == '' and 'services' in global_properties:
                    server.services = global_properties['services']
                if server.n1ql_port == '' and 'n1ql_port' in global_properties:
                    server.n1ql_port = global_properties['n1ql_port']
                if server.index_port == '' and 'index_port' in global_properties:
                    server.index_port = global_properties['index_port']
                if server.eventing_port == '' and 'eventing_port' in global_properties:
                    server.eventing_port = global_properties['eventing_port']
                if server.es_username == '' and 'es_username' in global_properties:
                    server.es_username = global_properties['es_username']
                if server.es_password == '' and 'es_password' in global_properties:
                    server.es_password = global_properties['es_password']
        return servers

    @staticmethod
    def get_server_ips(config, section):
        ips = []
        options = config.options(section)
        for option in options:
            ips.append(config.get(section, option))
        return ips

    @staticmethod
    def get_elastic_config(config, section, global_properties):
        server = TestInputServer()
        options = config.options(section)
        for option in options:
            if option == 'ip':
                server.ip = config.get(section, option)
            if option == 'port':
                server.port = config.get(section, option)
            if option == 'es_username':
                server.es_username = config.get(section, option)
            if option == 'es_password':
                server.es_password = config.get(section, option)
            if option == 'username':
                server.ssh_username = config.get(section, option)
            if option == 'password':
                server.ssh_password = config.get(section, option)

        if server.ssh_username == '' and 'username' in global_properties:
            server.ssh_username = global_properties['username']
        if server.ssh_password == '' and 'password' in global_properties:
            server.ssh_password = global_properties['password']
        return server

    @staticmethod
    def get_cbbackupmgr_config(config, section):
        options = {}
        for option in config.options(section):
            options[option] = config.get(section, option)
        return options

    @staticmethod
    def get_bkrs_client_config(config, section, global_properties,
                               ui_settings):
        server = TestInputServer()
        options = config.options(section)
        for option in options:
            if option == 'ip':
                server.ip = config.get(section, option)
            if option == 'password':
                server.ssh_password = config.get(section, option)
            if option == 'port':
                server.port = config.get(section, option)
        if 'username' not in options:
            server.ssh_username = global_properties['username']
        if 'password' not in options:
            server.ssh_password = global_properties['password']
        if 'port' not in option:
            server.port = global_properties['port']
        if ui_settings is None:
            try:
                ui_settings = TestInputParser.get_membase_settings(config, "membase")
            except Exception:
                raise Exception("Ini file needs 'membase' section")
        server.rest_username = ui_settings.rest_username
        server.rest_password = ui_settings.rest_password
        server.bkrs_client = True
        return server

    @staticmethod
    def get_collection_config(collection, config):
        collection_config = {}
        for section in config.sections():
            if section == collection:
                options = config.options(section)
                for option in options:
                    if option == 'bucket':
                        collection_config['bucket'] = config.get(section, option)
                    if option == 'scope':
                        collection_config['scope'] = config.get(section, option)
                    if option.lower() == 'maxttl':
                        collection_config['maxTTL'] = config.get(section, option)
        return collection_config

    @staticmethod
    def get_server(ip, config):
        server = TestInputServer()
        server.ip = ip
        server.bkrs_client = False
        for section in config.sections():
            if section == ip:
                options = config.options(section)
                for option in options:
                    if option == 'username':
                        server.ssh_username = config.get(section, option)
                    if option == 'password':
                        server.ssh_password = config.get(section, option)
                    if option == 'cli':
                        server.cli_path = config.get(section, option)
                    if option == 'ssh_key':
                        server.ssh_key = config.get(section, option)
                    if option == 'port':
                        server.port = config.get(section, option)
                    if option == 'ip':
                        server.ip = config.get(section, option)
                    if option == 'internal_ip':
                        server.internal_ip = config.get(section, option)
                    if option == 'services':
                        server.services = config.get(section, option)
                    if option == 'n1ql_port':
                        server.n1ql_port = config.get(section, option)
                    if option == 'index_port':
                        server.index_port = config.get(section, option)
                    if option == 'fts_port':
                        server.fts_port = config.get(section, option)
                    if option == 'eventing_port':
                        server.eventing_port = config.get(section, option)
                    if option == 'collections':
                        # collections_map = {collection: {bucket:'', scope:'', param:''}}
                        collections = config.get(section, option).split(',')
                        for collection in collections:
                            server.collections_map[collection] = TestInputParser\
                                .get_collection_config(collection, config)
                break
                #get username
                #get password
                #get port
                #get cli_path
                #get key
        return server

    @staticmethod
    def get_membase_build(config, section):
        membase_build = TestInputBuild()
        for option in config.options(section):
            if option == 'version':
                pass
            if option == 'url':
                pass
        return membase_build

    @staticmethod
    def get_membase_settings(config, section):
        membase_settings = TestInputMembaseSetting()
        for option in config.options(section):
            if option == 'rest_username':
                membase_settings.rest_username = config.get(section, option)
            if option == 'rest_password':
                membase_settings.rest_password = config.get(section, option)
        return membase_settings

    @staticmethod
    def parse_from_command_line(argv):

        input = TestInput()

        try:
            # -f : won't be parse here anynore
            # -s will have comma separated list of servers
            # -t : wont be parsed here anymore
            # -v : version
            # -u : url
            # -b : will have the path to cli
            # -k : key file
            # -p : for smtp ( taken care of by jenkins)
            # -o : taken care of by jenkins
            servers = []
            membase_setting = None
            (opts, args) = getopt.getopt(argv[1:], 'h:t:c:i:p:', [])
            #first let's loop over and find out if user has asked for help
            need_help = False
            for option, argument in opts:
                if option == "-h":
                    print('usage...')
                    need_help = True
                    break
            if need_help:
                return
            #first let's populate the server list and the version number
            for option, argument in opts:
                if option == "-s":
                    #handle server list
                    servers = TestInputParser.handle_command_line_s(argument)
                elif option == "-u" or option == "-v":
                    input_build = TestInputParser.handle_command_line_u_or_v(option, argument)

            #now we can override the username pass and cli_path info
            for option, argument in opts:
                if option == "-k":
                    #handle server list
                    for server in servers:
                        if server.ssh_key == '':
                            server.ssh_key = argument
                elif option == "--username":
                    #handle server list
                    for server in servers:
                        if server.ssh_username == '':
                            server.ssh_username = argument
                elif option == "--password":
                    #handle server list
                    for server in servers:
                        if server.ssh_password == '':
                            server.ssh_password = argument
                elif option == "-b":
                    #handle server list
                    for server in servers:
                        if server.cli_path == '':
                            server.cli_path = argument
            # loop over stuff once again and set the default
            # value
            for server in servers:
                if server.ssh_username == '':
                    server.ssh_username = 'root'
                if server.ssh_password == '':
                    server.ssh_password = 'northscale!23'
                if server.cli_path == '':
                    server.cli_path = '/opt/membase/bin/'
                if not server.port:
                    server.port = 8091
            input.servers = servers
            input.membase_settings = membase_setting
            return input
        except Exception:
            log = logger.Logger.get_logger()
            log.error("unable to parse input arguments")
            raise

    @staticmethod
    def handle_command_line_u_or_v(option, argument):
        input_build = TestInputBuild()
        if option == "-u":
            # let's check whether this url exists or not
            # let's extract version from this url
            pass
        if option == "-v":
            allbuilds = BuildQuery().get_all_builds()
            for build in allbuilds:
                if build.product_version == argument:
                    input_build.url = build.url
                    input_build.version = argument
                    break
        return input_build

    @staticmethod
    def handle_command_line_s(argument):
        #ip:port:username:password:clipath

        ips = argument.split(",")
        servers = []

        for ip in ips:
            server = TestInputServer()
            if ip.find(":") == -1:
                pass
            else:
                info = ip.split(":")
                #info[0] : ip
                #info[1] : port
                #info[2] :username
                #info[3] : password
                #info[4] : cli path
                server.ip = info[0]
                server.port = info[1]
                server.ssh_username = info[2]
                server.ssh_password = info[3]
                server.cli_path = info[4]
                servers.append(server)

        return servers
