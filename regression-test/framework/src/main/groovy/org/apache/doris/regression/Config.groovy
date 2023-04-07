// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.regression

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import com.google.common.collect.Maps
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.doris.regression.util.FileUtils
import org.apache.doris.regression.util.JdbcUtils

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Predicate

import static org.apache.doris.regression.ConfigOptions.*

@Slf4j
@CompileStatic
class Config {
    public String jdbcUrl
    public String jdbcUser
    public String jdbcPassword
    public String defaultDb

    public String feHttpAddress
    public String feHttpUser
    public String feHttpPassword

    public String feCloudHttpAddress
    public String feCloudHttpUser
    public String feCloudHttpPassword

    public String instanceId
    public String cloudUniqueId
    public String metaServiceHttpAddress
    public String recycleServiceHttpAddress

    public String suitePath
    public String dataPath
    public String realDataPath
    public String sf1DataPath
    public String cacheDataPath
    public String pluginPath

    public String testGroups
    public String excludeGroups
    public String testSuites
    public String excludeSuites
    public String testDirectories
    public String excludeDirectories
    public boolean generateOutputFile
    public boolean forceGenerateOutputFile
    public boolean randomOrder
    public boolean stopWhenFail
    public boolean dryRun

    public Properties otherConfigs = new Properties()

    public Set<String> suiteWildcard = new HashSet<>()
    public Set<String> groups = new HashSet<>()
    public Set<String> directories = new HashSet<>()

    public Set<String> excludeSuiteWildcard = new HashSet<>()
    public Set<String> excludeGroupSet = new HashSet<>()
    public Set<String> excludeDirectorySet = new HashSet<>()

    public InetSocketAddress feHttpInetSocketAddress
    public InetSocketAddress feCloudHttpInetSocketAddress
    public InetSocketAddress metaServiceHttpInetSocketAddress
    public InetSocketAddress recycleServiceHttpInetSocketAddress
    public Integer parallel
    public Integer suiteParallel
    public Integer actionParallel
    public Integer times
    public boolean withOutLoadData
    public boolean isSmokeTest
    public String multiClusterBes
    public String metaServiceToken
    public String multiClusterInstance
    public String upgradeNewBeIp
    public String upgradeNewBeHbPort
    public String upgradeNewBeHttpPort
    public String upgradeNewBeUniqueId

    public String stageIamEndpoint
    public String stageIamRegion
    public String stageIamBucket
    public String stageIamPolicy
    public String stageIamRole
    public String stageIamArn
    public String stageIamAk
    public String stageIamSk
    public String stageIamUserId

    Config() {}

    Config(String defaultDb, String jdbcUrl, String jdbcUser, String jdbcPassword,
           String feHttpAddress, String feHttpUser, String feHttpPassword,
           String feCloudHttpAddress, String feCloudHttpUser, String feCloudHttpPassword, String instanceId,
           String cloudUniqueId, String metaServiceHttpAddress, String recycleServiceHttpAddress, String suitePath,
           String dataPath, String realDataPath, String sf1DataPath, String cacheDataPath, String testGroups,
           String excludeGroups, String testSuites, String excludeSuites, String testDirectories, String excludeDirectories,
           String pluginPath, String multiClusterBes, String metaServiceToken, String multiClusterInstance,
           String upgradeNewBeIp, String upgradeNewBeHbPort, String upgradeNewBeHttpPort, String upgradeNewBeUniqueId,
           String stageIamEndpoint, String stageIamRegion, String stageIamBucket, String stageIamPolicy,
           String stageIamRole, String stageIamArn, String stageIamAk, String stageIamSk, String stageIamUserId) {
        this.defaultDb = defaultDb
        this.jdbcUrl = jdbcUrl
        this.jdbcUser = jdbcUser
        this.jdbcPassword = jdbcPassword
        this.feHttpAddress = feHttpAddress
        this.feHttpUser = feHttpUser
        this.feHttpPassword = feHttpPassword
        this.feCloudHttpAddress = feCloudHttpAddress
        this.feCloudHttpUser = feCloudHttpUser
        this.feCloudHttpPassword = feCloudHttpPassword
        this.instanceId = instanceId;
        this.cloudUniqueId = cloudUniqueId;
        this.metaServiceHttpAddress = metaServiceHttpAddress
        this.recycleServiceHttpAddress = recycleServiceHttpAddress;
        this.suitePath = suitePath
        this.dataPath = dataPath
        this.realDataPath = realDataPath
        this.sf1DataPath = sf1DataPath
        this.cacheDataPath = cacheDataPath
        this.testGroups = testGroups
        this.excludeGroups = excludeGroups
        this.testSuites = testSuites
        this.excludeSuites = excludeSuites
        this.testDirectories = testDirectories
        this.excludeDirectories = excludeDirectories
        this.pluginPath = pluginPath
        this.multiClusterBes = multiClusterBes
        this.metaServiceToken = metaServiceToken
        this.multiClusterInstance = multiClusterInstance
        this.upgradeNewBeIp = upgradeNewBeIp
        this.upgradeNewBeHbPort = upgradeNewBeHbPort
        this.upgradeNewBeHttpPort = upgradeNewBeHttpPort
        this.upgradeNewBeUniqueId = upgradeNewBeUniqueId
        this.stageIamEndpoint = stageIamEndpoint
        this.stageIamRegion = stageIamRegion
        this.stageIamBucket = stageIamBucket
        this.stageIamPolicy = stageIamPolicy
        this.stageIamRole = stageIamRole
        this.stageIamArn = stageIamArn
        this.stageIamAk = stageIamAk
        this.stageIamSk = stageIamSk
        this.stageIamUserId = stageIamUserId
    }

    static Config fromCommandLine(CommandLine cmd) {
        String confFilePath = cmd.getOptionValue(confFileOpt, "")
        File confFile = new File(confFilePath)
        Config config = new Config()
        if (confFile.exists() && confFile.isFile()) {
            log.info("Load config file ${confFilePath}".toString())
            def configSlurper = new ConfigSlurper()
            def systemProperties = Maps.newLinkedHashMap(System.getProperties())
            configSlurper.setBinding(systemProperties)
            ConfigObject configObj = configSlurper.parse(new File(confFilePath).toURI().toURL())
            String customConfFilePath = confFile.getParentFile().getPath() + "/regression-conf-custom.groovy"
            File custFile = new File(customConfFilePath)
            if (custFile.exists() && custFile.isFile()) {
                ConfigObject custConfigObj = configSlurper.parse(new File(customConfFilePath).toURI().toURL())
                configObj.merge(custConfigObj)
            }
            config = Config.fromConfigObject(configObj)
        }
        fillDefaultConfig(config)

        config.suitePath = FileUtils.getCanonicalPath(cmd.getOptionValue(pathOpt, config.suitePath))
        config.dataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(dataOpt, config.dataPath))
        config.realDataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(realDataOpt, config.realDataPath))
        config.sf1DataPath = cmd.getOptionValue(sf1DataOpt, config.sf1DataPath)
        config.cacheDataPath = cmd.getOptionValue(cacheDataOpt, config.cacheDataPath)
        config.pluginPath = FileUtils.getCanonicalPath(cmd.getOptionValue(pluginOpt, config.pluginPath))
        config.suiteWildcard = cmd.getOptionValue(suiteOpt, config.testSuites)
                .split(",")
                .collect({s -> s.trim()})
                .findAll({s -> s != null && s.length() > 0})
                .toSet()
        config.groups = cmd.getOptionValue(groupsOpt, config.testGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.directories = cmd.getOptionValue(directoriesOpt, config.testDirectories)
                .split(",")
                .collect({d -> d.trim()})
                .findAll({d -> d != null && d.length() > 0})
                .toSet()
        config.excludeSuiteWildcard = cmd.getOptionValue(excludeSuiteOpt, config.excludeSuites)
                .split(",")
                .collect({s -> s.trim()})
                .findAll({s -> s != null && s.length() > 0})
                .toSet()
        config.excludeGroupSet = cmd.getOptionValue(excludeGroupsOpt, config.excludeGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.excludeDirectorySet = cmd.getOptionValue(excludeDirectoriesOpt, config.excludeDirectories)
                .split(",")
                .collect({d -> d.trim()})
                .findAll({d -> d != null && d.length() > 0})
                .toSet()

        if (!config.suiteWildcard && !config.groups && !config.directories && !config.excludeSuiteWildcard
            && !config.excludeGroupSet && !config.excludeDirectorySet) {
            log.info("no suites/directories/groups specified, set groups to p0".toString())
            config.groups = ["p0"].toSet()
        }

        config.feHttpAddress = cmd.getOptionValue(feHttpAddressOpt, config.feHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.feHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.feHttpAddress.split(":")[1])
            config.feHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse stream load address: ${config.feHttpAddress}", t)
        }

        config.feCloudHttpAddress = cmd.getOptionValue(feCloudHttpAddressOpt, config.feCloudHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.feCloudHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.feCloudHttpAddress.split(":")[1])
            config.feCloudHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse fe cloud http address: ${config.feCloudHttpAddress}", t)
        }
        log.info("feCloudHttpAddress : $config.feCloudHttpAddress, socketAddr : $config.feCloudHttpInetSocketAddress")

        config.instanceId = cmd.getOptionValue(instanceIdOpt, config.instanceId)
        log.info("instanceId : ${config.instanceId}")

        config.cloudUniqueId = cmd.getOptionValue(cloudUniqueIdOpt, config.cloudUniqueId)
        log.info("cloudUniqueId : ${config.cloudUniqueId}")

        config.metaServiceHttpAddress = cmd.getOptionValue(metaServiceHttpAddressOpt, config.metaServiceHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.metaServiceHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.metaServiceHttpAddress.split(":")[1])
            config.metaServiceHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse meta service address: ${config.metaServiceHttpAddress}", t)
        }
        log.info("msAddr : $config.metaServiceHttpAddress, socketAddr : $config.metaServiceHttpInetSocketAddress")

        config.multiClusterBes = cmd.getOptionValue(multiClusterBesOpt, config.multiClusterBes)
        log.info("multiClusterBes is ${config.multiClusterBes}".toString())

        config.metaServiceToken = cmd.getOptionValue(metaServiceTokenOpt, config.metaServiceToken)
        log.info("metaServiceToken is ${config.metaServiceToken}".toString())

        config.multiClusterInstance = cmd.getOptionValue(multiClusterInstanceOpt, config.multiClusterInstance)
        log.info("multiClusterInstance is ${config.multiClusterInstance}".toString())

        config.upgradeNewBeIp = cmd.getOptionValue(upgradeNewBeIpOpt, config.upgradeNewBeIp)
        log.info("upgradeNewBeIp is ${config.upgradeNewBeIp}".toString())

        config.upgradeNewBeHbPort = cmd.getOptionValue(upgradeNewBeHbPortOpt, config.upgradeNewBeHbPort)
        log.info("upgradeNewBeHbPort is ${config.upgradeNewBeHbPort}".toString())

        config.upgradeNewBeHttpPort = cmd.getOptionValue(upgradeNewBeHttpPortOpt, config.upgradeNewBeHttpPort)
        log.info("upgradeNewBeHttpPort is ${config.upgradeNewBeHttpPort}".toString())

        config.upgradeNewBeUniqueId = cmd.getOptionValue(upgradeNewBeUniqueIdOpt, config.upgradeNewBeUniqueId)
        log.info("upgradeNewBeUniqueId is ${config.upgradeNewBeUniqueId}".toString())

        config.stageIamEndpoint = cmd.getOptionValue(stageIamEndpointOpt, config.stageIamEndpoint)
        log.info("stageIamEndpoint is ${config.stageIamEndpoint}".toString())
        config.stageIamRegion = cmd.getOptionValue(stageIamRegionOpt, config.stageIamRegion)
        log.info("stageIamRegion is ${config.stageIamRegion}".toString())
        config.stageIamBucket = cmd.getOptionValue(stageIamBucketOpt, config.stageIamBucket)
        log.info("stageIamBucket is ${config.stageIamBucket}".toString())
        config.stageIamPolicy = cmd.getOptionValue(stageIamPolicyOpt, config.stageIamPolicy)
        log.info("stageIamPolicy is ${config.stageIamPolicy}".toString())
        config.stageIamRole = cmd.getOptionValue(stageIamRoleOpt, config.stageIamRole)
        log.info("stageIamRole is ${config.stageIamRole}".toString())
        config.stageIamArn = cmd.getOptionValue(stageIamArnOpt, config.stageIamArn)
        log.info("stageIamArn is ${config.stageIamArn}".toString())
        config.stageIamAk = cmd.getOptionValue(stageIamAkOpt, config.stageIamAk)
        log.info("stageIamAk is ${config.stageIamAk}".toString())
        config.stageIamSk = cmd.getOptionValue(stageIamSkOpt, config.stageIamSk)
        log.info("stageIamSk is ${config.stageIamSk}".toString())
        config.stageIamUserId = cmd.getOptionValue(stageIamUserIdOpt, config.stageIamUserId)
        log.info("stageIamUserId is ${config.stageIamUserId}".toString())

        config.recycleServiceHttpAddress = cmd.getOptionValue(recycleServiceHttpAddressOpt, config.recycleServiceHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.recycleServiceHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.recycleServiceHttpAddress.split(":")[1])
            config.recycleServiceHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse recycle service address: ${config.recycleServiceHttpAddress}", t)
        }
        log.info("recycleAddr : $config.recycleServiceHttpAddress, socketAddr : $config.recycleServiceHttpInetSocketAddress")

        config.defaultDb = cmd.getOptionValue(defaultDbOpt, config.defaultDb)
        config.jdbcUrl = cmd.getOptionValue(jdbcOpt, config.jdbcUrl)
        config.jdbcUser = cmd.getOptionValue(userOpt, config.jdbcUser)
        config.jdbcPassword = cmd.getOptionValue(passwordOpt, config.jdbcPassword)
        config.feHttpUser = cmd.getOptionValue(feHttpUserOpt, config.feHttpUser)
        config.feHttpPassword = cmd.getOptionValue(feHttpPasswordOpt, config.feHttpPassword)
        config.feCloudHttpUser = cmd.getOptionValue(feHttpUserOpt, config.feCloudHttpUser)
        config.feCloudHttpPassword = cmd.getOptionValue(feHttpPasswordOpt, config.feCloudHttpPassword)
        config.generateOutputFile = cmd.hasOption(genOutOpt)
        config.forceGenerateOutputFile = cmd.hasOption(forceGenOutOpt)
        config.parallel = Integer.parseInt(cmd.getOptionValue(parallelOpt, "10"))
        config.suiteParallel = Integer.parseInt(cmd.getOptionValue(suiteParallelOpt, "10"))
        config.actionParallel = Integer.parseInt(cmd.getOptionValue(actionParallelOpt, "10"))
        config.times = Integer.parseInt(cmd.getOptionValue(timesOpt, "1"))
        config.randomOrder = cmd.hasOption(randomOrderOpt)
        config.stopWhenFail = cmd.hasOption(stopWhenFailOpt)
        config.withOutLoadData = cmd.hasOption(withOutLoadDataOpt)
        config.isSmokeTest = cmd.hasOption(isSmokeTestOpt)
        config.dryRun = cmd.hasOption(dryRunOpt)

        log.info("randomOrder is ${config.randomOrder}".toString())
        log.info("stopWhenFail is ${config.stopWhenFail}".toString())
        log.info("withOutLoadData is ${config.withOutLoadData}".toString())
        log.info("dryRun is ${config.dryRun}".toString())

        Properties props = cmd.getOptionProperties("conf")
        config.otherConfigs.putAll(props)

        config.tryCreateDbIfNotExist()
        config.buildUrlWithDefaultDb()

        return config
    }

    static Config fromConfigObject(ConfigObject obj) {
        def config = new Config(
            configToString(obj.defaultDb),
            configToString(obj.jdbcUrl),
            configToString(obj.jdbcUser),
            configToString(obj.jdbcPassword),
            configToString(obj.feHttpAddress),
            configToString(obj.feHttpUser),
            configToString(obj.feHttpPassword),
            configToString(obj.feCloudHttpAddress),
            configToString(obj.feCloudHttpUser),
            configToString(obj.feCloudHttpPassword),
            configToString(obj.instanceId),
            configToString(obj.cloudUniqueId),
            configToString(obj.metaServiceHttpAddress),
            configToString(obj.recycleServiceHttpAddress),
            configToString(obj.suitePath),
            configToString(obj.dataPath),
            configToString(obj.realDataPath),
            configToString(obj.sf1DataPath),
            configToString(obj.cacheDataPath),
            configToString(obj.testGroups),
            configToString(obj.excludeGroups),
            configToString(obj.testSuites),
            configToString(obj.excludeSuites),
            configToString(obj.testDirectories),
            configToString(obj.excludeDirectories),
            configToString(obj.pluginPath),
            configToString(obj.multiClusterBes),
            configToString(obj.metaServiceToken),
            configToString(obj.multiClusterInstance),
            configToString(obj.upgradeNewBeIp),
            configToString(obj.upgradeNewBeHbPort),
            configToString(obj.upgradeNewBeHttpPort),
            configToString(obj.upgradeNewBeUniqueId),
            configToString(obj.stageIamEndpoint),
            configToString(obj.stageIamRegion),
            configToString(obj.stageIamBucket),
            configToString(obj.stageIamPolicy),
            configToString(obj.stageIamRole),
            configToString(obj.stageIamArn),
            configToString(obj.stageIamAk),
            configToString(obj.stageIamSk),
            configToString(obj.stageIamUserId),
        )

        def declareFileNames = config.getClass()
                .getDeclaredFields()
                .collect({f -> f.name})
                .toSet()
        for (def kv : obj.toProperties().entrySet()) {
            String key = kv.getKey() as String
            if (!declareFileNames.contains(key)) {
                config.otherConfigs.put(key, kv.getValue())
            }
        }

        // check smoke config
        if (obj.isSmokeTest) {
            config.isSmokeTest = true
            String env = config.otherConfigs.getOrDefault("smokeEnv", "UNKNOWN")
            log.info("Start to check $env config")
            def c = config.otherConfigs
            c.put("feCloudHttpAddress", obj.feCloudHttpAddress)
            checkCloudSmokeEnv(c)
        }

        return config
    }

    static String getProvider(String endpoint) {
        def providers = ["cos", "oss", "s3", "obs", "bos"]
        for (final def provider in providers) {
            if (endpoint.containsIgnoreCase(provider)) {
                return provider
            }
        }
        return ""
    }

    static void checkCloudSmokeEnv(Properties properties) {
        // external stage obj info
        String s3Endpoint = properties.getOrDefault("s3Endpoint", "")
        String feCloudHttpAddress = properties.getOrDefault("feCloudHttpAddress", "")
        String s3Region = properties.getOrDefault("s3Region", "")
        String s3BucketName = properties.getOrDefault("s3BucketName", "")
        String s3AK = properties.getOrDefault("ak", "")
        String s3SK = properties.getOrDefault("sk", "")

        def items = [
                fecloudHttpAddrConf:feCloudHttpAddress,
                s3RegionConf:s3Region,
                s3EndpointConf:s3Endpoint,
                s3BucketConf:s3BucketName,
                s3AKConf:s3AK,
                s3SKConf:s3SK,
                s3ProviderConf:getProvider(s3Endpoint)
        ]
        for (final def item in items) {
            if (item.value == null || item.value.isEmpty()) {
                throw new IllegalStateException("cloud smoke conf err, plz check " + item.key)
            }
        }
    }

    static void fillDefaultConfig(Config config) {
        if (config.defaultDb == null) {
            config.defaultDb = "regression_test"
            log.info("Set defaultDb to '${config.defaultDb}' because not specify.".toString())
        }

        if (config.jdbcUrl == null) {
            //jdbcUrl needs parameter here. Refer to function: buildUrl(String dbName)
            config.jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true"
            log.info("Set jdbcUrl to '${config.jdbcUrl}' because not specify.".toString())
        }

        if (config.jdbcUser == null) {
            config.jdbcUser = "root"
            log.info("Set jdbcUser to '${config.jdbcUser}' because not specify.".toString())
        }

        if (config.jdbcPassword == null) {
            config.jdbcPassword = ""
            log.info("Set jdbcPassword to empty because not specify.".toString())
        }

        if (config.feHttpAddress == null) {
            config.feHttpAddress = "127.0.0.1:8030"
            log.info("Set feHttpAddress to '${config.feHttpAddress}' because not specify.".toString())
        }

        if (config.instanceId == null) {
            config.instanceId = "instance_xxx"
            log.info("Set instanceId to '${config.instanceId}' because not specify.".toString())
        }

        if (config.cloudUniqueId == null) {
            config.cloudUniqueId = "cloud_unique_id_xxx"
            log.info("Set cloudUniqueId to '${config.cloudUniqueId}' because not specify.".toString())
        }

        if (config.metaServiceHttpAddress == null) {
            config.metaServiceHttpAddress = "127.0.0.1:5000"
            log.info("Set metaServiceHttpAddress to '${config.metaServiceHttpAddress}' because not specify.".toString())
        }

        if (config.recycleServiceHttpAddress == null) {
            config.recycleServiceHttpAddress = "127.0.0.1:5001"
            log.info("Set recycleServiceHttpAddress to '${config.recycleServiceHttpAddress}' because not specify.".toString())
        }

        if (config.feHttpUser == null) {
            config.feHttpUser = "root"
            log.info("Set feHttpUser to '${config.feHttpUser}' because not specify.".toString())
        }

        if (config.feHttpPassword == null) {
            config.feHttpPassword = ""
            log.info("Set feHttpPassword to empty because not specify.".toString())
        }

        if (config.feCloudHttpAddress == null) {
            config.feCloudHttpAddress = "127.0.0.1:8876"
            log.info("Set feCloudHttpAddress to '${config.feCloudHttpAddress}' because not specify.".toString())
        }

        if (config.feCloudHttpUser == null) {
            config.feCloudHttpUser = "root"
            log.info("Set feCloudHttpUser to '${config.feCloudHttpUser}' because not specify.".toString())
        }

        if (config.feCloudHttpPassword == null) {
            config.feCloudHttpPassword = ""
            log.info("Set feCloudHttpPassword to empty because not specify.".toString())
        }

        if (config.suitePath == null) {
            config.suitePath = "regression-test/suites"
            log.info("Set suitePath to '${config.suitePath}' because not specify.".toString())
        }

        if (config.dataPath == null) {
            config.dataPath = "regression-test/data"
            log.info("Set dataPath to '${config.dataPath}' because not specify.".toString())
        }

        if (config.realDataPath == null) {
            config.realDataPath = "regression-test/realData"
            log.info("Set realDataPath to '${config.realDataPath}' because not specify.".toString())
        }

        if (config.sf1DataPath == null) {
            config.sf1DataPath = "regression-test/sf1Data"
            log.info("Set sf1DataPath to '${config.sf1DataPath}' because not specify.".toString())
        }

        if (config.cacheDataPath == null) {
            config.cacheDataPath = "regression-test/cacheData"
            log.info("Set cacheDataPath to '${config.cacheDataPath}' because not specify.".toString())
        }

        if (config.pluginPath == null) {
            config.pluginPath = "regression-test/plugins"
            log.info("Set dataPath to '${config.pluginPath}' because not specify.".toString())
        }

        if (config.testGroups == null) {
            if (config.isSmokeTest){
                config.testGroups = "smoke"
            } else {
                config.testGroups = "default"
            }
            log.info("Set testGroups to '${config.testGroups}' because not specify.".toString())
        }

        if (config.excludeGroups == null) {
            config.excludeGroups = ""
            log.info("Set excludeGroups to empty because not specify.".toString())
        }

        if (config.testDirectories == null) {
            config.testDirectories = ""
            log.info("Set testDirectories to empty because not specify.".toString())
        }

        if (config.excludeDirectories == null) {
            config.excludeDirectories = ""
            log.info("Set excludeDirectories to empty because not specify.".toString())
        }

        if (config.testSuites == null) {
            config.testSuites = ""
            log.info("Set testSuites to empty because not specify.".toString())
        }

        if (config.excludeSuites == null) {
            config.excludeSuites = ""
            log.info("Set excludeSuites to empty because not specify.".toString())
        }

        if (config.parallel == null) {
            config.parallel = 1
            log.info("Set parallel to 1 because not specify.".toString())
        }

        if (config.suiteParallel == null) {
            config.suiteParallel = 1
            log.info("Set suiteParallel to 1 because not specify.".toString())
        }

        if (config.actionParallel == null) {
            config.actionParallel = 10
            log.info("Set actionParallel to 10 because not specify.".toString())
        }
    }

    static String configToString(Object obj) {
        return (obj instanceof String || obj instanceof GString) ? obj.toString() : null
    }

    void tryCreateDbIfNotExist() {
        tryCreateDbIfNotExist(defaultDb)
    }

    void tryCreateDbIfNotExist(String dbName) {
        // connect without specify default db
        try {
            String sql = "CREATE DATABASE IF NOT EXISTS ${dbName}"
            log.info("Try to create db, sql: ${sql}".toString())
            if (!dryRun) {
                getConnection().withCloseable { conn ->
                    JdbcUtils.executeToList(conn, sql)
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("Create database failed, jdbcUrl: ${jdbcUrl}", t)
        }
    }

    Connection getConnection() {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    }

    Connection getConnectionByDbName(String dbName) {
        String dbUrl = buildUrl(dbName)
        tryCreateDbIfNotExist(dbName)
        log.info("connect to ${dbUrl}".toString())
        return DriverManager.getConnection(dbUrl, jdbcUser, jdbcPassword)
    }

    String getDbNameByFile(File suiteFile) {
        String dir = new File(suitePath).relativePath(suiteFile.parentFile)
        // We put sql files under sql dir, so dbs and tables used by cases
        // under sql directory should be prepared by load.groovy under the
        // parent.
        //
        // e.g.
        // suites/tpcds_sf1/load.groovy
        // suites/tpcds_sf1/sql/q01.sql
        // suites/tpcds_sf1/sql/dir/q01.sql
        if (dir.indexOf(File.separator + "sql", dir.length() - 4) > 0 && dir.endsWith("sql")) {
            dir = dir.substring(0, dir.indexOf(File.separator + "sql", dir.length() - 4))
        }
        if (dir.indexOf(File.separator + "sql" + File.separator) > 0) {
            dir = dir.substring(0, dir.indexOf(File.separator + "sql" + File.separator))
        }

        dir = dir.replace('-', '_')
        dir = dir.replace('.', '_')

        return defaultDb + '_' + dir.replace(File.separator, '_')
    }

    Predicate<String> getDirectoryFilter() {
        return (Predicate<String>) { String directoryName ->
            if (directories.isEmpty() && excludeDirectorySet.isEmpty()) {
                return true
            }

            String relativePath = new File(suitePath).relativePath(new File(directoryName))
            List<String> allLevelPaths = new ArrayList<>()
            String parentPath = ""
            for (String pathName : relativePath.split(File.separator)) {
                String currentPath = parentPath + pathName
                allLevelPaths.add(currentPath)
                parentPath = currentPath + File.separator
            }

            if (!directories.isEmpty() && !allLevelPaths.any({directories.contains(it) })) {
                return false
            }
            if (!excludeDirectorySet.isEmpty() && allLevelPaths.any({ excludeDirectorySet.contains(it) })) {
                return false
            }
            return true
        }
    }

    private void buildUrlWithDefaultDb() {
        this.jdbcUrl = buildUrl(defaultDb)
        log.info("Reset jdbcUrl to ${jdbcUrl}".toString())
    }

    private String buildUrl(String dbName) {
        String urlWithDb = jdbcUrl
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        if (urlWithoutSchema.indexOf("/") >= 0) {
            if (jdbcUrl.contains("?")) {
                // e.g: jdbc:mysql://locahost:8080/?a=b
                urlWithDb = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"))
                urlWithDb += ("/" + dbName) + jdbcUrl.substring(jdbcUrl.lastIndexOf("?"))
            } else {
                // e.g: jdbc:mysql://locahost:8080/
                urlWithDb += dbName
            }
        } else {
            // e.g: jdbc:mysql://locahost:8080
            urlWithDb += ("/" + dbName)
        }

        return urlWithDb
    }
}
