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

package org.apache.doris.qe;

import org.apache.doris.analysis.AdminCancelRebalanceDiskStmt;
import org.apache.doris.analysis.AdminCancelRepairTableStmt;
import org.apache.doris.analysis.AdminCheckTabletsStmt;
import org.apache.doris.analysis.AdminCleanTrashStmt;
import org.apache.doris.analysis.AdminCompactTableStmt;
import org.apache.doris.analysis.AdminRebalanceDiskStmt;
import org.apache.doris.analysis.AdminRepairTableStmt;
import org.apache.doris.analysis.AdminSetConfigStmt;
import org.apache.doris.analysis.AdminSetReplicaStatusStmt;
import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.AlterClusterStmt;
import org.apache.doris.analysis.AlterColumnStatsStmt;
import org.apache.doris.analysis.AlterDatabasePropertyStmt;
import org.apache.doris.analysis.AlterDatabaseQuotaStmt;
import org.apache.doris.analysis.AlterDatabaseRename;
import org.apache.doris.analysis.AlterMaterializedViewStmt;
import org.apache.doris.analysis.AlterPolicyStmt;
import org.apache.doris.analysis.AlterResourceStmt;
import org.apache.doris.analysis.AlterRoutineLoadStmt;
import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStatsStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterUserStmt;
import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.CancelAlterSystemStmt;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CancelBackupStmt;
import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.CleanLabelStmt;
import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateClusterStmt;
import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateEncryptKeyStmt;
import org.apache.doris.analysis.CreateFileStmt;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateMultiTableMaterializedViewStmt;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateRepositoryStmt;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateStageStmt;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.DropClusterStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropEncryptKeyStmt;
import org.apache.doris.analysis.DropFileStmt;
import org.apache.doris.analysis.DropFunctionStmt;
import org.apache.doris.analysis.DropMaterializedViewStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.DropRepositoryStmt;
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropSqlBlockRuleStmt;
import org.apache.doris.analysis.DropStageStmt;
import org.apache.doris.analysis.DropTableStatsStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.analysis.LinkDbStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.MigrateDbStmt;
import org.apache.doris.analysis.PauseRoutineLoadStmt;
import org.apache.doris.analysis.PauseSyncJobStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshMaterializedViewStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.analysis.ResumeRoutineLoadStmt;
import org.apache.doris.analysis.ResumeSyncJobStmt;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.StopRoutineLoadStmt;
import org.apache.doris.analysis.StopSyncJobStmt;
import org.apache.doris.analysis.SyncStmt;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.analysis.UninstallPluginStmt;
import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.catalog.EncryptKeyHelper;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.CopyJob;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.sync.SyncJobManager;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.statistics.StatisticsRepository;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Use for execute ddl.
 **/
public class DdlExecutor {
    /**
     * Execute ddl.
     **/
    private static final Logger LOG = LogManager.getLogger(DdlExecutor.class);

    public static void execute(Env env, DdlStmt ddlStmt) throws Exception {
        if (ddlStmt instanceof CreateClusterStmt) {
            CreateClusterStmt stmt = (CreateClusterStmt) ddlStmt;
            env.createCluster(stmt);
        } else if (ddlStmt instanceof AlterClusterStmt) {
            env.processModifyCluster((AlterClusterStmt) ddlStmt);
        } else if (ddlStmt instanceof DropClusterStmt) {
            env.dropCluster((DropClusterStmt) ddlStmt);
        } else if (ddlStmt instanceof MigrateDbStmt) {
            env.migrateDb((MigrateDbStmt) ddlStmt);
        } else if (ddlStmt instanceof LinkDbStmt) {
            env.linkDb((LinkDbStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateDbStmt) {
            env.createDb((CreateDbStmt) ddlStmt);
        } else if (ddlStmt instanceof DropDbStmt) {
            env.dropDb((DropDbStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFunctionStmt) {
            env.createFunction((CreateFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFunctionStmt) {
            env.dropFunction((DropFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateEncryptKeyStmt) {
            EncryptKeyHelper.createEncryptKey((CreateEncryptKeyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropEncryptKeyStmt) {
            EncryptKeyHelper.dropEncryptKey((DropEncryptKeyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateMultiTableMaterializedViewStmt) {
            env.createMultiTableMaterializedView((CreateMultiTableMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableStmt) {
            env.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableLikeStmt) {
            env.createTableLike((CreateTableLikeStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableAsSelectStmt) {
            env.createTableAsSelect((CreateTableAsSelectStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            env.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateMaterializedViewStmt) {
            env.createMaterializedView((CreateMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            env.alterTable((AlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStatsStmt) {
            env.getStatisticsManager().alterTableStatistics((AlterTableStatsStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterColumnStatsStmt) {
            StatisticsRepository.alterColumnStatistics((AlterColumnStatsStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterViewStmt) {
            env.alterView((AlterViewStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelAlterTableStmt) {
            env.cancelAlter((CancelAlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof LoadStmt) {
            LoadStmt loadStmt = (LoadStmt) ddlStmt;
            EtlJobType jobType = loadStmt.getEtlJobType();
            if (jobType == EtlJobType.UNKNOWN) {
                throw new DdlException("Unknown load job type");
            }
            if (jobType == EtlJobType.HADOOP && Config.disable_hadoop_load) {
                throw new DdlException("Load job by hadoop cluster is disabled."
                        + " Try using broker load. See 'help broker load;'");
            }
            if (jobType == EtlJobType.HADOOP) {
                if (Config.isCloudMode()) {
                    LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                    throw new DdlException("Unsupported operation");
                }
                env.getLoadManager().createLoadJobV1FromStmt(loadStmt, jobType, System.currentTimeMillis());
            } else {
                env.getLoadManager().createLoadJobFromStmt(loadStmt);
            }
        } else if (ddlStmt instanceof CopyStmt) {
            executeCopyStmt(env, (CopyStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelLoadStmt) {
            env.getLoadManager().cancelLoadJob((CancelLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRoutineLoadStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getRoutineLoadManager().createRoutineLoadJob((CreateRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof PauseRoutineLoadStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getRoutineLoadManager().pauseRoutineLoadJob((PauseRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof ResumeRoutineLoadStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getRoutineLoadManager().resumeRoutineLoadJob((ResumeRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof StopRoutineLoadStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getRoutineLoadManager().stopRoutineLoadJob((StopRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterRoutineLoadStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getRoutineLoadManager().alterRoutineLoadJob((AlterRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof UpdateStmt) {
            env.getUpdateManager().handleUpdate((UpdateStmt) ddlStmt);
        } else if (ddlStmt instanceof DeleteStmt) {
            env.getDeleteHandler().process((DeleteStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateUserStmt) {
            CreateUserStmt stmt = (CreateUserStmt) ddlStmt;
            env.getAuth().createUser(stmt);
        } else if (ddlStmt instanceof DropUserStmt) {
            DropUserStmt stmt = (DropUserStmt) ddlStmt;
            env.getAuth().dropUser(stmt);
        } else if (ddlStmt instanceof GrantStmt) {
            GrantStmt stmt = (GrantStmt) ddlStmt;
            env.getAuth().grant(stmt);
        } else if (ddlStmt instanceof RevokeStmt) {
            RevokeStmt stmt = (RevokeStmt) ddlStmt;
            env.getAuth().revoke(stmt);
        } else if (ddlStmt instanceof CreateRoleStmt) {
            env.getAuth().createRole((CreateRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRoleStmt) {
            env.getAuth().dropRole((DropRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof SetUserPropertyStmt) {
            env.getAuth().updateUserProperty((SetUserPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSystemStmt) {
            AlterSystemStmt stmt = (AlterSystemStmt) ddlStmt;
            env.alterCluster(stmt);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
            env.cancelAlterCluster(stmt);
        } else if (ddlStmt instanceof AlterDatabaseQuotaStmt) {
            env.alterDatabaseQuota((AlterDatabaseQuotaStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabaseRename) {
            env.renameDatabase((AlterDatabaseRename) ddlStmt);
        } else if (ddlStmt instanceof RecoverDbStmt) {
            env.recoverDatabase((RecoverDbStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverTableStmt) {
            env.recoverTable((RecoverTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverPartitionStmt) {
            env.recoverPartition((RecoverPartitionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateViewStmt) {
            env.createView((CreateViewStmt) ddlStmt);
        } else if (ddlStmt instanceof BackupStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.backup((BackupStmt) ddlStmt);
        } else if (ddlStmt instanceof RestoreStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.restore((RestoreStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelBackupStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.cancelBackup((CancelBackupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRepositoryStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getBackupHandler().createRepository((CreateRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRepositoryStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getBackupHandler().dropRepository((DropRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof SyncStmt) {
            return;
        } else if (ddlStmt instanceof TruncateTableStmt) {
            env.truncateTable((TruncateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRepairTableStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getTabletChecker().repairTable((AdminRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRepairTableStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getTabletChecker().cancelRepairTable((AdminCancelRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCompactTableStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.compactTable((AdminCompactTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetConfigStmt) {
            if (Config.isCloudMode()
                    && !ConnectContext.get().getCurrentUserIdentity().getUser().equals(PaloAuth.ROOT_USER)) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.setConfig((AdminSetConfigStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFileStmt) {
            env.getSmallFileMgr().createFile((CreateFileStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFileStmt) {
            env.getSmallFileMgr().dropFile((DropFileStmt) ddlStmt);
        } else if (ddlStmt instanceof InstallPluginStmt) {
            env.installPlugin((InstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof UninstallPluginStmt) {
            env.uninstallPlugin((UninstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCheckTabletsStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.checkTablets((AdminCheckTabletsStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetReplicaStatusStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.setReplicaStatus((AdminSetReplicaStatusStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateResourceStmt) {
            env.getResourceMgr().createResource((CreateResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof DropResourceStmt) {
            env.getResourceMgr().dropResource((DropResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateDataSyncJobStmt) {
            CreateDataSyncJobStmt createSyncJobStmt = (CreateDataSyncJobStmt) ddlStmt;
            SyncJobManager syncJobMgr = env.getSyncJobManager();
            if (!syncJobMgr.isJobNameExist(createSyncJobStmt.getDbName(), createSyncJobStmt.getJobName())) {
                syncJobMgr.addDataSyncJob((CreateDataSyncJobStmt) ddlStmt);
            } else {
                throw new DdlException("The syncJob with jobName '" + createSyncJobStmt.getJobName()
                        + "' in database [" + createSyncJobStmt.getDbName() + "] is already exists.");
            }
        } else if (ddlStmt instanceof ResumeSyncJobStmt) {
            env.getSyncJobManager().resumeSyncJob((ResumeSyncJobStmt) ddlStmt);
        } else if (ddlStmt instanceof PauseSyncJobStmt) {
            env.getSyncJobManager().pauseSyncJob((PauseSyncJobStmt) ddlStmt);
        } else if (ddlStmt instanceof StopSyncJobStmt) {
            env.getSyncJobManager().stopSyncJob((StopSyncJobStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCleanTrashStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.cleanTrash((AdminCleanTrashStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRebalanceDiskStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getTabletScheduler().rebalanceDisk((AdminRebalanceDiskStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRebalanceDiskStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getTabletScheduler().cancelRebalanceDisk((AdminCancelRebalanceDiskStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().createSqlBlockRule((CreateSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().alterSqlBlockRule((AlterSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().dropSqlBlockRule((DropSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabasePropertyStmt) {
            throw new DdlException("Not implemented yet");
        } else if (ddlStmt instanceof RefreshTableStmt) {
            env.getRefreshManager().handleRefreshTable((RefreshTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshDbStmt) {
            env.getRefreshManager().handleRefreshDb((RefreshDbStmt) ddlStmt);
        } else if (ddlStmt instanceof AnalyzeStmt) {
            env.createAnalysisJob((AnalyzeStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterResourceStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getResourceMgr().alterResource((AlterResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof CreatePolicyStmt) {
            env.getPolicyMgr().createPolicy((CreatePolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropPolicyStmt) {
            env.getPolicyMgr().dropPolicy((DropPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterPolicyStmt) {
            if (Config.isCloudMode()) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
            env.getPolicyMgr().alterPolicy((AlterPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateCatalogStmt) {
            env.getCatalogMgr().createCatalog((CreateCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof DropCatalogStmt) {
            env.getCatalogMgr().dropCatalog((DropCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterCatalogNameStmt) {
            env.getCatalogMgr().alterCatalogName((AlterCatalogNameStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterCatalogPropertyStmt) {
            env.getCatalogMgr().alterCatalogProps((AlterCatalogPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof CleanLabelStmt) {
            env.getLoadManager().cleanLabel((CleanLabelStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterMaterializedViewStmt) {
            env.alterMaterializedView((AlterMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof DropMaterializedViewStmt) {
            env.dropMaterializedView((DropMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshMaterializedViewStmt) {
            env.refreshMaterializedView((RefreshMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshCatalogStmt) {
            env.getCatalogMgr().refreshCatalog((RefreshCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateStageStmt) {
            env.createStage((CreateStageStmt) ddlStmt);
        } else if (ddlStmt instanceof DropStageStmt) {
            env.dropStage((DropStageStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterUserStmt) {
            env.getAuth().alterUser((AlterUserStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStatsStmt) {
            env.getStatisticsManager().dropStats((DropTableStatsStmt) ddlStmt);
        } else {
            throw new DdlException("Unknown statement.");
        }
    }

    private static void executeCopyStmt(Env env, CopyStmt copyStmt) throws Exception {
        CopyJob job = (CopyJob) env.getLoadManager().createLoadJobFromStmt(copyStmt);
        if (!copyStmt.isAsync()) {
            // wait for execute finished
            waitJobCompleted(job);
            if (job.getState() == JobState.UNKNOWN || job.getState() == JobState.CANCELLED) {
                QueryState queryState = new QueryState();
                FailMsg failMsg = job.getFailMsg();
                EtlStatus loadingStatus = job.getLoadingStatus();
                List<List<String>> result = Lists.newArrayList();
                List<String> entry = Lists.newArrayList();
                entry.add(job.getCopyId());
                entry.add(job.getState().toString());
                entry.add(failMsg == null ? "" : failMsg.getCancelType().toString());
                entry.add(failMsg == null ? "" : failMsg.getMsg());
                entry.add("");
                entry.add("");
                entry.add("");
                entry.add(loadingStatus.getTrackingUrl());
                result.add(entry);
                queryState.setResultSet(new ShowResultSet(copyStmt.getMetaData(), result));
                copyStmt.getAnalyzer().getContext().setState(queryState);
                return;
            } else if (job.getState() == JobState.FINISHED) {
                EtlStatus loadingStatus = job.getLoadingStatus();
                Map<String, String> counters = loadingStatus.getCounters();
                QueryState queryState = new QueryState();
                List<List<String>> result = Lists.newArrayList();
                List<String> entry = Lists.newArrayList();
                entry.add(job.getCopyId());
                entry.add(job.getState().toString());
                entry.add("");
                entry.add("");
                entry.add(counters.getOrDefault(LoadJob.DPP_NORMAL_ALL, "0"));
                entry.add(counters.getOrDefault(LoadJob.DPP_ABNORMAL_ALL, "0"));
                entry.add(counters.getOrDefault(LoadJob.UNSELECTED_ROWS, "0"));
                entry.add(loadingStatus.getTrackingUrl());
                result.add(entry);
                queryState.setResultSet(new ShowResultSet(copyStmt.getMetaData(), result));
                copyStmt.getAnalyzer().getContext().setState(queryState);
                return;
            }
        }
        QueryState queryState = new QueryState();
        List<List<String>> result = Lists.newArrayList();
        List<String> entry = Lists.newArrayList();
        entry.add(job.getCopyId());
        entry.add(job.getState().toString());
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        result.add(entry);
        queryState.setResultSet(new ShowResultSet(copyStmt.getMetaData(), result));
        copyStmt.getAnalyzer().getContext().setState(queryState);
    }

    private static void waitJobCompleted(CopyJob job) throws InterruptedException {
        // check the job is completed or not.
        // sleep 10ms, 1000 times(10s)
        // sleep 100ms, 1000 times(100s + 10s = 110s)
        // sleep 1000ms, 1000 times(1000s + 110s = 1110s)
        // sleep 5000ms...
        long retry = 0;
        long currentInterval = 10;
        while (!job.isCompleted()) {
            Thread.sleep(currentInterval);
            if (retry > 3010) {
                continue;
            }
            retry++;
            if (retry > 3000) {
                currentInterval = 5000;
            } else if (retry > 2000) {
                currentInterval = 1000;
            } else if (retry > 1000) {
                currentInterval = 100;
            }
        }
    }
}
