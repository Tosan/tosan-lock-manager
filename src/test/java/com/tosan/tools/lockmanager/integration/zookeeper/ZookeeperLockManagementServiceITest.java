package com.tosan.tools.lockmanager.impl.zookeeper;

import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Hajihosseinkhani
 * @since 12/07/2021
 **/
public class ZookeeperLockManagementServiceITest {
    private static final String LOCKE_NAME_TYPE = "NAME";
    private static final String LOCK_NAME = "TEST";
    private static final int ZOOKEEPER_PORT = 2182;
    private static final String ZOOKEEPER_HOST = "localhost";

    private static ZookeeperLockManagementService zookeeperLockManagementService;
    private static ExecutorService executorService;
    private static Runnable runnable;

    @BeforeAll
    public static void setup() throws Exception {
        new TestingServer(ZOOKEEPER_PORT, true);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
                ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT, new RetryNTimes(10, 500));
        curatorFramework.start();
        ZookeeperLockService zookeeperLockService = new ZookeeperLockService(curatorFramework);
        zookeeperLockManagementService = new ZookeeperLockManagementService(zookeeperLockService);
        executorService = Executors.newSingleThreadExecutor();
        runnable = () -> zookeeperLockManagementService.requestWriteLock(LOCKE_NAME_TYPE, LOCK_NAME, 0, true);
    }

    @Test
    public void testRequestReadLockAndReleaseIt() {
        zookeeperLockManagementService.requestReadLock(LOCKE_NAME_TYPE, LOCK_NAME, 0, true);
        zookeeperLockManagementService.unlock(LOCKE_NAME_TYPE, LOCK_NAME);
    }

    @Test
    public void testRequestWriteLockAndReleaseIt() {
        zookeeperLockManagementService.requestWriteLock(LOCKE_NAME_TYPE, LOCK_NAME, 0, true);
        zookeeperLockManagementService.unlock(LOCKE_NAME_TYPE, LOCK_NAME);
    }

    @Test
    public void requestWriteLockAndConvertItToReadLockThenReleaseIt() {
        zookeeperLockManagementService.requestWriteLock(LOCKE_NAME_TYPE, LOCK_NAME, 0, true);
        zookeeperLockManagementService.convertToReadLock(LOCKE_NAME_TYPE, LOCK_NAME, 0);
        zookeeperLockManagementService.unlock(LOCKE_NAME_TYPE, LOCK_NAME);
    }

    @Test
    public void requestReadLockAndConvertItToWriteLockThenReleaseIt() {
        zookeeperLockManagementService.requestReadLock(LOCKE_NAME_TYPE, LOCK_NAME, 0, true);
        zookeeperLockManagementService.convertToWriteLock(LOCKE_NAME_TYPE, LOCK_NAME, 0);
        zookeeperLockManagementService.unlock(LOCKE_NAME_TYPE, LOCK_NAME);
    }

    @Test
    @Order(5)
    public void requestWriteLockWithDifferentThreads() throws ExecutionException, InterruptedException {
        executorService.submit(runnable).get();
        Assertions.assertThrows(LockManagerTimeoutException.class,
                () -> zookeeperLockManagementService.requestWriteLock(LOCKE_NAME_TYPE, LOCK_NAME, 5, true));
    }
}
