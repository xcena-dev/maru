// SPDX-License-Identifier: Apache-2.0
/*
 * test_pid_reuse.c - PID reuse birth_time security test
 *
 * Validates that MARUFS delegation checks include birth_time, preventing
 * a new process that receives a recycled PID from inheriting the old
 * process's permissions.
 *
 * Strategy:
 *   1. Fork child A, grant delegation to child A's PID
 *   2. Child A confirms it can mmap the region (delegation works)
 *   3. Kill child A
 *   4. Force PID reuse: write child A's PID to /proc/sys/kernel/ns_last_pid
 *   5. Fork child B (should get same PID as child A)
 *   6. Child B tries mmap → should get EACCES (birth_time mismatch)
 *
 * Requires: root (for /proc/sys/kernel/ns_last_pid)
 *
 * Usage: ./test_pid_reuse <mount1> <mount2> <peer_node>
 */

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

static int pass_count;
static int fail_count;

#define TEST(name, expr)                                       \
    do                                                         \
    {                                                          \
        if (expr)                                              \
        {                                                      \
            printf("  PASS: %s\n", name);                      \
            pass_count++;                                      \
        }                                                      \
        else                                                   \
        {                                                      \
            printf("  FAIL: %s (errno=%d: %s)\n", name, errno, \
                   strerror(errno));                           \
            fail_count++;                                      \
        }                                                      \
    } while (0)

#define SLOT_SIZE (2ULL * 1024 * 1024)

static int do_grant(int fd, unsigned node, unsigned pid, unsigned perms)
{
    struct marufs_perm_req preq;
    memset(&preq, 0, sizeof(preq));
    preq.node_id = (__u32)node;
    preq.pid = (__u32)pid;
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
}

static void sync_signal(int fd) { char c = 'G'; write(fd, &c, 1); }
static void sync_wait(int fd) { char c; read(fd, &c, 1); }

/*
 * Force next fork() to allocate a specific PID by writing to
 * /proc/sys/kernel/ns_last_pid. Requires root.
 * The next PID allocated will be (target_pid).
 * We write (target_pid - 1) so the kernel increments to target_pid.
 */
static int force_next_pid(pid_t target_pid)
{
    int fd;
    char buf[32];
    int len;

    fd = open("/proc/sys/kernel/ns_last_pid", O_WRONLY);
    if (fd < 0) {
        perror("open ns_last_pid (need root)");
        return -1;
    }

    len = snprintf(buf, sizeof(buf), "%d", target_pid - 1);
    if (write(fd, buf, len) != len) {
        perror("write ns_last_pid");
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int run_pid_reuse_test(const char *mount1, const char *mount2,
                               int peer_node)
{
    char filename[64];
    char filepath1[512], filepath2[512];
    int owner_fd;
    int p2c[2], c2p[2];
    pid_t child_a_pid, child_b_pid;
    int status;

    snprintf(filename, sizeof(filename), "pidreuse_%d", getpid());
    snprintf(filepath1, sizeof(filepath1), "%s/%s", mount1, filename);
    snprintf(filepath2, sizeof(filepath2), "%s/%s", mount2, filename);

    printf("\n--- Test: PID reuse birth_time security ---\n");

    /* Check we can write ns_last_pid (need root) */
    if (access("/proc/sys/kernel/ns_last_pid", W_OK) != 0) {
        printf("  SKIP: /proc/sys/kernel/ns_last_pid not writable (need root)\n");
        return 0;
    }

    /* Step 1: Owner creates region */
    owner_fd = open(filepath1, O_CREAT | O_RDWR, 0644);
    TEST("create region", owner_fd >= 0);
    if (owner_fd < 0) return -1;
    TEST("ftruncate", ftruncate(owner_fd, SLOT_SIZE) == 0);

    /* Step 2: Fork child A */
    pipe(p2c);
    pipe(c2p);

    child_a_pid = fork();
    if (child_a_pid == 0) {
        /* Child A */
        close(p2c[1]);
        close(c2p[0]);

        sync_wait(p2c[0]); /* Wait for grant */

        int peer_fd = open(filepath2, O_RDWR);
        void *map = (peer_fd >= 0) ?
            mmap(NULL, SLOT_SIZE, PROT_READ, MAP_SHARED, peer_fd, 0) :
            MAP_FAILED;

        char result = (map != MAP_FAILED) ? 'P' : 'F';
        write(c2p[1], &result, 1);

        if (map != MAP_FAILED) munmap(map, SLOT_SIZE);
        if (peer_fd >= 0) close(peer_fd);

        /* Wait to be killed */
        sync_wait(p2c[0]);
        _exit(0);
    }

    /* Parent: grant to child A */
    close(p2c[0]);
    close(c2p[1]);

    TEST("grant to child A",
         do_grant(owner_fd, peer_node, child_a_pid,
                  MARUFS_PERM_READ | MARUFS_PERM_WRITE) == 0);

    sync_signal(p2c[1]); /* Tell child A to try access */

    char result_a;
    read(c2p[0], &result_a, 1);
    TEST("child A mmap succeeds (delegation valid)", result_a == 'P');

    printf("  INFO: child A PID = %d\n", child_a_pid);

    /* Step 3: Kill child A */
    kill(child_a_pid, SIGKILL);
    waitpid(child_a_pid, &status, 0);
    close(p2c[1]);
    close(c2p[0]);

    /* Step 4: Force PID reuse */
    TEST("force next PID to child A's PID",
         force_next_pid(child_a_pid) == 0);

    /* Step 5: Fork child B — should get same PID */
    int p2c2[2], c2p2[2];
    pipe(p2c2);
    pipe(c2p2);

    child_b_pid = fork();
    if (child_b_pid == 0) {
        /* Child B — has same PID as child A (if force_next_pid worked) */
        close(p2c2[1]);
        close(c2p2[0]);

        /* Try to access region — should fail because birth_time differs */
        int peer_fd = open(filepath2, O_RDWR);
        int mmap_errno = 0;
        void *map = MAP_FAILED;

        if (peer_fd >= 0) {
            map = mmap(NULL, SLOT_SIZE, PROT_READ, MAP_SHARED, peer_fd, 0);
            if (map == MAP_FAILED)
                mmap_errno = errno;
        }

        /* Report: 'D' = denied (expected), 'A' = allowed (bad!) */
        char result = (map == MAP_FAILED) ? 'D' : 'A';
        write(c2p2[1], &result, 1);
        /* Also write the errno */
        write(c2p2[1], &mmap_errno, sizeof(mmap_errno));

        if (map != MAP_FAILED) munmap(map, SLOT_SIZE);
        if (peer_fd >= 0) close(peer_fd);
        _exit(0);
    }

    close(p2c2[0]);
    close(c2p2[1]);

    /* Check if PID reuse worked */
    printf("  INFO: child B PID = %d (wanted %d)\n", child_b_pid, child_a_pid);
    int pid_reused = (child_b_pid == child_a_pid);
    TEST("PID reuse achieved", pid_reused);

    if (!pid_reused) {
        printf("  SKIP: PID reuse failed (PID may have been taken by another process)\n");
        printf("  INFO: This can happen under high system load. Re-run the test.\n");
        kill(child_b_pid, SIGKILL);
        waitpid(child_b_pid, &status, 0);
        close(p2c2[1]);
        close(c2p2[0]);
        close(owner_fd);
        unlink(filepath1);
        return 0;
    }

    /* Collect child B's result */
    waitpid(child_b_pid, &status, 0);

    char result_b;
    int mmap_errno;
    read(c2p2[0], &result_b, 1);
    read(c2p2[0], &mmap_errno, sizeof(mmap_errno));

    TEST("child B denied access (birth_time check)", result_b == 'D');
    if (result_b == 'D') {
        TEST("errno is EACCES", mmap_errno == EACCES);
    } else {
        printf("  SECURITY: child B with recycled PID accessed region!\n");
    }

    /* Cleanup */
    close(p2c2[1]);
    close(c2p2[0]);
    close(owner_fd);
    unlink(filepath1);

    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 4) {
        fprintf(stderr,
                "Usage: %s <mount1> <mount2> <peer_node>\n"
                "  Requires root (for /proc/sys/kernel/ns_last_pid)\n",
                argv[0]);
        return 1;
    }

    const char *mount1 = argv[1];
    const char *mount2 = argv[2];
    int peer_node = atoi(argv[3]);

    setbuf(stdout, NULL);
    printf("========================================\n");
    printf("MARUFS PID Reuse Birth-Time Security Test\n");
    printf("========================================\n");

    run_pid_reuse_test(mount1, mount2, peer_node);

    printf("\n========================================\n");
    printf("Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n");
    return (fail_count > 0) ? 1 : 0;
}
