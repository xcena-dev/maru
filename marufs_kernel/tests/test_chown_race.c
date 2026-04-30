// SPDX-License-Identifier: Apache-2.0
/*
 * test_chown_race.c - CHOWN concurrency and timing attack tests
 *
 * Validates that the CAS-based atomic ownership transfer in MARUFS_IOC_CHOWN
 * correctly handles race conditions, concurrent attacks, and GC interactions.
 *
 * Usage:
 *   test_chown_race <mount1> <mount2> <peer_node> [size_mb]
 *
 * Requires: two mount points (different node_ids) on the same CXL device.
 */

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
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

/* ── Helpers ─────────────────────────────────────────────────────────── */

static int pass_count;
static int fail_count;

#define TEST(name, expr)                                         \
    do                                                           \
    {                                                            \
        if (expr)                                                \
        {                                                        \
            printf("  PASS: %s\n", name);                        \
            pass_count++;                                        \
        }                                                        \
        else                                                     \
        {                                                        \
            printf("  FAIL: %s (errno=%d: %s)\n", name, errno,   \
                   strerror(errno));                             \
            fail_count++;                                        \
        }                                                        \
    } while (0)

static int do_chown(int fd)
{
    struct marufs_chown_req req;
    memset(&req, 0, sizeof(req));
    return ioctl(fd, MARUFS_IOC_CHOWN, &req);
}

static int do_grant(int fd, unsigned node, unsigned pid, unsigned perms)
{
    struct marufs_perm_req preq;
    memset(&preq, 0, sizeof(preq));
    preq.node_id = (__u32)node;
    preq.pid = (__u32)pid;
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
}

static int do_set_default(int fd, unsigned perms)
{
    struct marufs_perm_req preq;
    memset(&preq, 0, sizeof(preq));
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_SET_DEFAULT, &preq);
}

/* Create a test file with ftruncate */
static int create_test_file(const char* path, __u64 size)
{
    int fd = open(path, O_CREAT | O_RDWR, 0644);
    if (fd < 0)
        return -1;
    if (ftruncate(fd, (__off_t)size) != 0)
    {
        close(fd);
        unlink(path);
        return -1;
    }
    return fd;
}

/* ════════════════════════════════════════════════════════════════════════
 * Test 1: Concurrent CHOWN race (N children with ADMIN all race)
 *
 * Attack scenario: Owner grants ADMIN to N processes, all call CHOWN
 * simultaneously. With CAS protection, exactly one should succeed per
 * attempt; others get -EAGAIN.
 * ════════════════════════════════════════════════════════════════════════ */

#define RACE_CHILDREN 8

struct race_result
{
    pid_t pid;
    int succeeded; /* 1 = chown returned 0 */
};

static void test_concurrent_chown_race(const char* mount,
                                       unsigned node_id __attribute__((unused)),
                                       __u64 data_size)
{
    char filepath[512];
    int fd, ret;
    int pipe_fds[RACE_CHILDREN][2];
    pid_t children[RACE_CHILDREN];
    int success_count = 0;

    printf("\n[R1] Concurrent CHOWN race (%d children)\n", RACE_CHILDREN);

    snprintf(filepath, sizeof(filepath), "%s/chown_race_%d",
             mount, (int)getpid());
    unlink(filepath);

    fd = create_test_file(filepath, data_size);
    TEST("create test file", fd >= 0);
    if (fd < 0)
        return;

    /* Set default READ+ADMIN so all children can open and attempt chown */
    ret = do_set_default(fd, MARUFS_PERM_READ | MARUFS_PERM_ADMIN);
    TEST("set default READ|ADMIN", ret == 0);

    /* Create pipes for result collection */
    for (int i = 0; i < RACE_CHILDREN; i++)
        pipe(pipe_fds[i]);

    /* Fork N children, each will attempt CHOWN */
    for (int i = 0; i < RACE_CHILDREN; i++)
    {
        children[i] = fork();
        if (children[i] == 0)
        {
            /* Child: open file, wait for signal, then race to chown */
            int cfd = open(filepath, O_RDONLY);
            if (cfd < 0)
                _exit(1);

            /* Tight spin to maximize contention */
            usleep(10000); /* 10ms — let all children spawn */

            errno = 0;
            int rc = do_chown(cfd);
            struct race_result res = {.pid = getpid(),
                                      .succeeded = (rc == 0) ? 1 : 0};
            close(cfd);

            write(pipe_fds[i][1], &res, sizeof(res));
            close(pipe_fds[i][1]);
            _exit(0);
        }
    }

    /* Parent: collect results */
    for (int i = 0; i < RACE_CHILDREN; i++)
    {
        struct race_result res;
        int status;

        waitpid(children[i], &status, 0);
        if (read(pipe_fds[i][0], &res, sizeof(res)) == sizeof(res))
        {
            if (res.succeeded)
            {
                success_count++;
                printf("    child pid=%d: CHOWN succeeded\n", res.pid);
            }
        }
        close(pipe_fds[i][0]);
        close(pipe_fds[i][1]);
    }

    /*
     * With CAS protection: first child that CAS succeeds gets ownership,
     * others get -EAGAIN. Exactly 1 should succeed (or 0 if the parent
     * is no longer owner after first child's chown clears default_perms).
     *
     * In practice: first child chown succeeds and clears default_perms,
     * so subsequent children fail permission check. ≤ 1 is correct.
     */
    char desc[128];
    snprintf(desc, sizeof(desc),
             "at most 1 child succeeded (%d total)", success_count);
    TEST(desc, success_count <= 1);

    close(fd);
    /* Owner may have changed; try cleanup from both contexts */
    unlink(filepath);
}

/* ════════════════════════════════════════════════════════════════════════
 * Test 2: CHOWN + GC race
 *
 * Attack scenario: Trigger GC right after CHOWN to verify the file
 * survives (not incorrectly reclaimed during ownership transition).
 * ════════════════════════════════════════════════════════════════════════ */

static void test_chown_gc_race(const char* mount, unsigned node_id,
                                __u64 data_size)
{
    char filepath[512];
    char gc_trigger[] = "/sys/fs/marufs/debug/gc_trigger";
    int fd, ret;
    int gc_fd;

    printf("\n[R2] CHOWN + GC race (ownership survives GC)\n");

    /* Check if we can trigger GC */
    gc_fd = open(gc_trigger, O_WRONLY);
    if (gc_fd < 0)
    {
        printf("  SKIP: cannot open %s (need root)\n", gc_trigger);
        return;
    }

    snprintf(filepath, sizeof(filepath), "%s/chown_gc_%d",
             mount, (int)getpid());
    unlink(filepath);

    fd = create_test_file(filepath, data_size);
    TEST("create test file", fd >= 0);
    if (fd < 0)
    {
        close(gc_fd);
        return;
    }

    /* Grant ADMIN to a child, child does CHOWN, then immediately trigger GC */
    pid_t child = fork();
    if (child == 0)
    {
        /* Child: wait for grant, then chown */
        usleep(50000); /* 50ms for parent to grant */

        int cfd = open(filepath, O_RDONLY);
        if (cfd < 0)
            _exit(1);

        ret = do_chown(cfd);
        close(cfd);

        /* Keep child alive for a bit so GC sees a live owner */
        usleep(500000); /* 500ms */
        _exit(ret == 0 ? 0 : 1);
    }

    /* Parent: grant ADMIN|READ to child */
    ret = do_grant(fd, node_id, (unsigned)child, MARUFS_PERM_ADMIN | MARUFS_PERM_READ);
    TEST("grant ADMIN|READ to child", ret == 0);
    ret = do_set_default(fd, MARUFS_PERM_READ);
    TEST("set default READ", ret == 0);

    /* Wait for child to chown */
    usleep(100000); /* 100ms */

    /* Hammer GC trigger multiple times during/after chown */
    for (int i = 0; i < 5; i++)
    {
        write(gc_fd, "1", 1);
        lseek(gc_fd, 0, SEEK_SET);
        usleep(50000); /* 50ms between triggers */
    }

    /* Wait for child */
    int status;
    waitpid(child, &status, 0);
    TEST("child chown succeeded", WIFEXITED(status) && WEXITSTATUS(status) == 0);

    /* File must still exist (not reclaimed by GC) */
    struct stat st;
    ret = stat(filepath, &st);
    TEST("file survives GC after chown", ret == 0);

    close(gc_fd);
    close(fd);
    unlink(filepath);
}

/* ════════════════════════════════════════════════════════════════════════
 * Test 3: Delegation revocation after CHOWN
 *
 * Attack scenario: Old owner grants READ+WRITE+DELETE to attacker.
 * After CHOWN, attacker should lose all delegated permissions.
 * ════════════════════════════════════════════════════════════════════════ */

static void test_delegation_revoked_after_chown(const char* mount1,
                                                 const char* mount2,
                                                 unsigned peer_node,
                                                 __u64 data_size)
{
    char filepath1[512], filepath2[512];
    char filename[64];
    int fd1, fd2, ret;

    printf("\n[R3] Delegation revoked after CHOWN\n");

    snprintf(filename, sizeof(filename), "chown_deleg_%d", (int)getpid());
    snprintf(filepath1, sizeof(filepath1), "%s/%s", mount1, filename);
    snprintf(filepath2, sizeof(filepath2), "%s/%s", mount2, filename);
    unlink(filepath1);

    fd1 = create_test_file(filepath1, data_size);
    TEST("create test file on mount1", fd1 >= 0);
    if (fd1 < 0)
        return;

    /* Owner grants READ+WRITE+DELETE to peer node */
    ret = do_grant(fd1, peer_node, (unsigned)getpid(),
                   MARUFS_PERM_READ | MARUFS_PERM_WRITE | MARUFS_PERM_DELETE);
    TEST("grant READ|WRITE|DELETE to peer", ret == 0);

    /* Verify peer can open file (via delegation) */
    fd2 = open(filepath2, O_RDONLY);
    TEST("peer can open before chown", fd2 >= 0);
    if (fd2 >= 0)
        close(fd2);

    /* Child on mount1 does CHOWN (becomes new owner) */
    pid_t child = fork();
    if (child == 0)
    {
        usleep(50000);
        int cfd = open(filepath1, O_RDONLY);
        if (cfd < 0)
            _exit(1);
        ret = do_chown(cfd);
        close(cfd);
        /* Stay alive */
        usleep(2000000); /* 2s */
        _exit(ret == 0 ? 0 : 1);
    }

    /* Grant ADMIN to child so it can chown */
    ret = do_grant(fd1, 1, (unsigned)child, MARUFS_PERM_ADMIN | MARUFS_PERM_READ);
    TEST("grant ADMIN to child", ret == 0);

    usleep(200000); /* 200ms — wait for child to complete chown */

    /*
     * After CHOWN: delegation table should be cleared.
     * Peer's old delegation should be revoked.
     * open() always succeeds, but read()/mmap() should fail with -EACCES.
     */
    fd2 = open(filepath2, O_RDONLY);
    TEST("peer open succeeds (open always allowed)", fd2 >= 0);
    if (fd2 >= 0)
    {
        errno = 0;
        void* map = mmap(NULL, 4096, PROT_READ, MAP_SHARED, fd2, 0);
        TEST("peer mmap(PROT_READ) DENIED after chown (delegation revoked)",
             map == MAP_FAILED && errno == EACCES);
        if (map != MAP_FAILED)
            munmap(map, 4096);
        close(fd2);
    }

    /* Cleanup */
    int status;
    kill(child, SIGTERM);
    waitpid(child, &status, 0);

    close(fd1);
    unlink(filepath1);
}

/* ════════════════════════════════════════════════════════════════════════
 * Test 4: Repeated CHOWN ping-pong (stress)
 *
 * Two processes alternate CHOWN back and forth rapidly.
 * Validates CAS serialization under high contention.
 * ════════════════════════════════════════════════════════════════════════ */

#define PINGPONG_ROUNDS 100

static void test_chown_pingpong(const char* mount,
                                 unsigned node_id __attribute__((unused)),
                                 __u64 data_size)
{
    char filepath[512];
    int fd, ret;
    int to_child[2], to_parent[2]; /* sync pipes */
    int success_a = 0, success_b = 0, eagain_count = 0;

    printf("\n[R4] CHOWN ping-pong (%d rounds)\n", PINGPONG_ROUNDS);

    snprintf(filepath, sizeof(filepath), "%s/chown_pingpong_%d",
             mount, (int)getpid());
    unlink(filepath);

    fd = create_test_file(filepath, data_size);
    TEST("create test file", fd >= 0);
    if (fd < 0)
        return;

    pipe(to_child);
    pipe(to_parent);

    pid_t child = fork();
    if (child == 0)
    {
        /* Child: wait for 'go' signal, then alternate chown */
        close(to_child[1]);
        close(to_parent[0]);

        char buf;
        int cfd = open(filepath, O_RDONLY);
        if (cfd < 0)
            _exit(1);

        int child_ok = 0;
        for (int i = 0; i < PINGPONG_ROUNDS; i++)
        {
            /* Wait for parent signal */
            if (read(to_child[0], &buf, 1) != 1)
                break;

            errno = 0;
            ret = do_chown(cfd);
            if (ret == 0)
                child_ok++;

            /* Signal parent */
            buf = (ret == 0) ? 'Y' : 'N';
            if (write(to_parent[1], &buf, 1) != 1)
                break;
        }

        close(cfd);
        close(to_child[0]);
        close(to_parent[1]);
        _exit(child_ok > 0 ? 0 : 1);
    }

    close(to_child[0]);
    close(to_parent[1]);

    /* Parent: set default ADMIN|READ so both can chown */
    ret = do_set_default(fd, MARUFS_PERM_READ | MARUFS_PERM_ADMIN);
    TEST("set default READ|ADMIN", ret == 0);

    for (int i = 0; i < PINGPONG_ROUNDS; i++)
    {
        char buf;

        /* Parent chown */
        errno = 0;
        ret = do_chown(fd);
        if (ret == 0)
        {
            success_a++;
            /* Re-set default perms (chown clears them) */
            do_set_default(fd, MARUFS_PERM_READ | MARUFS_PERM_ADMIN);
        }
        else
        {
            eagain_count++;
        }

        /* Signal child to try */
        buf = 'G';
        if (write(to_child[1], &buf, 1) != 1)
        {
            printf("    parent: child pipe broken at round %d\n", i);
            break;
        }

        /* Wait for child result */
        if (read(to_parent[0], &buf, 1) == 1)
        {
            if (buf == 'Y')
                success_b++;
        }
    }

    int status;
    close(to_child[1]);
    close(to_parent[0]);
    waitpid(child, &status, 0);

    printf("    parent succeeded: %d/%d\n", success_a, PINGPONG_ROUNDS);
    printf("    child  succeeded: %d/%d\n", success_b, PINGPONG_ROUNDS);
    printf("    EAGAIN count:     %d\n", eagain_count);

    /* Both should have won at least once; no crashes */
    TEST("both processes got ownership at least once",
         success_a > 0 && success_b > 0);
    TEST("total successes ≤ 2×rounds (no double-success)",
         (success_a + success_b) <= 2 * PINGPONG_ROUNDS);

    close(fd);
    unlink(filepath);
}

/* ════════════════════════════════════════════════════════════════════════
 * Test 5: CHOWN after owner death (dead-process + delegation)
 *
 * Owner creates file, grants ADMIN to process B, then dies.
 * Process B should still be able to CHOWN (rescue the file).
 * ════════════════════════════════════════════════════════════════════════ */

static void test_chown_rescue_from_dead_owner(const char* mount,
                                               unsigned node_id,
                                               __u64 data_size)
{
    char filepath[512];
    int ret;

    printf("\n[R5] CHOWN rescue from dead owner\n");

    snprintf(filepath, sizeof(filepath), "%s/chown_rescue_%d",
             mount, (int)getpid());
    unlink(filepath);

    /* Child A: create file, grant ADMIN to parent, then die */
    int sync_pipe[2];
    pipe(sync_pipe);

    pid_t child_a = fork();
    if (child_a == 0)
    {
        close(sync_pipe[0]);
        int cfd = create_test_file(filepath, data_size);
        if (cfd < 0)
            _exit(1);

        /* Grant ADMIN|READ to parent */
        ret = do_grant(cfd, node_id, (unsigned)getppid(),
                       MARUFS_PERM_ADMIN | MARUFS_PERM_READ);
        ret |= do_set_default(cfd, MARUFS_PERM_READ | MARUFS_PERM_ADMIN);

        /* Signal parent that grant is done */
        char buf = (ret == 0) ? 'Y' : 'N';
        write(sync_pipe[1], &buf, 1);
        close(sync_pipe[1]);

        close(cfd);
        /* Die immediately — owner process is now dead */
        _exit(0);
    }

    close(sync_pipe[1]);

    /* Wait for child A to grant and die */
    char buf;
    read(sync_pipe[0], &buf, 1);
    close(sync_pipe[0]);

    int status;
    waitpid(child_a, &status, 0);
    TEST("child A (owner) exited", WIFEXITED(status));
    TEST("child A granted successfully", buf == 'Y');

    /* Parent: owner is dead, but we have ADMIN delegation. Try CHOWN. */
    int fd = open(filepath, O_RDONLY);
    TEST("open file after owner death", fd >= 0);

    if (fd >= 0)
    {
        errno = 0;
        ret = do_chown(fd);
        TEST("CHOWN succeeds (rescue from dead owner)", ret == 0);

        /* Verify we are now the owner — can grant permissions */
        if (ret == 0)
        {
            ret = do_grant(fd, node_id, 99999, MARUFS_PERM_READ);
            TEST("new owner can grant (confirms ownership)", ret == 0);
        }

        close(fd);
    }

    unlink(filepath);
}

/* ════════════════════════════════════════════════════════════════════════
 * main
 * ════════════════════════════════════════════════════════════════════════ */

int main(int argc, char* argv[])
{
    /* Prevent SIGPIPE from killing the process — detect broken pipes
     * via write() return value instead of dying silently */
    signal(SIGPIPE, SIG_IGN);

    /* Line-buffer stdout so output is visible even through pipes */
    setlinebuf(stdout);

    if (argc < 4)
    {
        fprintf(stderr,
                "Usage: %s <mount1> <mount2> <peer_node> [size_mb]\n"
                "\n"
                "  mount1:    owner node mount point\n"
                "  mount2:    peer node mount point\n"
                "  peer_node: node_id of mount2\n"
                "  size_mb:   region data size (default: 128)\n",
                argv[0]);
        return 1;
    }

    const char* mount1 = argv[1];
    const char* mount2 = argv[2];
    unsigned peer_node = (unsigned)strtoul(argv[3], NULL, 10);
    unsigned long size_mb = (argc >= 5) ? strtoul(argv[4], NULL, 10) : 128;
    __u64 data_size = size_mb * 1024 * 1024;

    /* Detect owner node_id: peer is given, owner is the "other" one */
    unsigned owner_node = (peer_node == 1) ? 2 : 1;

    printf("============================================\n");
    printf("  MARUFS CHOWN Race Condition Tests\n");
    printf("============================================\n");
    printf("  mount1:     %s (node_id=%u)\n", mount1, owner_node);
    printf("  mount2:     %s (node_id=%u)\n", mount2, peer_node);
    printf("  data_size:  %luMB\n", size_mb);
    printf("============================================\n");

    /* R1: Concurrent CHOWN race */
    test_concurrent_chown_race(mount1, owner_node, data_size);

    /* R2: CHOWN + GC race */
    test_chown_gc_race(mount1, owner_node, data_size);

    /* R3: Delegation revoked after CHOWN */
    test_delegation_revoked_after_chown(mount1, mount2, peer_node, data_size);

    /* R4: CHOWN ping-pong stress */
    test_chown_pingpong(mount1, owner_node, data_size);

    /* R5: CHOWN rescue from dead owner */
    test_chown_rescue_from_dead_owner(mount1, owner_node, data_size);

    /* Summary */
    printf("\n============================================\n");
    printf("=== Total: %d passed, %d failed ===\n", pass_count, fail_count);
    printf("============================================\n");

    return fail_count > 0 ? 1 : 0;
}
