// SPDX-License-Identifier: Apache-2.0
/*
 * test_negative.c - Error path and negative test cases for MARUFS
 *
 * Tests error codes for invalid operations:
 *   - GRANT escalation prevention (EPERM)
 *   - Delegation table full (ENOSPC)
 *   - NRHT init parameter validation (EINVAL, EEXIST)
 *   - Unknown ioctl command (ENOTTY)
 *
 * Usage:
 *   test_negative <mount>                       Single-node
 *   test_negative <mount1> <mount2> <peer_node>  Multi-node (for GRANT escalation)
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

/* --- Test helpers --- */

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

/* Helper: ioctl grant on an open fd */
static int do_grant(int fd, unsigned int node, unsigned int pid,
                    unsigned int perms)
{
    struct marufs_perm_req preq;

    memset(&preq, 0, sizeof(preq));
    preq.node_id = (__u32)node;
    preq.pid = (__u32)pid;
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
}

/* Helper: ioctl nrht init on an open fd */
static int do_nrht_init(int fd, __u32 max_entries, __u32 num_shards,
                        __u32 num_buckets)
{
    struct marufs_nrht_init_req req;

    memset(&req, 0, sizeof(req));
    req.max_entries = max_entries;
    req.num_shards = num_shards;
    req.num_buckets = num_buckets;
    return ioctl(fd, MARUFS_IOC_NRHT_INIT, &req);
}

/*
 * Section 1: Unknown ioctl (M3)
 *
 * An unrecognized ioctl number must return -1 with errno==ENOTTY.
 */
static void test_unknown_ioctl(const char *mount)
{
    char filepath[512];
    int fd, ret, dummy = 0;

    printf("\n=== Section 1: Unknown ioctl (M3) ===\n");

    snprintf(filepath, sizeof(filepath), "%s/neg_unk_%d", mount, getpid());
    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        printf("  SKIP: cannot create test file errno=%d (%s)\n",
               errno, strerror(errno));
        return;
    }
    if (ftruncate(fd, (off_t)SLOT_SIZE) < 0) {
        printf("  SKIP: ftruncate failed errno=%d (%s)\n",
               errno, strerror(errno));
        close(fd);
        unlink(filepath);
        return;
    }

    errno = 0;
    ret = ioctl(fd, _IOW('X', 99, int), &dummy);
    TEST("unknown ioctl returns -1 with ENOTTY",
         ret == -1 && errno == ENOTTY);

    close(fd);
    unlink(filepath);
}

/*
 * Section 2: NRHT init parameter validation (M2)
 *
 * - Non-power-of-2 num_shards must fail with EINVAL.
 * - Valid (all-zero / default) init must succeed.
 * - A second init on the same region must fail with EEXIST.
 */
static void test_nrht_init(const char *mount)
{
    char filepath[512];
    int fd, ret;

    printf("\n=== Section 2: NRHT init validation (M2) ===\n");

    snprintf(filepath, sizeof(filepath), "%s/neg_nrht_%d", mount, getpid());
    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        printf("  SKIP: cannot create test file errno=%d (%s)\n",
               errno, strerror(errno));
        return;
    }
    if (ftruncate(fd, (off_t)SLOT_SIZE) < 0) {
        printf("  SKIP: ftruncate failed errno=%d (%s)\n",
               errno, strerror(errno));
        close(fd);
        unlink(filepath);
        return;
    }

    /* num_shards=3 is not a power of 2 — must fail with EINVAL */
    errno = 0;
    ret = do_nrht_init(fd, 0, 3, 0);
    TEST("nrht_init with non-power-of-2 num_shards returns EINVAL",
         ret == -1 && errno == EINVAL);

    /* Small params that fit in 2MB region — may succeed or return EEXIST
     * if the region's physical space retains NRHT data from a prior run
     * (CXL memory is persistent across file delete/recreate). */
    errno = 0;
    ret = do_nrht_init(fd, 1024, 4, 256);
    if (ret == 0) {
        TEST("nrht_init with valid small params succeeds", 1);
    } else if (errno == EEXIST) {
        printf("  INFO: region already NRHT-formatted (persistent CXL data)\n");
        TEST("nrht_init on pre-formatted region returns EEXIST", 1);
    } else {
        TEST("nrht_init with valid small params succeeds", 0);
    }

    /* Double init on the same region — must fail with EEXIST */
    errno = 0;
    ret = do_nrht_init(fd, 1024, 4, 256);
    TEST("nrht_init double-init returns EEXIST",
         ret == -1 && errno == EEXIST);

    close(fd);
    unlink(filepath);
}

/*
 * Section 3: Delegation table full (H6)
 *
 * Fill all MARUFS_DELEG_MAX (29) delegation slots then verify that
 * the 30th grant fails with ENOSPC.
 *
 * Use node_id values 1..3 and pid values 100..109 to generate 29
 * unique (node_id, pid) pairs without repeating any combination.
 */
static void test_deleg_full(const char *mount)
{
    char filepath[512];
    int fd, ret, i;

    printf("\n=== Section 3: Delegation table full (H6) ===\n");

    snprintf(filepath, sizeof(filepath), "%s/neg_deleg_%d", mount, getpid());
    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        printf("  SKIP: cannot create test file errno=%d (%s)\n",
               errno, strerror(errno));
        return;
    }
    if (ftruncate(fd, (off_t)SLOT_SIZE) < 0) {
        printf("  SKIP: ftruncate failed errno=%d (%s)\n",
               errno, strerror(errno));
        close(fd);
        unlink(filepath);
        return;
    }

    /* Fill MARUFS_DELEG_MAX slots with unique (node_id, pid) pairs.
     * node_id = i/10 + 1  -> 1, 1, ..., 1 (10x), 2 (10x), 3 (9x)
     * pid     = i%10 + 100 -> 100..109 cycling */
    for (i = 0; i < MARUFS_DELEG_MAX; i++) {
        unsigned int node = (unsigned int)(i / 10) + 1;
        unsigned int pid = (unsigned int)(i % 10) + 100;

        ret = do_grant(fd, node, pid, MARUFS_PERM_READ);
        if (ret != 0) {
            printf("  FAIL: grant %d/%d failed early errno=%d (%s)\n",
                   i + 1, MARUFS_DELEG_MAX, errno, strerror(errno));
            fail_count++;
            close(fd);
            unlink(filepath);
            return;
        }
    }
    printf("  INFO: filled %d delegation slots\n", MARUFS_DELEG_MAX);

    /* 30th grant — table is full, must fail with ENOSPC */
    errno = 0;
    ret = do_grant(fd, 4, 200, MARUFS_PERM_READ);
    TEST("grant beyond DELEG_MAX returns ENOSPC",
         ret == -1 && errno == ENOSPC);

    close(fd);
    unlink(filepath);
}

/*
 * Section 4: GRANT escalation prevention (H4) — multi-node only
 *
 * Owner grants PERM_GRANT|PERM_READ|PERM_WRITE to peer_node.
 * The peer must not be able to grant permissions beyond its own
 * granted set (no ADMIN, no GRANT-itself escalation).
 * The peer must be able to grant READ (within its granted scope).
 */
static void test_grant_escalation(const char *mount1, const char *mount2,
                                  unsigned int peer_node)
{
    char filepath1[512], filepath2[512], filename[128];
    int fd1, fd2, ret;

    printf("\n=== Section 4: GRANT escalation prevention (H4) ===\n");

    snprintf(filename, sizeof(filename), "neg_esc_%d", getpid());
    snprintf(filepath1, sizeof(filepath1), "%s/%s", mount1, filename);
    snprintf(filepath2, sizeof(filepath2), "%s/%s", mount2, filename);

    /* Owner creates and sizes the file */
    fd1 = open(filepath1, O_CREAT | O_RDWR, 0644);
    if (fd1 < 0) {
        printf("  SKIP: cannot create test file on mount1 errno=%d (%s)\n",
               errno, strerror(errno));
        return;
    }
    if (ftruncate(fd1, (off_t)SLOT_SIZE) < 0) {
        printf("  SKIP: ftruncate failed errno=%d (%s)\n",
               errno, strerror(errno));
        close(fd1);
        unlink(filepath1);
        return;
    }

    /* Owner grants PERM_GRANT|PERM_READ|PERM_WRITE to peer_node with
     * THIS process's PID — so when we open via mount2, the delegation
     * matches our (node_id, pid) and escalation checks are exercised. */
    ret = do_grant(fd1, peer_node, (unsigned int)getpid(),
                   MARUFS_PERM_GRANT | MARUFS_PERM_READ | MARUFS_PERM_WRITE);
    if (ret != 0) {
        printf("  SKIP: owner grant to peer failed errno=%d (%s)\n",
               errno, strerror(errno));
        close(fd1);
        unlink(filepath1);
        return;
    }
    printf("  INFO: owner granted GRANT|READ|WRITE to node=%u pid=%d\n",
           peer_node, getpid());
    close(fd1);

    /* Open same file from mount2 (peer node view) */
    fd2 = open(filepath2, O_RDWR);
    if (fd2 < 0) {
        printf("  SKIP: cannot open file on mount2 errno=%d (%s)\n",
               errno, strerror(errno));
        unlink(filepath1);
        return;
    }

    /* Peer tries to grant ADMIN — must fail with EPERM (not in granted set) */
    errno = 0;
    ret = do_grant(fd2, 3, 999, MARUFS_PERM_ADMIN);
    TEST("peer cannot escalate grant to ADMIN (EPERM)",
         ret == -1 && errno == EPERM);

    /* Peer tries to grant GRANT itself — must fail with EPERM */
    errno = 0;
    ret = do_grant(fd2, 3, 999, MARUFS_PERM_GRANT);
    TEST("peer cannot escalate grant to GRANT (EPERM)",
         ret == -1 && errno == EPERM);

    /* Peer grants READ to a third party — must succeed (within scope) */
    errno = 0;
    ret = do_grant(fd2, 3, 999, MARUFS_PERM_READ);
    TEST("peer can grant READ within its granted scope",
         ret == 0);

    close(fd2);
    unlink(filepath1);
}

int main(int argc, char *argv[])
{
    const char *mount1, *mount2;
    unsigned int peer_node;
    int multi_node;

    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s <mount>                          Single-node\n"
                "       %s <mount1> <mount2> <peer_node>   Multi-node\n",
                argv[0], argv[0]);
        return 1;
    }

    mount1 = argv[1];
    multi_node = (argc >= 4);

    if (multi_node) {
        mount2 = argv[2];
        peer_node = (unsigned int)atoi(argv[3]);
    } else {
        mount2 = NULL;
        peer_node = 0;
    }

    printf("========================================\n");
    printf("MARUFS Negative / Error-Path Tests\n");
    printf("========================================\n");
    printf("Mount:  %s\n", mount1);
    if (multi_node)
        printf("Mount2: %s  peer_node=%u\n", mount2, peer_node);
    printf("========================================\n");

    test_unknown_ioctl(mount1);
    test_nrht_init(mount1);
    test_deleg_full(mount1);

    if (multi_node)
        test_grant_escalation(mount1, mount2, peer_node);
    else
        printf("\n=== Section 4: GRANT escalation (H4) — skipped (single-node) ===\n");

    printf("\n========================================\n");
    printf("Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n");

    return fail_count > 0 ? 1 : 0;
}
