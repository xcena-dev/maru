// SPDX-License-Identifier: Apache-2.0
/*
 * test_ioctl.c - MARUFS ioctl + permission delegation test program
 *
 * Usage:
 *   test_ioctl <mount>                                          Single-node tests
 *   test_ioctl <mount> <size_mb> [owner_node]                   Single-node, custom size
 *   test_ioctl <mount1> <mount2>                                Single + multi-node tests
 *   test_ioctl <mount1> <mount2> <peer_node>                    Specify peer node_id (default: 2)
 *   test_ioctl <mount1> <mount2> <peer_node> <size> [owner_node] Full options
 *
 * Multi-node: mount1 = owner node, mount2 = reader node (different node_id).
 * Both mounts must be on the same CXL device (shared memory).
 * The test process creates files via mount1 (owner) and verifies access
 * control via mount2 (non-owner) in a single run.
 *
 * Subcommands (for shell-script orchestration across VMs):
 *   test_ioctl <mount> perm-setup   <file> <peer_node> [size_mb]
 *   test_ioctl <mount> perm-read    <file>
 *   test_ioctl <mount> perm-write   <file>
 *   test_ioctl <mount> perm-delete  <file>
 *   test_ioctl <mount> perm-grant   <file> <node> <pid> <perms_hex>
 *   test_ioctl <mount> perm-default <file> <perms_hex>
 *   test_ioctl <mount> perm-cleanup <file>
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

#define MARUFS_NAME_LEN (MARUFS_NAME_MAX + 1)

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

/* Helper: ioctl set default on an open fd */
static int do_set_default(int fd, unsigned int perms)
{
    struct marufs_perm_req preq;

    memset(&preq, 0, sizeof(preq));
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_SET_DEFAULT, &preq);
}

/* Helper: ioctl chown (transfer ownership to caller) */
static int do_chown(int fd)
{
    struct marufs_chown_req req;

    memset(&req, 0, sizeof(req));
    return ioctl(fd, MARUFS_IOC_CHOWN, &req);
}

/* Helper: create + init NRHT file, returns fd or -1 */
static int create_nrht(const char *mount_point, int pid, __u32 max_entries)
{
    char path[512];
    struct marufs_nrht_init_req ninit;
    int fd, ret;

    snprintf(path, sizeof(path), "%s/nrht_test_%d", mount_point, pid);
    unlink(path);  /* clean up stale */

    fd = open(path, O_CREAT | O_RDWR, 0644);
    if (fd < 0)
        return -1;

    memset(&ninit, 0, sizeof(ninit));
    ninit.max_entries = max_entries;
    /* num_shards=0, num_buckets=0 → defaults */

    ret = ioctl(fd, MARUFS_IOC_NRHT_INIT, &ninit);
    if (ret != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

/* Helper: try open + read 1 byte, returns 0 if readable */
static int try_read(const char* path)
{
    char buf[1];
    int fd;
    ssize_t n;

    fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    n = read(fd, buf, 1);
    close(fd);
    return (n >= 0) ? 0 : -1;
}

/* Helper: try open O_RDWR + mmap PROT_WRITE, returns 0 if writable */
static int try_write_mmap(const char* path)
{
    int fd;
    void* map;
    struct stat st;

    fd = open(path, O_RDWR);
    if (fd < 0)
        return -1;

    if (fstat(fd, &st) || st.st_size == 0)
    {
        close(fd);
        return -1;
    }

    map = mmap(NULL, SLOT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (map == MAP_FAILED)
        return -1;

    munmap(map, SLOT_SIZE);
    return 0;
}

/* ================================================================
 * Multi-node permission tests
 *
 * mount1 = owner node mount (file created here)
 * mount2 = reader node mount (access tested here, different node_id)
 * peer_node = node_id of mount2 (e.g., 2)
 * ================================================================ */

static int run_multinode_tests(const char* mount1, const char* mount2,
                               unsigned int peer_node,
                               unsigned long data_size_mb)
{
    __u64 data_size = data_size_mb * 1024 * 1024;
    char filepath1[512], filepath2[512];
    char filename[64];
    int fd1, ret;

    snprintf(filename, sizeof(filename), "perm_mn_%d", (int)getpid());
    snprintf(filepath1, sizeof(filepath1), "%s/%s", mount1, filename);
    snprintf(filepath2, sizeof(filepath2), "%s/%s", mount2, filename);

    printf("=== MARUFS Multi-Node Permission Tests ===\n");
    printf("  mount1 (owner):  %s\n", mount1);
    printf("  mount2 (reader): %s\n", mount2);
    printf("  peer_node_id: %u\n", peer_node);
    printf("  file: %s, size: %luMB\n\n", filename, data_size_mb);

    /* Clean up stale file */
    unlink(filepath1);

    /* ---- Setup: create file on mount1 (owner) ---- */
    printf("[M0] Setup: create + ftruncate on owner mount\n");

    fd1 = open(filepath1, O_CREAT | O_RDWR, 0644);
    TEST("open(O_CREAT) on mount1", fd1 >= 0);
    if (fd1 < 0)
        return 1;

    ret = ftruncate(fd1, (__off_t)data_size);
    TEST("ftruncate", ret == 0);
    if (ret)
    {
        close(fd1);
        unlink(filepath1);
        return 1;
    }
    printf("\n");

    /* ---- M1: Non-owner denied without any grants ---- */
    printf("[M1] Non-owner denied (no grants)\n");

    errno = 0;
    ret = try_read(filepath2);
    TEST("mount2 read DENIED (no grants)", ret != 0);

    errno = 0;
    ret = try_write_mmap(filepath2);
    TEST("mount2 write DENIED (no grants)", ret != 0);
    printf("\n");

    /* ---- M2: Grant READ → mount2 can read, cannot write ---- */
    printf("[M2] Grant READ to peer node\n");

    ret = do_grant(fd1, peer_node, (unsigned)getpid(), MARUFS_PERM_READ);
    TEST("GRANT READ to peer node", ret == 0);

    errno = 0;
    ret = try_read(filepath2);
    TEST("mount2 read OK (READ granted)", ret == 0);

    errno = 0;
    ret = try_write_mmap(filepath2);
    TEST("mount2 write DENIED (only READ)", ret != 0);
    printf("\n");

    /* ---- M3: Add WRITE → mount2 can read + write ---- */
    printf("[M3] Grant WRITE (add to existing READ)\n");

    ret = do_grant(fd1, peer_node, (unsigned)getpid(), MARUFS_PERM_WRITE);
    TEST("GRANT WRITE to peer node", ret == 0);

    errno = 0;
    ret = try_read(filepath2);
    TEST("mount2 read OK (READ|WRITE)", ret == 0);

    errno = 0;
    ret = try_write_mmap(filepath2);
    TEST("mount2 write OK (READ|WRITE)", ret == 0);
    printf("\n");

    /* ---- M4: Grant DELETE → mount2 can delete ---- */
    printf("[M4] Grant DELETE to peer node\n");

    ret = do_grant(fd1, peer_node, (unsigned)getpid(), MARUFS_PERM_DELETE);
    TEST("GRANT DELETE to peer node", ret == 0);

    errno = 0;
    ret = try_read(filepath2);
    TEST("mount2 read OK (READ|WRITE|DELETE)", ret == 0);

    errno = 0;
    ret = unlink(filepath2);
    TEST("mount2 unlink OK (DELETE granted)", ret == 0);

    close(fd1);
    /* File already deleted by M4 — no cleanup needed */
    printf("\n");

    /* ---- M5: SET_DEFAULT tests (separate file, no grants) ---- */
    {
        char fn2[128], fp1_2[512], fp2_2[512];
        int fd2;

        snprintf(fn2, sizeof(fn2), "perm_def_%d", (int)getpid());
        snprintf(fp1_2, sizeof(fp1_2), "%s/%s", mount1, fn2);
        snprintf(fp2_2, sizeof(fp2_2), "%s/%s", mount2, fn2);
        unlink(fp1_2);

        printf("[M5] SET_DEFAULT tests (separate file, no grants)\n");

        fd2 = open(fp1_2, O_CREAT | O_RDWR, 0644);
        TEST("open new file for default_perms test", fd2 >= 0);
        if (fd2 < 0)
            return 1;

        ret = ftruncate(fd2, (__off_t)data_size);
        TEST("ftruncate", ret == 0);

        /* No grants — peer denied */
        errno = 0;
        ret = try_read(fp2_2);
        TEST("mount2 read DENIED (no grants, no default)", ret != 0);

        /* SET_DEFAULT READ */
        ret = do_set_default(fd2, MARUFS_PERM_READ);
        TEST("SET_DEFAULT perms=READ", ret == 0);

        errno = 0;
        ret = try_read(fp2_2);
        TEST("mount2 read OK (via default_perms)", ret == 0);

        errno = 0;
        ret = try_write_mmap(fp2_2);
        TEST("mount2 write DENIED (default is READ only)", ret != 0);

        /* Clear default */
        ret = do_set_default(fd2, 0);
        TEST("SET_DEFAULT perms=0 (owner-only)", ret == 0);

        errno = 0;
        ret = try_read(fp2_2);
        TEST("mount2 read DENIED (default cleared)", ret != 0);

        close(fd2);
        unlink(fp1_2);
    }

    printf("\n");
    return 0;
}

/* ================================================================
 * Single-node tests (offset naming + permission ioctl)
 * ================================================================ */

static int run_single_node_tests(const char* mount_point,
                                 unsigned long data_size_mb)
{
    __u64 data_size = data_size_mb * 1024 * 1024;
    char filepath[512];
    int fd;
    int ret;
    int pid = (int)getpid();
    int nrht_fd = -1;
    char nrht_path[512];

    printf("=== MARUFS Single-Node Tests (offset naming + permissions) ===\n");
    printf("  mount: %s, data_size: %luMB\n\n", mount_point, data_size_mb);

    snprintf(filepath, sizeof(filepath), "%s/ioctl_test_%d",
             mount_point, (int)getpid());

    /* ----------------------------------------------------------------
     * Test 1: Two-phase create
     * ---------------------------------------------------------------- */
    printf("[1] Two-phase create\n");

    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    TEST("open(O_CREAT) succeeds", fd >= 0);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }

    {
        struct stat st;
        ret = fstat(fd, &st);
        TEST("fstat succeeds", ret == 0);
        TEST("initial size is 0", st.st_size == 0);
    }

    ret = ftruncate(fd, (__off_t)data_size);
    TEST("ftruncate to data_size succeeds", ret == 0);

    {
        struct stat st;
        ret = fstat(fd, &st);
        TEST("fstat after ftruncate succeeds", ret == 0);
        TEST("size matches requested data_size",
             (__u64)st.st_size == data_size);
        printf("  -> file size = %llu bytes (%lluMB)\n",
               (unsigned long long)st.st_size,
               (unsigned long long)st.st_size / (1024 * 1024));
    }

    errno = 0;
    ret = ftruncate(fd, (__off_t)(data_size * 2));
    TEST("WORM: second ftruncate rejected", ret != 0);

    /* Setup NRHT for name-ref tests */
    snprintf(nrht_path, sizeof(nrht_path), "%s/nrht_test_%d",
             mount_point, pid);

    nrht_fd = create_nrht(mount_point, pid, 8192);
    TEST("NRHT create + init", nrht_fd >= 0);
    if (nrht_fd < 0) {
        close(fd);
        unlink(filepath);
        return 1;
    }

    /* ----------------------------------------------------------------
     * Test 2: NAME_OFFSET
     * ---------------------------------------------------------------- */
    printf("\n[2] NAME_OFFSET\n");

    char names[5][192];
    __u64 offsets[5];

    snprintf(names[0], 192, "llama3-layer32-kv-%d", pid);
    snprintf(names[1], 192, "llama3-layer33-kv-%d", pid);
    snprintf(names[2], 192, "mistral-layer1-kv-%d", pid);
    snprintf(names[3], 192, "gpt4-layer0-attention-cache-%d", pid);
    snprintf(names[4], 192,
             "very-long-name-for-testing-maximum-length-"
             "capabilities-of-the-new-global-index-with-191-char-names-"
             "for-kv-cache-keys-in-llm-inference-workloads-%d",
             pid);

    for (int i = 0; i < 5; i++)
        offsets[i] = (__u64)i * SLOT_SIZE;

    int num_names = 5;
    while (num_names > 0 && offsets[num_names - 1] >= data_size)
        num_names--;

    for (int i = 0; i < num_names; i++)
    {
        struct marufs_name_offset_req req;
        memset(&req, 0, sizeof(req));
        strncpy(req.name, names[i], sizeof(req.name) - 1);
        req.offset = offsets[i];
        req.target_region_fd = fd;

        ret = ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);

        char desc[256];
        snprintf(desc, sizeof(desc), "name '%s' -> offset 0x%llx",
                 names[i], (unsigned long long)offsets[i]);
        TEST(desc, ret == 0);
    }

    if (num_names > 0)
    {
        struct marufs_name_offset_req req;
        memset(&req, 0, sizeof(req));
        strncpy(req.name, names[0], sizeof(req.name) - 1);
        req.offset = offsets[0];
        req.target_region_fd = fd;
        ret = ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
        TEST("duplicate name returns EEXIST", ret != 0);
    }

    if (num_names > 1)
    {
        struct marufs_name_offset_req req;
        memset(&req, 0, sizeof(req));
        strncpy(req.name, names[0], sizeof(req.name) - 1);
        req.offset = offsets[1];
        req.target_region_fd = fd;
        ret = ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
        TEST("duplicate name returns EEXIST", ret != 0);
    }

    /* ----------------------------------------------------------------
     * Test 3: FIND_NAME (global lookup)
     * ---------------------------------------------------------------- */
    printf("\n[3] FIND_NAME\n");

    for (int i = 0; i < num_names; i++)
    {
        struct marufs_find_name_req freq;
        memset(&freq, 0, sizeof(freq));
        strncpy(freq.name, names[i], sizeof(freq.name) - 1);

        ret = ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);

        char desc[256];
        snprintf(desc, sizeof(desc), "find '%s'", names[i]);
        TEST(desc, ret == 0);

        if (ret == 0)
        {
            char desc2[256];
            snprintf(desc2, sizeof(desc2),
                     "  -> offset 0x%llx matches expected 0x%llx",
                     (unsigned long long)freq.offset,
                     (unsigned long long)offsets[i]);
            TEST(desc2, freq.offset == offsets[i]);
        }
    }

    {
        struct marufs_find_name_req freq;
        memset(&freq, 0, sizeof(freq));
        strncpy(freq.name, "nonexistent-kv-cache-entry",
                sizeof(freq.name) - 1);
        errno = 0;
        ret = ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
        TEST("find nonexistent returns error", ret != 0);
    }

    /* ----------------------------------------------------------------
     * Test 4: CLEAR_NAME
     * ---------------------------------------------------------------- */
    printf("\n[4] CLEAR_NAME\n");

    if (num_names >= 3)
    {
        struct marufs_name_offset_req req;
        memset(&req, 0, sizeof(req));
        strncpy(req.name, names[1], sizeof(req.name) - 1);

        ret = ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
        TEST("clear name succeeds", ret == 0);

        {
            struct marufs_find_name_req freq;
            memset(&freq, 0, sizeof(freq));
            strncpy(freq.name, names[1], sizeof(freq.name) - 1);
            errno = 0;
            ret = ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
            TEST("cleared name not found (ENOENT)", ret != 0);
        }

        {
            struct marufs_find_name_req freq;
            memset(&freq, 0, sizeof(freq));
            strncpy(freq.name, names[0], sizeof(freq.name) - 1);
            ret = ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
            TEST("other names still accessible after clear", ret == 0);
        }

        memset(&req, 0, sizeof(req));
        snprintf(req.name, sizeof(req.name), "recycled-slot-%d", pid);
        req.offset = offsets[1];
        req.target_region_fd = fd;
        ret = ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
        TEST("re-name cleared offset succeeds", ret == 0);

        {
            struct marufs_find_name_req freq;
            memset(&freq, 0, sizeof(freq));
            snprintf(freq.name, sizeof(freq.name), "recycled-slot-%d", pid);
            ret = ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
            TEST("find recycled name succeeds",
                 ret == 0 && freq.offset == offsets[1]);
        }
    }

    {
        struct marufs_name_offset_req req;
        memset(&req, 0, sizeof(req));
        strncpy(req.name, "never-existed-name", sizeof(req.name) - 1);
        errno = 0;
        ret = ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
        TEST("clear nonexistent returns error", ret != 0);
    }

    /* ----------------------------------------------------------------
     * Test 5: Offset ordering
     * ---------------------------------------------------------------- */
    printf("\n[5] Offset ordering\n");
    if (num_names >= 3)
    {
        struct marufs_find_name_req f0, f1;
        memset(&f0, 0, sizeof(f0));
        memset(&f1, 0, sizeof(f1));
        strncpy(f0.name, names[0], sizeof(f0.name) - 1);
        strncpy(f1.name, names[2], sizeof(f1.name) - 1);

        ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &f0);
        ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &f1);

        __u64 diff = f1.offset - f0.offset;
        char desc[256];
        snprintf(desc, sizeof(desc),
                 "offset diff = %lluMB (expected %lluMB)",
                 (unsigned long long)diff / (1024 * 1024),
                 (unsigned long long)(offsets[2] - offsets[0]) / (1024 * 1024));
        TEST(desc, diff == (offsets[2] - offsets[0]));
    }

    /* ----------------------------------------------------------------
     * Test 6: Bulk naming (stress)
     * ---------------------------------------------------------------- */
    printf("\n[6] Bulk naming (stress)\n");
    {
        int bulk_count = 100;
        int bulk_pass = 0;
        int bulk_find = 0;

        while (bulk_count > 0 &&
               (__u64)(bulk_count - 1) * SLOT_SIZE >= data_size)
            bulk_count /= 2;

        for (int i = 0; i < bulk_count; i++)
        {
            struct marufs_name_offset_req req;
            memset(&req, 0, sizeof(req));
            snprintf(req.name, sizeof(req.name),
                     "bulk-kv-entry-%d-%d", pid, i);
            req.offset = (__u64)i * SLOT_SIZE;
            req.target_region_fd = fd;

            ret = ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
            if (ret == 0)
                bulk_pass++;
        }

        char desc[128];
        snprintf(desc, sizeof(desc), "named %d/%d bulk entries",
                 bulk_pass, bulk_count);
        TEST(desc, bulk_pass == bulk_count);

        for (int i = 0; i < bulk_count; i++)
        {
            struct marufs_find_name_req freq;
            memset(&freq, 0, sizeof(freq));
            snprintf(freq.name, sizeof(freq.name),
                     "bulk-kv-entry-%d-%d", pid, i);

            ret = ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
            if (ret == 0 && freq.offset == (__u64)i * SLOT_SIZE)
                bulk_find++;
        }

        snprintf(desc, sizeof(desc), "found %d/%d bulk entries",
                 bulk_find, bulk_count);
        TEST(desc, bulk_find == bulk_count);
    }

    /* ----------------------------------------------------------------
     * Test 7: Permission delegation (single-node, owner-only)
     * ---------------------------------------------------------------- */
    printf("\n[7] Permission delegation (single-node)\n");

    {
        ret = do_grant(fd, 99, 10001, MARUFS_PERM_READ);
        TEST("GRANT READ to node=99 pid=10001", ret == 0);
    }

    {
        ret = do_grant(fd, 2, 12345, MARUFS_PERM_READ | MARUFS_PERM_WRITE);
        TEST("GRANT READ|WRITE to node=2 pid=12345", ret == 0);
    }

    {
        ret = do_grant(fd, 3, 10002, MARUFS_PERM_ADMIN | MARUFS_PERM_READ);
        TEST("GRANT ADMIN|READ to node=3 pid=10002", ret == 0);
    }

    {
        ret = do_grant(fd, 99, 10001, MARUFS_PERM_IOCTL);
        TEST("GRANT upsert (add IOCTL to node=99 pid=10001)", ret == 0);
    }

    {
        errno = 0;
        ret = do_grant(fd, 1, 0, MARUFS_PERM_READ);
        TEST("GRANT with pid=0 rejected", ret != 0);
    }

    {
        ret = do_grant(fd, 0, 1, MARUFS_PERM_READ);
        TEST("GRANT with node_id=0 rejected", ret != 0);
    }

    {
        ret = do_set_default(fd, MARUFS_PERM_READ);
        TEST("SET_DEFAULT to READ", ret == 0);
    }

    {
        ret = do_set_default(fd, 0);
        TEST("SET_DEFAULT to 0 (owner-only)", ret == 0);
    }

    {
        struct marufs_perm_req preq;
        memset(&preq, 0, sizeof(preq));
        preq.node_id = 1;
        preq.pid = 1;
        preq.perms = 0;
        errno = 0;
        ret = ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
        TEST("GRANT with zero perms rejected", ret != 0);
    }

    {
        struct marufs_perm_req preq;
        memset(&preq, 0, sizeof(preq));
        preq.node_id = 1;
        preq.pid = 1;
        preq.perms = 0xFFFF;
        errno = 0;
        ret = ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
        TEST("GRANT with invalid perms rejected", ret != 0);
    }

    {
        int grant_pass = 0;
        int grant_count = MARUFS_DELEG_MAX - 3; /* minus 3 from earlier tests */

        for (int i = 0; i < grant_count; i++)
        {
            ret = do_grant(fd, (unsigned)(100 + i), (unsigned)(20000 + i),
                           MARUFS_PERM_READ);
            if (ret == 0)
                grant_pass++;
        }

        char desc[128];
        snprintf(desc, sizeof(desc), "bulk GRANT %d/%d entries",
                 grant_pass, grant_count);
        TEST(desc, grant_pass == grant_count);
    }

    /* ----------------------------------------------------------------
     * Test 8: BATCH_FIND_NAME
     * ---------------------------------------------------------------- */
    printf("\n[8] BATCH_FIND_NAME\n");

    /* Re-open file (closed by perm tests) */
    fd = open(filepath, O_RDWR);
    if (fd < 0)
    {
        printf("  SKIP: cannot reopen file for batch test\n");
        goto batch_done;
    }

    /* 8a: batch lookup of bulk entries registered in Test 6 */
    {
        int batch_n = 32;
        struct marufs_find_name_req* bent;
        struct marufs_batch_find_req breq;

        while (batch_n > 0 &&
               (__u64)(batch_n - 1) * SLOT_SIZE >= data_size)
            batch_n /= 2;

        bent = calloc((size_t)batch_n, sizeof(*bent));
        if (!bent)
        {
            printf("  SKIP: calloc failed\n");
            goto batch_done;
        }

        for (int i = 0; i < batch_n; i++)
            snprintf(bent[i].name, sizeof(bent[i].name),
                     "bulk-kv-entry-%d-%d", pid, i);

        memset(&breq, 0, sizeof(breq));
        breq.count = (__u32)batch_n;
        breq.entries = (__u64)(unsigned long)bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
        TEST("batch ioctl succeeds", ret == 0);

        {
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "batch found=%u/%d entries", breq.found, batch_n);
            TEST(desc, (int)breq.found == batch_n);
        }

        {
            int match = 0;
            for (int i = 0; i < batch_n; i++)
            {
                if (bent[i].status == 0 &&
                    bent[i].offset == (__u64)i * SLOT_SIZE)
                    match++;
            }
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "batch offsets correct: %d/%d", match, batch_n);
            TEST(desc, match == batch_n);
        }

        free(bent);
    }

    /* 8b: mixed batch — some exist, some don't */
    {
        struct marufs_find_name_req bent[4];
        struct marufs_batch_find_req breq;

        memset(bent, 0, sizeof(bent));
        snprintf(bent[0].name, sizeof(bent[0].name),
                 "bulk-kv-entry-%d-0", pid); /* exists */
        snprintf(bent[1].name, sizeof(bent[1].name),
                 "nonexistent-key-batch-1"); /* missing */
        snprintf(bent[2].name, sizeof(bent[2].name),
                 "bulk-kv-entry-%d-1", pid); /* exists */
        snprintf(bent[3].name, sizeof(bent[3].name),
                 "nonexistent-key-batch-2"); /* missing */

        memset(&breq, 0, sizeof(breq));
        breq.count = 4;
        breq.entries = (__u64)(unsigned long)bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
        TEST("mixed batch ioctl succeeds", ret == 0);
        TEST("mixed batch found=2", breq.found == 2);
        TEST("entry[0] found", bent[0].status == 0);
        TEST("entry[1] ENOENT", bent[1].status == -ENOENT);
        TEST("entry[2] found", bent[2].status == 0);
        TEST("entry[3] ENOENT", bent[3].status == -ENOENT);
    }

    /* 8c: single-entry batch */
    {
        struct marufs_find_name_req bent;
        struct marufs_batch_find_req breq;

        memset(&bent, 0, sizeof(bent));
        snprintf(bent.name, sizeof(bent.name),
                 "bulk-kv-entry-%d-0", pid);

        memset(&breq, 0, sizeof(breq));
        breq.count = 1;
        breq.entries = (__u64)(unsigned long)&bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
        TEST("single-entry batch succeeds", ret == 0 && breq.found == 1);
        TEST("single-entry offset correct",
             bent.offset == 0 && bent.status == 0);
    }

    /* 8d: empty batch (count=0) rejected */
    {
        struct marufs_batch_find_req breq;
        memset(&breq, 0, sizeof(breq));
        breq.count = 0;
        breq.entries = 0;

        errno = 0;
        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
        TEST("count=0 rejected (EINVAL)", ret != 0 && errno == EINVAL);
    }

    /* 8e: oversized batch (count > max) rejected */
    {
        struct marufs_batch_find_req breq;
        memset(&breq, 0, sizeof(breq));
        breq.count = MARUFS_BATCH_FIND_MAX + 1;
        breq.entries = (__u64)(unsigned long)&breq; /* dummy */

        errno = 0;
        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
        TEST("count>max rejected (EINVAL)", ret != 0 && errno == EINVAL);
    }

    /* 8f: latency comparison — N individual vs 1 batch */
    {
        int bench_n = 32;
        struct timespec ts0, ts1, ts2;
        long us_individual, us_batch;

        while (bench_n > 0 &&
               (__u64)(bench_n - 1) * SLOT_SIZE >= data_size)
            bench_n /= 2;

        /* Individual FIND_NAME × bench_n */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 0; i < bench_n; i++)
        {
            struct marufs_find_name_req freq;
            memset(&freq, 0, sizeof(freq));
            snprintf(freq.name, sizeof(freq.name),
                     "bulk-kv-entry-%d-%d", pid, i);
            ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);

        /* Batch FIND_NAME × 1 */
        {
            struct marufs_find_name_req* bent;
            struct marufs_batch_find_req breq;

            bent = calloc((size_t)bench_n, sizeof(*bent));
            if (bent)
            {
                for (int i = 0; i < bench_n; i++)
                    snprintf(bent[i].name, sizeof(bent[i].name),
                             "bulk-kv-entry-%d-%d", pid, i);

                memset(&breq, 0, sizeof(breq));
                breq.count = (__u32)bench_n;
                breq.entries = (__u64)(unsigned long)bent;

                ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
                clock_gettime(CLOCK_MONOTONIC, &ts2);

                us_individual = (ts1.tv_sec - ts0.tv_sec) * 1000000 +
                                (ts1.tv_nsec - ts0.tv_nsec) / 1000;
                us_batch = (ts2.tv_sec - ts1.tv_sec) * 1000000 +
                           (ts2.tv_nsec - ts1.tv_nsec) / 1000;

                printf("  -> %d lookups: individual=%ldus, batch=%ldus",
                       bench_n, us_individual, us_batch);
                if (us_batch > 0)
                    printf(" (%.1fx faster)\n",
                           (double)us_individual / (double)us_batch);
                else
                    printf("\n");

                TEST("batch faster than individual", us_batch <= us_individual);
                free(bent);
            }
        }
    }

    /* ----------------------------------------------------------------
     * Test 9: BATCH_NAME_OFFSET (batched store)
     * ---------------------------------------------------------------- */
    printf("\n[9] BATCH_NAME_OFFSET\n");

    /* 9a: batch store new entries */
    {
        int batch_n = 32;
        struct marufs_name_offset_req* bent;
        struct marufs_batch_name_offset_req breq;

        while (batch_n > 0 &&
               (__u64)(batch_n - 1) * SLOT_SIZE >= data_size)
            batch_n /= 2;

        bent = calloc((size_t)batch_n, sizeof(*bent));
        if (!bent)
        {
            printf("  SKIP: calloc failed\n");
            goto batch_store_done;
        }

        for (int i = 0; i < batch_n; i++)
        {
            snprintf(bent[i].name, sizeof(bent[i].name),
                     "bstore-%d-%d", pid, i);
            bent[i].offset = (__u64)i * SLOT_SIZE;
            bent[i].target_region_fd = fd;
            bent[i].status = -1;
        }

        memset(&breq, 0, sizeof(breq));
        breq.count = (__u32)batch_n;
        breq.entries = (__u64)(unsigned long)bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
        TEST("batch store ioctl succeeds", ret == 0);

        {
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "batch stored=%u/%d entries", breq.stored, batch_n);
            TEST(desc, (int)breq.stored == batch_n);
        }

        {
            int ok = 0;
            for (int i = 0; i < batch_n; i++)
                if (bent[i].status == 0)
                    ok++;
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "all status=0: %d/%d", ok, batch_n);
            TEST(desc, ok == batch_n);
        }

        /* 9b: verify via BATCH_FIND_NAME */
        {
            struct marufs_find_name_req* fbent;
            struct marufs_batch_find_req fbreq;

            fbent = calloc((size_t)batch_n, sizeof(*fbent));
            if (fbent)
            {
                for (int i = 0; i < batch_n; i++)
                    snprintf(fbent[i].name, sizeof(fbent[i].name),
                             "bstore-%d-%d", pid, i);

                memset(&fbreq, 0, sizeof(fbreq));
                fbreq.count = (__u32)batch_n;
                fbreq.entries = (__u64)(unsigned long)fbent;

                ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &fbreq);
                TEST("batch-stored entries found via BATCH_FIND",
                     ret == 0 && (int)fbreq.found == batch_n);

                {
                    int match = 0;
                    for (int i = 0; i < batch_n; i++)
                        if (fbent[i].status == 0 &&
                            fbent[i].offset == (__u64)i * SLOT_SIZE)
                            match++;
                    char desc[128];
                    snprintf(desc, sizeof(desc),
                             "batch-stored offsets verified: %d/%d",
                             match, batch_n);
                    TEST(desc, match == batch_n);
                }

                free(fbent);
            }
        }

        /* 9c: batch upsert — same names, different offsets — NRHT returns EEXIST */
        for (int i = 0; i < batch_n; i++)
        {
            bent[i].offset = (__u64)(batch_n - 1 - i) * SLOT_SIZE;
            bent[i].status = -1;
        }

        breq.stored = 0;
        breq.entries = (__u64)(unsigned long)bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
        TEST("batch upsert returns EEXIST", ret == 0);

        {
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "batch upsert stored=%u (EEXIST)", breq.stored);
            TEST(desc, (int)breq.stored == 0);
        }

        /* 9d: batch upsert — same names, same offsets — NRHT returns EEXIST */
        for (int i = 0; i < batch_n; i++)
            bent[i].status = -1;

        breq.stored = 0;
        breq.entries = (__u64)(unsigned long)bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
        TEST("batch upsert returns EEXIST", ret == 0);
        TEST("no-op upsert stored=0 (EEXIST)",
             (int)breq.stored == 0);

        free(bent);
    }

batch_store_done:

    /* 9e: single-entry batch store */
    {
        struct marufs_name_offset_req bent;
        struct marufs_batch_name_offset_req breq;

        memset(&bent, 0, sizeof(bent));
        snprintf(bent.name, sizeof(bent.name), "single-bstore-%d", pid);
        bent.offset = 0;
        bent.target_region_fd = fd;
        bent.status = -1;

        memset(&breq, 0, sizeof(breq));
        breq.count = 1;
        breq.entries = (__u64)(unsigned long)&bent;

        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
        TEST("single-entry batch store succeeds",
             ret == 0 && breq.stored == 1 && bent.status == 0);
    }

    /* 9f: empty batch (count=0) rejected */
    {
        struct marufs_batch_name_offset_req breq;
        memset(&breq, 0, sizeof(breq));
        breq.count = 0;
        breq.entries = 0;

        errno = 0;
        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
        TEST("store count=0 rejected (EINVAL)", ret != 0 && errno == EINVAL);
    }

    /* 9g: oversized batch rejected */
    {
        struct marufs_batch_name_offset_req breq;
        memset(&breq, 0, sizeof(breq));
        breq.count = MARUFS_BATCH_STORE_MAX + 1;
        breq.entries = (__u64)(unsigned long)&breq; /* dummy */

        errno = 0;
        ret = ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
        TEST("store count>max rejected (EINVAL)",
             ret != 0 && errno == EINVAL);
    }

    /* 9h: latency comparison — individual vs batch store */
    {
        int bench_n = 32;
        struct timespec ts0, ts1, ts2;
        long us_individual, us_batch;

        while (bench_n > 0 &&
               (__u64)(bench_n - 1) * SLOT_SIZE >= data_size)
            bench_n /= 2;

        /* Individual NAME_OFFSET × bench_n (upsert) */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 0; i < bench_n; i++)
        {
            struct marufs_name_offset_req req;
            memset(&req, 0, sizeof(req));
            snprintf(req.name, sizeof(req.name),
                     "bstore-%d-%d", pid, i);
            req.offset = (__u64)i * SLOT_SIZE;
            req.target_region_fd = fd;
            ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);

        /* Batch NAME_OFFSET × 1 (upsert) */
        {
            struct marufs_name_offset_req* bent;
            struct marufs_batch_name_offset_req breq;

            bent = calloc((size_t)bench_n, sizeof(*bent));
            if (bent)
            {
                for (int i = 0; i < bench_n; i++)
                {
                    snprintf(bent[i].name, sizeof(bent[i].name),
                             "bstore-%d-%d", pid, i);
                    bent[i].offset = (__u64)i * SLOT_SIZE;
                    bent[i].target_region_fd = fd;
                }

                memset(&breq, 0, sizeof(breq));
                breq.count = (__u32)bench_n;
                breq.entries = (__u64)(unsigned long)bent;

                ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
                clock_gettime(CLOCK_MONOTONIC, &ts2);

                us_individual = (ts1.tv_sec - ts0.tv_sec) * 1000000 +
                                (ts1.tv_nsec - ts0.tv_nsec) / 1000;
                us_batch = (ts2.tv_sec - ts1.tv_sec) * 1000000 +
                           (ts2.tv_nsec - ts1.tv_nsec) / 1000;

                printf("  -> %d stores: individual=%ldus, batch=%ldus",
                       bench_n, us_individual, us_batch);
                if (us_batch > 0)
                    printf(" (%.1fx faster)\n",
                           (double)us_individual / (double)us_batch);
                else
                    printf("\n");

                TEST("batch store faster than individual",
                     us_batch <= us_individual);
                free(bent);
            }
        }
    }

    close(nrht_fd);
    unlink(nrht_path);
    close(fd);

batch_done:
    unlink(filepath);

    return 0;
}

/* ================================================================
 * CHOWN (ownership transfer) tests
 * ================================================================ */

static int run_chown_tests(const char* mount_point, unsigned long data_size_mb,
                           unsigned int node_id)
{
    __u64 data_size = data_size_mb * 1024 * 1024;
    char filepath[512];
    char filename[64];
    int fd, fd2, ret;
    pid_t child;

    snprintf(filename, sizeof(filename), "chown_test_%d", (int)getpid());
    snprintf(filepath, sizeof(filepath), "%s/%s", mount_point, filename);

    printf("=== MARUFS CHOWN Tests ===\n\n");

    /* Clean up stale file */
    unlink(filepath);

    /* ---- C0: Create file (owner = current process) ---- */
    printf("[C0] Setup: create file as owner\n");

    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    TEST("open(O_CREAT)", fd >= 0);
    if (fd < 0)
        return 1;

    ret = ftruncate(fd, (__off_t)data_size);
    TEST("ftruncate", ret == 0);
    if (ret)
    {
        close(fd);
        unlink(filepath);
        return 1;
    }
    printf("\n");

    /* ---- C1: Owner can chown to self (no-op, should succeed) ---- */
    printf("[C1] Owner chown (self-transfer)\n");
    errno = 0;
    ret = do_chown(fd);
    TEST("owner chown succeeds", ret == 0);
    printf("\n");

    /* Set default READ so children can open the file for ioctl */
    ret = do_set_default(fd, MARUFS_PERM_READ);
    TEST("SET_DEFAULT READ for child access", ret == 0);
    printf("\n");

    /* ---- C2: Non-owner with READ but no ADMIN → chown denied ---- */
    printf("[C2] Non-owner chown denied (READ only, no ADMIN)\n");

    child = fork();
    if (child == 0)
    {
        /* Child: different PID, has READ via default_perms, no ADMIN */
        fd2 = open(filepath, O_RDONLY);
        if (fd2 < 0)
            _exit(1);
        errno = 0;
        ret = do_chown(fd2);
        /* Should fail with -EACCES */
        close(fd2);
        _exit(ret == 0 ? 1 : 0); /* exit 0 if correctly denied */
    }
    else if (child > 0)
    {
        int status;
        waitpid(child, &status, 0);
        TEST("non-owner chown DENIED (no ADMIN)", WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    printf("\n");

    /* ---- C3: Grant ADMIN to child → child chown succeeds ---- */
    printf("[C3] Grant ADMIN → child chown succeeds\n");

    child = fork();
    if (child == 0)
    {
        /* Child: wait briefly for parent to grant ADMIN */
        usleep(50000);

        fd2 = open(filepath, O_RDONLY);
        if (fd2 < 0)
            _exit(2);

        errno = 0;
        ret = do_chown(fd2);
        close(fd2);
        _exit(ret == 0 ? 0 : 1); /* exit 0 if chown succeeded */
    }
    else if (child > 0)
    {
        int status;

        /* Parent: grant ADMIN to child (same node) */
        ret = do_grant(fd, node_id, (unsigned)child,
                       MARUFS_PERM_ADMIN | MARUFS_PERM_READ);
        TEST("GRANT ADMIN|READ to child", ret == 0);

        waitpid(child, &status, 0);
        TEST("child chown succeeds (ADMIN granted)", WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    printf("\n");

    /* ---- C4: After chown, original owner is no longer owner ---- */
    printf("[C4] Original owner loses ownership after chown\n");

    /* Ownership transferred to child (now dead).
     * Parent is no longer owner, but still has default READ → can open.
     * chown requires ADMIN → should fail */
    errno = 0;
    ret = do_chown(fd);
    TEST("original owner chown DENIED (no longer owner)", ret != 0);
    printf("\n");

    close(fd);
    unlink(filepath);

    return 0;
}

/* ================================================================
 * Subcommands (for shell-script / cross-VM orchestration)
 * ================================================================ */

static int cmd_perm_setup(const char* mount, const char* filename,
                          int peer_node, unsigned long size_mb)
{
    char filepath[512];
    __u64 data_size = size_mb * 1024 * 1024;
    int fd, ret;

    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    unlink(filepath);

    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }

    ret = ftruncate(fd, (__off_t)data_size);
    if (ret)
    {
        perror("ftruncate");
        close(fd);
        unlink(filepath);
        return 1;
    }

    ret = do_grant(fd, (unsigned)peer_node, (unsigned)getpid(), MARUFS_PERM_READ);
    if (ret)
    {
        perror("PERM_GRANT");
        close(fd);
        unlink(filepath);
        return 1;
    }

    printf("READY fd=%d file=%s size=%lluMB granted=READ to node=%d pid=%d\n",
           fd, filepath, (unsigned long long)size_mb, peer_node, (int)getpid());
    fflush(stdout);

    {
        char buf[16];
        if (fgets(buf, sizeof(buf), stdin) == NULL)
        { /* EOF */
        }
    }

    close(fd);
    return 0;
}

static int cmd_perm_read(const char* mount, const char* filename)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    int r = try_read(filepath);
    printf("%s: read %s\n", filename, r == 0 ? "OK" : "DENIED");
    return r;
}

static int cmd_perm_write(const char* mount, const char* filename)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    int r = try_write_mmap(filepath);
    printf("%s: write %s\n", filename, r == 0 ? "OK" : "DENIED");
    return r;
}

static int cmd_perm_delete(const char* mount, const char* filename)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    int r = unlink(filepath);
    printf("%s: delete %s\n", filename, r == 0 ? "OK" : "DENIED");
    return r;
}

static int cmd_perm_grant(const char* mount, const char* filename,
                          unsigned int node, unsigned int pid,
                          unsigned int perms)
{
    char filepath[512];
    int fd, ret;
    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    fd = open(filepath, O_RDWR);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }
    ret = do_grant(fd, node, pid, perms);
    close(fd);
    if (ret)
    {
        perror("PERM_GRANT");
        return 1;
    }
    printf("OK: granted 0x%x to node=%u pid=%u\n", perms, node, pid);
    return 0;
}

static int cmd_perm_default(const char* mount, const char* filename,
                            unsigned int perms)
{
    char filepath[512];
    int fd, ret;
    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    fd = open(filepath, O_RDWR);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }
    ret = do_set_default(fd, perms);
    close(fd);
    if (ret)
    {
        perror("PERM_SET_DEFAULT");
        return 1;
    }
    printf("OK: set default_perms=0x%x\n", perms);
    return 0;
}

static int cmd_perm_cleanup(const char* mount, const char* filename)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", mount, filename);
    unlink(filepath);
    printf("OK: cleanup %s\n", filepath);
    return 0;
}

/* ================================================================
 * Main
 * ================================================================ */

static void usage(const char* prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s <mount>                                          Single-node tests\n"
            "  %s <mount> <size_mb> [owner_node]                   Single-node, custom size\n"
            "  %s <mount1> <mount2>                                Single + multi-node tests\n"
            "  %s <mount1> <mount2> <peer_node>                    Specify peer node_id\n"
            "  %s <mount1> <mount2> <peer_node> <size> [owner_node] Full options\n"
            "\n"
            "  Subcommands:\n"
            "  %s <mount> perm-setup   <file> <peer_node> [size_mb]\n"
            "  %s <mount> perm-read    <file>\n"
            "  %s <mount> perm-write   <file>\n"
            "  %s <mount> perm-delete  <file>\n"
            "  %s <mount> perm-grant   <file> <node> <pid> <perms_hex>\n"
            "  %s <mount> perm-default <file> <perms_hex>\n"
            "  %s <mount> perm-cleanup <file>\n",
            prog, prog, prog, prog, prog,
            prog, prog, prog, prog, prog, prog, prog);
}

/* Check if string looks like a mount path (starts with '/') */
static int is_path(const char* s)
{
    return s && s[0] == '/';
}

/* Check if string looks like a subcommand (starts with "perm-") */
static int is_subcmd(const char* s)
{
    return s && strncmp(s, "perm-", 5) == 0;
}

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }

    const char* mount1 = argv[1];

    /* ---- Subcommand dispatch ---- */
    if (argc >= 3 && is_subcmd(argv[2]))
    {
        const char* subcmd = argv[2];

        if (strcmp(subcmd, "perm-setup") == 0 && argc >= 5)
            return cmd_perm_setup(mount1, argv[3], atoi(argv[4]),
                                  argc >= 6 ? strtoul(argv[5], NULL, 10) : 128);
        if (strcmp(subcmd, "perm-read") == 0 && argc >= 4)
            return cmd_perm_read(mount1, argv[3]);
        if (strcmp(subcmd, "perm-write") == 0 && argc >= 4)
            return cmd_perm_write(mount1, argv[3]);
        if (strcmp(subcmd, "perm-delete") == 0 && argc >= 4)
            return cmd_perm_delete(mount1, argv[3]);
        if (strcmp(subcmd, "perm-grant") == 0 && argc >= 7)
            return cmd_perm_grant(mount1, argv[3],
                                  (unsigned)strtoul(argv[4], NULL, 10),
                                  (unsigned)strtoul(argv[5], NULL, 10),
                                  (unsigned)strtoul(argv[6], NULL, 16));
        if (strcmp(subcmd, "perm-default") == 0 && argc >= 5)
            return cmd_perm_default(mount1, argv[3],
                                    (unsigned)strtoul(argv[4], NULL, 16));
        if (strcmp(subcmd, "perm-cleanup") == 0 && argc >= 4)
            return cmd_perm_cleanup(mount1, argv[3]);

        fprintf(stderr, "Bad subcommand or missing args: %s\n", subcmd);
        usage(argv[0]);
        return 1;
    }

    /* ---- Auto-detect mode from arguments ---- */
    unsigned long data_size_mb = 128;
    const char* mount2 = NULL;
    unsigned int peer_node = 2;  /* default peer node_id */
    unsigned int owner_node = 1; /* default owner node_id */

    if (argc >= 3 && is_path(argv[2]))
    {
        /* Two mount paths → single + multi-node */
        mount2 = argv[2];
        if (argc >= 4)
            peer_node = (unsigned)strtoul(argv[3], NULL, 10);
        if (argc >= 5)
            data_size_mb = strtoul(argv[4], NULL, 10);
        if (argc >= 6)
            owner_node = (unsigned)strtoul(argv[5], NULL, 10);
    }
    else if (argc >= 3)
    {
        /* Number → single-node with custom size */
        data_size_mb = strtoul(argv[2], NULL, 10);
        if (argc >= 4)
            owner_node = (unsigned)strtoul(argv[3], NULL, 10);
    }

    /* ---- Run single-node tests ---- */
    run_single_node_tests(mount1, data_size_mb);

    /* ---- Run chown tests ---- */
    printf("\n");
    run_chown_tests(mount1, data_size_mb, owner_node);

    /* ---- Run multi-node tests if mount2 given ---- */
    if (mount2)
    {
        printf("\n");
        run_multinode_tests(mount1, mount2, peer_node, data_size_mb);
    }

    /* ---- Final summary ---- */
    printf("\n============================================\n");
    printf("=== Total: %d passed, %d failed ===\n", pass_count, fail_count);
    printf("============================================\n");

    return fail_count > 0 ? 1 : 0;
}
