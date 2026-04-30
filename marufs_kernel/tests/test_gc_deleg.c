// SPDX-License-Identifier: Apache-2.0
/* Expose POSIX.1-2008 + GNU extensions (ftruncate, kill, etc.) */
#define _GNU_SOURCE
/*
 * test_gc_deleg.c - GC dead delegation sweep verification
 *
 * Tests that the GC thread correctly reclaims delegation entries
 * belonging to dead processes:
 *   1. Owner grants delegation to a child process
 *   2. Child successfully accesses region via delegation
 *   3. Child is killed (SIGKILL)
 *   4. GC trigger → dead delegation should be swept
 *   5. New process with same (node, pid) cannot access without re-grant
 *
 * Usage: ./test_gc_deleg <mount1> <mount2> <peer_node> <sysfs_dir>
 *   mount1: owner mount point
 *   mount2: peer mount point (different node_id)
 *   peer_node: node_id of mount2
 *   sysfs_dir: /sys/fs/marufs/<device> (for gc_trigger, perm_info)
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

/* ── Helpers ────────────────────────────────────────────────────────────── */

static int do_grant(int fd, unsigned node, unsigned pid, unsigned perms)
{
    struct marufs_perm_req preq;
    memset(&preq, 0, sizeof(preq));
    preq.node_id = (__u32)node;
    preq.pid     = (__u32)pid;
    preq.perms   = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
}

static int do_set_default(int fd, unsigned perms)
{
    struct marufs_perm_req preq;
    memset(&preq, 0, sizeof(preq));
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_SET_DEFAULT, &preq);
}

static int trigger_gc(const char *sysfs_dir)
{
    char path[512];
    int fd;
    snprintf(path, sizeof(path), "%s/debug/gc_trigger", sysfs_dir);
    fd = open(path, O_WRONLY);
    if (fd < 0)
        return -1;
    write(fd, "1", 1);
    close(fd);
    return 0;
}

/*
 * find_region_id_by_name - scan region_info sysfs to find the RAT index
 * for a given filename.  Returns the region_id, or -1 on failure.
 *
 * region_info format:
 *   RAT_Entry\tNode\tPID\tState\tSize\tOffset\tName
 *   0\t1\t1234\tALLOCATED\t2097152\t0x...\tgc_deleg_5678
 */
static int find_region_id_by_name(const char *sysfs_dir, const char *filename)
{
    char path[512];
    FILE *fp;
    char line[1024];

    snprintf(path, sizeof(path), "%s/region_info", sysfs_dir);
    fp = fopen(path, "r");
    if (!fp)
        return -1;

    /* Skip header line */
    if (!fgets(line, sizeof(line), fp)) {
        fclose(fp);
        return -1;
    }

    while (fgets(line, sizeof(line), fp)) {
        int rid;
        char name[256];
        /* Parse: RAT_Entry\tNode\tPID\tState\tSize\tOffset\tName */
        if (sscanf(line, "%d\t%*s\t%*s\t%*s\t%*s\t%*s\t%255s", &rid, name) == 2) {
            if (strcmp(name, filename) == 0) {
                fclose(fp);
                return rid;
            }
        }
    }

    fclose(fp);
    return -1;
}

/*
 * check_delegation - use deleg_info sysfs to check whether a specific
 * (node_id, pid) delegation exists for the named region.
 *
 * Writes the region_id to deleg_info, then reads back delegation entries.
 * deleg_info output:
 *   region: <id>  name: <name>
 *   deleg[i]: state=2 node=<n> pid=<p> perms=0x... birth_time=...
 *
 * Returns:
 *   1  - delegation entry found
 *   0  - not found (swept or never existed)
 *  -1  - sysfs could not be accessed
 */
static int check_delegation(const char *sysfs_dir, const char *filename,
                             unsigned node_id, unsigned pid)
{
    char path[512];
    FILE *fp;
    char line[1024];
    char search_deleg[128];
    int rid, found = 0;

    rid = find_region_id_by_name(sysfs_dir, filename);
    if (rid < 0)
        return -1;

    /* Write region_id to deleg_info */
    snprintf(path, sizeof(path), "%s/deleg_info", sysfs_dir);
    {
        int wfd = open(path, O_WRONLY);
        char tmp[16];
        int len;

        if (wfd < 0)
            return -1;
        len = snprintf(tmp, sizeof(tmp), "%d", rid);
        if (write(wfd, tmp, len) < 0) {
            close(wfd);
            return -1;
        }
        close(wfd);
    }

    /* Read back delegation entries */
    fp = fopen(path, "r");
    if (!fp)
        return -1;

    snprintf(search_deleg, sizeof(search_deleg), "node=%u pid=%u", node_id, pid);

    while (fgets(line, sizeof(line), fp)) {
        if (strstr(line, "deleg[") && strstr(line, search_deleg)) {
            found = 1;
            break;
        }
    }

    fclose(fp);
    return found;
}

/* Pipe-based synchronization primitives (same convention as test_cross_process.c) */
static void sync_signal(int fd) { char c = 'G'; write(fd, &c, 1); }
static void sync_wait(int fd)   { char c; read(fd, &c, 1); }

/* ── Main test ──────────────────────────────────────────────────────────── */

static int run_gc_deleg_test(const char *mount1, const char *mount2,
                              int peer_node, const char *sysfs_dir)
{
    char filepath1[512], filepath2[512];
    char filename[64];
    int owner_fd;
    int p2c[2], c2p[2]; /* parent-to-child, child-to-parent pipes */
    pid_t child_pid;
    int status;

    snprintf(filename,  sizeof(filename),  "gc_deleg_%d", getpid());
    snprintf(filepath1, sizeof(filepath1), "%s/%s", mount1, filename);
    snprintf(filepath2, sizeof(filepath2), "%s/%s", mount2, filename);

    printf("\n--- Test: GC dead delegation sweep ---\n");

    /* Step 1: Owner creates and initializes region */
    owner_fd = open(filepath1, O_CREAT | O_RDWR, 0644);
    TEST("owner create", owner_fd >= 0);
    if (owner_fd < 0)
        return -1;

    TEST("owner ftruncate", ftruncate(owner_fd, SLOT_SIZE) == 0);

    /*
     * Disable default access for non-owner nodes so that only explicitly
     * granted processes can reach the region.  This makes the post-GC
     * verification meaningful: if the swept delegation were still active,
     * the new process would gain access; without it, open/mmap must fail.
     */
    do_set_default(owner_fd, 0);

    /* Write a known pattern so the child can verify it read real data */
    {
        void *owner_map = mmap(NULL, SLOT_SIZE, PROT_READ | PROT_WRITE,
                               MAP_SHARED, owner_fd, 0);
        TEST("owner mmap", owner_map != MAP_FAILED);
        if (owner_map != MAP_FAILED) {
            memset(owner_map, 0xAB, 4096);
            munmap(owner_map, SLOT_SIZE);
        }
    }

    /* Step 2: Fork child, get its PID, then grant delegation */
    if (pipe(p2c) < 0 || pipe(c2p) < 0) {
        perror("pipe");
        close(owner_fd);
        return -1;
    }

    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        close(owner_fd);
        return -1;
    }

    if (child_pid == 0) {
        /* ── Child: peer-node accessor ── */
        close(p2c[1]);
        close(c2p[0]);

        /* Wait for parent to grant delegation before touching the file */
        sync_wait(p2c[0]);

        {
            int peer_fd = open(filepath2, O_RDWR);
            if (peer_fd < 0) {
                char result = 'F';
                write(c2p[1], &result, 1);
                _exit(1);
            }

            void *peer_map = mmap(NULL, SLOT_SIZE, PROT_READ, MAP_SHARED,
                                  peer_fd, 0);
            if (peer_map == MAP_FAILED) {
                char result = 'F';
                write(c2p[1], &result, 1);
                close(peer_fd);
                _exit(1);
            }

            /* Verify the pattern written by the owner */
            unsigned char *p = (unsigned char *)peer_map;
            char result = (p[0] == 0xAB && p[1] == 0xAB) ? 'P' : 'F';
            write(c2p[1], &result, 1);

            munmap(peer_map, SLOT_SIZE);
            close(peer_fd);
        }

        /*
         * Block here so that the parent can observe the delegation in
         * perm_info before issuing SIGKILL.  The read will never complete —
         * we rely on being killed by the parent.
         */
        sync_wait(p2c[0]);
        _exit(0);
    }

    /* ── Parent: owner / orchestrator ── */
    close(p2c[0]);
    close(c2p[1]);

    /*
     * Grant delegation BEFORE signalling the child.  We use the child's
     * known PID (returned by fork) so the kernel can match it to the
     * process that will open the file on the peer mount.
     */
    {
        int grant_ret = do_grant(owner_fd, peer_node, child_pid,
                                 MARUFS_PERM_READ | MARUFS_PERM_WRITE);
        TEST("grant to child", grant_ret == 0);
    }

    /* Unblock child: delegation is now in place */
    sync_signal(p2c[1]);

    /* Collect child's access result */
    {
        char child_result = 'F';
        read(c2p[0], &child_result, 1);
        TEST("child mmap read via delegation", child_result == 'P');
    }

    /* Step 3: Confirm the delegation is visible in perm_info */
    {
        int deleg_before = check_delegation(sysfs_dir, filename,
                                                          peer_node, child_pid);
        TEST("delegation visible in perm_info before kill", deleg_before == 1);
    }

    /* Step 4: Kill the child — makes the delegation a dead entry */
    kill(child_pid, SIGKILL);
    waitpid(child_pid, &status, 0);
    TEST("child killed",
         WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    /* Step 5: Trigger GC and allow the sweep thread to run */
    TEST("gc trigger", trigger_gc(sysfs_dir) == 0);
    sleep(3); /* GC is asynchronous; give it time to complete */

    /* Step 6: Dead delegation must be gone from perm_info */
    {
        int deleg_after = check_delegation(sysfs_dir, filename,
                                                         peer_node, child_pid);
        TEST("delegation swept after GC", deleg_after == 0);
    }

    /* Cleanup */
    close(p2c[1]);
    close(c2p[0]);
    close(owner_fd);
    unlink(filepath1);

    return 0;
}

/* ── Entry point ────────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s <mount1> <mount2> <peer_node> <sysfs_dir>\n"
                "  sysfs_dir: /sys/fs/marufs\n",
                argv[0]);
        return 1;
    }

    const char *mount1    = argv[1];
    const char *mount2    = argv[2];
    int peer_node         = atoi(argv[3]);
    const char *sysfs_dir = argv[4];

    setbuf(stdout, NULL);
    printf("========================================\n");
    printf("MARUFS GC Dead Delegation Sweep Test\n");
    printf("========================================\n");

    run_gc_deleg_test(mount1, mount2, peer_node, sysfs_dir);

    printf("\n========================================\n");
    printf("Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n");
    return (fail_count > 0) ? 1 : 0;
}
