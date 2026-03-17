// SPDX-License-Identifier: Apache-2.0
/*
 * test_overlap.c - Concurrent ftruncate physical space overlap test
 *
 * Two processes on separate mount points simultaneously create files
 * and ftruncate them, then verify via /sys/fs/marufs/region_info
 * that no two regions share the same physical address range.
 *
 * Uses pipe barrier to maximize collision probability.
 * Runs multiple rounds to increase chance of catching races.
 *
 * Usage: ./test_overlap <mount_a> <mount_b> <sysfs_region_info> [rounds]
 *   e.g., ./test_overlap /mnt/marufs /mnt/marufs2 /sys/fs/marufs/region_info 20
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define TRUNC_SIZE (2 * 1024 * 1024) /* 2MB: minimum DEV_DAX alignment */
#define FILES_PER_ROUND 3
#define DEFAULT_ROUNDS 20

static void sync_signal(int fd)
{
    char c = 'G';
    if (write(fd, &c, 1) != 1) {
        perror("sync_signal");
        exit(1);
    }
}

static void sync_wait(int fd)
{
    char c;
    if (read(fd, &c, 1) != 1) {
        perror("sync_wait");
        exit(1);
    }
}

/*
 * Child process: create FILES_PER_ROUND files, wait at barrier,
 * then ftruncate all simultaneously with parent.
 */
static int run_node(const char *mount, const char *prefix,
                    int sig_fd, int wait_fd, int round)
{
    int fds[FILES_PER_ROUND];
    char path[512];
    int i;
    int created = 0;

    for (i = 0; i < FILES_PER_ROUND; i++) {
        snprintf(path, sizeof(path), "%s/%s_r%d_%d", mount, prefix, round, i);
        fprintf(stderr, "[DBG] %s: open(%s)\n", prefix, path);
        fds[i] = open(path, O_CREAT | O_RDWR, 0644);
        if (fds[i] < 0) {
            fprintf(stderr, "%s: open(%s) failed: %s\n",
                    prefix, path, strerror(errno));
            break;
        }
        created++;
    }
    fprintf(stderr, "[DBG] %s: %d files created, barrier\n", prefix, created);

    /* Barrier: signal ready, wait for other side */
    sync_signal(sig_fd);
    sync_wait(wait_fd);
    fprintf(stderr, "[DBG] %s: barrier done, ftruncating\n", prefix);

    /* Phase 2: ftruncate all files (racing with other node) */
    for (i = 0; i < created; i++) {
        fprintf(stderr, "[DBG] %s: ftruncate[%d]...\n", prefix, i);
        if (ftruncate(fds[i], TRUNC_SIZE) != 0) {
            fprintf(stderr, "%s: ftruncate[%d] failed: %s\n",
                    prefix, i, strerror(errno));
        }
        fprintf(stderr, "[DBG] %s: ftruncate[%d] ok\n", prefix, i);
        close(fds[i]);
    }

    fprintf(stderr, "[DBG] %s: ftruncate done\n", prefix);
    return created < FILES_PER_ROUND ? -1 : 0;
}

/*
 * Parse region_info sysfs and check for physical address overlap.
 * Format: "entry\tnode\tpid\tstate\tsize\t0xoffset\tname\n"
 */
static int check_overlap(const char *sysfs_path)
{
    FILE *fp;
    char line[1024];
    struct { unsigned long long offset, end; } regions[256];
    int count = 0;
    int i, j;

    fp = fopen(sysfs_path, "r");
    if (!fp) {
        fprintf(stderr, "cannot open %s: %s\n", sysfs_path, strerror(errno));
        return -1;
    }

    while (fgets(line, sizeof(line), fp)) {
        unsigned int entry, node, pid;
        unsigned long long size, offset;
        char state[32];

        if (sscanf(line, "%u\t%u\t%u\t%31s\t%llu\t0x%llx",
                   &entry, &node, &pid, state, &size, &offset) < 6)
            continue;

        if (offset == 0 || size == 0)
            continue;

        regions[count].offset = offset;
        regions[count].end = offset + size;
        count++;
        if (count >= 256)
            break;
    }
    fclose(fp);

    /* Insertion sort by offset */
    for (i = 1; i < count; i++) {
        typeof(regions[0]) tmp = regions[i];
        j = i;
        while (j > 0 && regions[j - 1].offset > tmp.offset) {
            regions[j] = regions[j - 1];
            j--;
        }
        regions[j] = tmp;
    }

    /* Check adjacent pairs for overlap */
    for (i = 0; i < count - 1; i++) {
        if (regions[i].end > regions[i + 1].offset) {
            fprintf(stderr, "  OVERLAP: [0x%llx, 0x%llx) vs [0x%llx, 0x%llx)\n",
                    regions[i].offset, regions[i].end,
                    regions[i + 1].offset, regions[i + 1].end);
            return 1;
        }
    }

    return 0;
}

static void cleanup_files(const char *mount_a, const char *mount_b,
                          const char *sysfs_path, int round)
{
    char path[512];
    char gc_path[512];
    int i;

    /* Parent owns nodeA files — unlink directly */
    for (i = 0; i < FILES_PER_ROUND; i++) {
        snprintf(path, sizeof(path), "%s/nodeA_r%d_%d", mount_a, round, i);
        unlink(path);
    }

    /*
     * nodeB files owned by dead child process — trigger GC to reclaim.
     * Parent can't unlink them (owner_pid mismatch).
     */
    snprintf(gc_path, sizeof(gc_path), "%.*s/gc_trigger",
             (int)(strrchr(sysfs_path, '/') - sysfs_path), sysfs_path);
    {
        int fd = open(gc_path, O_WRONLY);
        if (fd >= 0) {
            write(fd, "1", 1);
            close(fd);
            usleep(500000); /* 500ms: wait for GC sweep */
        }
    }

    /* Try unlink nodeB files (should now be gone via GC) */
    for (i = 0; i < FILES_PER_ROUND; i++) {
        snprintf(path, sizeof(path), "%s/nodeB_r%d_%d", mount_b, round, i);
        unlink(path); /* OK if ENOENT — GC already cleaned */
    }
}

int main(int argc, char *argv[])
{
    const char *mount_a, *mount_b, *sysfs_path;
    int rounds;
    int round;
    int overlap_found = 0;

    if (argc < 4) {
        fprintf(stderr,
                "Usage: %s <mount_a> <mount_b> <sysfs_region_info> [rounds]\n"
                "  e.g., %s /mnt/marufs /mnt/marufs2 /sys/fs/marufs/region_info 20\n",
                argv[0], argv[0]);
        return 1;
    }

    mount_a = argv[1];
    mount_b = argv[2];
    sysfs_path = argv[3];
    rounds = argc >= 5 ? atoi(argv[4]) : DEFAULT_ROUNDS;

    /* Disable stdout buffering to avoid fork() buffer duplication */
    setbuf(stdout, NULL);

    printf("========================================\n");
    printf("MARUFS Concurrent ftruncate Overlap Test\n");
    printf("========================================\n");
    printf("Node A: %s\n", mount_a);
    printf("Node B: %s\n", mount_b);
    printf("Rounds: %d (%d files/node/round)\n", rounds, FILES_PER_ROUND);
    printf("========================================\n");

    for (round = 0; round < rounds; round++) {
        int a_to_b[2], b_to_a[2];
        pid_t pid;
        int status;

        if (pipe(a_to_b) < 0 || pipe(b_to_a) < 0) {
            perror("pipe");
            return 1;
        }

        fprintf(stderr, "[DBG] round %d: forking...\n", round);

        pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        }

        if (pid == 0) {
            /* Child = Node B */
            close(a_to_b[1]);
            close(b_to_a[0]);
            fprintf(stderr, "[DBG] child: entering run_node\n");
            run_node(mount_b, "nodeB", b_to_a[1], a_to_b[0], round);
            fprintf(stderr, "[DBG] child: run_node done, exiting\n");
            close(a_to_b[0]);
            close(b_to_a[1]);
            _exit(0);  /* _exit: no atexit/buffer flush */
        }

        /* Parent = Node A */
        close(a_to_b[0]);
        close(b_to_a[1]);
        fprintf(stderr, "[DBG] parent: entering run_node\n");
        run_node(mount_a, "nodeA", a_to_b[1], b_to_a[0], round);
        fprintf(stderr, "[DBG] parent: run_node done\n");

        close(a_to_b[1]);
        close(b_to_a[0]);
        fprintf(stderr, "[DBG] parent: waitpid...\n");
        waitpid(pid, &status, 0);
        fprintf(stderr, "[DBG] parent: waitpid done, status=%d\n", status);

        /* Verify: no physical overlap */
        if (check_overlap(sysfs_path) != 0) {
            printf("  [FAIL] Round %d: OVERLAP DETECTED\n", round + 1);
            overlap_found++;
        } else {
            printf("  [PASS] Round %d/%d\n", round + 1, rounds);
        }

        /* Cleanup for next round */
        cleanup_files(mount_a, mount_b, sysfs_path, round);
        usleep(100000); /* 100ms: let GC/cleanup settle */
    }

    printf("\n========================================\n");
    if (overlap_found > 0) {
        printf("RESULT: FAIL (%d/%d rounds had overlap)\n",
               overlap_found, rounds);
        printf("========================================\n");
        return 1;
    }

    printf("RESULT: PASS (%d rounds, no overlap)\n", rounds);
    printf("========================================\n");
    return 0;
}
