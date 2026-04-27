// SPDX-License-Identifier: Apache-2.0
/*
 * test_dupname.c - Concurrent same-name file creation test
 *
 * Two processes on separate mount points simultaneously create files
 * with the same name, then verify via /sys/fs/marufs/region_info
 * that at most one region exists for that name.
 *
 * Uses pipe barrier to maximize collision probability.
 * Runs multiple rounds to increase chance of catching races.
 *
 * Usage: ./test_dupname <mount_a> <mount_b> <sysfs_region_info> [rounds]
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

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
 * Try to create a file. Returns fd on success, -1 on failure.
 */
static int try_create(const char *mount, const char *filename)
{
    char path[512];
    int fd;

    snprintf(path, sizeof(path), "%s/%s", mount, filename);
    fd = open(path, O_CREAT | O_RDWR | O_EXCL, 0644);
    return fd;
}

/*
 * Count how many entries in region_info have the given name.
 * Format: "entry\tnode\tpid\tstate\tsize\t0xoffset\tname\n"
 */
static int count_name_in_sysfs(const char *sysfs_path, const char *name)
{
    FILE *fp;
    char line[1024];
    int count = 0;

    fp = fopen(sysfs_path, "r");
    if (!fp) {
        fprintf(stderr, "cannot open %s: %s\n", sysfs_path, strerror(errno));
        return -1;
    }

    while (fgets(line, sizeof(line), fp)) {
        char *tab;
        char *entry_name;
        int tabs = 0;

        /* Find 6th tab-separated field (name) */
        entry_name = line;
        for (tab = line; *tab && tabs < 6; tab++) {
            if (*tab == '\t') {
                tabs++;
                if (tabs == 6)
                    entry_name = tab + 1;
            }
        }

        /* Trim trailing newline */
        {
            char *nl = strchr(entry_name, '\n');
            if (nl) *nl = '\0';
        }

        if (strcmp(entry_name, name) == 0)
            count++;
    }

    fclose(fp);
    return count;
}

static void cleanup_file(const char *mount, const char *filename,
                         const char *sysfs_path)
{
    char path[512];
    char gc_path[512];

    snprintf(path, sizeof(path), "%s/%s", mount, filename);
    unlink(path);

    /* Trigger GC to clean up dead-process files */
    snprintf(gc_path, sizeof(gc_path), "%.*s/debug/gc_trigger",
             (int)(strrchr(sysfs_path, '/') - sysfs_path), sysfs_path);
    {
        int fd = open(gc_path, O_WRONLY);
        if (fd >= 0) {
            write(fd, "1", 1);
            close(fd);
            usleep(500000);
        }
    }
}

int main(int argc, char *argv[])
{
    const char *mount_a, *mount_b, *sysfs_path;
    int rounds;
    int round;
    int dup_found = 0;

    if (argc < 4) {
        fprintf(stderr,
                "Usage: %s <mount_a> <mount_b> <sysfs_region_info> [rounds]\n",
                argv[0]);
        return 1;
    }

    mount_a = argv[1];
    mount_b = argv[2];
    sysfs_path = argv[3];
    rounds = argc >= 5 ? atoi(argv[4]) : DEFAULT_ROUNDS;

    setbuf(stdout, NULL);

    printf("========================================\n");
    printf("MARUFS Concurrent Duplicate Name Test\n");
    printf("========================================\n");
    printf("Node A: %s\n", mount_a);
    printf("Node B: %s\n", mount_b);
    printf("Rounds: %d\n", rounds);
    printf("========================================\n");

    for (round = 0; round < rounds; round++) {
        int a_to_b[2], b_to_a[2];
        pid_t pid;
        int status;
        char filename[64];
        int name_count;

        snprintf(filename, sizeof(filename), "duptest_r%d", round);

        if (pipe(a_to_b) < 0 || pipe(b_to_a) < 0) {
            perror("pipe");
            return 1;
        }

        pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        }

        if (pid == 0) {
            /* Child = Node B */
            int fd;
            close(a_to_b[1]);
            close(b_to_a[0]);

            /* Barrier: signal ready, wait for go */
            sync_signal(b_to_a[1]);
            sync_wait(a_to_b[0]);

            fd = try_create(mount_b, filename);
            if (fd >= 0)
                close(fd);

            close(a_to_b[0]);
            close(b_to_a[1]);
            _exit(0);
        }

        /* Parent = Node A */
        {
            int fd;
            close(a_to_b[0]);
            close(b_to_a[1]);

            /* Wait for child ready, then signal go */
            sync_wait(b_to_a[0]);
            sync_signal(a_to_b[1]);

            fd = try_create(mount_a, filename);
            if (fd >= 0)
                close(fd);

            close(a_to_b[1]);
            close(b_to_a[0]);
            waitpid(pid, &status, 0);
        }

        /* Verify: at most one entry with this name in region_info */
        name_count = count_name_in_sysfs(sysfs_path, filename);
        if (name_count > 1) {
            printf("  [FAIL] Round %d: %d entries for '%s'\n",
                   round + 1, name_count, filename);
            dup_found++;
        } else {
            printf("  [PASS] Round %d/%d (%d entry)\n",
                   round + 1, rounds, name_count);
        }

        /* Cleanup */
        cleanup_file(mount_a, filename, sysfs_path);
        cleanup_file(mount_b, filename, sysfs_path);
        usleep(100000);
    }

    printf("\n========================================\n");
    if (dup_found > 0) {
        printf("RESULT: FAIL (%d/%d rounds had duplicates)\n",
               dup_found, rounds);
        printf("========================================\n");
        return 1;
    }

    printf("RESULT: PASS (%d rounds, no duplicates)\n", rounds);
    printf("========================================\n");
    return 0;
}
