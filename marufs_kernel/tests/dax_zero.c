#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

int main(int argc, char *argv[])
{
	int fd;
	void *addr;
	off_t size;

	if (argc != 3) {
		fprintf(stderr, "Usage: %s <devdax_path> <size_bytes>\n",
			argv[0]);
		return 1;
	}

	fd = open(argv[1], O_RDWR);
	if (fd < 0) {
		perror("open");
		return 1;
	}

	size = strtoull(argv[2], NULL, 0);
	if (size <= 0) {
		fprintf(stderr, "Invalid size\n");
		return 1;
	}

	addr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (addr == MAP_FAILED) {
		perror("mmap");
		return 1;
	}

	memset(addr, 0, size);

	munmap(addr, size);
	close(fd);

	return 0;
}