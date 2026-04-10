# SPDX-License-Identifier: Apache-2.0
#
# Makefile for MARUFS - WORM filesystem for CXL shared memory
#
# Partitioned Global Index architecture
#
# Build artifacts are collected in the 'build/' directory
#

# Module name configuration
# Change this to avoid conflicts with existing modules
MODULE_NAME := marufs

obj-m := $(MODULE_NAME).o

# Source files (from src/ directory)
$(MODULE_NAME)-objs := src/super.o src/inode.o src/file.o src/dir.o src/region.o src/index.o src/nrht.o src/acl.o src/cache.o src/sysfs.o src/gc.o

# Kernel build directory (default to running kernel)
KDIR ?= /lib/modules/$(shell uname -r)/build

# Pass module name as compiler definition
ccflags-y += -DMARUFS_MODULE_NAME=\"$(MODULE_NAME)\"

# Include path for marufs_uapi.h (shared kernel/userspace header)
ccflags-y += -I$(src)/include

# CXL 2.0 compatibility: explicit cache flush for cross-host visibility
ccflags-y += -DCONFIG_MARUFS_CXL2_COMPAT

# Optional daxheap integration (WC mmap + DMA-BUF export for GPU)
# Set DAXHEAP_DIR to enable: make DAXHEAP_DIR=/home/mcpark/daxheap
ifdef DAXHEAP_DIR
ccflags-y += -DCONFIG_DAXHEAP -I$(DAXHEAP_DIR)/kernel/core/include
DAXHEAP_SYMVERS := KBUILD_EXTRA_SYMBOLS=$(DAXHEAP_DIR)/kernel/core/Module.symvers
endif

# Build output directory
BUILD_DIR := build

# Current directory (use CURDIR — immune to stale PWD env var)
PWD := $(CURDIR)

# Default target: build the module
all: collect-artifacts

# Create build directory
prepare:
	@mkdir -p $(BUILD_DIR)

# Build kernel module in-place (depends on prepare)
build-module: prepare
	$(MAKE) -C $(KDIR) M=$(PWD) $(DAXHEAP_SYMVERS) modules

# Collect all build artifacts to build/ and clean source dirs (depends on build)
collect-artifacts: build-module
	@echo "Collecting build artifacts to $(BUILD_DIR)/"
	@mkdir -p $(BUILD_DIR)
	@cp -f *.o *.ko *.mod* .*.o .*.cmd Module.symvers modules.order $(BUILD_DIR)/ 2>/dev/null || true
	@cp -f src/*.o src/.*.cmd $(BUILD_DIR)/ 2>/dev/null || true
	@rm -f *.o *.ko *.mod *.mod.c *.mod.o .*.o .*.cmd Module.symvers modules.order 2>/dev/null || true
	@rm -f src/*.o src/.*.cmd 2>/dev/null || true
	@echo "Build artifacts collected in $(BUILD_DIR)/"

# Clean build artifacts
clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
	@sh -c 'rm -f *.o *.ko *.mod* .*.cmd Module.symvers modules.order 2>/dev/null; true'
	@sh -c 'rm -f src/*.o src/.*.cmd 2>/dev/null; true'
	@rm -rf $(BUILD_DIR)
	@echo "✓ Build artifacts cleaned"

# Install the module
install: all
	sudo insmod $(BUILD_DIR)/$(MODULE_NAME).ko

# Load the module
load: all
	sudo insmod $(BUILD_DIR)/$(MODULE_NAME).ko

# Unload the module
unload:
	sudo rmmod marufs

# Reload the module
reload: unload load

# Show module info
info: all
	modinfo $(BUILD_DIR)/$(MODULE_NAME).ko

# Check for coding style issues (requires kernel checkpatch.pl)
check:
	$(KDIR)/scripts/checkpatch.pl --no-tree -f *.c *.h

.PHONY: all clean install load unload reload info check
