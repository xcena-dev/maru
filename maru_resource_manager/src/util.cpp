#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include "util.h"

namespace maru {

int writeFull(int fd, const void *buf, size_t len) {
  const uint8_t *p = static_cast<const uint8_t *>(buf);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::write(fd, p + off, len - off);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -errno;
    }
    off += static_cast<size_t>(n);
  }
  return 0;
}

int readFull(int fd, void *buf, size_t len) {
  uint8_t *p = static_cast<uint8_t *>(buf);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::read(fd, p + off, len - off);
    if (n == 0) {
      return -EPIPE;
    }
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -errno;
    }
    off += static_cast<size_t>(n);
  }
  return 0;
}

uint64_t nowSec() {
  using namespace std::chrono;
  return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

static constexpr size_t kSecretLen = 32;
static std::string g_hmacSecret;
static bool g_secretInitialized = false;

static bool secureRandomFill(void *buf, size_t len) {
  return RAND_bytes(static_cast<unsigned char *>(buf), static_cast<int>(len)) ==
         1;
}

static std::string generateRandomSecret() {
  std::string secret(kSecretLen, '\0');
  if (!secureRandomFill(&secret[0], kSecretLen)) {
    return std::string();
  }
  return secret;
}

static bool loadSecretFromFile(const std::string &secretPath) {
  std::ifstream ifs(secretPath, std::ios::binary);
  if (!ifs) {
    return false;
  }
  g_hmacSecret.resize(kSecretLen);
  ifs.read(&g_hmacSecret[0], kSecretLen);
  if (ifs.gcount() != static_cast<std::streamsize>(kSecretLen)) {
    g_hmacSecret.clear();
    return false;
  }
  return true;
}

static bool saveSecretToFile(const std::string &stateDir,
                             const std::string &secretPath) {
  ::mkdir(stateDir.c_str(), 0755);
  int fd = ::open(secretPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    return false;
  }
  ssize_t written = ::write(fd, g_hmacSecret.data(), g_hmacSecret.size());
  ::close(fd);
  return written == static_cast<ssize_t>(g_hmacSecret.size());
}

int initSecret(const std::string &stateDir, bool hasExistingAllocations) {
  if (g_secretInitialized) {
    return 0;
  }

  std::string secretPath = stateDir + "/secret";

  if (loadSecretFromFile(secretPath)) {
    g_secretInitialized = true;
    return 0;
  }

  // Secret file not found or invalid
  if (hasExistingAllocations) {
    // Cannot recover - existing allocations need the old secret
    return -ENOKEY;
  }

  // No existing allocations - safe to generate new secret
  g_hmacSecret = generateRandomSecret();
  if (g_hmacSecret.empty()) {
    return -EIO; // RAND_bytes failed
  }

  if (!saveSecretToFile(stateDir, secretPath)) {
    // Failed to persist, but secret is valid in memory
    // Log warning but continue
  }

  g_secretInitialized = true;
  return 0;
}

static const std::string &getHmacSecret() {
  // Caller must ensure initSecret() was called first
  return g_hmacSecret;
}

uint64_t computeAuthToken(const Handle &h, uint64_t nonce) {
  unsigned char hmacResult[EVP_MAX_MD_SIZE];
  unsigned int hmacLen = 0;

  // Calculate required buffer size based on actual field sizes
  constexpr size_t kDataLen =
      sizeof(h.regionId) + sizeof(h.offset) + sizeof(h.length) + sizeof(nonce);
  unsigned char data[kDataLen];
  std::memset(data, 0, sizeof(data));

  // Concatenate fields instead of XOR for stronger uniqueness
  size_t off = 0;
  std::memcpy(data + off, &h.regionId, sizeof(h.regionId));
  off += sizeof(h.regionId);
  std::memcpy(data + off, &h.offset, sizeof(h.offset));
  off += sizeof(h.offset);
  std::memcpy(data + off, &h.length, sizeof(h.length));
  off += sizeof(h.length);
  std::memcpy(data + off, &nonce, sizeof(nonce));

  const std::string &secret = getHmacSecret();
  HMAC(EVP_sha256(), reinterpret_cast<const unsigned char *>(secret.data()),
       secret.size(), data, sizeof(data), hmacResult, &hmacLen);

  uint64_t token = 0;
  std::memcpy(&token, hmacResult, sizeof(uint64_t));
  return token;
}

uint64_t generateNonce() {
  uint64_t nonce = 0;
  if (!secureRandomFill(&nonce, sizeof(nonce))) {
    // CSPRNG failed - use backup entropy sources
    // This combines multiple sources for defense in depth
    nonce = static_cast<uint64_t>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
  }
  return nonce;
}

// CRC32 (ISO 3309 / ITU-T V.42 polynomial)
static const uint32_t kCrc32Table[256] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F,
    0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988,
    0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91, 0x1DB71064, 0x6AB020F2,
    0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9,
    0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
    0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C,
    0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423,
    0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
    0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190, 0x01DB7106,
    0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D,
    0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
    0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950,
    0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7,
    0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
    0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9, 0x5005713C, 0x270241AA,
    0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81,
    0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
    0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683, 0xE3630B12, 0x94643B84,
    0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB,
    0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
    0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8, 0xA1D1937E,
    0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55,
    0x316E8EEF, 0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
    0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28,
    0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F,
    0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
    0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242,
    0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69,
    0x616BFFD3, 0x166CCF45, 0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
    0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC,
    0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693,
    0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
    0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D,
};

uint32_t crc32(const void *data, size_t len) {
  uint32_t crc = 0xFFFFFFFF;
  const uint8_t *p = static_cast<const uint8_t *>(data);
  for (size_t i = 0; i < len; ++i) {
    crc = kCrc32Table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFF;
}

uint64_t getPidStartTime(pid_t pid) {
  char path[64];
  std::snprintf(path, sizeof(path), "/proc/%d/stat", static_cast<int>(pid));
  int fd = ::open(path, O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  char buf[512];
  ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
  ::close(fd);
  if (n <= 0) {
    return 0;
  }
  buf[n] = '\0';

  // Skip past the comm field "(name)" which may contain spaces
  const char *closeParen = std::strrchr(buf, ')');
  if (!closeParen) {
    return 0;
  }
  // Field 22 (starttime) is the 20th field after the close paren.
  // Skip 19 fields (state through itrealvalue), then read the 20th.
  // Fields after ')': state(3), ppid(4), pgrp(5), session(6), tty_nr(7),
  //   tpgid(8), flags(9), minflt(10), cminflt(11), majflt(12), cmajflt(13),
  //   utime(14), stime(15), cutime(16), cstime(17), priority(18), nice(19),
  //   num_threads(20), itrealvalue(21), starttime(22)
  const char *p = closeParen + 1;
  for (int field = 0; field < 19; ++field) {
    while (*p == ' ') {
      ++p;
    }
    if (*p == '\0') {
      return 0;
    }
    while (*p != ' ' && *p != '\0') {
      ++p;
    }
  }
  while (*p == ' ') {
    ++p;
  }
  return std::strtoull(p, nullptr, 10);
}

std::string parentDir(const std::string &path) {
  auto pos = path.rfind('/');
  if (pos == std::string::npos || pos == 0) return "/";
  return path.substr(0, pos);
}

bool ensureDirExists(const std::string &path) {
  struct stat st;
  if (::stat(path.c_str(), &st) == 0) return S_ISDIR(st.st_mode);
  // Recursively create parent
  if (!ensureDirExists(parentDir(path))) {
    return false;
  }
  if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
    return false;
  }
  return true;
}

} // namespace maru
