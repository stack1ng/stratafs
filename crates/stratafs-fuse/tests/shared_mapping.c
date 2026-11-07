// Maps the first two pages of the file it is given shared and writable, writes 'm' at its start, and says "dirtied".
// Once it reads a line, it writes the mapping out with msync and prints msync's errno, or 0 when it worked.
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>

int main(int argc, char **argv) {
    int file = open(argv[1], O_RDWR);
    char *mapped = mmap(NULL, 8192, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);
    if (mapped == MAP_FAILED) {
        perror(argv[1]);
        return 1;
    }
    mapped[0] = 'm';
    printf("dirtied\n");
    fflush(stdout);
    char line[8];
    fgets(line, sizeof line, stdin);
    printf("%d\n", msync(mapped, 8192, MS_SYNC) == 0 ? 0 : errno);
    return 0;
}
